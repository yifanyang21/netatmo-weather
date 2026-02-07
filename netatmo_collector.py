#!/usr/bin/env python3
"""
Netatmo 三大城市数据采集器 (Utrecht, Amsterdam, Rotterdam)
用于 GitHub Actions 自动化运行
"""

import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
import math

# -----------------------------
# 配置
# -----------------------------

# 从环境变量读取凭证
CLIENT_ID = os.environ.get("NETATMO_CLIENT_ID")
CLIENT_SECRET = os.environ.get("NETATMO_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("NETATMO_REFRESH_TOKEN")

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN]):
    print("错误：缺少必要的环境变量")
    print("请在 GitHub Secrets 中配置：")
    print("  - NETATMO_CLIENT_ID")
    print("  - NETATMO_CLIENT_SECRET")
    print("  - NETATMO_REFRESH_TOKEN")
    sys.exit(1)

# 定义三大城市的区域
CITIES = {
    "utrecht": {
        "name": "Utrecht",
        "center_lat": 52.0908,
        "center_lon": 5.1222,
        "radius_km": 10,
    },
    "amsterdam": {
        "name": "Amsterdam",
        "center_lat": 52.3676,
        "center_lon": 4.9041,
        "radius_km": 15,
    },
    "rotterdam": {
        "name": "Rotterdam",
        "center_lat": 51.9225,
        "center_lon": 4.4792,
        "radius_km": 15,
    },
}

# 每个城市分成多少个 tiles
TILES_PER_CITY = {
    "rows": 2,
    "cols": 2,
}

# API 端点
TOKEN_URL = "https://api.netatmo.com/oauth2/token"
GETPUBLICDATA_URL = "https://api.netatmo.com/api/getpublicdata"

# 数据保存目录
DATA_DIR = "data"

# 请求间隔
DELAY_BETWEEN_TILES = 2

# 阿姆斯特丹时区偏移（冬季 UTC+1）
AMSTERDAM_OFFSET = timedelta(hours=1)

# -----------------------------
# 函数定义
# -----------------------------

def refresh_access_token(refresh_token):
    """使用 refresh_token 获取新的 access_token"""
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    try:
        r = requests.post(TOKEN_URL, data=payload, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Token 刷新失败: {e}")
        sys.exit(1)


def km_to_degrees(km, latitude):
    """将公里转换为经纬度度数"""
    lat_degree = km / 111.0
    lon_degree = km / (111.0 * math.cos(math.radians(latitude)))
    return lat_degree, lon_degree


def generate_city_tiles(city_config, num_rows, num_cols):
    """为单个城市生成 tiles"""
    center_lat = city_config["center_lat"]
    center_lon = city_config["center_lon"]
    radius_km = city_config["radius_km"]
    
    lat_delta, lon_delta = km_to_degrees(radius_km, center_lat)
    
    bounds = {
        "lat_max": center_lat + lat_delta,
        "lat_min": center_lat - lat_delta,
        "lon_max": center_lon + lon_delta,
        "lon_min": center_lon - lon_delta,
    }
    
    lat_step = (bounds["lat_max"] - bounds["lat_min"]) / num_rows
    lon_step = (bounds["lon_max"] - bounds["lon_min"]) / num_cols
    
    tiles = []
    tile_id = 1
    
    for row in range(num_rows):
        lat_ne = bounds["lat_max"] - row * lat_step
        lat_sw = lat_ne - lat_step
        
        for col in range(num_cols):
            lon_sw = bounds["lon_min"] + col * lon_step
            lon_ne = lon_sw + lon_step
            
            tiles.append({
                "id": f"{city_config['name']}_T{tile_id}",
                "city": city_config["name"],
                "row": row,
                "col": col,
                "lat_ne": round(lat_ne, 4),
                "lon_ne": round(lon_ne, 4),
                "lat_sw": round(lat_sw, 4),
                "lon_sw": round(lon_sw, 4),
            })
            tile_id += 1
    
    return tiles


def get_public_data(access_token, region):
    """获取公共天气站数据"""
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {
        "lat_ne": region["lat_ne"],
        "lon_ne": region["lon_ne"],
        "lat_sw": region["lat_sw"],
        "lon_sw": region["lon_sw"],
        "required_data": "temperature",
        "filter": "true",
    }
    
    try:
        r = requests.post(GETPUBLICDATA_URL, headers=headers, data=payload, timeout=60)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP 错误: {e}")
        return {"body": []}
    except Exception as e:
        print(f"请求失败: {e}")
        return {"body": []}


def parse_public_data(public_json, tile_info):
    """解析 API 返回的数据为 DataFrame"""
    body = public_json.get("body", [])
    rows = []
    
    for item in body:
        place = item.get("place", {})
        measures = item.get("measures", {})
        
        for dev_id, m in measures.items():
            m_types = m.get("type", [])
            
            if not any(str(t).lower() in ["temperature", "temp"] for t in m_types):
                continue
            
            latest_temp = None
            latest_ts = None
            
            res = m.get("res")
            if isinstance(res, dict) and len(res) > 0:
                try:
                    ts = max(int(k) for k in res.keys())
                    vals = res.get(str(ts)) or res.get(ts)
                    if isinstance(vals, list) and len(vals) > 0:
                        latest_temp = vals[0]
                        latest_ts = ts
                except Exception:
                    pass
            
            # 时间转换
            if latest_ts:
                time_utc = datetime.fromtimestamp(latest_ts, tz=timezone.utc)
                time_amsterdam = time_utc + AMSTERDAM_OFFSET
                timestamp_amsterdam = time_amsterdam.strftime("%Y-%m-%d %H:%M:%S")
            else:
                timestamp_amsterdam = None
            
            location = place.get("location", [None, None])
            rows.append({
                "city_area": tile_info["city"],
                "tile_id": tile_info["id"],
                "device_id": dev_id,
                "timestamp_amsterdam": timestamp_amsterdam,
                "temperature_c": latest_temp,
                "latitude": location[1] if isinstance(location, list) and len(location) > 1 else None,
                "longitude": location[0] if isinstance(location, list) and len(location) > 0 else None,
                "altitude_m": place.get("altitude"),
                "location_name": place.get("city"),
                "country": place.get("country"),
            })
    
    df = pd.DataFrame(rows)
    
    if len(df) > 0:
        df = df.drop_duplicates(subset=["device_id"])
        df = df.dropna(subset=["temperature_c"])
    
    return df


def download_tile(access_token, tile):
    """下载单个 tile 的数据"""
    public_json = get_public_data(access_token, tile)
    device_count = len(public_json.get("body", []))
    
    if device_count == 0:
        return pd.DataFrame()
    
    df = parse_public_data(public_json, tile)
    return df


def save_data(df, timestamp):
    """保存数据到 CSV"""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    filename = f"{DATA_DIR}/3cities_weather_{timestamp}.csv"
    df.to_csv(filename, index=False, encoding="utf-8")
    
    return filename


# -----------------------------
# 主程序
# -----------------------------

def main():
    print("=" * 70)
    print("Netatmo 三大城市温度数据采集")
    print("=" * 70)
    
    start_time = time.time()
    now = datetime.now(timezone.utc)
    timestamp = now.strftime('%Y%m%d_%H%M')
    
    print(f"运行时间: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"城市: Utrecht, Amsterdam, Rotterdam")
    print(f"Tile 配置: {TILES_PER_CITY['rows']}×{TILES_PER_CITY['cols']} 每城市")
    
    # 1. 刷新 token
    print("\n 1 刷新访问令牌...")
    tokens = refresh_access_token(REFRESH_TOKEN)
    access_token = tokens["access_token"]
    print("   Token 刷新成功")
    
    # 2. 生成所有城市的 tiles
    print("\n 2 生成 Tile 网格...")
    all_tiles = []
    for city_key, city_config in CITIES.items():
        tiles = generate_city_tiles(
            city_config,
            TILES_PER_CITY["rows"],
            TILES_PER_CITY["cols"]
        )
        all_tiles.extend(tiles)
        print(f"   {city_config['name']:12s}: {len(tiles)} tiles")
    
    print(f"   总计: {len(all_tiles)} tiles")
    
    # 3. 下载数据
    print(f"\n 3 开始下载数据（每个 tile 间隔 {DELAY_BETWEEN_TILES}s）...")
    
    all_data = []
    city_stats = {city["name"]: 0 for city in CITIES.values()}
    successful_tiles = 0
    failed_tiles = 0
    
    for i, tile in enumerate(all_tiles, 1):
        city_name = tile["city"]
        print(f"\n  [{i}/{len(all_tiles)}] {tile['id']}", end="")
        
        try:
            df = download_tile(access_token, tile)
            
            if len(df) > 0:
                all_data.append(df)
                city_stats[city_name] += len(df)
                successful_tiles += 1
                avg_temp = df['temperature_c'].mean()
                print(f" {len(df)} 站点，平均 {avg_temp:.1f}°C")
            else:
                print(f" 无数据")
            
            if i < len(all_tiles):
                time.sleep(DELAY_BETWEEN_TILES)
        
        except Exception as e:
            print(f" 错误: {e}")
            failed_tiles += 1
            continue
    
    # 4. 合并数据
    print(f"\n 4 合并数据...")
    
    if len(all_data) == 0:
        print("  没有任何有效数据")
        sys.exit(1)
    
    df_combined = pd.concat(all_data, ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=["device_id"])
    
    print(f"   合并完成：共 {len(df_combined)} 个唯一天气站")
    
    # 5. 保存数据
    print(f"\n 5 保存数据...")
    filename = save_data(df_combined, timestamp)
    
    # 6. 统计信息
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("任务完成")
    print("=" * 70)
    print(f"成功 tiles: {successful_tiles}/{len(all_tiles)}")
    print(f"失败 tiles: {failed_tiles}/{len(all_tiles)}")
    print()
    
    for city_name in ["Utrecht", "Amsterdam", "Rotterdam"]:
        city_data = df_combined[df_combined['city_area'] == city_name]
        if len(city_data) > 0:
            avg_temp = city_data['temperature_c'].mean()
            print(f"{city_name:15s}: {len(city_data):4d} 站点，平均温度 {avg_temp:.1f}°C")
        else:
            print(f"{city_name:15s}: 无数据")
    
    print()
    print(f"总站点数: {len(df_combined)}")
    print(f"数据文件: {filename}")
    print(f"总耗时: {elapsed:.1f} 秒")
    print("=" * 70)


if __name__ == "__main__":
    main()
