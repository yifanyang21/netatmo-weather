#!/usr/bin/env python3
"""
Netatmo Netherlands Weather Data Collector with Tile System
使用分 tile 方式下载整个荷兰的数据
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone

# -----------------------------
# 配置
# -----------------------------

CLIENT_ID = os.environ.get("NETATMO_CLIENT_ID")
CLIENT_SECRET = os.environ.get("NETATMO_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("NETATMO_REFRESH_TOKEN")

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN]):
    print("❌ 错误：缺少必要的环境变量")
    sys.exit(1)

# 荷兰边界
NL_BOUNDS = {
    "lat_max": 53.6,
    "lat_min": 50.7,
    "lon_max": 7.3,
    "lon_min": 3.2,
}

# Tile 划分：4×3 网格 = 12 个 tile
NUM_ROWS = 3    # 南北方向分 3 块
NUM_COLS = 4    # 东西方向分 4 块

# API 端点
TOKEN_URL = "https://api.netatmo.com/oauth2/token"
GETPUBLICDATA_URL = "https://api.netatmo.com/api/getpublicdata"

# 数据保存目录
DATA_DIR = "data"

# 每个 tile 之间的延迟（避免 API 限流）
DELAY_BETWEEN_TILES = 2  # 秒

# -----------------------------
# 函数
# -----------------------------

def generate_tiles(bounds, num_rows, num_cols):
    """
    生成 tile 列表
    
    返回格式：
    [
        {"id": "T1", "lat_ne": ..., "lon_ne": ..., "lat_sw": ..., "lon_sw": ...},
        {"id": "T2", ...},
        ...
    ]
    """
    lat_step = (bounds["lat_max"] - bounds["lat_min"]) / num_rows
    lon_step = (bounds["lon_max"] - bounds["lon_min"]) / num_cols
    
    tiles = []
    tile_id = 1
    
    # 从北到南（lat 从大到小）
    for row in range(num_rows):
        lat_ne = bounds["lat_max"] - row * lat_step
        lat_sw = lat_ne - lat_step
        
        # 从西到东（lon 从小到大）
        for col in range(num_cols):
            lon_sw = bounds["lon_min"] + col * lon_step
            lon_ne = lon_sw + lon_step
            
            tiles.append({
                "id": f"T{tile_id}",
                "row": row,
                "col": col,
                "lat_ne": round(lat_ne, 4),
                "lon_ne": round(lon_ne, 4),
                "lat_sw": round(lat_sw, 4),
                "lon_sw": round(lon_sw, 4),
            })
            tile_id += 1
    
    return tiles


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
        print(f"❌ Token 刷新失败: {e}")
        sys.exit(1)


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
        print(f"⚠️  HTTP 错误: {e}")
        return {"body": []}
    except Exception as e:
        print(f"⚠️  请求失败: {e}")
        return {"body": []}


def parse_public_data(public_json, tile_id):
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
            
            location = place.get("location", [None, None])
            rows.append({
                "tile_id": tile_id,
                "device_id": dev_id,
                "timestamp_utc": datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat() if latest_ts else None,
                "temperature_c": latest_temp,
                "latitude": location[1] if isinstance(location, list) and len(location) > 1 else None,
                "longitude": location[0] if isinstance(location, list) and len(location) > 0 else None,
                "altitude_m": place.get("altitude"),
                "city": place.get("city"),
                "country": place.get("country"),
            })
    
    df = pd.DataFrame(rows)
    
    if len(df) > 0:
        df = df.drop_duplicates(subset=["device_id"])
        df = df.dropna(subset=["temperature_c"])
    
    return df


def download_tile(access_token, tile):
    """下载单个 tile 的数据"""
    print(f"\n  📍 Tile {tile['id']} ({tile['lat_sw']:.2f}°N-{tile['lat_ne']:.2f}°N, "
          f"{tile['lon_sw']:.2f}°E-{tile['lon_ne']:.2f}°E)")
    
    # 获取数据
    public_json = get_public_data(access_token, tile)
    device_count = len(public_json.get("body", []))
    
    if device_count == 0:
        print(f"     ⚠️  无数据")
        return pd.DataFrame()
    
    # 解析数据
    df = parse_public_data(public_json, tile["id"])
    
    if len(df) > 0:
        avg_temp = df['temperature_c'].mean()
        print(f"     ✅ {len(df)} 个站点，平均温度 {avg_temp:.1f}°C")
    else:
        print(f"     ⚠️  解析后无有效数据")
    
    return df


def save_data(df, timestamp):
    """保存数据到 CSV"""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    filename = f"{DATA_DIR}/netherlands_weather_{timestamp}.csv"
    df.to_csv(filename, index=False, encoding="utf-8")
    
    return filename


# -----------------------------
# 主程序
# -----------------------------

def main():
    print("=" * 70)
    print("🌡️  Netatmo 荷兰全境温度数据采集 (Tile System)")
    print("=" * 70)
    
    start_time = time.time()
    now = datetime.now(timezone.utc)
    timestamp = now.strftime('%Y%m%d_%H%M')
    
    print(f"运行时间: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Tile 配置: {NUM_ROWS}×{NUM_COLS} = {NUM_ROWS * NUM_COLS} 个 tiles")
    
    # 1. 生成 tiles
    print("\n1️⃣ 生成 Tile 网格...")
    tiles = generate_tiles(NL_BOUNDS, NUM_ROWS, NUM_COLS)
    print(f"   ✅ 已生成 {len(tiles)} 个 tiles")
    
    # 2. 刷新 token
    print("\n2️⃣ 刷新访问令牌...")
    tokens = refresh_access_token(REFRESH_TOKEN)
    access_token = tokens["access_token"]
    print("   ✅ Token 刷新成功")
    
    # 3. 下载每个 tile
    print(f"\n3️⃣ 开始下载数据（每个 tile 间隔 {DELAY_BETWEEN_TILES}s）...")
    
    all_data = []
    successful_tiles = 0
    failed_tiles = 0
    
    for i, tile in enumerate(tiles, 1):
        print(f"\n  [{i}/{len(tiles)}]", end="")
        
        try:
            df = download_tile(access_token, tile)
            if len(df) > 0:
                all_data.append(df)
                successful_tiles += 1
            
            # 延迟（最后一个 tile 不需要延迟）
            if i < len(tiles):
                time.sleep(DELAY_BETWEEN_TILES)
        
        except Exception as e:
            print(f"     ❌ 错误: {e}")
            failed_tiles += 1
            continue
    
    # 4. 合并数据
    print(f"\n4️⃣ 合并数据...")
    
    if len(all_data) == 0:
        print("   ❌ 没有任何有效数据")
        sys.exit(1)
    
    df_combined = pd.concat(all_data, ignore_index=True)
    
    # 去除可能的跨 tile 重复设备
    df_combined = df_combined.drop_duplicates(subset=["device_id"])
    
    print(f"   ✅ 合并完成：共 {len(df_combined)} 个唯一天气站")
    
    # 5. 保存数据
    print(f"\n5️⃣ 保存数据...")
    filename = save_data(df_combined, timestamp)
    
    # 6. 统计信息
    elapsed = time.time() - start_time
    avg_temp = df_combined['temperature_c'].mean()
    
    print("\n" + "=" * 70)
    print("✅ 任务完成")
    print("=" * 70)
    print(f"成功 tiles: {successful_tiles}/{len(tiles)}")
    print(f"失败 tiles: {failed_tiles}/{len(tiles)}")
    print(f"总天气站: {len(df_combined)}")
    print(f"平均温度: {avg_temp:.1f}°C")
    print(f"数据文件: {filename}")
    print(f"耗时: {elapsed:.1f} 秒")
    print("=" * 70)


if __name__ == "__main__":
    main()
