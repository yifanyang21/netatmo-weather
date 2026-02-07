#!/usr/bin/env python3
"""
Netatmo Utrecht Weather Data Collector for GitHub Actions
每小时自动下载 Utrecht 地区 Netatmo 公共天气站数据
"""

import os
import sys
import json
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

# -----------------------------
# 配置
# -----------------------------

# 从环境变量读取凭证
CLIENT_ID = os.environ.get("NETATMO_CLIENT_ID")
CLIENT_SECRET = os.environ.get("NETATMO_CLIENT_SECRET")
REFRESH_TOKEN = os.environ.get("NETATMO_REFRESH_TOKEN")

if not all([CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN]):
    print("❌ 错误：缺少必要的环境变量")
    print("请在 GitHub Secrets 中配置：")
    print("  - NETATMO_CLIENT_ID")
    print("  - NETATMO_CLIENT_SECRET")
    print("  - NETATMO_REFRESH_TOKEN")
    sys.exit(1)

# Utrecht 区域边界（约 15km × 15km）
UTRECHT_CENTER_LAT = 52.0908
UTRECHT_CENTER_LON = 5.1222
DELTA_LAT = 0.08
DELTA_LON = 0.12

REGION = {
    "lat_ne": UTRECHT_CENTER_LAT + DELTA_LAT,
    "lon_ne": UTRECHT_CENTER_LON + DELTA_LON,
    "lat_sw": UTRECHT_CENTER_LAT - DELTA_LAT,
    "lon_sw": UTRECHT_CENTER_LON - DELTA_LON,
}

# API 端点
TOKEN_URL = "https://api.netatmo.com/oauth2/token"
GETPUBLICDATA_URL = "https://api.netatmo.com/api/getpublicdata"

# 数据保存目录
DATA_DIR = "data"

# -----------------------------
# 函数
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
        print(f"❌ Token 刷新失败: {e}")
        sys.exit(1)


def get_public_data(access_token, region):
    """获取公共天气站数据"""
    headers = {"Authorization": f"Bearer {access_token}"}
    payload = {
        **region,
        "required_data": "temperature",
        "filter": "true",
    }
    try:
        r = requests.post(GETPUBLICDATA_URL, headers=headers, data=payload, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ 数据获取失败: {e}")
        sys.exit(1)


def parse_public_data(public_json):
    """解析 API 返回的数据为 DataFrame"""
    body = public_json.get("body", [])
    rows = []
    
    for item in body:
        place = item.get("place", {})
        measures = item.get("measures", {})
        
        for dev_id, m in measures.items():
            m_types = m.get("type", [])
            
            # 只保留包含温度数据的设备
            if not any(str(t).lower() in ["temperature", "temp"] for t in m_types):
                continue
            
            # 提取最新温度
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
    
    # 移除重复设备
    df = df.drop_duplicates(subset=["device_id"])
    
    # 移除缺失温度的行
    df = df.dropna(subset=["temperature_c"])
    
    return df


def save_data(df):
    """保存数据到 CSV"""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    now = datetime.now(timezone.utc)
    filename = f"{DATA_DIR}/utrecht_weather_{now.strftime('%Y%m%d_%H%M')}.csv"
    
    df.to_csv(filename, index=False, encoding="utf-8")
    print(f"✅ 数据已保存: {filename}")
    print(f"   共 {len(df)} 个天气站，平均温度: {df['temperature_c'].mean():.1f}°C")
    
    return filename


# -----------------------------
# 主程序
# -----------------------------

def main():
    print("=" * 60)
    print("🌡️  Netatmo Utrecht 温度数据采集")
    print("=" * 60)
    print(f"运行时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    # 1. 刷新 token
    print("\n1️⃣ 刷新访问令牌...")
    tokens = refresh_access_token(REFRESH_TOKEN)
    access_token = tokens["access_token"]
    print("   ✅ Token 刷新成功")
    
    # 2. 获取数据
    print("\n2️⃣ 获取公共天气站数据...")
    public_json = get_public_data(access_token, REGION)
    print(f"   ✅ API 返回 {len(public_json.get('body', []))} 个设备")
    
    # 3. 解析数据
    print("\n3️⃣ 解析温度数据...")
    df = parse_public_data(public_json)
    print(f"   ✅ 解析出 {len(df)} 个有效温度读数")
    
    if len(df) == 0:
        print("\n⚠️  警告：未找到任何温度数据")
        sys.exit(0)
    
    # 4. 保存数据
    print("\n4️⃣ 保存数据...")
    save_data(df)
    
    print("\n" + "=" * 60)
    print("✅ 任务完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
