import sqlite3
import requests
import os
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

# 下載設定 (使用公開 URL 或本地測試)
# 如果 R2 是私有的，我們直接用你設定好的 credentials

def download_from_r2(date_str: Optional[str] = None, db_type: str = 'news'):
    """嘗試從 R2 下載指定日期的新聞或 RSS 資料"""
    import boto3
    from botocore.config import Config
    
    # 從環境變數讀取
    endpoint_url = os.environ.get('S3_ENDPOINT_URL', '')
    access_key = os.environ.get('S3_ACCESS_KEY_ID', '')
    secret_key = os.environ.get('S3_SECRET_ACCESS_KEY', '')
    bucket_name = 'trendradar-news'
    
    if not all([endpoint_url, access_key, secret_key]):
        print(f"⚠️ [{db_type}] 未偵測到完整環境變數 (S3_ENDPOINT_URL, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY)")
        return None
    
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4')
    )
    
    target_date = date_str or datetime.now().strftime('%Y-%m-%d')
    local_path = f'trendradar_{db_type}_{target_date}.db'
    
    # 如果本地已有，先不重複下載 (除非強制更新)
    if os.path.exists(local_path):
        # print(f"ℹ️ 本地已有 {db_type} 檔案: {local_path}")
        return local_path
        
    try:
        print(f"📡 正在從 R2 下載 {db_type}/{target_date} 資料...")
        s3.download_file(bucket_name, f'{db_type}/{target_date}.db', local_path)
        print(f"✅ 成功下載: {local_path}")
        return local_path
    except Exception as e:
        print(f"❌ 下載失敗 ({db_type}/{target_date}): {e}")
        return None

def view_news_data(db_path: str):
    """查看新聞資料庫內容"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 查看表結構
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"\n📊 資料表: {[t['name'] for t in tables]}")
    
    # 查看 news_items 表的欄位
    cursor.execute("PRAGMA table_info(news_items)")
    cols = cursor.fetchall()
    print(f"📋 news_items 欄位: {[c[1] for c in cols]}")
    
    # 查看新聞資料
    try:
        cursor.execute("""
            SELECT n.*, p.name as platform_name 
            FROM news_items n
            LEFT JOIN platforms p ON n.platform_id = p.id
            ORDER BY n.id DESC 
            LIMIT 20
        """)
        rows = cursor.fetchall()
        
        print(f"\n📰 最新 20 條新聞:\n")
        print("-" * 80)
        
        for i, row in enumerate(rows, 1):
            platform = row['platform_name'] or row['platform_id'] or 'Unknown'
            title = row['title'][:50]
            print(f"{i:2}. [{platform:15}] {title}...")
        
        print("-" * 80)
        
        # 統計各平台數量
        cursor.execute("""
            SELECT p.name as platform_name, n.platform_id, COUNT(*) as count 
            FROM news_items n
            LEFT JOIN platforms p ON n.platform_id = p.id
            GROUP BY n.platform_id 
            ORDER BY count DESC
        """)
        stats = cursor.fetchall()
        
        print(f"\n📈 平台統計:")
        for stat in stats:
            p_name = stat['platform_name'] or stat['platform_id']
            print(f"   • {p_name}: {stat['count']} 條")
        
        # 總計
        cursor.execute("SELECT COUNT(*) as total FROM news_items")
        total = cursor.fetchone()['total']
        print(f"\n📊 總計: {total} 條新聞")
            
    except Exception as e:
        print(f"查詢失敗: {e}")
    
    conn.close()

if __name__ == "__main__":
    print("=" * 60)
    print("TrendRadar 新聞資料檢視器")
    print("=" * 60)
    
    # 自動流程：優先找今日，沒有則找 R2
    today_str = datetime.now().strftime('%Y-%m-%d')
    local_today = f'trendradar_news_{today_str}.db'
    
    db_path = download_from_r2(today_str, 'news')
    rss_path = download_from_r2(today_str, 'rss')
    
    if db_path:
        view_news_data(db_path)
    else:
        # 備選
        local_files = [f for f in os.listdir('.') if f.endswith('.db') and 'trendradar_news' in f.lower()]
        if local_files:
            print(f"\n⚠️ 無法同步今日最新新聞，顯示最近期的本地檔案: {local_files[0]}")
            view_news_data(local_files[0])
            
    if rss_path:
        print(f"\n✅ 同步 RSS 數據庫成功: {rss_path}")
