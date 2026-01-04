# --- START OF FILE step_trendradar.py (v1.0) ---
"""
TrendRadar 財經新聞整合模組

此模組獨立運作，用於從 TrendRadar SQLite 資料庫讀取財經新聞。
不會修改任何現有程式碼，可獨立測試後再整合。

使用方式：
    from step_trendradar import get_hot_news, get_industry_news
    
    # 取得今日財經熱點
    news = get_hot_news()
    
    # 取得特定產業新聞
    industry_news = get_industry_news("半導體")
"""

import sqlite3
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json

# ==========================================
# 配置區
# ==========================================

# TrendRadar 資料目錄 (相對於 stock_project)
TRENDRADAR_DATA_DIR = os.environ.get("TRENDRADAR_DATA_DIR", "../TrendRadar/output/news")

# 財經相關平台 ID
FINANCE_PLATFORMS = [
    "wallstreetcn-hot",  # 華爾街見聞
    "cls-hot",           # 財聯社
    "xueqiu",            # 雪球
    "eastmoney",         # 東方財富
    "gelonghui",         # 格隆匯
]

# 平台名稱對照
PLATFORM_NAMES = {
    "wallstreetcn-hot": "華爾街見聞",
    "cls-hot": "財聯社",
    "xueqiu": "雪球",
    "eastmoney": "東方財富",
    "gelonghui": "格隆匯",
    "zhihu": "知乎",
    "weibo": "微博",
    "toutiao": "今日頭條",
}

# 產業關鍵詞對照 (對應 industry_logic.json)
INDUSTRY_KEYWORDS = {
    "Foundry_TSMC": ["台積電", "TSMC", "CoWoS", "先進製程", "晶圓代工"],
    "AI_Server": ["AI伺服器", "GB200", "液冷", "Nvidia", "H100", "H200"],
    "Semiconductor": ["半導體", "IC設計", "聯發科", "聯電", "日月光"],
    "Shipping": ["長榮", "陽明", "萬海", "SCFI", "運價", "航運"],
    "EV": ["電動車", "特斯拉", "Tesla", "鴻海", "MIH"],
    "Biotech": ["生技", "新藥", "臨床試驗"],
    "Finance": ["金融", "銀行", "壽險", "升息", "降息"],
}


# 2026-01-04 Updated: Focused on Taiwan & US Markets
RSS_SOURCES = [
    # --- Taiwan Market ---
    {"name": "MoneyDJ_TW", "url": "https://news.google.com/rss/search?q=site:moneydj.com&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"},
    {"name": "Yahoo_TW_Market", "url": "https://tw.stock.yahoo.com/rss?category=tw-market"},
    {"name": "TW_Semiconductor", "url": "https://news.google.com/rss/search?q=%E5%8F%B0%E7%A9%8D%E9%9B%BB+OR+%E8%81%AF%E7%99%BC%E7%A7%91+OR+%E5%8D%8A%E5%B0%8E%E9%AB%94+when:1d&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"},

    # --- US Market ---
    {"name": "CNBC_US", "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664"},
    {"name": "TechCrunch_AI", "url": "https://techcrunch.com/category/artificial-intelligence/feed/"},
    {"name": "US_Tech_Giants", "url": "https://news.google.com/rss/search?q=NVIDIA+OR+Apple+OR+Microsoft+OR+AMD+when:1d&hl=en-US&gl=US&ceid=US:en"}
]

# ==========================================
# 核心函式
# ==========================================

def _get_db_path(date: Optional[datetime] = None, is_rss: bool = False) -> str:
    """取得指定日期的 DB 路徑"""
    if date is None:
        date = datetime.now()
    date_str = date.strftime('%Y-%m-%d')
    prefix = "trendradar_rss_" if is_rss else "trendradar_news_"
    # 預設存放在當前目錄，方便 GitHub Actions 讀取
    return f"{prefix}{date_str}.db"

def init_rss_db(db_path: str):
    """初始化 RSS 資料庫"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # 建立 RSS 新聞表
    cursor.execute('CREATE TABLE IF NOT EXISTS rss_items (id INTEGER PRIMARY KEY, feed_id INTEGER, title TEXT, link TEXT, pub_date TEXT, description TEXT, first_crawl_time TEXT DEFAULT CURRENT_TIMESTAMP)')
    # 建立來源表
    cursor.execute('CREATE TABLE IF NOT EXISTS rss_feeds (id INTEGER PRIMARY KEY, name TEXT UNIQUE, url TEXT, category TEXT)')
    
    # 預填來源
    for src in RSS_SOURCES:
        cursor.execute('INSERT OR IGNORE INTO rss_feeds (name, url, category) VALUES (?, ?, ?)', 
                      (src['name'], src['url'], "Finance"))
    
    conn.commit()
    conn.close()

def crawl_rss_to_db():
    """執行 RSS 爬蟲並寫入資料庫"""
    import requests
    import xml.etree.ElementTree as ET
    
    db_path = _get_db_path(is_rss=True)
    print(f"  > [RSS Crawler] Target DB: {db_path}")
    init_rss_db(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    total_added = 0
    
    for src in RSS_SOURCES:
        try:
            print(f"  > [RSS] Fetching {src['name']}...")
            r = requests.get(src['url'], headers=headers, timeout=15)
            if r.status_code != 200:
                print(f"    ⚠️ HTTP {r.status_code}")
                continue
                
            # 解析 XML
            try:
                root = ET.fromstring(r.content)
                items = root.findall('.//item')
                if not items:
                    items = root.findall('.//{http://www.w3.org/2005/Atom}entry')
                
                # 取得 Feed ID
                cursor.execute('SELECT id FROM rss_feeds WHERE name = ?', (src['name'],))
                feed_row = cursor.fetchone()
                feed_id = feed_row[0] if feed_row else 0
                
                src_count = 0
                for item in items[:20]: # 每個來源最多存 20 條
                    title = item.find('title').text if item.find('title') is not None else ""
                    link = item.find('link').text if item.find('link') is not None else ""
                    
                    if not title or not link:
                        continue
                        
                    # 避免重複
                    cursor.execute('SELECT id FROM rss_items WHERE link = ?', (link,))
                    if cursor.fetchone():
                        continue
                        
                    cursor.execute('''
                        INSERT INTO rss_items (feed_id, title, link, first_crawl_time)
                        VALUES (?, ?, ?, ?)
                    ''', (feed_id, title, link, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                    src_count += 1
                
                conn.commit()
                print(f"    ✅ Added {src_count} items.")
                total_added += src_count
                
            except ET.ParseError:
                print(f"    ❌ XML Parse Error")
                
        except Exception as e:
            print(f"    ❌ Error: {e}")
            
    conn.close()
    print(f"  > [RSS Crawler] Done. Total new items: {total_added}")
    return db_path

# ==========================================
# 主程式
# ==========================================

if __name__ == '__main__':
    print("=" * 60)
    print("TrendRadar RSS Crawler (TW/US Focus)")
    print("=" * 60)
    
    # 直接執行爬蟲
    crawl_rss_to_db()


# --- END OF FILE step_trendradar.py ---
