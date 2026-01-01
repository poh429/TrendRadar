# --- step39_news_filter.py (Truly Standalone Version v3.0) ---
"""
Vestra Data Utility Engine - AI 新聞過濾與評分模組
Standalone 版本：內建 AI 呼叫 + R2 下載邏輯，不依賴任何外部專案檔。
"""

import sqlite3
import os
import json
import requests
import re
import base64
import time
from datetime import datetime
from typing import List, Dict, Tuple, Optional

# R2 下載功能需要 boto3
try:
    import boto3
    from botocore.client import Config
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    print("  > [System] 未安裝 boto3，無法從 R2 下載數據。")

# Supabase 整合
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False

# 配置區
INVESTMENT_DB = "investment_news.db"
DEFAULT_MODEL = "google/gemini-2.0-flash-exp:free"
MIN_SCORE_THRESHOLD = 5

# ==================== 1. 內建 R2 下載功能 ====================
def download_from_r2(date_str, db_type='news'):
    """從 R2 下載指定日期的資料庫 (內建版)"""
    if not BOTO3_AVAILABLE:
        return None
        
    # 從環境變數讀取配置
    r2_endpoint = os.environ.get("S3_ENDPOINT_URL")
    r2_key_id = os.environ.get("S3_ACCESS_KEY_ID")
    r2_secret = os.environ.get("S3_SECRET_ACCESS_KEY")
    bucket_name = os.environ.get("S3_BUCKET_NAME", "trendradar-news")
    
    if not (r2_endpoint and r2_key_id and r2_secret):
        print("  > [R2] 缺少 R2 環境變數 (S3_ENDPOINT_URL 等)")
        return None

    try:
        s3 = boto3.client('s3',
            endpoint_url=r2_endpoint,
            aws_access_key_id=r2_key_id,
            aws_secret_access_key=r2_secret,
            config=Config(signature_version='s3v4'),
            region_name='auto'
        )
        
        file_name = f'trendradar_{db_type}_{date_str}.db'
        object_key = f'{db_type}/{date_str}.db'
        local_path = file_name
        
        print(f"  > [R2] 正在下載: {object_key} -> {local_path} ...")
        s3.download_file(bucket_name, object_key, local_path)
        print(f"  > [R2] ✅ 下載成功: {local_path}")
        return local_path
        
    except Exception as e:
        print(f"  > [R2] ⚠️ 下載失敗 ({object_key}): {e}")
        return None

# ==================== 2. 內建 AI 核心 (Rate Limited) ====================
def initialize_services():
    """為了相容性保留，實際上不需要做太多事"""
    pass

def call_openrouter(model, messages, temperature=0.3):
    """內建簡易版 OpenRouter Caller (含重試機制)"""
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        print("  > [AI] ❌ 缺少 OPENROUTER_API_KEY")
        return None
    
    # 定義重試次數
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            res = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {key}",
                    "HTTP-Referer": "http://localhost:8501",
                    "X-Title": "Vestra AI Filter"
                },
                json={"model": model, "messages": messages, "temperature": temperature},
                timeout=60
            )
            
            if res.status_code == 200: 
                return res.json()['choices'][0]['message']['content']
            elif res.status_code == 429:
                # 遇到限速，等待後重試 (更加激進的退避)
                wait_time = 30 * (attempt + 1)
                print(f"  > [AI] ⚠️ 觸發限速 (429)，等待 {wait_time} 秒後重試 ({attempt+1}/{max_retries})...")
                time.sleep(wait_time)
                continue
            else:
                print(f"  > [AI] API Error: {res.text}")
                return None
                
        except Exception as e:
            print(f"  > [AI] Request Error: {e}")
            time.sleep(10)
            continue
            
    print("  > [AI] ❌ 重試多次失敗，放棄此條目。")
    return None

# ==================== Supabase 整合 ====================
def init_supabase_client() -> Optional['Client']:
    """初始化 Supabase 客戶端 (需要環境變數 SUPABASE_URL & SUPABASE_KEY)"""
    if not SUPABASE_AVAILABLE:
        return None
    
    url = os.environ.get('SUPABASE_URL', '')
    key = os.environ.get('SUPABASE_KEY', '')
    
    if not url or not key:
        print("  > [Supabase] 未設定 SUPABASE_URL 或 SUPABASE_KEY 環境變數，跳過雲端同步。")
        return None
    
    try:
        client = create_client(url, key)
        print(f"  > [Supabase] ✅ 成功連接至 {url[:30]}...")
        return client
    except Exception as e:
        print(f"  > [Supabase] ❌ 連接失敗: {e}")
        return None

def resolve_google_news_url(url: str) -> str:
    """嘗試解析 Google News 轉址連結為原始 URL (模擬瀏覽器 + 頁面內容解析)"""
    if "news.google.com" not in url:
        return url
    
    try:
        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Referer": "https://news.google.com/",
        }
        
        # 1. 嘗試 HTTP 請求
        resp = session.get(url, headers=headers, allow_redirects=True, timeout=10)
        
        # 2. 檢查是否已經跳轉離開 google
        if "news.google.com" not in resp.url:
            return resp.url
        
        # 3. 如果還在 google，嘗試從內容中尋找目標連結
        # 很多時候 google 會回傳一個頁面，中間有一個 <a href="..."> 或 <noscript>
        content = resp.text
        
        # 策略 A: 找 <a href="..." jsname="..."> (Google 常見的跳轉按鈕)
        # 尋找所有連結，並排除 google 自身的連結
        links = re.findall(r'href="([^"]+)"', content)
        candidates = [l for l in links if l.startswith('http') and "google.com" not in l and "google.cn" not in l]
        
        if candidates:
            # print(f"    [Page Parse] Found: {candidates[0][:50]}...")
            return candidates[0]
            
        # 策略 B: 找 window.location.replace("...")
        js_redirect = re.search(r'window\.location\.replace\("([^"]+)"\)', content)
        if js_redirect:
            return js_redirect.group(1)

    except Exception:
        pass

    # 失敗回傳原網址
    return url

def clean_text(text: str) -> str:
    """清洗文字，移除可能導致 JSON 錯誤的無效字元"""
    if not text: return ""
    # 移除 null bytes 和控制字元，並確保是 valid utf-8
    return text.encode('utf-8', 'ignore').decode('utf-8').replace('\x00', '')

def sync_to_supabase(supabase_client: 'Client', news_list: List[Dict]) -> int:
    """
    將高價值新聞同步到 Supabase ai_news 表。
    包含：Check-and-Insert、連結解析強化、資料清洗
    """
    if not supabase_client or not news_list:
        return 0
    
    synced = 0
    total_items = len(news_list)
    print(f"  > [Supabase] 準備同步 {total_items} 條新聞 (含內容深度解析)...")
    
    ALLOWED_CATEGORIES = ["半導體", "AI/科技", "金融", "傳產/航運", "宏觀/政策", "其它"]
    
    for i, news in enumerate(news_list):
        try:
            # 0. 解析真實連結
            final_url = resolve_google_news_url(news['url'])
            
            # 0.5 強制清洗分類
            raw_cat = clean_text(news['category'])
            final_cat = raw_cat if raw_cat in ALLOWED_CATEGORIES else "其它"

            # 1. 先查詢是否存在
            existing = supabase_client.table('ai_news')\
                .select('id')\
                .eq('title', news['title'])\
                .eq('source', news['source'])\
                .execute()
            
            # 準備寫入的資料 (全部清洗一遍)
            data = {
                'title': clean_text(news['title']),
                'score': news['score'],
                'category': final_cat,
                'insight': clean_text(news['insight']),
                'url': clean_text(final_url),
                'source': clean_text(news['source']),
                'created_at': news['created_at']
            }

            if existing.data and len(existing.data) > 0:
                # 2. Update
                record_id = existing.data[0]['id']
                supabase_client.table('ai_news').update(data).eq('id', record_id).execute()
            else:
                # 3. Insert
                supabase_client.table('ai_news').insert(data).execute()
                
            synced += 1
        except Exception as e:
            # 簡化錯誤訊息，避免印出太多
            err_msg = str(e).replace('\n', ' ')
            print(f"  > [Supabase] 同步失敗: {news['title'][:10]}... Err: {err_msg[:100]}")
    
    return synced
# ========================================================

def prepare_data_sources() -> List[str]:
    """
    準備數據源：
    1. 嘗試下載今日的 'news' 和 'rss' 資料庫。
    2. 如果今日皆無，則尋找本地最新的 'news' 資料庫日期作為 fallback。
    3. 返回有效的資料庫路徑列表。
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    valid_dbs = []

    print(f"  > [Data Source] 檢查日期: {today_str}")

    # 嘗試下載今日數據
    for db_type in ['news', 'rss']:
        path = download_from_r2(today_str, db_type)
        if path and os.path.exists(path):
            valid_dbs.append(path)
    
    # 如果今日完全沒數據（可能是早上剛開始或者是測試環境），回退找本地最新的
    if not valid_dbs:
        print("  > [Data Source] 今日尚無數據，搜尋本地歷史檔案...")
        files = [f for f in os.listdir('.') if f.startswith('trendradar_news_') and f.endswith('.db')]
        if files:
            latest_db = sorted(files)[-1]
            date_part = latest_db.replace('trendradar_news_', '').replace('.db', '')
            print(f"  > [Data Source] 找到最新歷史日期: {date_part}")
            
            # 使用該日期重新檢查 news 和 rss
            valid_dbs.append(latest_db)
            rss_path = f'trendradar_rss_{date_part}.db'
            if os.path.exists(rss_path):
                valid_dbs.append(rss_path)
    
    return valid_dbs

def analyze_news_item(title: str) -> Dict:
    """使用 LLM 分析新聞標題的投資價值"""
    messages = [
        {
            "role": "system", 
            "content": "你是一位專業的證券分析師。請分析新聞標題，並給出投資價值評分與板塊歸類。嚴格以 JSON 格式回覆。"
        },
        {
            "role": "user", 
            "content": f"""
新聞標題："{title}"

請以嚴格的「投資人視角」進行分析，回覆 JSON：
{{
  "score": 1-10 (整數),
  "category": "半導體" | "AI/科技" | "金融" | "傳產/航運" | "生技" | "宏觀/政策" | "其它",
  "reason": "簡短理由(20字內)"
}}

# 評分標準 (Score):
- **10分 (Market Mover)**: 重大突發(如戰爭/降息)、台積電/輝達財報暴雷或驚喜、國家級政策變動。
- **8-9分 (High Impact)**: 權值股營收創新高/低、大型併購、產業龍頭漲跌停。
- **6-7分 (Moderate)**: 一般個股財報、法說會消息、外資升降評、產業趨勢新聞。
- **4-5分 (Low)**: 個股盤中波動、例行性營收公告(無驚喜)、股東會流水帳。
- **1-3分 (Noise)**: 廣告、花邊新聞、與經濟無關的政治口水、農場標題。

# 注意事項:
- 若標題包含「盤中速報」、「股價拉至漲停」等，視為即時行情，給予 4-5 分（除非是權值股如台積電/鴻海則更高）。
- 嚴格過濾非財經類新聞 (如娛樂、體育)，直接給 1 分。
"""
        }
    ]
    try:
        response = call_openrouter(DEFAULT_MODEL, messages)
        if not response:
            return None
        
        # 清理可能的 markdown 標籤
        clean_json = response.replace("```json", "").replace("```", "").strip()
        data = json.loads(clean_json)
        return data
    except Exception as e:
        print(f"  > [AI Filter] 分析失敗: {e}")
        return None

def init_investment_db():
    """初始化本地投資新聞資料庫"""
    conn = sqlite3.connect(INVESTMENT_DB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_news (
            original_id INTEGER,
            platform_name TEXT,
            title TEXT,
            url TEXT,
            score INTEGER,
            category TEXT,
            analysis TEXT,
            crawl_time TEXT,
            processed_at TEXT,
            PRIMARY KEY (original_id, platform_name)
        )
    ''')
    conn.commit()
    conn.close()

def process_latest_news():
    """主處理流程"""
    db_paths = prepare_data_sources()
    if not db_paths:
        print("  > [AI Filter] ❌ 找不到任何資料庫 (News 或 RSS)。")
        return

    print(f"  > [AI Filter] 將處理以下資料庫: {db_paths}")
    init_investment_db()
    
    # 彙整所有來源的新聞
    all_rows = []
    
    for db_path in db_paths:
        try:
            print(f"  > [AI Filter] 讀取 {db_path} ...")
            conn_raw = sqlite3.connect(db_path)
            conn_raw.row_factory = sqlite3.Row
            cursor_raw = conn_raw.cursor()
            
            # 判斷是否為 RSS 來源 (透過檔名)
            is_rss = 'rss' in db_path
            source_label = 'RSS' if is_rss else 'News'
            
            # 定義不同資料庫類型的表名
            table_name = "rss_items" if is_rss else "news_items"
            platform_table = "rss_feeds" if is_rss else "platforms"
            platform_id_col = "feed_id" if is_rss else "platform_id"
            
            # 取得新聞
            cursor_raw.execute(f"""
                SELECT n.id, n.title, n.url, n.first_crawl_time, p.name as platform_name 
                FROM {table_name} n
                LEFT JOIN {platform_table} p ON n.{platform_id_col} = p.id
                ORDER BY n.id DESC
                LIMIT 200
            """)
            rows = cursor_raw.fetchall()
            
            # 將 row 轉換為 dict 並加上來源標記，方便後續處理
            for r in rows:
                item = dict(r)
                item['source_db'] = source_label
                # 若 RSS 的 platform_name 為空，使用預設值
                if not item['platform_name']:
                    item['platform_name'] = f"{source_label}_Feed"
                all_rows.append(item)
                
            conn_raw.close()
        except Exception as e:
            print(f"  > [AI Filter] ⚠️ 讀取 {db_path} 失敗: {e}")

    print(f"  > [AI Filter] 共取得 {len(all_rows)} 條原始新聞，準備進行分析...")

    # 連接到輸出庫
    conn_inv = sqlite3.connect(INVESTMENT_DB)
    cursor_inv = conn_inv.cursor()

    count_processed = 0
    count_high_value = 0

    for row in all_rows:
        # 檢查是否已處理過
        cursor_inv.execute("SELECT 1 FROM processed_news WHERE original_id=? AND platform_name=?", 
                          (row['id'], row['platform_name']))
        if cursor_inv.fetchone():
            continue

        print(f"  > [AI Filter] 正在分析: {row['title'][:30]}...")
        analysis = analyze_news_item(row['title'])
        
        # === 關鍵修改：強制休息 ===
        # OpenRouter 免費版限制約 20 req/min，所以每次休息 10 秒 + 執行時間，剛好安全
        time.sleep(10) 
        # ========================

        if not analysis:
            continue
            
        score = analysis.get('score', 0)
        raw_cat = analysis.get('category', '其它')
        reason = analysis.get('reason', '')
        
        # Lovable Cloud Category Constraint Fix
        # 資料庫有 CHECK 約束，必須對應允許的清單
        ALLOWED_CATEGORIES = ["半導體", "AI/科技", "金融", "傳產/航運", "宏觀/政策", "其它"]
        category = raw_cat if raw_cat in ALLOWED_CATEGORIES else "其它"

        # 存入資料庫
        cursor_inv.execute("""
            INSERT INTO processed_news 
            (original_id, platform_name, title, url, score, category, analysis, crawl_time, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row['id'], 
            row['platform_name'], 
            row['title'], 
            row['url'], 
            score, 
            category, 
            reason, 
            row['first_crawl_time'],
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        count_processed += 1
        if score >= MIN_SCORE_THRESHOLD:
            count_high_value += 1
            print(f"    🌟 高價值發現! [{category}] 分數: {score} - {reason}")

    conn_inv.commit()
    conn_inv.close()
    
    print(f"\n✅ 本地處理完成！")
    print(f"   • 新增處理: {count_processed} 條")
    print(f"   • 其中高價值 (>={MIN_SCORE_THRESHOLD}): {count_high_value} 條")
    
    # ===== 同步至 Supabase (可選) =====
    supabase = init_supabase_client()
    if supabase:
        # 從本地資料庫讀取今日高分新聞
        conn_sync = sqlite3.connect(INVESTMENT_DB)
        conn_sync.row_factory = sqlite3.Row
        cursor_sync = conn_sync.cursor()
        today_str = datetime.now().strftime('%Y-%m-%d')
        cursor_sync.execute(f"""
            SELECT title, score, category, analysis as insight, url, platform_name as source, processed_at as created_at
            FROM processed_news
            WHERE score >= {MIN_SCORE_THRESHOLD} AND processed_at LIKE '{today_str}%'
        """)
        high_value_news = [dict(row) for row in cursor_sync.fetchall()]
        conn_sync.close()
        
        if high_value_news:
            synced_count = sync_to_supabase(supabase, high_value_news)
            print(f"\n☁️ Supabase 同步完成！共推送 {synced_count} 條高價值新聞。")
        else:
            print("\n☁️ 今日無新增高價值新聞需同步至 Supabase。")

if __name__ == "__main__":
    initialize_services()
    process_latest_news()
