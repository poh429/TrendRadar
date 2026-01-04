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
# 免費模型優先順序列表 (當遇到 429 時自動切換)
FREE_MODELS = [
    "google/gemini-2.0-flash-exp:free",       # 首選：Google 最新、速度快
    "xiaomi/mimo-v2-flash:free",              # 小米 MoE 309B，推理能力頂級
    "tngtech/deepseek-r1t2-chimera:free",     # DeepSeek R1T2 Chimera (671B MoE)
    "nex-agi/nex-n1-deepseek-v3.1:free",      # DeepSeek V3.1 旗艦系列
    "google/gemma-3-27b-it:free",             # Google Gemma 27B
    "meta-llama/llama-3.3-70b-instruct:free", # Meta Llama 3.3
    "meta-llama/llama-3.1-405b-instruct:free", # Meta 最大模型
    "nvidia/nemotron-3-nano-30b-a3b:free",    # NVIDIA 30B MoE (高效率)
    "mistralai/devstral-2-2512:free",         # Mistral 123B (Coding 專家)
    "kwaipilot/kat-coder-pro:free",           # KAT-Coder-Pro (Agentic Coding)
]
DEFAULT_MODEL = FREE_MODELS[0]
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
    """
    內建 OpenRouter Caller (含多模型 Fallback 機制)
    當遇到 429 限速時，自動切換到下一個免費模型嘗試
    """
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        print("  > [AI] ❌ 缺少 OPENROUTER_API_KEY")
        return None
    
    # 建立要嘗試的模型列表 (從傳入的 model 開始)
    models_to_try = [model]
    for m in FREE_MODELS:
        if m not in models_to_try:
            models_to_try.append(m)
    
    # 嘗試每個模型
    for current_model in models_to_try:
        max_retries = 2  # 每個模型最多重試 2 次
        
        for attempt in range(max_retries):
            try:
                res = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {key}",
                        "HTTP-Referer": "http://localhost:8501",
                        "X-Title": "Vestra AI Filter"
                    },
                    json={"model": current_model, "messages": messages, "temperature": temperature},
                    timeout=60
                )
                
                if res.status_code == 200: 
                    return res.json()['choices'][0]['message']['content']
                elif res.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = 5 * (attempt + 1)
                        print(f"  > [AI] ⚠️ {current_model.split('/')[1][:15]} 限速，等待 {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        # 此模型重試完畢，嘗試下一個模型
                        next_idx = models_to_try.index(current_model) + 1
                        if next_idx < len(models_to_try):
                            print(f"  > [AI] 🔄 切換模型: {models_to_try[next_idx].split('/')[1][:20]}")
                        break
                else:
                    print(f"  > [AI] API Error ({res.status_code}): {res.text[:100]}")
                    return None
                    
            except Exception as e:
                print(f"  > [AI] Request Error: {e}")
                time.sleep(5)
                continue
    
    print("  > [AI] ❌ 所有模型皆失敗，放棄此條目。")
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
    """
    Google News URL 處理
    
    經測試確認：Google News RSS URL (如 /articles/CBMi...) 
    在瀏覽器中點擊時會自動重導向到原始新聞網站。
    
    因此不需要複雜的解碼邏輯，直接返回原 URL 即可。
    使用者在 Lovable 前端點擊連結時，瀏覽器會自動處理重導向。
    """
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
            "content": "你是一位專精於「台股」與「美股」的首席投資分析師。你的任務是從雜亂的新聞中，篩選出對台灣股市(TWSE)或美國股市(NASDAQ/NYSE/S&P)有實質影響的情報。嚴格以 JSON 格式回覆。"
        },
        {
            "role": "user", 
            "content": f"""
新聞標題："{title}"

請以嚴格的「台/美股投資人視角」進行分析，回覆 JSON：
{{
  "score": 1-10 (整數),
  "category": "半導體" | "AI/科技" | "金融" | "傳產/航運" | "生技" | "宏觀/政策" | "其它",
  "reason": "簡短理由(20字內)"
}}

# 評分標準 (Score):
- **10分 (Market Mover)**: 重大突發(如戰爭/Fed降息)、台積電/輝達/蘋果/AMD財報暴雷或驚喜、國家級政策直接影響供應鏈。
- **8-9分 (High Impact)**: 台/美權值股(如鴻海, 聯發科, Tesla, MSFT)營收與動態、大型併購。
- **6-7分 (Moderate)**: 半導體/AI供應鏈消息、美股科技巨頭動態、同業競爭、關鍵原物料價格。
- **4-5分 (Low)**: 一般個股波動、非核心產業(如陸股白酒/內需)、例行性公告。
- **1-3分 (Noise)**: **中國內地社會新聞(如社會案件)**、**與台美股無關的政治口水**、廣告、純娛樂、體育。

# 關鍵過濾規則:
1. **主要關注**: 台灣企業、美國科技巨頭、全球宏觀經濟(通膨/油價/美債)。
2. **自動降級**: 若新聞僅涉及「中國A股特定板塊(如茅台、A股內資)」且無全球影響力，請給予 3 分以下。
3. **噪音剔除**: 社會案件(如殺人/車禍)、非財經類政治新聞(無經濟制裁內容)，一律 1 分。
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
        
        # 如果 AI 回傳的是 list，取第一個元素
        if isinstance(data, list) and len(data) > 0:
            data = data[0]
        
        # 確保有必要的欄位
        if isinstance(data, dict) and 'score' in data:
            return data
        
        return None
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
    
    # 彙整所有來源的新聞，並嚴格限制總量
    all_rows = []
    GLOBAL_LIMIT = 60  # 全局總限制 (每次最多處理 60 條)
    
    for db_path in db_paths:
        if len(all_rows) >= GLOBAL_LIMIT:
            break
            
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
            
            # 計算剩餘額度
            remaining_limit = GLOBAL_LIMIT - len(all_rows)
            
            # 取得新聞
            cursor_raw.execute(f"""
                SELECT n.id, n.title, n.url, n.first_crawl_time, p.name as platform_name 
                FROM {table_name} n
                LEFT JOIN {platform_table} p ON n.{platform_id_col} = p.id
                ORDER BY n.id DESC
                LIMIT {remaining_limit}
            """)
            rows = cursor_raw.fetchall()
            
            # 將 row 轉換為 dict 並加上來源標記，方便後續處理
            for r in rows:
                item = dict(r)
                # 統一時間欄位
                item['created_at'] = item['first_crawl_time'] 
                item['source'] = f"{source_label}-{item['platform_name']}"
                all_rows.append(item)
                
            conn_raw.close()
            
        except Exception as e:
            print(f"  > [Data Source] 讀取 {db_path} 錯誤: {e}")

    print(f"  > [AI Filter] 共取得 {len(all_rows)} 條原始新聞 (Global Limit: {GLOBAL_LIMIT})，準備進行分析...")

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
        
        # === 請求間隔 ===
        # 避免觸發 OpenRouter 每分鐘請求限制
        time.sleep(10)

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
            
            # ===== 清理舊資料 (保留 7 天 + 最多 100 條) =====
            try:
                from datetime import timedelta
                cutoff_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                
                # 1. 刪除超過 7 天的舊新聞
                supabase.table("ai_news").delete().lt("created_at", cutoff_date).execute()
                print(f"  > [Cleanup] 已刪除 {cutoff_date} 之前的舊新聞")
                
                # 2. 如果超過 100 條，只保留最新的 100 條
                result = supabase.table("ai_news").select("id", count="exact").execute()
                total_count = result.count if result.count else 0
                
                if total_count > 100:
                    # 取得第 101 條之後的 ID 並刪除
                    old_records = supabase.table("ai_news").select("id").order("created_at", desc=True).range(100, total_count).execute()
                    old_ids = [r['id'] for r in old_records.data] if old_records.data else []
                    
                    if old_ids:
                        for old_id in old_ids:
                            supabase.table("ai_news").delete().eq("id", old_id).execute()
                        print(f"  > [Cleanup] 已刪除超過 100 條限制的 {len(old_ids)} 條舊新聞")
                        
            except Exception as cleanup_err:
                print(f"  > [Cleanup] 清理時發生錯誤: {cleanup_err}")
        else:
            print("\n☁️ 今日無新增高價值新聞需同步至 Supabase。")

if __name__ == "__main__":
    initialize_services()
    process_latest_news()
