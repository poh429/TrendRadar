# --- START OF FILE llm_core.py (v2.2 - 免費防火牆版) ---

import os
import json
import requests
import time
import hashlib
import sqlite3
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
try:
    from ollama import Client
except ImportError:
    Client = None
# Google GenAI SDK (v2.0 遷移至新版)
try:
    from google import genai
    from google.genai import types
    GOOGLE_GENAI_AVAILABLE = True
except ImportError:
    try:
        import google.generativeai as genai
        from google.generativeai.types import GenerationConfig
        GOOGLE_GENAI_AVAILABLE = True
    except ImportError:
        GOOGLE_GENAI_AVAILABLE = False

try:
    from ddgs import DDGS
except ImportError:
    DDGS = None

try:
    from openai import OpenAI
except ImportError:
    class OpenAI: pass

# --- Global State ---
ollama_client = None
google_client_ready = False
github_client = None

# --- Configuration ---
CACHE_DB = "daily_signals.db"
CACHE_TABLE = "llm_response_cache"
# 【v2.2 新增】 強制免費模式開關
# 如果設為 True，call_openrouter 會拒絕任何沒有 ":free" 後綴的模型
FORCE_FREE_MODE = True 

def _get_connection():
    conn = sqlite3.connect(CACHE_DB, timeout=30.0, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def initialize_services():
    global ollama_client, google_client_ready, github_client
    load_dotenv()
    _init_cache_table()
    _cleanup_cache()
    
    # 1. Ollama
    ollama_key = os.getenv("OLLAMA_API_KEY")
    if ollama_key:
        try: ollama_client = Client(host="https://ollama.com", headers={'Authorization': f'Bearer {ollama_key}'})
        except: pass

    # 2. Google
    google_key = os.getenv("GOOGLE_API_KEY")
    if google_key and GOOGLE_GENAI_AVAILABLE:
        try:
            # 新版 API 使用環境變數或直接設定
            os.environ["GOOGLE_API_KEY"] = google_key
            google_client_ready = True
        except: pass

    # 3. GitHub Models
    github_token = os.getenv("GITHUB_TOKEN")
    if github_token:
        try: github_client = OpenAI(base_url="https://models.inference.ai.azure.com", api_key=github_token)
        except: pass

def _init_cache_table():
    try:
        with _get_connection() as conn:
            conn.execute(f'''CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (prompt_hash TEXT PRIMARY KEY, model_name TEXT, response TEXT, created_date TEXT)''')
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_llm_date ON {CACHE_TABLE} (created_date)")
    except: pass

def _cleanup_cache(days_to_keep=7):
    try:
        cutoff = (date.today() - timedelta(days=days_to_keep)).isoformat()
        with _get_connection() as conn:
            conn.execute(f"DELETE FROM {CACHE_TABLE} WHERE created_date < ?", (cutoff,))
            conn.commit()
    except: pass

def _get_cache(model_name, prompt_str):
    try:
        today = date.today().isoformat()
        raw_key = f"{model_name}|{prompt_str}|{today}"
        key_hash = hashlib.md5(raw_key.encode('utf-8')).hexdigest()
        with _get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT response FROM {CACHE_TABLE} WHERE prompt_hash=?", (key_hash,))
            row = cursor.fetchone()
            if row: return row[0]
    except: pass
    return None

def _set_cache(model_name, prompt_str, response):
    if not response: return
    try:
        today = date.today().isoformat()
        raw_key = f"{model_name}|{prompt_str}|{today}"
        key_hash = hashlib.md5(raw_key.encode('utf-8')).hexdigest()
        with _get_connection() as conn:
            conn.execute(f"REPLACE INTO {CACHE_TABLE} VALUES (?, ?, ?, ?)", (key_hash, model_name, response, today))
            conn.commit()
    except: pass

initialize_services()

# --- 1. OpenRouter Caller (With Free Guard) ---
def call_openrouter(models, messages, temperature=0.3):
    key = os.getenv("OPENROUTER_API_KEY")
    if not key: return None
    if isinstance(models, str): models = [models]
    
    prompt_str = json.dumps(messages, sort_keys=True)
    
    for model in models:
        # 【v2.2 核心】 免費防火牆
        if FORCE_FREE_MODE and ":free" not in model:
            # print(f"  > [OpenRouter] 攔截付費模型請求: {model}")
            continue

        cached = _get_cache(model, prompt_str)
        if cached: return cached

        try:
            res = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {key}",
                    "HTTP-Referer": "http://localhost:8501",
                    "X-Title": "Quantamental Engine"
                },
                json={"model": model, "messages": messages, "temperature": temperature},
                timeout=120
            )
            
            if res.status_code == 200: 
                content = res.json()['choices'][0]['message']['content']
                _set_cache(model, prompt_str, content)
                return content
            else:
                time.sleep(1)
                continue
        except: continue
            
    return None

# --- 2. Google SDK Caller (v2.0 新版 API) ---
def call_google_sdk(model_name, prompt, temperature=0.3):
    if not google_client_ready or not GOOGLE_GENAI_AVAILABLE: return None
    cached = _get_cache(model_name, prompt)
    if cached: return cached

    try:
        # 檢查是否為新版 API
        if hasattr(genai, 'Client'):
            # 新版 google.genai
            client = genai.Client()
            response = client.models.generate_content(
                model=model_name,
                contents=[prompt],
                config=types.GenerateContentConfig(temperature=temperature)
            )
            content = response.text.strip()
        else:
            # 舊版 google.generativeai (fallback)
            model = genai.GenerativeModel(model_name)
            config = GenerationConfig(temperature=temperature)
            content = model.generate_content(prompt, generation_config=config).text.strip()
        
        _set_cache(model_name, prompt, content)
        return content
    except Exception as e:
        # 靜默失敗，返回 None
        return None

# --- 3. GitHub Models Caller ---
def call_github_models(model_name, messages, temperature=0.3):
    if not github_client: return None
    prompt_str = json.dumps(messages, sort_keys=True)
    cached = _get_cache(model_name, prompt_str)
    if cached: return cached

    try:
        msgs_to_send = list(messages)
        if msgs_to_send and msgs_to_send[0]['role'] != 'system':
            msgs_to_send.insert(0, {"role": "system", "content": "You are a helpful AI assistant."})

        response = github_client.chat.completions.create(
            messages=msgs_to_send, 
            model=model_name, 
            temperature=temperature, 
            max_tokens=4096, 
            timeout=90
        )
        content = response.choices[0].message.content
        _set_cache(model_name, prompt_str, content)
        return content
    except: return None

# --- 4. Ollama Cloud Caller ---
def call_ollama_cloud(model, messages, temperature=0.3):
    if not ollama_client: return None
    prompt_str = json.dumps(messages, sort_keys=True)
    cached = _get_cache(model, prompt_str)
    if cached: return cached
    
    try:
        res = ollama_client.chat(model=model, messages=messages, options={'temperature': temperature})
        content = res['message']['content']
        _set_cache(model, prompt_str, content)
        return content
    except: return None

def call_mistral_api(prompt): return None 
def call_groq_api(prompt, messages=None): return None

# --- Web Search Tools ---
def perform_web_search_tool(query_text):
    if ollama_client:
        try:
            key = os.getenv("OLLAMA_API_KEY")
            res = requests.post(
                "https://ollama.com/api/web_search",
                headers={'Authorization': f'Bearer {key}'},
                json={"query": query_text}, timeout=20
            )
            if res.status_code == 200:
                results = res.json().get('results', [])
                if results:
                    return "\n\n".join([f"Title: {i.get('title')}\nContent: {i.get('content')}" for i in results[:3]])
        except: pass
    
    if DDGS:
        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query_text, max_results=3))
                if results:
                    return "\n\n".join([f"Title: {i.get('title')}\nContent: {i.get('body')}" for i in results])
        except Exception: pass
        
    return "No search results found."

# --- END OF FILE llm_core.py ---
