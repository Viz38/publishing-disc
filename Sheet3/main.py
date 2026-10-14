import os
import sys

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_DIR = os.path.dirname(_BASE_DIR)
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

from config import settings
from common import (
    TracxnKeyManager,
    NIMKeyManager,
    TechCrawlerClient,
    WaterfallScraper,
    LocalSheetCache,
    ExecutionTracker,
    is_5am_ist_cutoff,
    is_pre_cutoff_window,
    get_now_ist,
    get_ist_timestamp,
    safe_str,
    BacklogManager,
    TracxnClient,
    PipelineCoordinator,
    trim_memory,
    SystemHealthMonitor,
)

import asyncio
import aiohttp
import gspread_asyncio
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import logging
import re
import ssl
import time
from typing import List, Dict, Tuple, Optional
from openpyxl.utils import get_column_letter
import json
import pandas as pd
import numpy as np
from datetime import date, timedelta
import gspread
import concurrent.futures
import requests
from multiprocessing import Pool, TimeoutError
import os
import datetime
try:
    import zoneinfo
    IST = zoneinfo.ZoneInfo("Asia/Kolkata")
except Exception:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
import itertools

# from datetime import datetime  # Correct way
from datetime import date
from datetime import date, timedelta
from pprint import pprint


max_threads = os.cpu_count()

_SHEET_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_CACHE = LocalSheetCache(os.path.join(_SHEET_DIR, "discovery_cache.csv"))
EXEC_TRACKER = ExecutionTracker(os.path.join(_SHEET_DIR, ".execution_state.json"))
SYSTEM_HEALTH = SystemHealthMonitor(cpu_threshold=75.0, mem_threshold=75.0)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Linux-specific: proactively raise file descriptor limit for high-concurrency async I/O
def _optimize_file_descriptor_limit(target_limit: int = 65536) -> int:
    """Programmatically optimize the OS file descriptor limit (RLIMIT_NOFILE).
    Critical on Linux to prevent EMFILE errors under high concurrency."""
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        hard_limit = hard if hard > 0 else target_limit
        for limit in [min(target_limit, hard_limit), 32768, 10240, 4096, 2048]:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))
                logging.info(f"File descriptor limit set to {limit} (was {soft})")
                return limit
            except Exception:
                continue
        return soft
    except Exception:
        return 256

_optimize_file_descriptor_limit()
try:
    if hasattr(os, "nice"):
        os.nice(10)
except Exception:
    pass

# SR Orchestration Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(BASE_DIR, 'Logs')
PROGRESS_FILE = os.path.join(BASE_DIR, '.progress.json')
STOP_FILE = os.path.join(BASE_DIR, '.stop_requested')
os.makedirs(LOGS_DIR, exist_ok=True)
discovery_log_path = os.path.join(LOGS_DIR, 'discovery.log')

# Dual file and console logging
root_logger = logging.getLogger()
file_handler = logging.FileHandler(discovery_log_path, mode='a')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
root_logger.addHandler(file_handler)

def update_progress_file(current: int, total: int, success: int, fail: int, phase: str = "", stage_details: str = "", pending_tc: int = 0, mode: str = ""):
    prog_data = {
        'current': current,
        'total': total,
        'success': success,
        'fail': fail,
        'phase': phase,
        'stage_details': stage_details,
        'pending_tc': pending_tc,
        'mode': mode
    }
    tmp_path = f"{PROGRESS_FILE}.tmp.{os.getpid()}"
    try:
        with open(tmp_path, 'w') as pf:
            json.dump(prog_data, pf)
        os.replace(tmp_path, PROGRESS_FILE)
    except Exception:
        try:
            with open(PROGRESS_FILE, 'w') as pf:
                json.dump(prog_data, pf)
        except Exception:
            pass

    try:
        EXEC_TRACKER.update_heartbeat()
    except Exception:
        pass

import signal

def _handle_user_signal(signum, frame):
    logging.info(f"Signal {signum} received by main process. Recording user stop...")
    try:
        EXEC_TRACKER.record_user_stop()
    except Exception:
        pass
    sys.exit(130 if signum == signal.SIGINT else 143)

try:
    signal.signal(signal.SIGINT, _handle_user_signal)
    signal.signal(signal.SIGTERM, _handle_user_signal)
except Exception:
    pass

# **************************************************************************************************************************
# *************************************** Configuration *************************************************
# **************************************************************************************************************************

# Global variables
geminiCalls = 0
MAIN_PROMPTS = []  # Fixed the issue by initializing as an empty list

MAX_PROMPT_SIZE = 40000
SLICE_SIZE = 40000

# Request headers
mainheaders = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}


# Google Sheets Configuration
CF_SHEET_ID = settings.CF_SHEET_ID
if "--sheet_id" in sys.argv:
    try:
        _idx = sys.argv.index("--sheet_id")
        if _idx + 1 < len(sys.argv):
            CF_SHEET_ID = sys.argv[_idx + 1]
    except Exception:
        pass
CF_EXTRACTING_SHEET_NAME = "CF_IF_Engine_Database"
IF_SHEET_ID = settings.IF_SHEET_ID
IF_EXTRACTING_SHEET_NAME = "IF_Engine_Database"

CREDENTIALS_FILE = settings.CREDENTIALS_FILE
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))  # Scaled for up to 60% resource utilization (3,500 - 4,500+ domains/hr)
BATCH_SIZE = settings.BATCH_SIZE  # Default 100 rows per batch
_WORKSHEET_CACHE = {}
Config_Sheet_ID = settings.Config_Sheet_ID

To_Review_TAG_NAME = "bu_llm_domainformanualreview" #"domainformanualreview"
Wrong_Validated_IF_Tag_Name = "bu_llm_domainwrong_if_validation" #"domainwrong_if_validation"
Wrong_Validated_Feed_Tag_Name = "bu_llm_domainwrong_feed_validation" #"domainwrong_feed_validation"
Discovery_Processing_TAG_NAME = "bu_llm_discovery_processing"
COMMENT = "This is a Write API Movement by Gemini"
Wrong_Validation_COMMENT = "This is a Write API Movement by Gemini(Marked for Manager review)"

GEMINI_TOKEN = settings.GEMINI_TOKEN
Tracxn_API_Token = settings.Tracxn_API_Token

TRACXN_KEY_MANAGER = TracxnKeyManager()
TRACXN_CLIENT = TracxnClient(token_rotator=TRACXN_KEY_MANAGER)
BACKLOG_MANAGER = BacklogManager()

Nvidia_Base_Model  = "meta/muse-glimmer-30b"
Nvidia_Endpoint = "https://integrate.api.nvidia.com/v1/chat/completions"

# NIM API keys loaded from environment (.env)
NIM_HARDCODED_KEYS = settings.NIM_API_KEYS

# Error strings to check
CHECK_STRINGS = ["403 Forbidden", "Not Acceptable", "Coming Soon", "Javascript Enable", "Enable JavaScript", "Error 403", "Page Not Found"]
CHECK_EXACT = "Website is not working."


# valid_dates = [datetime.now().strftime("%Y-%m-%d"), (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")]
valid_dates = [
    datetime.datetime.now().strftime("%Y-%m-%d"), 
    (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
]

def safe_str(val):
    """Utility helper to safely handle None values when doing string operations."""
    return str(val) if val is not None else ""

# ********************************** Google Sheet Configuration *******************************************************************

def get_service_account_credentials(scope):
    """Retrieve Google Service Account credentials from .env JSON or fallback file."""
    raw_json = getattr(settings, "GOOGLE_SERVICE_ACCOUNT_JSON", "") or os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if raw_json:
        raw_json = raw_json.strip()
        if (raw_json.startswith("'") and raw_json.endswith("'")) or (raw_json.startswith('"') and raw_json.endswith('"')):
            raw_json = raw_json[1:-1].strip()
        try:
            key_dict = json.loads(raw_json)
            return ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        except Exception as e:
            logging.error(f"Failed to load credentials from GOOGLE_SERVICE_ACCOUNT_JSON: {e}")

    # Fallback to local or root creds.json
    base_folder = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        getattr(settings, "CREDENTIALS_FILE", ""),
        os.getenv("CREDENTIALS_FILE", ""),
        os.path.join(base_folder, "creds.json"),
        os.path.join(os.path.dirname(base_folder), "creds.json")
    ]
    for c_file in candidates:
        if c_file and os.path.exists(c_file):
            try:
                with open(c_file, "r") as f:
                    content = f.read().strip()
                if (content.startswith("'") and content.endswith("'")) or (content.startswith('"') and content.endswith('"')):
                    content = content[1:-1].strip()
                key_dict = json.loads(content)
                return ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
            except Exception as ex:
                logging.warning(f"Could not load keyfile dict from {c_file}: {ex}")
                try:
                    return ServiceAccountCredentials.from_json_keyfile_name(c_file, scope)
                except Exception:
                    pass

    raise ValueError("No valid Google service account credentials found in .env or CREDENTIALS_FILE")

def get_gspread_client_General():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = get_service_account_credentials(scope)
    client = gspread.authorize(creds)
    return client


# Async Google Sheets Client
async def get_gspread_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = get_service_account_credentials(scope)
    return gspread_asyncio.AsyncioGspreadClientManager(lambda: creds)

def get_backup_service_account_credentials(scope):
    """Retrieve backup Google Service Account credentials from .env JSON or fallback."""
    raw_json = getattr(settings, "GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON", "") or os.getenv("GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON", "")
    if raw_json:
        raw_json = raw_json.strip()
        if (raw_json.startswith("'") and raw_json.endswith("'")) or (raw_json.startswith('"') and raw_json.endswith('"')):
            raw_json = raw_json[1:-1].strip()
        try:
            key_dict = json.loads(raw_json)
            return ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
        except Exception as e:
            logging.error(f"Failed to load credentials from GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON: {e}")
    return None

def get_gspread_client_General_Backup():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = get_backup_service_account_credentials(scope)
    if creds:
        try:
            return gspread.authorize(creds)
        except Exception as e:
            logging.error(f"Failed to authorize backup gspread client: {e}")
    return None

async def get_gspread_client_backup():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = get_backup_service_account_credentials(scope)
    if creds:
        return gspread_asyncio.AsyncioGspreadClientManager(lambda: creds)
    return None

def is_pipeline_sheet(sheet_id: str) -> bool:
    """Check if sheet_id is one of the 4 pipeline database sheets."""
    if not sheet_id:
        return False
    pipeline_ids = {
        getattr(settings, "CF_SHEET_ID", ""),
        getattr(settings, "IF_SHEET_ID", ""),
        os.getenv("SHEET1_ID", ""),
        os.getenv("SHEET2_ID", ""),
        os.getenv("SHEET3_ID", ""),
        os.getenv("SHEET4_ID", ""),
        "12nhpp7eIkVktYuTMyQ-zMbaP_-CfRRClg1_v0vAlUDQ",
    }
    return sheet_id.strip() in {s.strip() for s in pipeline_ids if s}

def open_sheet_with_fallback(client, sheet_id: str):
    """
    Open a Google Spreadsheet by key adhering to priority rules:
    - 4 Pipeline Sheets: try GOOGLE_SERVICE_ACCOUNT_JSON first; fallback to GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON.
    - All Other Sheets (e.g. Config): try GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON first; fallback to GOOGLE_SERVICE_ACCOUNT_JSON.
    """
    if not sheet_id:
        raise ValueError("sheet_id is empty or not provided")

    is_pipe = is_pipeline_sheet(sheet_id)
    primary_client = get_gspread_client_General()
    backup_client = get_gspread_client_General_Backup()

    if is_pipe:
        first_client = primary_client
        second_client = backup_client
        first_name, second_name = "Primary (GOOGLE_SERVICE_ACCOUNT_JSON)", "Backup (GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON)"
    else:
        first_client = backup_client or primary_client
        second_client = primary_client if backup_client else None
        first_name, second_name = "Backup (GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON)", "Primary (GOOGLE_SERVICE_ACCOUNT_JSON)"

    # Try first client
    if first_client:
        try:
            return first_client.open_by_key(sheet_id)
        except Exception as e:
            logging.warning(f"{first_name} failed to open sheet {sheet_id} ({e}). Retrying with {second_name}...")

    # Try second client
    if second_client:
        try:
            return second_client.open_by_key(sheet_id)
        except Exception as e:
            logging.error(f"{second_name} also failed to open sheet {sheet_id} ({e}).")
            raise

    if first_client:
        return first_client.open_by_key(sheet_id)
    raise ValueError(f"Unable to open sheet {sheet_id}: No valid credentials available.")


async def read_sheet_data(client, SHEET_ID, sheet_name, start_row=4, start_col=1):
    logging.info(f"Opening sheet {SHEET_ID} [{sheet_name}] to load rows starting from row {start_row}...")
    is_pipe = is_pipeline_sheet(SHEET_ID)
    primary_mgr = await get_gspread_client()
    backup_mgr = await get_gspread_client_backup()

    if is_pipe:
        first_mgr, second_mgr = primary_mgr, backup_mgr
    else:
        first_mgr, second_mgr = (backup_mgr or primary_mgr), (primary_mgr if backup_mgr else None)

    sheet = None
    if first_mgr:
        try:
            gc = await first_mgr.authorize()
            sheet = await gc.open_by_key(SHEET_ID)
        except Exception as e:
            if second_mgr:
                logging.warning(f"First async client failed for sheet {SHEET_ID} ({e}). Retrying with fallback...")
                gc = await second_mgr.authorize()
                sheet = await gc.open_by_key(SHEET_ID)
            else:
                raise
    elif second_mgr:
        gc = await second_mgr.authorize()
        sheet = await gc.open_by_key(SHEET_ID)

    worksheet = await sheet.worksheet(sheet_name)
    logging.info(f"Downloading rows from worksheet [{sheet_name}]...")
    if hasattr(worksheet, "get_values"):
        data = await worksheet.get_values()
    else:
        data = await worksheet.get_all_values()

    raw_total = len(data) if data else 0
    sliced_data = data[max(0, start_row - 1):] if data else []
    logging.info(f"Retrieved {raw_total} total rows from [{sheet_name}]. Sliced {len(sliced_data)} rows starting from row {start_row}.")
    if not sliced_data and raw_total > 0:
        logging.warning(f"⚠️ start_row={start_row} is beyond the total rows ({raw_total}) in worksheet [{sheet_name}]! No domains to process.")
    return sliced_data


async def write_sheet_data(client, SHEET_ID, sheet_name, data, start_row, start_col=4):
    if not data or not data[0]:
        return

    # Critical: safely cap all cell values to 49,900 chars to avoid Google Sheets 50,000 char per cell HTTP 400 fatal crash
    data = [
        [
            val[:49900] if isinstance(val, str) and len(val) > 49900 else val
            for val in row
        ]
        for row in data
    ]

    cache_key = (str(SHEET_ID).strip(), str(sheet_name).strip())
    worksheet = _WORKSHEET_CACHE.get(cache_key)

    is_pipe = is_pipeline_sheet(SHEET_ID)
    primary_mgr = await get_gspread_client()
    backup_mgr = await get_gspread_client_backup()

    if is_pipe:
        first_mgr, second_mgr = primary_mgr, backup_mgr
    else:
        first_mgr, second_mgr = (backup_mgr or primary_mgr), (primary_mgr if backup_mgr else None)

    if worksheet is None:
        sheet = None
        if first_mgr:
            try:
                gc = await first_mgr.authorize()
                sheet = await gc.open_by_key(SHEET_ID)
            except Exception as e:
                if second_mgr:
                    logging.warning(f"First async client failed for sheet {SHEET_ID} ({e}). Retrying with fallback...")
                    gc = await second_mgr.authorize()
                    sheet = await gc.open_by_key(SHEET_ID)
                else:
                    raise
        elif second_mgr:
            gc = await second_mgr.authorize()
            sheet = await gc.open_by_key(SHEET_ID)

        worksheet = await sheet.worksheet(sheet_name)
        _WORKSHEET_CACHE[cache_key] = worksheet

    cell_range = f"{get_column_letter(start_col)}{start_row}:{get_column_letter(start_col + len(data[0]) - 1)}{start_row + len(data) - 1}"
    try:
        try:
            await worksheet.update(values=data, range_name=cell_range, value_input_option='USER_ENTERED')
        except TypeError:
            await worksheet.update(cell_range, data, value_input_option='USER_ENTERED')
    except Exception as update_err:
        logging.warning(f"Worksheet update failed ({update_err}). Invalidating cache and retrying...")
        _WORKSHEET_CACHE.pop(cache_key, None)
        sheet = None
        gc = await (second_mgr or first_mgr).authorize() if second_mgr else await first_mgr.authorize()
        sheet = await gc.open_by_key(SHEET_ID)
        worksheet = await sheet.worksheet(sheet_name)
        _WORKSHEET_CACHE[cache_key] = worksheet
        try:
            await worksheet.update(values=data, range_name=cell_range, value_input_option='USER_ENTERED')
        except TypeError:
            await worksheet.update(cell_range, data, value_input_option='USER_ENTERED')


# ****************************** Data Cleaning ************************************************************************

async def fetch_page(session, url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            # logging.info(f"Fetching URL: {url} (Attempt {attempt + 1}/{retries})")
            async with session.get(url, timeout=10, allow_redirects=True, headers=mainheaders) as response:
                logging.info(f"Response status: {response.status} for URL: {url}")
                raw_bytes = await response.read()
                encoding = response.get_encoding() or "utf-8"
                html = raw_bytes.decode(encoding, errors="ignore")
                return html, response.status
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                return "", 500
    return "", 500

async def scrape_with_bs(session, url):
    html, status_code = await fetch_page(session, url)
    if html:
        cleaned = clean_html(html)
        return cleaned, status_code
    return "", status_code

def clean_html(html):
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        for s in soup(["script", "style", "noscript", "svg", "header", "footer"]):
            s.decompose()
        text = soup.get_text(separator=" ", strip=True)
        text = re.sub(r"\s+", " ", text)
        return text.strip()
    except Exception:
        return ""

def clean_date(d):
    """Normalize various date formats into YYYY-MM-DD."""
    if not d:
        return ""
    if isinstance(d, (datetime.date, datetime.datetime)):
        return d.strftime("%Y-%m-%d")
    s = str(d).strip()
    for fmt in ("%Y-%m-%d", "%d/%b/%Y", "%d/%b/%Y %H:%M:%S", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r"\d{4}-\d{2}-\d{2}", s)
    if m:
        return m.group(0)
    return s

def Mark_Error_Domains(Raw_Data):
    """Check if scraped content represents an error or failed domain."""
    if not Raw_Data:
        return True
    raw_str = safe_str(Raw_Data).strip()
    if not raw_str or raw_str == CHECK_EXACT:
        return True
    for cs in CHECK_STRINGS:
        if cs.lower() in raw_str.lower():
            return True
    return False

def get_nim_ssl_context() -> ssl.SSLContext:
    try:
        ctx = ssl.create_default_context()
        return ctx
    except Exception:
        return ssl._create_unverified_context()

class NIMKeyManager:
    """Manages round-robin rotation of NIM API keys with rate-limit cooldowns."""

    def __init__(self, keys: List[str]):
        self.keys = [k.strip() for k in keys if k.strip()]
        if not self.keys:
            raise ValueError("No valid NIM API keys provided.")
        self._index = 0
        self._lock: Optional[asyncio.Lock] = None
        self._cooldowns: Dict[str, float] = {}

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def get_next_key(self) -> str:
        now = time.time()
        expired = [k for k, expire_time in self._cooldowns.items() if now >= expire_time]
        for k in expired:
            del self._cooldowns[k]

        total = len(self.keys)
        for _ in range(total):
            key = self.keys[self._index % total]
            self._index += 1
            if key not in self._cooldowns:
                return key

        if self._cooldowns:
            earliest_key = min(self._cooldowns.keys(), key=lambda k: self._cooldowns[k])
            return earliest_key
        return self.keys[0]

    async def get_next_key_async(self) -> Tuple[str, float]:
        async with self._get_lock():
            now = time.time()
            expired = [k for k, exp in self._cooldowns.items() if now >= exp]
            for k in expired:
                del self._cooldowns[k]

            total = len(self.keys)
            for _ in range(total):
                key = self.keys[self._index % total]
                self._index += 1
                if key not in self._cooldowns:
                    return key, 0.0

            earliest_key = min(self._cooldowns.keys(), key=lambda k: self._cooldowns[k])
            wait_time = max(0.5, self._cooldowns[earliest_key] - now)
            return earliest_key, wait_time

    def mark_rate_limited(self, key: str, cooldown_seconds: float = 25.0):
        self._cooldowns[key] = time.time() + cooldown_seconds

    def mark_invalid(self, key: str):
        if key in self.keys:
            self.keys.remove(key)
        if key in self._cooldowns:
            del self._cooldowns[key]
        logging.warning(f"Permanently disabled invalid NIM key ...{key[-6:]}. Active keys remaining: {len(self.keys)}")

    @property
    def total_tokens(self) -> int:
        return len(self.keys)

TokenRotator = NIMKeyManager

#********************************** Getting NVDIA Tokens ************************************

def Return_NIM_Config_Details(client=None):
    """Retrieve NIM model name and API tokens directly from environment settings, skipping sheet fetch."""
    global _RUN_CACHE
    if _RUN_CACHE.get("nim_config"):
        return _RUN_CACHE["nim_config"]

    tokens = [["", "", "", "", k] for k in settings.NIM_API_KEYS]
    result_map = {
        "NIM_Model_To_Pick": settings.Nvidia_Base_Model,
        "tokenArray": tokens
    }
    _RUN_CACHE["nim_config"] = result_map
    return result_map

#*************************************** Output Sanitization (from Runner.py) *************************************

def sanitize_summary_output(raw_output: str) -> str:
    """Clean model outputs, stripping reasoning tags, markdown fences, echoed prompts, and planning babble."""
    if not raw_output:
        return ""
    text = str(raw_output).strip()
    text = re.sub(r"<think>[\s\S]*?</think>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^```(?:json|markdown)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        text = text[1:-1].strip()
    return text.strip()

_nim_use_unverified_ssl = False

async def Nvidia_API_ASYNC(promptMsg, token_rotator: NIMKeyManager = None, max_attempts: int = None, session: aiohttp.ClientSession = None):
    global _nim_use_unverified_ssl
    system_instruction = "You are a professional website information extractor and summarizer."
    if max_attempts is None:
        tt = getattr(token_rotator, "total_tokens", None)
        max_attempts = int(tt) * 2 if isinstance(tt, (int, float)) else 5

    payload = {
        "model": Nvidia_Base_Model,
        "messages": [
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": str(promptMsg)}
        ],
        "max_tokens": 1000,
        "temperature": 0.0,
        "stream": False
    }

    # Store the original user content for context-truncation retries
    original_user_content = str(promptMsg)

    ssl_ctx = False if _nim_use_unverified_ssl else get_nim_ssl_context()
    timeout = aiohttp.ClientTimeout(total=130.0)

    attempt = 0
    while attempt < max_attempts and (token_rotator is None or token_rotator.keys):
        attempt += 1

        # Get key with cooldown awareness
        if token_rotator:
            api_key, wait_time = await token_rotator.get_next_key_async()
            if wait_time > 0:
                logging.warning(f"All {len(token_rotator.keys)} NIM keys cooling down. Backing off {wait_time:.1f}s...")
                await asyncio.sleep(min(wait_time, 15.0))
        else:
            api_key = NIM_HARDCODED_KEYS[0] if NIM_HARDCODED_KEYS else ""

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        close_sess = False
        active_sess = session
        if active_sess is None:
            active_sess = aiohttp.ClientSession()
            close_sess = True
        try:
            async with active_sess.post(Nvidia_Endpoint, headers=headers, json=payload, timeout=timeout, ssl=ssl_ctx) as resp:

                    # ?? 200 OK ??
                    if resp.status == 200:
                        data = await resp.json()
                        choices = data.get("choices", [])
                        if choices:
                            msg = choices[0].get("message", {})
                            content = msg.get("content") or msg.get("reasoning_content", "")
                            cleaned = sanitize_summary_output(content)
                            if cleaned and "below is the website extraction" not in cleaned.lower():
                                return cleaned
                        # Empty/refused response ? retry with same temperature (always 0.0)
                        logging.warning(f"Empty/incomplete NIM response (attempt {attempt}/{max_attempts}), key ...{api_key[-6:]}")
                        await asyncio.sleep(0.5)

                    # ?? 401/403 Unauthorized / Forbidden ??
                    elif resp.status in (401, 403):
                        if token_rotator:
                            token_rotator.mark_invalid(api_key)
                        await asyncio.sleep(0.1)

                    # ?? 429 Rate Limited ??
                    elif resp.status == 429:
                        if token_rotator:
                            token_rotator.mark_rate_limited(api_key, cooldown_seconds=25.0)
                        logging.info(f"NIM 429 rate limit on key ...{api_key[-6:]}. Rotating (attempt {attempt})...")
                        await asyncio.sleep(0.3)

                    # ?? 400 Bad Request (context length overflow) ??
                    elif resp.status == 400:
                        resp_text = await resp.text()
                        logging.warning(f"NIM 400 Bad Request: {resp_text[:120]}. Shortening context and retrying...")
                        shorter = original_user_content[:15000] if len(original_user_content) > 15000 else original_user_content[:5000]
                        payload["messages"][1]["content"] = shorter
                        await asyncio.sleep(0.5)

                    # ?? 404 Not Found (account deactivated / model endpoint issue) ??
                    elif resp.status == 404:
                        resp_text = await resp.text()
                        logging.warning(f"NIM 404 on key ...{api_key[-6:]}: {resp_text[:120]}. Deactivating key...")
                        if token_rotator:
                            token_rotator.mark_invalid(api_key)
                        await asyncio.sleep(0.1)

                    # ?? 5xx Server Errors ??
                    elif resp.status in (500, 502, 503, 504):
                        backoff = min(15.0, 1.5 ** min(attempt, 4))
                        logging.warning(f"NIM server error {resp.status} on key ...{api_key[-6:]}. Backing off {backoff:.1f}s...")
                        await asyncio.sleep(backoff)

                    # ?? Other status codes ??
                    else:
                        resp_text = await resp.text()
                        logging.warning(f"NIM API {resp.status} on key ...{api_key[-6:]}: {resp_text[:120]}. Retrying...")
                        await asyncio.sleep(1.0)

        except aiohttp.ClientConnectorCertificateError:
            if not _nim_use_unverified_ssl:
                _nim_use_unverified_ssl = True
                ssl_ctx = False
                logging.warning("NIM SSL certificate verification failed. Auto-switching to unverified SSL mode.")
                await asyncio.sleep(0.5)
                continue
            backoff = min(10.0, 1.0 + attempt * 0.5)
            logging.warning(f"NIM SSL error (attempt {attempt}). Backing off {backoff:.1f}s...")
            await asyncio.sleep(backoff)

        except asyncio.TimeoutError:
            logging.warning(f"NIM API Timeout (attempt {attempt}/{max_attempts}) on key ...{api_key[-6:]}")
            await asyncio.sleep(min(5.0, 1.0 + attempt))

        except Exception as e:
            err_str = str(e).lower()
            if "too many open files" in err_str or "errno 24" in err_str:
                logging.warning("OS file descriptor limit reached in NIM worker. Pausing 1.5s for socket cleanup...")
                await asyncio.sleep(1.5)

            backoff = min(10.0, 1.0 + attempt * 0.5)
            err_name = type(e).__name__
            err_msg = str(e).strip()
            err_desc = f"{err_name}: {err_msg}" if err_msg else err_name
            logging.warning(f"NIM network exception (attempt {attempt}): {err_desc}. Backing off {backoff:.1f}s...")
            await asyncio.sleep(backoff)

    logging.error(f"All {max_attempts} attempts failed for Nvidia NIM API call.")
    return ""


#*************************************** Gemini API Caller *************************************

def Gemini_API(promptMsg, Gemini_Token=GEMINI_TOKEN):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite:generateContent?key={Gemini_Token}"

    formData = {
        "contents": [
            {
                "parts": [
                    {
                        "text": promptMsg
                    }
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0
        }
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(formData))
        response.raise_for_status()
        json_response = response.json()

        answer = json_response.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        return answer

    except requests.exceptions.RequestException as e:
        print(f"Error calling Gemini API: {e}")
        return ""


async def Gemini_API_ASYNC_Live_And_Working(promptMsg, Gemini_Token=GEMINI_TOKEN):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite:generateContent?key={Gemini_Token}"

    formData = {
        "contents": [
            {
                "parts": [
                    {
                        "text": promptMsg
                    }
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0
        }
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=formData) as response:
                response.raise_for_status()
                json_response = await response.json()

                answer = json_response.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                return answer

    except aiohttp.ClientError as e:
        print(f"Error calling Gemini API: {e}")
        return ""

# async def Gemini_API_WAIT_AND_HOLD(promptMsg, Gemini_Token="YOUR_TOKEN", timeout_limit=60):
async def Gemini_API_ASYNC(promptMsg, Gemini_Token=GEMINI_TOKEN, timeout_limit=60, session: aiohttp.ClientSession = None):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite:generateContent?key={Gemini_Token}"

    formData = {
        "contents": [{"parts": [{"text": promptMsg}]}],
        "generationConfig": {"temperature": 0}
    }
    headers = {"Content-Type": "application/json"}

    # Define how long to wait for the API to complete
    timeout = aiohttp.ClientTimeout(total=timeout_limit)
    
    resultMap = {
        "output" : "",
        "input_Token" : "",
        "thinking_Token" : "",
        "cached_Token" : "",
        "output_Token" : "",
        "total_Token" : "",
        "model" : ""
    }

    close_sess = False
    active_sess = session
    if active_sess is None:
        active_sess = aiohttp.ClientSession(timeout=timeout)
        close_sess = True
    try:
        async with active_sess.post(url, headers=headers, json=formData, timeout=timeout) as response:
                if response.status == 200:
                    json_res = await response.json()

                    resultMap["output"]    = json_res.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    resultMap["input_Token"]    = json_res.get("usageMetadata", {}).get("promptTokenCount", "")
                    resultMap["thinking_Token"]    = json_res.get("usageMetadata", {}).get("thoughtsTokenCount", "")
                    resultMap["cached_Token"]    = json_res.get("usageMetadata", {}).get("cachedContentTokenCount", "")
                    resultMap["output_Token"]    = json_res.get("usageMetadata", {}).get("candidatesTokenCount", "")
                    resultMap["total_Token"]    = json_res.get("usageMetadata", {}).get("totalTokenCount", "")
                    resultMap["model"]    = json_res.get("modelVersion", "")

                    return resultMap #json_res.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                else:
                    print(f"? Call Error: {await response.text()}")
                    return resultMap #""


    except asyncio.TimeoutError:
        print(f"The process was held for {timeout_limit}s but the API timed out.")
        return "TIMEOUT_ERROR"
    except Exception as e:
        print(f"Unexpected error: {e}")
        return ""
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass
    

async def Gemini_API_WITH_CACHE(user_input_msg, Gemini_Token, cache_name, timeout_limit=60, session: aiohttp.ClientSession = None):
    # Model must match the one used to create the cache
    model_id = "gemini-3.1-flash-lite"
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_id}:generateContent?key={Gemini_Token}"

    formData = {
        "contents": [
            {
                "parts": [{"text": user_input_msg}],
                "role": "user"
            }
        ],
        "cachedContent": cache_name,
        "generationConfig": {"temperature": 0}
    }

    resultMap = {
        "output" : "",
        "input_Token" : "",
        "thinking_Token" : "",
        "cached_Token" : "",
        "output_Token" : "",
        "total_Token" : "",
        "model" : ""
    }

    headers = {"Content-Type": "application/json"}

    # Define how long to wait for the API to complete
    timeout = aiohttp.ClientTimeout(total=timeout_limit)
    
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # The 'await' here ensures the process holds until completion
        async with session.post(url, headers=headers, json=formData) as response:
            if response.status == 200:
                json_res = await response.json()

                resultMap["output"]    = json_res.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                resultMap["input_Token"]    = json_res.get("usageMetadata", {}).get("promptTokenCount", "")
                resultMap["thinking_Token"]    = json_res.get("usageMetadata", {}).get("thoughtsTokenCount", "")
                resultMap["cached_Token"]    = json_res.get("usageMetadata", {}).get("cachedContentTokenCount", "")
                resultMap["output_Token"]    = json_res.get("usageMetadata", {}).get("candidatesTokenCount", "")
                resultMap["total_Token"]    = json_res.get("usageMetadata", {}).get("totalTokenCount", "")
                resultMap["model"]    = json_res.get("modelVersion", "")

                return resultMap #json_res.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            else:
                print(f"? Call Error: {await response.text()}")
                return resultMap #""


#*************************************** Gemini API - Caching (Sync Version) *************************************

def Create_Daily_Cache_Non_ASYNC(Gemini_Token, system_instruction):
    # Model: gemini-3.1-flash-lite
    url = f"https://generativelanguage.googleapis.com/v1beta/cachedContents?key={Gemini_Token}"
    
    formData = {
        "model": "models/gemini-3.1-flash-lite",
        "contents": [
            {
                "parts": [{"text": system_instruction}],
                "role": "user"
            }
        ],
        "ttl": "86400s" # 24 hours
    }

    try:
        # Using requests.post instead of aiohttp
        response = requests.post(url, json=formData)
        res = response.json()

        if response.status_code == 200:
            print(f"? Daily Cache Created: {res['name']}")
            return res['name']
        else:
            print(f"? Cache Creation Failed: {res}")
            return None
            
    except Exception as e:
        print(f"? Request Error: {e}")
        return None

#*************************************** Gemini API - Caching *************************************

async def Create_Daily_Cache(Gemini_Token, system_instruction):
    # Model: gemini-1.5-flash-lite-preview-0514
    url = f"https://generativelanguage.googleapis.com/v1beta/cachedContents?key={Gemini_Token}"
    
    formData = {
        "model": "models/gemini-3.1-flash-lite",
        "contents": [
            {
                "parts": [{"text": system_instruction}],
                "role": "user"
            }
        ],
        "ttl": "86400s" #"14400s"  # 4 hours - Adjust this based on how long your script runs
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=formData) as response:
            res = await response.json()
            if response.status == 200:
                print(f"? Daily Cache Created: {res['name']}")
                return res['name']
            else:
                print(f"? Cache Creation Failed: {res}")
                return None

async def Delete_Cache(cache_name, Gemini_Token):
    url = f"https://generativelanguage.googleapis.com/v1beta/{cache_name}?key={Gemini_Token}"
    async with aiohttp.ClientSession() as session:
        async with session.delete(url) as response:
            if response.status == 200:
                print("?? Cache cleared successfully.")

async def Delete_All_Caches(cache_data, token):
    for sector, cache_id in cache_data.items():
        # Only attempt to delete if the cache_id is not None
        if cache_id:
            print(f"? Deleting cache for: {sector}...")
            await Delete_Cache(cache_id, token)
        else:
            print(f"? Skipping {sector} (No cache ID found)")

#*************************** Replace Response with Replacable charecter Data **************************** */


def Utility_Replace_Response(addableData, basePrompt, replacableChar):
    
    outputQuery = ""

    if addableData != "" and basePrompt != "" and replacableChar != "":
        outputQuery = basePrompt.replace(replacableChar, addableData)
    else:
        outputQuery = f"{basePrompt}\n\n{addableData}"

    return outputQuery




# ********************************** Local Run Config Cache ******************************************************
_RUN_CACHE = {
    "basic_config": None,
    "funnel_prompts": None,
    "feed_mappings": None,
    "nim_config": None,
    "cache_id_IF_Prediction": None,
    "token_IF_Prediction": None,
    "date_IF_Prediction": None,
    "status_IF_Prediction": None,
    "exp_date_IF_Prediction": None,
    "timestamp": 0
}

def load_or_refresh_run_config(gc, force_refresh=False):
    """
    Download and cache all Config Sheet data locally at the start of a run.
    Reuses cached prompts and feed mappings throughout the run, eliminating
    150+ redundant Google Sheet read calls and preventing 429 quota exhaustion.
    Includes automated 3x retry with backoff and instant fallback to local run_cache.json.
    """
    global _RUN_CACHE
    if not force_refresh and _RUN_CACHE.get("basic_config") is not None:
        return _RUN_CACHE

    dir_path = os.path.dirname(os.path.abspath(__file__))
    candidate_cache_files = [
        os.path.join(dir_path, "run_cache.json"),
        os.path.join(dir_path, ".run_cache.json"),
        os.path.join(os.path.dirname(dir_path), "run_cache.json"),
        os.path.join(os.path.dirname(dir_path), ".run_cache.json"),
    ]

    if not Config_Sheet_ID:
        logging.warning("Config_Sheet_ID is empty. Checking local disk cache fallback...")
        for cf in candidate_cache_files:
            if os.path.exists(cf):
                try:
                    with open(cf, "r") as f:
                        _RUN_CACHE = json.load(f)
                    return _RUN_CACHE
                except Exception:
                    pass
        return _RUN_CACHE

    logging.info(f"Fetching fresh Config Sheet data from {Config_Sheet_ID} (local run cache)...")
    
    success = False
    for attempt in range(3):
        try:
            cfg_sheet = open_sheet_with_fallback(gc, Config_Sheet_ID)
            
            # 1. Base_Prompt_FunnelWise (CF>>IF>>Feed)
            ws_bp = cfg_sheet.worksheet("Base_Prompt_FunnelWise (CF>>IF>>Feed)")
            bp_rows = ws_bp.get_all_values()
            
            replacable_charecter = bp_rows[0][3] if len(bp_rows) > 0 and len(bp_rows[0]) > 3 else "[Domain_Extraction]"
            Basic_Prompt_Summery = bp_rows[1][3] if len(bp_rows) > 1 and len(bp_rows[1]) > 3 else ""
            Basic_Prompt_IF_Pred = bp_rows[2][3] if len(bp_rows) > 2 and len(bp_rows[2]) > 3 else ""
            replacable_charecter_Dom = bp_rows[3][3] if len(bp_rows) > 3 and len(bp_rows[3]) > 3 else "[Extract_Domain]"

            cache_id_IF = bp_rows[2][5] if len(bp_rows) > 2 and len(bp_rows[2]) > 5 else ""
            token_IF = bp_rows[2][8] if len(bp_rows) > 2 and len(bp_rows[2]) > 8 else ""
            date_IF = bp_rows[2][11] if len(bp_rows) > 2 and len(bp_rows[2]) > 11 else ""
            status_IF = bp_rows[2][16] if len(bp_rows) > 2 and len(bp_rows[2]) > 16 else ""
            exp_date_IF = bp_rows[2][17] if len(bp_rows) > 2 and len(bp_rows[2]) > 17 else ""

            basic_config = {
                "Replacable_Charecter": replacable_charecter,
                "Replacable_Charecter_Domain": replacable_charecter_Dom,
                "Basic_Prompt_Summery": Basic_Prompt_Summery,
                "Basic_Prompt_IF_Predctn": Basic_Prompt_IF_Pred
            }

            funnel_id_map = {}
            funnel_name_map = {}
            feed_cache_id_map = {}
            feed_api_token_map = {}
            feed_model_map = {}
            feed_gen_date_map = {}
            feed_status_map = {}

            for row in bp_rows[7:]:
                if len(row) < 3:
                    continue
                f_name, f_num, status = row[0], row[1], row[2]
                if status == "Live":
                    funnel_name_map[f_name] = row
                    funnel_id_map[f_num] = row
                    if len(row) > 10:
                        feed_cache_id_map[f_name] = row[10]
                    if len(row) > 13:
                        feed_api_token_map[f_name] = row[13]
                    if len(row) > 15:
                        feed_model_map[f_name] = row[15]
                    if len(row) > 16:
                        feed_gen_date_map[f_name] = row[16]
                    if len(row) > 21:
                        feed_status_map[f_name] = row[21]

            funnel_prompts = {
                "FunnelIdPromptMap": funnel_id_map,
                "FunnelNamePromptMap": funnel_name_map,
                "Feed_cache_Id": feed_cache_id_map,
                "Feed_cache_Info_API_Token": feed_api_token_map,
                "Feed_cache_Info_Model": feed_model_map,
                "Feed_cache_Info_Gen_Date": feed_gen_date_map,
                "Feed_cache_Info_Final_Status": feed_status_map,
                "inputResult": []
            }

            # 2. Feed_Mapping_PF
            ws_feed = cfg_sheet.worksheet("Feed_Mapping_PF")
            feed_rows = ws_feed.get_all_values()
            feed_num_map = {}
            feed_name_map_dict = {}
            for row in feed_rows[3:]:
                if len(row) < 3:
                    continue
                number_key = f"{row[0]}$${row[1]}"
                name_key = f"{row[0]}$${row[2]}"
                feed_num_map[number_key] = row
                feed_name_map_dict[name_key] = row

            feed_mappings = {
                "FeedNumFunnelMap": feed_num_map,
                "FeedNameFunnelMap": feed_name_map_dict,
                "inputResult": []
            }

            # 3. NIM - Models & Keys (Direct from .env, sheet fetch skipped)
            tokens = [["", "", "", "", k] for k in settings.NIM_API_KEYS]
            nim_config = {
                "NIM_Model_To_Pick": settings.Nvidia_Base_Model,
                "tokenArray": tokens
            }

            _RUN_CACHE = {
                "basic_config": basic_config,
                "funnel_prompts": funnel_prompts,
                "feed_mappings": feed_mappings,
                "nim_config": nim_config,
                "cache_id_IF_Prediction": cache_id_IF,
                "token_IF_Prediction": token_IF,
                "date_IF_Prediction": date_IF,
                "status_IF_Prediction": status_IF,
                "exp_date_IF_Prediction": exp_date_IF,
                "timestamp": time.time()
            }

            for cf in [candidate_cache_files[0], candidate_cache_files[1]]:
                try:
                    with open(cf, "w") as f:
                        json.dump(_RUN_CACHE, f)
                except Exception:
                    pass

            logging.info("✅ Local run config cache initialized successfully.")
            success = True
            return _RUN_CACHE
        except Exception as e:
            logging.warning(f"Attempt {attempt+1}/3 to load Config Sheet from {Config_Sheet_ID} failed: {e}")
            if attempt < 2:
                time.sleep(3 * (attempt + 1))

    if not success:
        logging.warning("Failed to refresh Config Sheet from Google Sheets API. Checking fallback disk cache...")
        for cf in candidate_cache_files:
            if os.path.exists(cf):
                try:
                    with open(cf, "r") as f:
                        _RUN_CACHE = json.load(f)
                    logging.info(f"✅ Loaded run config from fallback local disk cache: {cf}")
                    return _RUN_CACHE
                except Exception as ce:
                    logging.error(f"Error reading local run cache from {cf}: {ce}")

    return _RUN_CACHE

def Return_Basic_Config_Info(gc):
    global _RUN_CACHE
    if _RUN_CACHE.get("basic_config"):
        return _RUN_CACHE["basic_config"]

    if gc is None:
        gc = get_gspread_client()

    replacable_charecter = "[Domain_Extraction]"
    replacable_charecter_Dom = "[Extract_Domain]"
    Basic_Prompt_Summery = ""
    Basic_Prompt_IF_Pred = ""

    try:
        if Config_Sheet_ID:
            sheet = open_sheet_with_fallback(gc, Config_Sheet_ID)
            worksheet = sheet.worksheet("Base_Prompt_FunnelWise (CF>>IF>>Feed)")
            replacable_charecter        = worksheet.acell("D1").value or replacable_charecter
            replacable_charecter_Dom    = worksheet.acell("D4").value or replacable_charecter_Dom
            Basic_Prompt_Summery        = worksheet.acell("D2").value or Basic_Prompt_Summery
            Basic_Prompt_IF_Pred        = worksheet.acell("D3").value or Basic_Prompt_IF_Pred
    except Exception as e:
        logging.warning(f"Failed to fetch config from Config_Sheet_ID ({Config_Sheet_ID}): {e}")

    resultMap = {
        "Replacable_Charecter": replacable_charecter,
        "Replacable_Charecter_Domain": replacable_charecter_Dom,
        "Basic_Prompt_Summery": Basic_Prompt_Summery,
        "Basic_Prompt_IF_Predctn": Basic_Prompt_IF_Pred
    }

    return resultMap

#********************************** Getting Config Sheet Data ************************************

def Return_Base_Prompt_FunnelWise(client, funnel_id="", funnel_name=""):
    global _RUN_CACHE
    if _RUN_CACHE.get("funnel_prompts"):
        cached = _RUN_CACHE["funnel_prompts"]
        res = dict(cached)
        res["inputResult"] = []
        if funnel_id and funnel_id in res["FunnelIdPromptMap"]:
            res["inputResult"] = res["FunnelIdPromptMap"][funnel_id]
        if funnel_name and funnel_name in res["FunnelNamePromptMap"]:
            res["inputResult"] = res["FunnelNamePromptMap"][funnel_name]
        return res

    gc = client
    result_map = {
        "FunnelIdPromptMap": {},
        "FunnelNamePromptMap": {},
        "inputResult": [],
        "Feed_cache_Id": {},
        "Validation_cache_Id": {},
        "Feed_cache_Info_API_Token": {},
        "Feed_cache_Info_Model": {},
        "Feed_cache_Info_Gen_Date": {},
        "Feed_cache_Info_Final_Status": {},
    }

    try:
        if not Config_Sheet_ID:
            return result_map
        sheet = open_sheet_with_fallback(gc, Config_Sheet_ID)
        worksheet = sheet.worksheet("Base_Prompt_FunnelWise (CF>>IF>>Feed)")

        last_row = len(worksheet.get_all_values())
        if last_row <= 7:
            return result_map

        base_prompt_database = worksheet.get(f"A8:{gspread.utils.rowcol_to_a1(last_row, worksheet.col_count)}")

        funnel_id_prompt_map = {}
        funnel_name_prompt_map = {}

        for row in base_prompt_database:
            funnelName = row[0]
            funnelNum  = row[1]
            funnelCached_Validation_Prompt  = row[8]
            funnelCached_Feed_Prompt  = row[9]
            funnelCached_Feed_Prompt_Cache_ID  = row[10]
            funnelCached_Feed_Prompt_API_Token  = row[13]
            funnelCached_Feed_Prompt_Model  = row[15]
            funnelCached_Feed_Prompt_Cache_Gen_Date  = row[16]
            funnelCached_Feed_Prompt_Cache_Final_Status  = row[21]

            if len(row) >= 3 and row[2] == "Live":
                if row[0] not in funnel_name_prompt_map:
                    funnel_name_prompt_map[funnelName] = row

                    if funnelName not in result_map["Feed_cache_Id"]:
                        result_map["Feed_cache_Id"][funnelName] = funnelCached_Feed_Prompt_Cache_ID
                        result_map["Feed_cache_Info_API_Token"][funnelName] = funnelCached_Feed_Prompt_API_Token 
                        result_map["Feed_cache_Info_Model"][funnelName] = funnelCached_Feed_Prompt_Model 
                        result_map["Feed_cache_Info_Gen_Date"][funnelName] = funnelCached_Feed_Prompt_Cache_Gen_Date 
                        result_map["Feed_cache_Info_Final_Status"][funnelName] = funnelCached_Feed_Prompt_Cache_Final_Status 

                if row[1] not in funnel_id_prompt_map:
                    funnel_id_prompt_map[row[1]] = row

        result_map["FunnelIdPromptMap"] = funnel_id_prompt_map
        result_map["FunnelNamePromptMap"] = funnel_name_prompt_map

        if funnel_id and funnel_id in funnel_id_prompt_map:
            result_map["inputResult"] = funnel_id_prompt_map[funnel_id]

        if funnel_name and funnel_name in funnel_name_prompt_map:
            result_map["inputResult"] = funnel_name_prompt_map[funnel_name]
    except Exception as e:
        logging.warning(f"Failed to fetch funnel prompts from Config_Sheet_ID ({Config_Sheet_ID}): {e}")

    return result_map


#********************************** Config - Return Funnel Feed Wise ************************************



def Return_Funnel_FeedWise(Client, feed_num="", feed_name=""):
    global _RUN_CACHE
    if _RUN_CACHE.get("feed_mappings"):
        cached = _RUN_CACHE["feed_mappings"]
        res = dict(cached)
        res["inputResult"] = []
        if feed_num and feed_num in res["FeedNumFunnelMap"]:
            res["inputResult"] = res["FeedNumFunnelMap"][feed_num]
        if feed_name and feed_name in res["FeedNameFunnelMap"]:
            res["inputResult"] = res["FeedNameFunnelMap"][feed_name]
        return res

    gc = Client
    result_map = {
        "FeedNumFunnelMap": {},
        "FeedNameFunnelMap": {},
        "inputResult": []
    }

    try:
        if not Config_Sheet_ID:
            return result_map
        sheet = open_sheet_with_fallback(gc, Config_Sheet_ID)
        worksheet = sheet.worksheet("Feed_Mapping_PF")

        last_row = len(worksheet.get_all_values())
        if last_row <= 3:
            return result_map

        feed_wise_database = worksheet.get(
            f"A4:{gspread.utils.rowcol_to_a1(last_row, worksheet.col_count)}"
        )

        feed_num_funnel_map = {}
        feed_name_funnel_map = {}

        for row in feed_wise_database:
            if len(row) < 3:
                continue

            number_key = f"{row[0]}$${row[1]}"
            name_key = f"{row[0]}$${row[2]}"

            if name_key not in feed_name_funnel_map:
                feed_name_funnel_map[name_key] = row
            if number_key not in feed_num_funnel_map:
                feed_num_funnel_map[number_key] = row

        result_map["FeedNumFunnelMap"] = feed_num_funnel_map
        result_map["FeedNameFunnelMap"] = feed_name_funnel_map

        if feed_num and feed_num in feed_num_funnel_map:
            result_map["inputResult"] = feed_num_funnel_map[feed_num]

        if feed_name and feed_name in feed_name_funnel_map:
            result_map["inputResult"] = feed_name_funnel_map[feed_name]
    except Exception as e:
        logging.warning(f"Failed to fetch feed mappings from Config_Sheet_ID ({Config_Sheet_ID}): {e}")

    return result_map


#********************************** Getting Config Sheet Data ************************************

def Utility_AI_Summery_Extraction(
    gc,
    Raw_Data,
    BasePromptForSummery,
    Gemini_Token=GEMINI_TOKEN
):

    # Getting configuration info
    config = Return_Basic_Config_Info(gc)
    Replacable_Char = config["Replacable_Charecter"]

    Summery_Extraction_Result = {
        "Summery_Result": "",
        "Base_Prompt": "",
        "Revised_Input": ""
    }

    try:
        # Replace Replacable Character
        inputData_Summery = Utility_Replace_Response(
            Raw_Data,
            BasePromptForSummery,
            Replacable_Char
        )

        Summery_Extraction_Result["Revised_Input"] = inputData_Summery
        Summery_Extraction_Result["Base_Prompt"] = BasePromptForSummery

        try:
            outputData_Summery = Gemini_API(inputData_Summery, Gemini_Token)
        except Exception as e:
            outputData_Summery = ""
            print("Inner Gemini API call failed:", e)

        if outputData_Summery:
            Summery_Extraction_Result["Summery_Result"] = outputData_Summery

        # Logging for debugging
        print("Config Summery - Revised Input -", inputData_Summery)
        print("Config Summery - Base Prompt -", BasePromptForSummery)
        print("Config Summery - Output -", outputData_Summery)

    except Exception as error:
        print("Issue is coming with this Summery result :-", error)

    return Summery_Extraction_Result


#**********************************  Config - Gemin IF prediction ************************************


def Utility_AI_IF_Prediction(gc, Domain_Summery, Replacable_Charecter=None, Base_Prompt_IF=None, Gemini_Token=GEMINI_TOKEN):

    # Get configuration if not passed explicitly
    if Replacable_Charecter is None or Base_Prompt_IF is None or Replacable_Charecter == "" or Base_Prompt_IF == "" :
        config = Return_Basic_Config_Info(gc)
        if Replacable_Charecter is None or Replacable_Charecter == "" :
            Replacable_Charecter = config['Replacable_Charecter']
        if Base_Prompt_IF is None or Base_Prompt_IF == "":
            Base_Prompt_IF = config['Basic_Prompt_IF_Predctn']

    sh = open_sheet_with_fallback(gc, CF_SHEET_ID)
    funnelDetails = sh.worksheet("Funnel_Details")
    funnelData = funnelDetails.get_all_values()[1:]  # Skip header row

    funnelNameMap = {"name": [], "id": []}
    funnelIdMap = {"name": [], "id": []}
    funnelNumMap = {"name": [], "id": []}

    for row in funnelData:
        name, fid, *_rest, num = row[0], row[1], *row[2:], row[8] if len(row) > 8 else ""

        if name not in funnelNameMap:
            funnelNameMap[name] = row
            funnelNameMap["name"].append(name)
            funnelNameMap["id"].append(fid)
        if fid not in funnelIdMap:
            funnelIdMap[fid] = row
            funnelIdMap["name"].append(name)
            funnelIdMap["id"].append(fid)
        if num not in funnelNumMap:
            funnelNumMap[num] = row
            funnelNumMap["name"].append(name)
            funnelNumMap["id"].append(fid)

    inputData_For_IF = Utility_Replace_Response(Domain_Summery, Base_Prompt_IF, Replacable_Charecter)

    try:
        outputData_IF = Gemini_API(inputData_For_IF, Gemini_Token)
        if not outputData_IF:
            return
    except Exception as e:
        logging.error(f"Issue is coming with this IF Prediction result: {e}")
        return

    outputData_IF = outputData_IF.strip()

    outputData_IF_Name = ""
    outputData_IF_ID = ""

    if outputData_IF in funnelNumMap:
        outputData_IF_Name = funnelNumMap[outputData_IF][0]
        outputData_IF_ID = funnelNumMap[outputData_IF][1]
    elif outputData_IF in funnelNameMap:
        outputData_IF_Name = funnelNameMap[outputData_IF][0]
        outputData_IF_ID = funnelNameMap[outputData_IF][1]
    elif outputData_IF in funnelIdMap:
        outputData_IF_Name = funnelIdMap[outputData_IF][0]
        outputData_IF_ID = funnelIdMap[outputData_IF][1]

    IF_Prediction_Result = {
        "IF_Result": outputData_IF,
        "Revised_Input": inputData_For_IF,
        "Base_Prompt": Base_Prompt_IF,
        "FinalIFName": outputData_IF_Name,
        "FinalIFID": outputData_IF_ID
    }

    # logging.info("Config IF Prediction - Input Data For IF - %s", inputData_For_IF)
    # logging.info("Config IF Prediction - Base Prompt For IF - %s", Base_Prompt_IF)
    # logging.info("Config IF Prediction - Output Result - %s", outputData_IF)
    # logging.info("Config IF Prediction - Funnel Name - %s", outputData_IF_Name)
    # logging.info("Config IF Prediction - Funnel ID - %s", outputData_IF_ID)

    return IF_Prediction_Result

#**********************************  Config - Gemin IF Validation ************************************


def Utility_AI_IF_Validtn_Prediction(
    gc,
    Domain_Summery,
    Replacable_Char=None,
    IF_Name_Output_Result="",
    IF_ID_Output_Result="",
    Gemini_Token=GEMINI_TOKEN
):
    # Default result object
    IF_Validtn_Prediction_Result = {
        "Validation_Result": "",
        "Base_Prompt_Validation": "",
        "Revised_Input_Validation": ""
    }

    # Configs
    basic_config = Return_Basic_Config_Info(gc)
    if Replacable_Char is None or Replacable_Char == "" :
        Replacable_Char = basic_config["Replacable_Charecter"]

    # Funnel Prompt Mappings
    ConfigFunnelPrompt = Return_Base_Prompt_FunnelWise(gc,IF_ID_Output_Result,IF_Name_Output_Result)
    ConfigFunnelIDPrompt = ConfigFunnelPrompt["FunnelIdPromptMap"]
    ConfigFunnelNamePrompt = ConfigFunnelPrompt["FunnelNamePromptMap"]

    basePromptFor_IF_Validation = ""
    inputData_For_IF_Validation = ""

    # print("IF_Name_Output_Result : ",IF_Name_Output_Result)

    # Decide prompt
    if IF_Name_Output_Result and IF_Name_Output_Result in ConfigFunnelNamePrompt:
        basePromptFor_IF_Validation = ConfigFunnelNamePrompt[IF_Name_Output_Result][4]
    elif IF_ID_Output_Result and IF_ID_Output_Result in ConfigFunnelIDPrompt:
        basePromptFor_IF_Validation = ConfigFunnelIDPrompt[IF_ID_Output_Result][4]

    # print("Base Prompt Validation : ",basePromptFor_IF_Validation)

    if basePromptFor_IF_Validation:
        IF_Validtn_Prediction_Result["Base_Prompt_Validation"] = basePromptFor_IF_Validation

        inputSumry_For_IF_Validation = Domain_Summery
        inputData_For_IF_Validation = Utility_Replace_Response(
            inputSumry_For_IF_Validation,
            basePromptFor_IF_Validation,
            Replacable_Char
        )
        IF_Validtn_Prediction_Result["Revised_Input_Validation"] = inputData_For_IF_Validation

        try:
            outputData_IF_Validation = Gemini_API(inputData_For_IF_Validation, Gemini_Token)
        except Exception as e:
            print("Issue is coming with this IF Prediction result :-", e)
            outputData_IF_Validation = ""

        IF_Validtn_Prediction_Result["Validation_Result"] = outputData_IF_Validation

    # Logging
    print("Config IF Validation - Base Prompt -", basePromptFor_IF_Validation)
    print("Config IF Validation - Input Data -", inputData_For_IF_Validation)
    print("Config IF Validation - Output -", IF_Validtn_Prediction_Result["Validation_Result"])

    return IF_Validtn_Prediction_Result


#**********************************  Config - Gemin Feed Prediction ************************************

def Utility_AI_Feed_Prediction(
    gc,
    Domain_Summery,
    Replacable_Char=None,
    IF_Name_Output_Result="",
    IF_ID_Output_Result="",
    Gemini_Token=GEMINI_TOKEN
):
    # Result template
    Feed_Prediction_Result = {
        "Feed_Result": "",
        "Revised_Input": "",
        "Base_Prompt": "",
        "FinalFeedName": "",
        "FinalFeedID": "",
        "FinalPubFunnelName": "",
        "FinalPubFunnelId": ""
    }

    # Configs
    basic_config = Return_Basic_Config_Info(gc)
    if Replacable_Char is None:
        Replacable_Char = basic_config["Replacable_Charecter"]

    ConfigFunnelPrompt = Return_Base_Prompt_FunnelWise(gc,IF_ID_Output_Result,IF_Name_Output_Result)
    ConfigFunnelIDPrompt = ConfigFunnelPrompt["FunnelIdPromptMap"]
    ConfigFunnelNamePrompt = ConfigFunnelPrompt["FunnelNamePromptMap"]

    ConfigFeedWiseFunnel = Return_Funnel_FeedWise(gc)
    ConfigFeedNumFunnelMap = ConfigFeedWiseFunnel["FeedNumFunnelMap"]
    ConfigFeedNameFunnelMap = ConfigFeedWiseFunnel["FeedNameFunnelMap"]

    # Determine base prompt
    basePromptFor_Feed = ""
    if IF_Name_Output_Result and IF_Name_Output_Result in ConfigFunnelNamePrompt:
        basePromptFor_Feed = ConfigFunnelNamePrompt[IF_Name_Output_Result][5]
    elif IF_ID_Output_Result and IF_ID_Output_Result in ConfigFunnelIDPrompt:
        basePromptFor_Feed = ConfigFunnelIDPrompt[IF_ID_Output_Result][5]

    # Prepare input and call Gemini
    inputSumry_For_Feed = Domain_Summery
    inputData_For_Feed = Utility_Replace_Response(inputSumry_For_Feed, basePromptFor_Feed, Replacable_Char)
    
    Feed_Prediction_Result["Revised_Input"] = inputData_For_Feed
    Feed_Prediction_Result["Base_Prompt"] = basePromptFor_Feed

    outputData_Feed = ""

    if basePromptFor_Feed:
        try:
            outputData_Feed = Gemini_API(inputData_For_Feed, Gemini_Token)

            if outputData_Feed :
                outputData_Feed = outputData_Feed.strip()
                Feed_Prediction_Result["Feed_Result"] = outputData_Feed 
        except Exception as e:
            print("Issue is coming with this IF Prediction result:", e)

    # Publish Funnel Info Mapping
    FinalFeedName = ""
    FinalFeedId = ""
    FinalPubFunnelName = ""
    FinalPubFunnelId = ""

    if outputData_Feed:
        outputData_Feed = outputData_Feed.strip()
        keyFeed = f"{IF_Name_Output_Result}$${outputData_Feed}"

        print("Key For Feed:", keyFeed)

        feed_data = ConfigFeedNameFunnelMap.get(keyFeed) or ConfigFeedNumFunnelMap.get(keyFeed)
        if feed_data:
            FinalFeedName = feed_data[2]
            FinalFeedId = feed_data[3]
            FinalPubFunnelName = feed_data[5]
            FinalPubFunnelId = feed_data[6]

        Feed_Prediction_Result.update({
            "FinalFeedName": FinalFeedName,
            "FinalFeedID": FinalFeedId,
            "FinalPubFunnelName": FinalPubFunnelName,
            "FinalPubFunnelId": FinalPubFunnelId
        })

    # Logging
    print("Config Feed Prediction - Revised Input:", inputData_For_Feed)
    print("Config Feed Prediction - Base Prompt:", basePromptFor_Feed)
    print("Config Feed Prediction - Output of Feed:", outputData_Feed)
    print("Config Feed Prediction - Feed Name:", FinalFeedName)
    print("Config Feed Prediction - Feed ID:", FinalFeedId)
    print("Config Feed Prediction - Publish Funnel Name:", FinalPubFunnelName)
    print("Config Feed Prediction - Publish Funnel ID:", FinalPubFunnelId)

    return Feed_Prediction_Result

#********************************** Domain Hashtag Addition **********************************************


def get_api_result(payload=None, api_url="https://platform.tracxn.com/api/2.2/domains", method='POST', token=Tracxn_API_Token):
    if token == '':
        print("Please pass the Token as it is mandatory for this project")
        return

    if method == '':
        method = 'POST'
    
    headers = {
        "X-Request-Source": "Discovery Team",
        "accessToken": token,
        "cache-control": "no-cache",
        "Content-Type": "application/json"
    }
    
    options = {
        'headers': headers,
        'allow_redirects': True
    }
    
    if payload is None:
        payload = {}
    
    if 'dataset' not in payload:
        payload['dataset'] = 'internal'
    
    if 'blank' not in payload:
        options['json'] = payload

    try:
        if method.upper() == 'POST':
            response = requests.post(api_url, **options)
        elif method.upper() == 'PUT':
            # print("Payload :- ",payload)
            response = requests.put(api_url, headers=headers, json=payload)  #put(url, headers=headers, json=data)   #put(api_url, **options)
        else:
            raise ValueError("Invalid HTTP method. Use 'POST' or 'PUT'.")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error in getting API result: {e}")
        return {'error': str(e)}


async def get_api_result_async(payload=None, api_url="https://platform.tracxn.com/api/2.2/domains", method='POST', token=Tracxn_API_Token, session: aiohttp.ClientSession = None):
    if token == '':
        print("Please pass the Token as it is mandatory for this project")
        return

    if method == '':
        method = 'POST'

    headers = {
        "X-Request-Source": "Discovery Team",
        "accessToken": token,
        "cache-control": "no-cache",
        "Content-Type": "application/json"
    }

    if payload is None:
        payload = {}
    
    if 'dataset' not in payload:
        payload['dataset'] = 'internal'

    close_sess = False
    active_sess = session
    if active_sess is None:
        active_sess = aiohttp.ClientSession()
        close_sess = True
    try:
        if method.upper() == 'POST':
            async with active_sess.post(api_url, json=payload, headers=headers) as response:
                response.raise_for_status()
                return await response.json()
        elif method.upper() == 'PUT':
            async with active_sess.put(api_url, json=payload, headers=headers) as response:
                response.raise_for_status()
                return await response.json()
        else:
            raise ValueError("Invalid HTTP method. Use 'POST' or 'PUT'.")
    except aiohttp.ClientError as e:
        print(f"Error in getting API result: {e}")
        return {'error': str(e)}
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass
    

#**************************** Tag Addition ***********************************

# @timeout(20)
def Domain_HashTag(domain, newtag='NO', token=Tracxn_API_Token):
    payload = {"filter": {"domain": [domain]}, "dataset": "internal"}
    api_url = "https://platform.tracxn.com/api/2.2/domains"


    response = get_api_result(payload,api_url,'POST',token)


    if response and response.get("result") and response["result"][0] is not None:
        result = response["result"][0]

    else :
        print("Error Response in Company API : ",response)
        return 'NO'

    hastags = []
    msg = "-"
    dp = ''
    domainprofileid = ''

    try:
        domainprofileid = result["domainProfileId"]
    except KeyError:
        dp = 'NO'

    # print('New Tag : ', newtag)

    if dp == 'NO' or newtag == 'NO':
        return 'NO'

    # if result.get("keywords") and result["keywords"].get("hashTags"):
    if result.get('keywords') and result['keywords'].get('hashTags') is not None: 
        hastags = result['keywords']['hashTags']
    
    # print("HashTag Present : ",hastags)

    if newtag in hastags:
        return 'YES'
    elif newtag != 'NO' and newtag not in hastags:  
        hastags.append(newtag)   #hastags.append(newtag)   #[newtag]
        # print("Modified HashTag to Add : ",hastags)        
        Payload = {"id": domainprofileid, "keywords": {"value": {"HASHTAGS": hastags}}, "dataset": "internal"}
        API_URL = f"https://platform.tracxn.com/data/entities/2.0/domain-profile?domain={domain}&tab=COMPANY_PROFILE"
#         print(API_URL)

        writestatus = get_api_result(Payload,API_URL,'PUT',token)

#         print('Tag writestatus', writestatus)

        if "errorCode" in writestatus and writestatus["errorCode"] is not None and writestatus["errorCode"] != 400000000 and writestatus["message"] is not None:
            return 'NO'
        else:
            return 'YES'

async def Domain_HashTag_async(domain, newtag='NO', token=Tracxn_API_Token, session: aiohttp.ClientSession = None):
    payload = {"filter": {"domain": [domain]}, "dataset": "internal"}
    api_url = "https://platform.tracxn.com/api/2.2/domains"

    response = await get_api_result_async(payload, api_url, 'POST', token, session=session)

    if response and response.get("result") and response["result"][0] is not None:
        result = response["result"][0]
    else:
        print("Error Response in Company API: ", response)
        return 'NO'

    hashtags = []
    msg = "-"
    dp = ''
    domainprofileid = ''

    try:
        domainprofileid = result["domainProfileId"]
    except KeyError:
        dp = 'NO'

    if dp == 'NO' or newtag == 'NO':
        return 'NO'

    if result.get('keywords') and result['keywords'].get('hashTags') is not None:
        hashtags = result['keywords']['hashTags']

    if newtag in hashtags:
        return 'YES'
    elif newtag != 'NO' and newtag not in hashtags:
        hashtags.append(newtag)
        Payload = {"id": domainprofileid, "keywords": {"value": {"HASHTAGS": hashtags}}, "dataset": "internal"}
        API_URL = f"https://platform.tracxn.com/data/entities/2.0/domain-profile?domain={domain}&tab=COMPANY_PROFILE"

        writestatus = await get_api_result_async(Payload, API_URL, 'PUT', token, session=session)

        if "errorCode" in writestatus and writestatus["errorCode"] is not None and writestatus["errorCode"] != 400000000 and writestatus["message"] is not None:
            return 'NO'
        else:
            return 'YES'


#**************************** Tag Remove ***********************************

async def Domain_HashTag_Remove_async(domain, remove_tag='NO', token=Tracxn_API_Token):
    payload = {"filter": {"domain": [domain]}, "dataset": "internal"}
    api_url = "https://platform.tracxn.com/api/2.2/domains"

    response = await get_api_result_async(payload, api_url, 'POST', token)

    if response and response.get("result") and response["result"][0] is not None:
        result = response["result"][0]
    else:
        print("? Error in company API response: ", response)
        return 'NO'

    hashtags = []
    dp = ''
    domain_profile_id = ''

    try:
        domain_profile_id = result["domainProfileId"]
    except KeyError:
        dp = 'NO'

    if dp == 'NO' or remove_tag == 'NO':
        return 'NO'

    if result.get('keywords') and result['keywords'].get('hashTags'):
        hashtags = result['keywords']['hashTags']

    print("? Current hashtags:", hashtags)

    if remove_tag in hashtags:
        new_hashtags = [h for h in hashtags if h != remove_tag]
        print("? New hashtags after removal:", new_hashtags)

        update_payload = {
            "id": domain_profile_id,
            "keywords": {"value": {"HASHTAGS": new_hashtags}},
            "dataset": "internal"
        }

        update_url = f"https://platform.tracxn.com/data/entities/2.0/domain-profile?domain={domain}&tab=COMPANY_PROFILE"
        write_status = await get_api_result_async(update_payload, update_url, 'PUT', token, session=session)

        if (
            "errorCode" in write_status and write_status["errorCode"] is not None
            and write_status["errorCode"] != 400000000
            and write_status.get("message") is not None
        ):
            return 'NO'
        else:
            return 'YES'
    else:
        print("?? Remove tag not found in hashtags.")
        return 'YES'


#*****************************************************************************
#************************** Movement - Domain Moving ***************************
#*****************************************************************************


# ******************************* Funnel Movement - Caller  **********************************

def WriteAPI_Domain_Funnel(domainProfileId, source_funnelID, filterGroupId,filterId,dest_Funnel, status, comments, token = Tracxn_API_Token):
    if domainProfileId == "" or source_funnelID == '' or dest_Funnel == '' or status == '':
        return 'FALSE'
  
    # time.sleep(0.5)
  
    retdata = 'FALSE'
    ret = ''
  
  
    filterGroupId  = filterGroupId
    filterId       = filterId
  
    if status == 'SNOOZED' or status == 'MARKED_IRRELEVANT' or status == 'MARKED_FOR_LATER':
        Domain_Funnel_Assign(domainProfileId, source_funnelID,filterGroupId,filterId, comments, token)
        time.sleep(0.5)
        ret = Change_Domain_Funnel_Status(domainProfileId, source_funnelID,filterGroupId,filterId, status, comments, token)
  
    if status == 'MOVED_OUT':
        Domain_Funnel_Assign(domainProfileId, source_funnelID,filterGroupId,filterId, comments, token)
        time.sleep(0.5)
        ret = Change_Domain_Funnel_Moved_Out(domainProfileId, source_funnelID,filterGroupId,filterId, dest_Funnel, comments, token)
          
    # if status == 'SENT_BACK':
    #     Domain_Funnel_Assign(domainProfileId, source_funnelID, filterGroupId,filterId, comments,token)
    #     time.sleep(0.5)
    #     ret = Change_Domain_Funnel_Send_Back(domainProfileId, source_funnelID, comments, token)
  
    if ret == 'YES':
        retdata = 'TRUE'
  
    # print('retdata', retdata)
    return retdata

async def WriteAPI_Domain_Funnel_async(domainProfileId, source_funnelID, filterGroupId, filterId, dest_Funnel, status, comments, token = Tracxn_API_Token, session: aiohttp.ClientSession = None):
    if domainProfileId == "" or source_funnelID == '' or dest_Funnel == '' or status == '':
        return 'FALSE'

    retdata = 'FALSE'
    ret = ''

    if status in ('SNOOZED', 'MARKED_IRRELEVANT', 'MARKED_FOR_LATER','ESCALATED'):
        await Domain_Funnel_Assign_async(domainProfileId, source_funnelID, filterGroupId, filterId, comments, token)
        await asyncio.sleep(0.5)
        ret = await Change_Domain_Funnel_Status_async(domainProfileId, source_funnelID, filterGroupId, filterId, status, comments, token)

    elif status == 'MOVED_OUT':
        await Domain_Funnel_Assign_async(domainProfileId, source_funnelID, filterGroupId, filterId, comments, token)
        await asyncio.sleep(0.5)
        ret = await Change_Domain_Funnel_Moved_Out_async(domainProfileId, source_funnelID, filterGroupId, filterId, dest_Funnel, comments, token)

    # elif status == 'SENT_BACK':
    #     await Domain_Funnel_Assign_async(domainProfileId, source_funnelID, filterGroupId, filterId, comments, token)
    #     await asyncio.sleep(0.5)
    #     ret = await Change_Domain_Funnel_Send_Back_async(domainProfileId, source_funnelID, comments, token)

    if ret == 'YES':
        retdata = 'TRUE'

    return retdata


# ********************* Funnel Movement - Status Change call ****************************

def Change_Domain_Funnel_Status(domainProfileId, currentfunnelID,filterGroupId,filterId, status, comments, token = Tracxn_API_Token):
    ret = "NO"

    if(status == 'SNOOZED') :
        ret = Domain_Snoozed(domainProfileId, currentfunnelID,filterGroupId,filterId, status, comments, token)

    if(status == 'MARKED_IRRELEVANT') :
        ret = Domain_MARKED_IRRELEVANT(domainProfileId, currentfunnelID,filterGroupId,filterId, status, comments, token)

    if(status == "ESCALATED") :
        ret = Domain_ESCALATED_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token)

    if(status == "MARKED_FOR_LATER") :
        ret = Domain_MARKED_FOR_LATER_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token)
        

    return ret

async def Change_Domain_Funnel_Status_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token = Tracxn_API_Token):
    ret = "NO"

    if status == 'SNOOZED':
        ret = await Domain_Snoozed_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token)

    if status == 'MARKED_IRRELEVANT':
        ret = await Domain_MARKED_IRRELEVANT_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token)

    if(status == "ESCALATED") :
        ret = await Domain_ESCALATED_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token)

    if(status == "MARKED_FOR_LATER") :
        ret = await Domain_MARKED_FOR_LATER_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token)

    return ret

# ************************* Funnel Movement - MOVED_OUT *************************

def Change_Domain_Funnel_Moved_Out(domainProfileId, currentfunnelID,filterGroupId,filterId, dest_Funnel, comments, token = Tracxn_API_Token):
    url = "https://platform.tracxn.com/data/funnel-action/move"
    headers = {"accessToken": token, "Content-Type": 'application/json', "X-Request-Source": '123'}
    data = {
    "funnelId": currentfunnelID ,
    "domainProfileId": domainProfileId ,
    "movedTo": [dest_Funnel],
    "comment": comments ,
    "sourceDetails": {
        "source": "Write API",
        "sourceData": {
            "view": "Card",
            "tab": "Funnel Filter Group",
            "tabData": {
                "id": filterGroupId,
                "funnelFilterId":  filterId
             }
        }
    }
}  
    time.sleep(1)
    response = requests.put(url, headers=headers, json=data)


    if response.status_code == 200:
        return "YES"
    else :
        return "NO"
  
async def Change_Domain_Funnel_Moved_Out_async(domainProfileId, currentfunnelID, filterGroupId, filterId, dest_Funnel, comments, token = Tracxn_API_Token):
    url = "https://platform.tracxn.com/data/funnel-action/move"
    headers = {
        "accessToken": token,
        "Content-Type": "application/json",
        "X-Request-Source": "123"
    }
    data = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "movedTo": [dest_Funnel],
        "comment": comments,
        "sourceDetails": {
            "source": "Write API"
            ,"sourceData": {
                "view": "Card",
                "tab": "Funnel Filter Group",
                "tabData": {
                    "id": filterGroupId,
                    "funnelFilterId": filterId
                }
            }
        }
    }

    # if filterGroupId and str(filterGroupId).strip():  # Check if filterGroupId is not None and not empty after stripping whitespace
    #     data["sourceDetails"]["sourceData"] = {  #Corrected to sourceDetails instead of data
    #         "view": "Card",
    #         "tab": "Funnel Filter Group",
    #         "tabData": {
    #             "id": filterGroupId,
    #             "funnelFilterId": filterId
    #         }
    #     }

    await asyncio.sleep(1)  # Replacing time.sleep(1) with async sleep

    async with aiohttp.ClientSession() as session:
        async with session.put(url, headers=headers, json=data) as response:
            if response.status == 200:
                return "YES"
            else:
                print(f"Error Response: {response.status}, {await response.text()}")
                return "NO"


# ******************************* Funnel Movement - SNOOZED **********************************

def Domain_Snoozed(domainProfileId, currentfunnelID,filterGroupId,filterId, status, comments, token = Tracxn_API_Token):
    headers = {"accessToken": token}

    url = "https://platform.tracxn.com/data/funnel-action/snooze"

    data = {
        "funnelId": currentfunnelID ,
        "domainProfileId": domainProfileId,
        "comment": comments ,
        "sourceDetails": {
            "source": "Write API",
            "sourceData": {
                "view": "Card",
                "tab": "Funnel Filter Group",
                "tabData": {
                    "id": filterGroupId,
                    "funnelFilterId": filterId
                }
             }
        },
        "snoozeMethod": {
            "type": "period",
            "value": "30"
        }
    } 
    
    response = requests.put(url, headers=headers, json=data)

    print("For Snoozed , Response Code : ",response.status_code)


    if response.status_code == 200:
        return "YES"
    else :
        return "NO"


async def Domain_Snoozed_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token = Tracxn_API_Token):
    headers = {
        "accessToken": Tracxn_API_Token,
        "Content-Type": "application/json"
    }

    url = "https://platform.tracxn.com/data/funnel-action/snooze"

    data = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
        "sourceDetails": {
            "source": "Write API"
            ,"sourceData": {
                "view": "Card",
                "tab": "Funnel Filter Group",
                "tabData": {
                    "id": filterGroupId,
                    "funnelFilterId": filterId
                }
            }
        },
        "snoozeMethod": {
            "type": "period",
            "value": "30"
        }
    }

    async with aiohttp.ClientSession() as session:
        async with session.put(url, headers=headers, json=data) as response:
            print("For Snoozed, Response Code:", response.status)
            if response.status == 200:
                return "YES"
            else:
                print(f"Error Response: {response.status}, {await response.text()}")
                return "NO"
            
# *********************** Funnel Movement - ESCALATED *************************


async def Domain_ESCALATED_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token):
    url = "https://platform.tracxn.com/data/funnel-action/escalate"

    # Build base payload
    payload = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
        "sourceDetails": {
            "source": "Write API",
            "sourceData": {}
        }
    }

    # Conditionally add filterGroupId and filterId
    if filterGroupId:
        payload["sourceDetails"]["sourceData"] = {
            "view": "Card",
            "tab": "Funnel Filter Group",
            "tabData": {
                "id": filterGroupId
            }
        }

        if filterId:
            payload["sourceDetails"]["sourceData"]["tabData"]["funnelFilterId"] = filterId

    headers = {
        "accessToken": Tracxn_API_Token,
        "Content-Type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.put(url, headers=headers, json=payload) as response:
                response_text = await response.text()
                if response.status == 200:
                    try:
                        msg = await response.json()
                        return 'YES'
                    except Exception as e:
                        print("JSON parse error:", e)
                        return 'NO'
                else:
                    print(f"Error {response.status}: {response_text}")
                    return 'NO'
        except Exception as e:
            print(f"Request failed: {e}")
            return 'NO'


  
# ****************** Funnel Movement - MARKED_FOR_LATER *******************


async def Domain_MARKED_FOR_LATER_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token):
    url = "https://platform.tracxn.com/data/funnel-action/mark-for-later"

    # Build base payload
    payload = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
        "sourceDetails": {
            "source": "Write API",
            "sourceData": {}
        }
    }

    # Conditionally add filterGroupId and filterId
    if filterGroupId:
        payload["sourceDetails"]["sourceData"] = {
            "view": "Card",
            "tab": "Funnel Filter Group",
            "tabData": {
                "id": filterGroupId
            }
        }

        if filterId:
            payload["sourceDetails"]["sourceData"]["tabData"]["funnelFilterId"] = filterId

    headers = {
        "accessToken": Tracxn_API_Token,
        "Content-Type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.put(url, headers=headers, json=payload) as response:
                response_text = await response.text()
                if response.status == 200:
                    try:
                        msg = await response.json()
                        return 'YES'
                    except Exception as e:
                        print("JSON parse error:", e)
                        return 'NO'
                else:
                    print(f"Error {response.status}: {response_text}")
                    return 'NO'
        except Exception as e:
            print(f"Request failed: {e}")
            return 'NO'



# *********************** Funnel Movement - MARKED_IRRELEVANT *****************

def Domain_MARKED_IRRELEVANT(domainProfileId, currentfunnelID,filterGroupId,filterId, status, comments, token):

    headers = {"accessToken": Tracxn_API_Token}

    url = "https://platform.tracxn.com/data/funnel-action/mark-irrelevant"

    data = {
        "funnelId": currentfunnelID ,
        "domainProfileId": domainProfileId,
        "comment": comments ,
        "sourceDetails": {
            "source": "Write API",
            "sourceData": {
                "view": "Card",
                "tab": "Funnel Filter Group",
                "tabData": {
                    "id": filterGroupId,
                    "funnelFilterId": filterId
                }
             }
        }
    }
  
    response = requests.put(url, headers=headers, json=data)

    if response.status_code == 200:
        return "YES"
    else :
        return "NO"
  
async def Domain_MARKED_IRRELEVANT_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token):
    headers = {
        "accessToken": Tracxn_API_Token,
        "Content-Type": "application/json"
    }

    url = "https://platform.tracxn.com/data/funnel-action/mark-irrelevant"

    data = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
        "sourceDetails": {
            "source": "Write API"
            ,"sourceData": {
                "view": "Card",
                "tab": "Funnel Filter Group",
                "tabData": {
                    "id": filterGroupId,
                    "funnelFilterId": filterId
                }
            }
        }
    }

    async with aiohttp.ClientSession() as session:
        async with session.put(url, headers=headers, json=data) as response:
            if response.status == 200:
                return "YES"
            else:
                print(f"Error Response: {response.status}, {await response.text()}")
                return "NO"


# *******************************  Funnel Movement - Assign  **********************************

def Domain_Funnel_Assign(domainProfileId, funnelID,filterGroupId,filterId, comments,token):

    filterGroupId = filterGroupId
    filterId      = filterId

    # print("Token : ",token)

    url = "https://platform.tracxn.com/data/funnel-action/force-assign"
    headers = {"accessToken": Tracxn_API_Token}
    data = {
    "funnelId": funnelID,
    "domainProfileId": domainProfileId ,
    "comment": comments,
    "sourceDetails": {
        "source": "Write API",
        "sourceData": {
            "view": "Card",
            "tab": "Funnel Filter Group",
            "tabData": {
                "id": filterGroupId,
                "funnelFilterId":  filterId
             }
        }
    }
}
    
    time.sleep(1)
    response = requests.put(url, headers=headers, json=data)

    if response.status_code == 200:
        return "YES"
    else :
        return "NO"

    # return response
    

async def Domain_Funnel_Assign_async(domainProfileId, funnelID, filterGroupId, filterId, comments, token):
    url = "https://platform.tracxn.com/data/funnel-action/force-assign"
    headers = {"accessToken": Tracxn_API_Token, "Content-Type": "application/json"}
    data = {
        "funnelId": funnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
        "sourceDetails": {
            "source": "Write API"
            ,"sourceData": {
                "view": "Card",
                "tab": "Funnel Filter Group",
                "tabData": {
                    "id": filterGroupId,
                    "funnelFilterId": filterId
                }
            }
        }
    }
    
    # if filterGroupId and str(filterGroupId).strip():  # Check if filterGroupId is not None and not empty after stripping whitespace
    #     data["sourceDetails"]["sourceData"] = {  #Corrected to sourceDetails instead of data
    #         "view": "Card",
    #         "tab": "Funnel Filter Group",
    #         "tabData": {
    #             "id": filterGroupId,
    #             "funnelFilterId": filterId
    #         }
    #     }
        
    await asyncio.sleep(1)  # async version of time.sleep(1)

    async with aiohttp.ClientSession() as session:
        async with session.put(url, headers=headers, json=data) as response:
            if response.status == 200:
                return "YES"
            else:
                print(f"Error Response: {response.status}, {await response.text()}")
                return "NO"




# ********************************** Data Pull - Config Sheet *******************************************************************

token = Tracxn_API_Token
comment = ""
sheet_data = []
funnel_data = []
basic_config = {"Replacable_Charecter": "[Domain_Extraction]", "Replacable_Charecter_Domain": "[Extract_Domain]", "Basic_Prompt_Summery": "", "Basic_Prompt_IF_Predctn": ""}
replacable_char = "[Domain_Extraction]"
replacable_char_Domain = "[Extract_Domain]"
summery_base_prompt = ""
if_pred_base_prompt = ""
if_pred_base_prompt_cache = ""
funnel_name_map = {"name": [], "id": []}
funnel_id_map = {"name": [], "id": []}
funnel_num_map = {"name": [], "id": []}
config_funnel_prompt = {"FunnelIdPromptMap": {}, "FunnelNamePromptMap": {}, "Feed_cache_Id": {}, "Feed_cache_Info_API_Token": {}, "Feed_cache_Info_Model": {}, "Feed_cache_Info_Gen_Date": {}, "Feed_cache_Info_Final_Status": {}}
config_funnel_id_prompt = {}
config_funnel_name_prompt = {}
config_funnel_name_Feed_Cached_prompt = {}
config_funnel_name_Feed_Cached_API_Token = {}
config_funnel_name_Feed_Cached_Model = {}
config_funnel_name_Feed_Cached_Gen_Date = {}
config_funnel_name_Feed_Cached_Final_Status = {}
config_funnel_name_Validation_Cached_prompt = {}
config_feed_wise_funnel = {"FeedNumFunnelMap": {}, "FeedNameFunnelMap": {}}
config_feed_num_funnel_map = {}
config_feed_name_funnel_map = {}

try:
    client = get_gspread_client_General()
    spreadsheet = open_sheet_with_fallback(client, CF_SHEET_ID)

    config = None
    try:
        if Config_Sheet_ID:
            Config_spreadsheet = open_sheet_with_fallback(client, Config_Sheet_ID)
            config = Config_spreadsheet.worksheet("Base_Prompt_FunnelWise (CF>>IF>>Feed)")
    except Exception as e:
        logging.warning(f"Could not open Config sheet {Config_Sheet_ID}: {e}")

    # Get sheets
    sheet = spreadsheet.worksheet("CF_IF_Engine_Database")
    funnel_details = spreadsheet.worksheet("Funnel_Details")
    backend = spreadsheet.worksheet("Backend")

    # Get values from sheets
    token = Tracxn_API_Token
    comment = backend.acell("B9").value

    # Get sheet data (adjust indexing)
    sheet_data = sheet.get_all_values()[3 : sheet.row_count]
    funnel_data = funnel_details.get_all_values()[1:]

    # Get config data
    basic_config = Return_Basic_Config_Info(client)
    replacable_char = basic_config["Replacable_Charecter"]
    replacable_char_Domain = basic_config["Replacable_Charecter_Domain"]
    summery_base_prompt = basic_config["Basic_Prompt_Summery"]

    if config:
        try:
            if_pred_base_prompt = config.acell("D3").value
        except Exception:
            if_pred_base_prompt = basic_config.get("Basic_Prompt_IF_Predctn", "")
        try:
            if_pred_base_prompt_cache = config.acell("E3").value
        except Exception:
            if_pred_base_prompt_cache = ""
    else:
        if_pred_base_prompt = basic_config.get("Basic_Prompt_IF_Predctn", "")
        if_pred_base_prompt_cache = ""

    for e in funnel_data:
        if e[0] not in funnel_name_map:
            funnel_name_map[e[0]] = e
            funnel_name_map["name"].append(e[0])
            funnel_name_map["id"].append(e[1])
        if e[1] not in funnel_id_map:
            funnel_id_map[e[1]] = e
            funnel_id_map["name"].append(e[0])
            funnel_id_map["id"].append(e[1])
        if e[8] not in funnel_num_map:
            funnel_num_map[e[8]] = e
            funnel_num_map["name"].append(e[0])
            funnel_num_map["id"].append(e[1])

    config_funnel_prompt = Return_Base_Prompt_FunnelWise(client)
    config_funnel_id_prompt = config_funnel_prompt["FunnelIdPromptMap"]
    config_funnel_name_prompt = config_funnel_prompt["FunnelNamePromptMap"]
    config_funnel_name_Feed_Cached_prompt = config_funnel_prompt["Feed_cache_Id"]
    config_funnel_name_Feed_Cached_API_Token = config_funnel_prompt["Feed_cache_Info_API_Token"]
    config_funnel_name_Feed_Cached_Model = config_funnel_prompt["Feed_cache_Info_Model"]
    config_funnel_name_Feed_Cached_Gen_Date = config_funnel_prompt["Feed_cache_Info_Gen_Date"]
    config_funnel_name_Feed_Cached_Final_Status = config_funnel_prompt["Feed_cache_Info_Final_Status"]
    config_funnel_name_Validation_Cached_prompt = config_funnel_prompt.get("Validation_cache_Id", {})

    config_feed_wise_funnel = Return_Funnel_FeedWise(client)
    config_feed_num_funnel_map = config_feed_wise_funnel["FeedNumFunnelMap"]
    config_feed_name_funnel_map = config_feed_wise_funnel["FeedNameFunnelMap"]
except Exception as _e:
    logging.info(f"Module-level sheet pre-fetch skipped (will load per-run): {_e}")



# ********************************** Model Engine Process -> Prediction + Movement ******************************************



async def Extract_AI_Engine_Output(row,raw_data_beat_soup,SHEET_ID, EXTRACTING_SHEET_NAME,cache_id_IF_Prediction,token_IF_Prediction,date_IF_Prediction,status_IF_Prediction,exp_date_IF_Prediction,  config_funnel_prompt,config_funnel_id_prompt,config_funnel_name_prompt,config_funnel_name_Feed_Cached_prompt,config_funnel_name_Feed_Cached_API_Token,config_funnel_name_Feed_Cached_Model,config_funnel_name_Feed_Cached_Gen_Date,config_funnel_name_Feed_Cached_Final_Status,token_rotator,mode="full",session=None,precomputed_summary=None):

    start_time = datetime.datetime.now()
    today_date = datetime.datetime.now()

    final_map = {}
    all_output_data = [] #Accumulate all output data
    batch_output_data = [] #Accumulate all output data

    summary_input_Token = ""
    summary_thought_Token = ""
    summary_cache_Token = ""
    summary_output_Token = ""
    summary_total_Token = ""
    summary_llm_model = ""

    output_data_if_input_Token = ""
    output_data_if_thought_Token = ""
    output_data_if_cache_Token = ""
    output_data_if_output_Token = ""
    output_data_if_total_Token = ""
    output_data_if_llm_model = ""

    validation_if_input_Token = ""
    validation_if_thought_Token = ""
    validation_if_cache_Token = ""
    validation_if_output_Token = ""
    validation_if_total_Token = ""
    validation_if_llm_model = ""

    output_data_feed_input_Token = ""
    output_data_feed_thought_Token = ""
    output_data_feed_cache_Token = ""
    output_data_feed_output_Token = ""
    output_data_feed_total_Token = ""
    output_data_feed_llm_model = ""

    domain = row[0]
    domain_profile_id = row[10]
    filter_group_id = row[4]
    filter_id = row[6]
    crawl_status = row[21]

    # print("Domain Name : ",domain)

    # Gemini output for - Summery

    # raw_data_beat_soup = row[31]
    raw_data_play_wright = row[32]
    raw_data_selenium = row[33]
    raw_data_google_api = row[34]
    raw_data_platform = row[35]

    final_raw_data = ""
    base_prompt_for_summery = row[38]

    # Check Col AF (raw_data_beat_soup) first; fallback to Col AL (row[37]) if empty
    if raw_data_beat_soup and safe_str(raw_data_beat_soup).strip():
        final_raw_data = safe_str(raw_data_beat_soup).strip()
    elif len(row) > 37 and safe_str(row[37]).strip():
        final_raw_data = safe_str(row[37]).strip()
    elif raw_data_play_wright and safe_str(raw_data_play_wright).strip():
        final_raw_data = safe_str(raw_data_play_wright).strip()
    elif raw_data_selenium and safe_str(raw_data_selenium).strip():
        final_raw_data = safe_str(raw_data_selenium).strip()
    elif raw_data_google_api and safe_str(raw_data_google_api).strip():
        final_raw_data = safe_str(raw_data_google_api).strip()
    elif raw_data_platform and safe_str(raw_data_platform).strip():
        final_raw_data = safe_str(raw_data_platform).strip()

    if len(final_raw_data) > 48000:
        final_raw_data = final_raw_data[:48000]

    if not base_prompt_for_summery:
        base_prompt_for_summery = summery_base_prompt
    else:
        base_prompt_for_summery = summery_base_prompt

    input_data_summery = final_raw_data  # sheet.getRange(i+1,1).getValue();
    output_data_summery = ""


    #Skip the Logic if domain is empty or finalRawData is empty or DONE
    # if not domain and len(row) >= 61 and row[61] == "DONE": # or not final_raw_data or row[61] == "DONE" or row[21] == "To be checked" or (row[44] and row[49]): #Check if domain exists or not,if not then skip to the next loop
    #     continue

    # logging.info(f"Started For :- {domain}")

    
    # ******************************* Gemini output for - Summery ***********************

    # Summary status: SUCCESS, NO_RAW_DATA, or FAILED (written to Col AN / 40)
    summary_status = ""

    # Smart auto-resume: check if NIM summary already exists in row (Col 41 / index 41 or index 40)
    existing_summary = ""
    if len(row) > 41 and safe_str(row[41]).strip() and safe_str(row[41]).strip() != safe_str(if_pred_base_prompt).strip():
        existing_summary = safe_str(row[41]).strip()
    elif len(row) > 40 and safe_str(row[40]).strip() and safe_str(row[40]).strip() != safe_str(base_prompt_for_summery).strip() and not safe_str(row[40]).strip().startswith("http"):
        existing_summary = safe_str(row[40]).strip()

    if precomputed_summary:
        output_data_summery = precomputed_summary
        summary_status = "SUCCESS"
        logging.info(f"Using precomputed cycle NIM summary for {domain} (length {len(output_data_summery)}).")
    elif existing_summary:
        output_data_summery = existing_summary
        summary_status = safe_str(row[39]).strip() if (len(row) > 39 and safe_str(row[39]).strip()) else "SUCCESS"
        logging.info(f"Smart auto-resume: reusing existing summary for {domain} (length {len(output_data_summery)}) with status {summary_status}")
    elif not final_raw_data:
        output_data_summery = ""
        summary_status = "NO_RAW_DATA"
        logging.info(f"No raw data found in Col AF or Col AL for {domain}. Marking summary status NO_RAW_DATA.")
    elif input_data_summery:
        input_data_summery = Utility_Replace_Response(input_data_summery, base_prompt_for_summery, replacable_char)
        input_data_summery = Utility_Replace_Response(domain, input_data_summery, replacable_char_Domain)

        try:
            # 1. Primary Attempt: NVIDIA API Call
            output_data_summery = await Nvidia_API_ASYNC(input_data_summery, token_rotator=token_rotator, session=session)
            
            # 2. Fallback Attempt: Gemini API Call if NVIDIA failed or returned empty result
            if not output_data_summery:
                # logging.warning(f"Nvidia API returned empty result for {domain}. Falling back to Gemini API...")
                print(f"Nvidia API returned empty result for {domain}. Falling back to Gemini API...")
                
                try:
                    gemini_res = await Gemini_API_ASYNC(input_data_summery, session=session)
                    
                    # Extract Gemini metadata and output
                    summary_input_Token = gemini_res.get("input_Token", "")
                    summary_thought_Token = gemini_res.get("thinking_Token", "")
                    summary_cache_Token = gemini_res.get("cached_Token", "")
                    summary_output_Token = gemini_res.get("output_Token", "")
                    summary_total_Token = gemini_res.get("total_Token", "")
                    summary_llm_model = gemini_res.get("model", "")
                    output_data_summery = gemini_res.get("output", "")

                except Exception as e:
                    logging.error(f"Gemini API (Summary Fallback) failed: {e}")
                    output_data_summery = ""
                    summary_input_Token = ""
                    summary_thought_Token = ""
                    summary_cache_Token = ""
                    summary_output_Token = ""
                    summary_total_Token = ""
                    summary_llm_model = ""

        except Exception as error:
            logging.error(f"Issue during Summary extraction pipeline for domain {domain}: {error}")
            output_data_summery = ""

        if output_data_summery and output_data_summery.strip():
            summary_status = "SUCCESS"
        else:
            summary_status = "FAILED"

    if mode == "phase2":
        output_data = [
            final_raw_data, base_prompt_for_summery, summary_status, output_data_summery,
            "", "", "", "", "",
            "", "", "", "", "", "", "",
            "", "", "", "", "", "", "", "", "", "", "",
            summary_input_Token, summary_thought_Token, summary_cache_Token, summary_output_Token, summary_total_Token, summary_llm_model,
            *[""] * 18
        ]
        return output_data

    # logging.info("First Gemini Result Feed - ")# + output_data_summery)

# **************************************************************************************************************
# ************************************* Engine For CF >> IF >> PF **********************************************
# **************************************************************************************************************

    input_data_for_if = ""
    base_prompt_for_if = ""
    output_data_if = ""
    output_data_if_name = ""
    output_data_if_id = ""

    input_data_for_if_validation = ""
    input_sumry_for_if_validation = ""
    base_prompt_for_if_validation = ""
    output_data_if_validation = ""

    input_sumry_for_feed = ""
    base_prompt_for_feed = ""
    input_data_for_feed = ""
    output_data_feed = ""

    final_feed_name = ""
    final_feed_id = ""
    final_pub_funnel_name = ""
    final_pub_funnel_id = ""

    if EXTRACTING_SHEET_NAME != IF_EXTRACTING_SHEET_NAME:

        # ******************************* Gemini output for - IF ***********************
        input_data_for_if = output_data_summery  # Summery Input will be the Input for Second Query
        base_prompt_for_if = row[41]
        output_data_if = ""
        output_data_if_name = ""
        output_data_if_id = ""

        if len(row) > 44 and safe_str(row[44]).strip():
            output_data_if = safe_str(row[44]).strip()
            if len(row) > 45 and safe_str(row[45]).strip():
                output_data_if_name = safe_str(row[45]).strip()
            if len(row) > 46 and safe_str(row[46]).strip():
                output_data_if_id = safe_str(row[46]).strip()
            logging.info(f"Smart resume: reusing existing IF prediction for {domain}: {output_data_if}")
        elif output_data_summery:
            if not base_prompt_for_if:
                base_prompt_for_if = if_pred_base_prompt
            else:
                base_prompt_for_if = if_pred_base_prompt

            input_data_for_if = Utility_Replace_Response(input_data_for_if, base_prompt_for_if, replacable_char)
            input_data_for_if = Utility_Replace_Response(domain, input_data_for_if, replacable_char_Domain)

            try:
                try:
                    output_data_summery  = "Domain URL : " + domain + "\n" + "\n" + "Company Description: " + "\n" + output_data_summery
                    
                    # output_data_if = await Gemini_API_WITH_CACHE Gemini_API_ASYNC(input_data_for_if)
                    logging.debug(f"IF Prediction cache check for {domain}: token_match={GEMINI_TOKEN == token_IF_Prediction}")

                    if GEMINI_TOKEN == token_IF_Prediction and clean_date(date_IF_Prediction) in valid_dates and clean_date(exp_date_IF_Prediction) and status_IF_Prediction == "Sucess":
                        try:
                            output_data_if = await Gemini_API_WITH_CACHE(output_data_summery, GEMINI_TOKEN, cache_id_IF_Prediction, session=session)
                        except Exception:
                            output_data_if = await Gemini_API_ASYNC(input_data_for_if, GEMINI_TOKEN, session=session)
                    else:
                        output_data_if = await Gemini_API_ASYNC(input_data_for_if, GEMINI_TOKEN, session=session)

                    # output_data_if = await Gemini_API_WITH_CACHE(output_data_summery,GEMINI_TOKEN,cache_id_IF_Prediction)
                    
                    output_data_if_input_Token = output_data_if.get("input_Token","")
                    output_data_if_thought_Token = output_data_if.get("thinking_Token","")
                    output_data_if_cache_Token = output_data_if.get("cached_Token","")
                    output_data_if_output_Token = output_data_if.get("output_Token","")
                    output_data_if_total_Token = output_data_if.get("total_Token","")
                    output_data_if_llm_model = output_data_if.get("model","")
                    output_data_if = output_data_if.get("output","")

                except Exception as e:
                    logging.error(f"Gemini API (IF) failed: {e}")
                    output_data_if = ""
                    output_data_if_input_Token = ""
                    output_data_if_thought_Token = ""
                    output_data_if_cache_Token = ""
                    output_data_if_output_Token = ""
                    output_data_if_total_Token = ""
                    output_data_if_llm_model = ""

            except Exception as error:
                logging.error(f"Issue is coming with this IF Prediction result: {error}")

            # logging.info(f"Second Gemini Result OUTPUT IF - {output_data_if}")

            # Config - Funnel Map
            if output_data_if:
                output_data_if = output_data_if.strip()

                if output_data_if in funnel_num_map:
                    output_data_if_name = funnel_num_map[output_data_if][0]
                    output_data_if_id = funnel_num_map[output_data_if][1]
                elif output_data_if in funnel_name_map:
                    output_data_if_name = funnel_name_map[output_data_if][0]
                    output_data_if_id = funnel_name_map[output_data_if][1]
                elif output_data_if in funnel_id_map:
                    output_data_if_name = funnel_id_map[output_data_if][0]
                    output_data_if_id = funnel_id_map[output_data_if][1]

        # logging.info(f"Second Gemini Result OUTPUT IF with Name - {output_data_if_name}")
        # logging.info(f"Second Gemini Result OUTPUT IF with ID - {output_data_if_id}")


        # ******************************* Gemini output for - IF VALIDATION ***********************

        # Gemini output for - Industry Validation
        input_data_for_if_validation = ""  # IF Output will be the Input for Second Query
        input_sumry_for_if_validation = ""  # IF Output will be the Input for Second Query
        base_prompt_for_if_validation = row[47]
        base_prompt_for_if_validation_cache_id = ""
        output_data_if_validation = ""

        if output_data_if_name and output_data_if_name in config_funnel_name_prompt:
            base_prompt_for_if_validation = config_funnel_name_prompt[output_data_if_name][4]
            
            base_prompt_for_if_validation_cache_id = config_funnel_name_Validation_Cached_prompt.get(output_data_if_name, "")

        elif output_data_if_id and output_data_if_id in config_funnel_id_prompt:
            base_prompt_for_if_validation = config_funnel_id_prompt[output_data_if_id][4]

        if base_prompt_for_if_validation:
            input_sumry_for_if_validation = output_data_summery
            input_data_for_if_validation = Utility_Replace_Response(input_sumry_for_if_validation,
                                                                    base_prompt_for_if_validation, replacable_char)

            # logging.info("Input For IF Validation :" + input_data_for_if_validation)

            try:
                try:
                    # output_data_summery  = "Domain URL : " + domain + "\n" + "\n" + "Company Description: " + "\n" + output_data_summery
                    # # output_data_if = await Gemini_API_WITH_CACHE Gemini_API_ASYNC(input_data_for_if)
                    # try :
                        # output_data_if = await Gemini_API_WITH_CACHE(output_data_summery,GEMINI_TOKEN,base_prompt_for_if_validation_cache_id)
                    # except :
                    #     print("**** LLM Feed entered in Normal Call, As Error Found in Cache ****")
                    #     output_data_feed = await Gemini_API_ASYNC(input_data_for_feed, GEMINI_TOKEN, session=session)

                    output_data_if_validation = "Yes" #await Gemini_API_ASYNC(input_data_for_if_validation)
                    # validation_if_input_Token = output_data_if_validation.get("input_Token","")
                    # validation_if_thought_Token = output_data_if_validation.get("thinking_Token","")
                    # validation_if_cache_Token = output_data_if_validation.get("cached_Token","")
                    # validation_if_output_Token = output_data_if_validation.get("output_Token","")
                    # validation_if_total_Token = output_data_if_validation.get("total_Token","")
                    # validation_if_llm_model = output_data_if_validation.get("model","")                  
                except Exception as e:
                    logging.error(f"Gemini API (IF Validation) failed: {e}")
                    output_data_if_validation = ""
                    validation_if_input_Token = ""
                    validation_if_thought_Token = ""
                    validation_if_cache_Token = ""
                    validation_if_output_Token = ""
                    validation_if_total_Token = ""
                    validation_if_llm_model = ""

            except Exception as error:
                logging.error(f"Issue is coming with this IF Prediction result: {error}")

        # logging.info(f"Third Gemini Result IF Validation - {output_data_if_validation}")



# **************************************************************************************************************
# ************************************* Engine For IF >> PF ****************************************************
# **************************************************************************************************************

    if EXTRACTING_SHEET_NAME == IF_EXTRACTING_SHEET_NAME :


        # ******************************* Gemini output for - IF VALIDATION ***********************

        # Gemini output for - Industry Validation
        input_data_for_if_validation = ""  # IF Output will be the Input for Second Query
        input_sumry_for_if_validation = ""  # IF Output will be the Input for Second Query
        base_prompt_for_if_validation = row[47]
        output_data_if_validation = ""

        input_data_for_if = output_data_summery  # Summery Input will be the Input for Second Query
        base_prompt_for_if = row[41]
        output_data_if = ""
        output_data_if_name = ""
        output_data_if_id = ""

        output_data_if_name = row[1]
        output_data_if_id = row[2]

        if output_data_if_name and output_data_if_name in config_funnel_name_prompt:
            base_prompt_for_if_validation = config_funnel_name_prompt[output_data_if_name][4]

        elif output_data_if_id and output_data_if_id in config_funnel_id_prompt:
            base_prompt_for_if_validation = config_funnel_id_prompt[output_data_if_id][4]

        if base_prompt_for_if_validation:
            input_sumry_for_if_validation = output_data_summery
            input_data_for_if_validation = Utility_Replace_Response(input_sumry_for_if_validation,base_prompt_for_if_validation, replacable_char)
            input_data_for_if_validation = Utility_Replace_Response(domain,input_data_for_if_validation, replacable_char_Domain)

            # logging.info("Input For IF Validation :" + input_data_for_if_validation)

            try:
                try:
                    output_data_if_validation = "Yes" #await Gemini_API_ASYNC(input_data_for_if_validation)
                except Exception as e:
                    logging.error(f"Gemini API (IF Validation) failed: {e}")
                    output_data_if_validation = ""

            except Exception as error:
                logging.error(f"Issue is coming with this IF Prediction result: {error}")

        # logging.info(f"Third Gemini Result IF Validation - {output_data_if_validation}")

        if "No" in output_data_if_validation :
            
            output_data_if_validation = "Different IF Found" #"Yes"   

            # ******************************* Gemini output for - IF ***********************

            # if output_data_summery:
            #     if not base_prompt_for_if:
            #         base_prompt_for_if = if_pred_base_prompt
            #     else:
            #         base_prompt_for_if = if_pred_base_prompt

            #     input_data_for_if = Utility_Replace_Response(input_data_for_if, base_prompt_for_if, replacable_char)
            #     input_data_for_if = Utility_Replace_Response(domain, input_data_for_if, replacable_char_Domain)

            #     try:
            #         try:
            #             output_data_if = await Gemini_API_ASYNC(input_data_for_if)
            #         except Exception as e:
            #             logging.error(f"Gemini API (IF) failed: {e}")
            #             output_data_if = ""

            #     except Exception as error:
            #         logging.error(f"Issue is coming with this IF Prediction result: {error}")

            #     # logging.info(f"Second Gemini Result OUTPUT IF - {output_data_if}")

            #     # Config - Funnel Map
            #     if output_data_if:
            #         output_data_if = output_data_if.strip()

            #         if output_data_if in funnel_num_map:
            #             output_data_if_name = funnel_num_map[output_data_if][0]
            #             output_data_if_id = funnel_num_map[output_data_if][1]
            #         elif output_data_if in funnel_name_map:
            #             output_data_if_name = funnel_name_map[output_data_if][0]
            #             output_data_if_id = funnel_name_map[output_data_if][1]
            #         elif output_data_if in funnel_id_map:
            #             output_data_if_name = funnel_id_map[output_data_if][0]
            #             output_data_if_id = funnel_id_map[output_data_if][1]
            
            # # logging.info(f"Second Gemini Result OUTPUT IF with Name - {output_data_if_name}")
            # # logging.info(f"Second Gemini Result OUTPUT IF with ID - {output_data_if_id}")



# **************************************************************************************************************
# ************************************* Engine For Integrated ****************************************************
# **************************************************************************************************************

    # ******************************* Gemini output for - Feed Output ***********************

    # Gemini output for - Feed
    input_data_for_feed = ""  # IF Output will be the Input for Second Query
    input_sumry_for_feed = ""
    base_prompt_for_feed = row[30]
    base_prompt_for_feed_cache_id = ""
    status_for_feed_cache_id = ""
    token_for_feed_cache_id = ""
    date_prompt_for_feed_cache_id = ""
    output_data_feed = ""

    if len(row) > 54 and safe_str(row[54]).strip():
        output_data_feed = safe_str(row[54]).strip()
        logging.info(f"Smart resume: reusing existing Feed prediction for {domain}: {output_data_feed}")
    elif output_data_if_validation and ((EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME and "Yes" in output_data_if_validation) or (EXTRACTING_SHEET_NAME == IF_EXTRACTING_SHEET_NAME and "Yes" in output_data_if_validation)): # and "Different IF Found" in output_data_if_validation  #outputData_IF_Validation.indexOf("Yes") >= 0


        if output_data_if_name and output_data_if_name in config_funnel_name_prompt:
            base_prompt_for_feed = config_funnel_name_prompt[output_data_if_name][5]
            print("output_data_if_name - ")
            print(output_data_if_name)
            base_prompt_for_feed_cache_id = config_funnel_name_Feed_Cached_prompt.get(output_data_if_name, "")
            status_for_feed_cache_id = config_funnel_name_Feed_Cached_Final_Status.get(output_data_if_name, "")
            token_for_feed_cache_id = config_funnel_name_Feed_Cached_API_Token.get(output_data_if_name, "")
            date_prompt_for_feed_cache_id = config_funnel_name_Feed_Cached_Gen_Date.get(output_data_if_name, "")

        elif output_data_if_id and output_data_if_id in config_funnel_id_prompt:
            base_prompt_for_feed = config_funnel_id_prompt[output_data_if_id][5]

            # base_prompt_for_feed = config_funnel_id_prompt[output_data_if_id][5]

        input_sumry_for_feed = output_data_summery
        input_data_for_feed = Utility_Replace_Response(input_sumry_for_feed, base_prompt_for_feed, replacable_char)
        input_data_for_feed = Utility_Replace_Response(domain, input_data_for_feed, replacable_char_Domain)

        input_sumry_for_feed = "Domain URL : " + domain + "\n" + "\n" + "Company Description: " + "\n" + input_sumry_for_feed

        # logging.info("Input For Feed Prediction :" + input_data_for_feed)

        print("base_prompt_for_feed_cache_id - ")
        print(base_prompt_for_feed_cache_id)
        
        try:
            try:
                # output_data_feed = Gemini_API(input_data_for_feed)
                if GEMINI_TOKEN == token_for_feed_cache_id and clean_date(date_prompt_for_feed_cache_id) in valid_dates and  status_for_feed_cache_id == "Sucess" :
                    try :
                        output_data_feed = await Gemini_API_WITH_CACHE(input_sumry_for_feed, GEMINI_TOKEN, base_prompt_for_feed_cache_id, session=session)
                    except :
                        print("**** LLM Feed entered in Normal Call, As Error Found in Cache ****")
                        output_data_feed = await Gemini_API_ASYNC(input_data_for_feed,GEMINI_TOKEN)

                else :
                    print("**** LLM Feed entered in Normal Call, As some mismatch ****")
                    output_data_feed = await Gemini_API_ASYNC(input_data_for_feed,GEMINI_TOKEN)

                output_data_feed_input_Token = output_data_feed.get("input_Token","")
                output_data_feed_thought_Token = output_data_feed.get("thinking_Token","")
                output_data_feed_cache_Token = output_data_feed.get("cached_Token","")
                output_data_feed_output_Token = output_data_feed.get("output_Token","")
                output_data_feed_total_Token = output_data_feed.get("total_Token","")
                output_data_feed_llm_model = output_data_feed.get("model","")
                output_data_feed = output_data_feed.get("output","")

                print("output_data_feed - ")
                print(output_data_feed)

            except Exception as e:
                logging.error(f"Gemini API (Feed) failed: {e}")
                output_data_feed = ""
                output_data_feed_input_Token = ""
                output_data_feed_thought_Token = ""
                output_data_feed_cache_Token = ""
                output_data_feed_output_Token = ""
                output_data_feed_total_Token = ""
                output_data_feed_llm_model = ""

        except Exception as error:
            logging.error(f"Issue is coming with this IF Prediction result: {error}")

    # logging.info(f"Fourth Gemini Result Feed - {output_data_feed}")

    # Config - Publish Funnel output
    final_feed_name = ""
    final_feed_id = ""
    final_pub_funnel_name = ""
    final_pub_funnel_id = ""

    if output_data_feed:
        output_data_feed = output_data_feed.strip()

        key_feed = output_data_if_name + "$$" + output_data_feed

        # print("Key For Feed :- " + key_feed)

        if key_feed in config_feed_name_funnel_map:
            final_feed_name = config_feed_name_funnel_map[key_feed][2]
            final_feed_id = config_feed_name_funnel_map[key_feed][3]
            final_pub_funnel_name = config_feed_name_funnel_map[key_feed][5]
            final_pub_funnel_id = config_feed_name_funnel_map[key_feed][6] if len(config_feed_name_funnel_map[key_feed]) > 6 and config_feed_name_funnel_map[key_feed][6] is not None else ""

        elif key_feed in config_feed_num_funnel_map:
            final_feed_name = config_feed_num_funnel_map[key_feed][2]
            final_feed_id = config_feed_num_funnel_map[key_feed][3]
            final_pub_funnel_name = config_feed_num_funnel_map[key_feed][5]
            final_pub_funnel_id = config_feed_num_funnel_map[key_feed][6]  if len(config_feed_num_funnel_map[key_feed]) > 6 and config_feed_num_funnel_map[key_feed][6] is not None else ""

        # logging.info("Output of Feed  :" + final_feed_name)
        # logging.info("Output of Publish Funnel  :" + final_pub_funnel_name)

# ***************************** Write API (Placeholder - implement your actual API calls) ******************************
    domain = row[0]
    source_funnel_id = row[2]
    industry_funnel_id = output_data_if_id
    cf_if_action = "MOVED_OUT"
    publish_funnel_id = final_pub_funnel_id
    if_pf_action = "MOVED_OUT"
    cf_if_status = safe_str(row[62]).strip() if len(row) > 62 else ""
    cf_if_time = safe_str(row[63]).strip() if len(row) > 63 else ""
    if_pf_status = safe_str(row[64]).strip() if len(row) > 64 else ""
    if_pf_time = safe_str(row[65]).strip() if len(row) > 65 else ""
    tag_status_if = safe_str(row[66]).strip() if len(row) > 66 else ""
    tag_status_pf = safe_str(row[67]).strip() if len(row) > 67 else ""
    tag_status_discovery = safe_str(row[69]).strip() if len(row) > 69 else ""
    tag_name_if = "bu_llm_industry_prediction"
    tag_name_pf = "bu_llm_feed_prediction"


    #********************************************* Write API - Industry Funnel ************************************************

    if mode not in ("phase2", "phase3"):
        if cf_if_status == "DONE":
            logging.info(f"Smart resume: CF->IF write API already completed for {domain}")
        elif EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME and output_data_if_validation and "Yes" not in output_data_if_validation:
                tag_status_if = await Domain_HashTag_async(domain, Wrong_Validated_IF_Tag_Name, token)  

                print("Validation Came NO with Tag Status : " ,tag_status_if)

                cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

                if cf_if_status == "TRUE":
                    cf_if_status = "DONE"
                elif source_funnel_id :
                    # logging.info(f"This Domain will be escalated as in ESCALATED, Result :- {output_data_if}")

                    # cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id,filter_id,source_funnel_id, "ESCALATED", comment, Wrong_Validation_COMMENT, session=session) 

                    cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id,filter_id,source_funnel_id, "MARKED_FOR_LATER", Wrong_Validation_COMMENT,token, session=session)  

                    cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

                    if cf_if_status == "TRUE":
                        cf_if_status = "DONE"

        if EXTRACTING_SHEET_NAME == IF_EXTRACTING_SHEET_NAME and output_data_if_validation == "Different IF Found" :
                tag_status_if = await Domain_HashTag_async(domain, Wrong_Validated_Feed_Tag_Name, token)  

                cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

                if cf_if_status == "TRUE":
                    cf_if_status = "DONE"
                elif source_funnel_id :
                    # logging.info(f"This Domain will be escalated as in ESCALATED, Result :- {output_data_if}")

                    cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, industry_funnel_id, filter_group_id,filter_id,industry_funnel_id, "MARKED_FOR_LATER", Wrong_Validation_COMMENT,token, session=session) #DiscoveryCentralFunctions.WriteAPI_Domain_Funnel_With_Tab(domain_profile_id, source_funnel_id,source_funnel_id, filter_group_id, filter_id, "ESCALATED", comment, token, 10)  #Need to define/implement
                    cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

                    if cf_if_status == "TRUE":
                        cf_if_status = "DONE"



        if (EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME and  "Yes" in output_data_if_validation) or (EXTRACTING_SHEET_NAME == IF_EXTRACTING_SHEET_NAME and output_data_if_validation != "Different IF Found" ) :

            if source_funnel_id and industry_funnel_id:
                # print("Domain For HashTag : ",domain)
                tag_status_if = await Domain_HashTag_async(domain, tag_name_if, token)  
                # DiscoveryCentralFunctions.Domain_HashTag(domain, tag_name_if, token) #Need to define/implement
                # logging.info(f"This Domain will Sucessfully Move to this Funnel :- {output_data_if_name}")

                cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id,filter_id,industry_funnel_id, "MOVED_OUT", comment, token, session=session)
                #DiscoveryCentralFunctions.WriteAPI_Domain_Funnel_With_Tab(domain_profile_id, source_funnel_id,industry_funnel_id, filter_group_id, filter_id, cf_if_action, comment, token, 10) #Need to define/implement
                cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

                if cf_if_status == "TRUE":
                    cf_if_status = "DONE"
            elif source_funnel_id and (not industry_funnel_id and (output_data_if == "22" or "MARKED_IRRELEVANT" in output_data_if)):
                # logging.info(f"This Domain will be Rejected as in MARKED_IRRELEVANT, Result :- {output_data_if}")

                cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id,filter_id,source_funnel_id, "MARKED_IRRELEVANT", comment, token, session=session)
                #DiscoveryCentralFunctions.WriteAPI_Domain_Funnel_With_Tab(domain_profile_id, source_funnel_id,source_funnel_id, filter_group_id, filter_id, "MARKED_IRRELEVANT", comment, token, 10) #Need to define/implement

                cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

                if cf_if_status == "TRUE":
                    cf_if_status = "DONE"
            elif source_funnel_id and (not industry_funnel_id and (output_data_if == "23" or "ESCALATED" in output_data_if)):
                # logging.info(f"This Domain will be escalated as in ESCALATED, Result :- {output_data_if}")

                cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id,filter_id,source_funnel_id, "ESCALATED", comment, token, session=session) #DiscoveryCentralFunctions.WriteAPI_Domain_Funnel_With_Tab(domain_profile_id, source_funnel_id,source_funnel_id, filter_group_id, filter_id, "ESCALATED", comment, token, 10)  #Need to define/implement
                cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

                if cf_if_status == "TRUE":
                    cf_if_status = "DONE"
            else:
                # print(f"This Domain will be Snoozed as in SNOOZED, Result :- {output_data_if}")
                cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id,filter_id,source_funnel_id, "SNOOZED", comment, token, session=session)
                #DiscoveryCentralFunctions.WriteAPI_Domain_Funnel_With_Tab(domain_profile_id, source_funnel_id,source_funnel_id, filter_group_id, filter_id, "SNOOZED", comment, token, 10)  #Need to define/implement
                cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

                if cf_if_status == "TRUE":
                    cf_if_status = "DONE"

        #********************************************* Write API - Publish Funnel ************************************************

        if if_pf_status == "DONE":
            logging.info(f"Smart resume: IF->PF write API already completed for {domain}")
        elif industry_funnel_id and publish_funnel_id:
            tag_status_pf = await Domain_HashTag_async(domain, tag_name_pf, token) 
            #DiscoveryCentralFunctions.Domain_HashTag(domain, tag_name_pf, token) #Need to define/implement

            if_pf_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, industry_funnel_id,filter_group_id, filter_id,publish_funnel_id,if_pf_action, comment, token, session=session) 
            #DiscoveryCentralFunctions.WriteAPI_Domain_Funnel_With_Tab(domain_profile_id, industry_funnel_id,publish_funnel_id, filter_group_id, filter_id, if_pf_action, comment, token, 10) #Need to define/implement
            if_pf_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

            if if_pf_status == "TRUE":
                if_pf_status = "DONE"

        # Add discovery processing hashtag only when Write API Status (Publish Funnel) is completed and marked "Done"
        tag_status_discovery = ""
        if if_pf_status == "DONE":
            tag_status_discovery = await Domain_HashTag_async(domain, Discovery_Processing_TAG_NAME, token, session=session)

    # inbetween_sheet.update_acell("B7", dstart + i) #Save point for next execution.

    #******************************************** Prepare Output ******************************************************************


    output_data = [final_raw_data, base_prompt_for_summery, summary_status, output_data_summery, base_prompt_for_if,
                    input_data_for_if, output_data_if, output_data_if_name, output_data_if_id,
                    input_sumry_for_if_validation, base_prompt_for_if_validation, input_data_for_if_validation,
                    output_data_if_validation, input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed,
                    output_data_feed, final_feed_name, final_feed_id, final_pub_funnel_name, final_pub_funnel_id,cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf, tag_status_discovery, summary_input_Token, summary_thought_Token, summary_cache_Token, summary_output_Token, summary_total_Token, summary_llm_model, output_data_if_input_Token, output_data_if_thought_Token, output_data_if_cache_Token, output_data_if_output_Token, output_data_if_total_Token, output_data_if_llm_model, validation_if_input_Token, validation_if_thought_Token, validation_if_cache_Token, validation_if_output_Token, validation_if_total_Token, validation_if_llm_model, output_data_feed_input_Token, output_data_feed_thought_Token, output_data_feed_cache_Token, output_data_feed_output_Token, output_data_feed_total_Token, output_data_feed_llm_model]


    final_map[domain] = [final_raw_data, base_prompt_for_summery, input_data_summery, output_data_summery, base_prompt_for_if,
                    input_data_for_if, output_data_if, output_data_if_name, output_data_if_id,
                    input_sumry_for_if_validation, base_prompt_for_if_validation, input_data_for_if_validation,
                    output_data_if_validation, input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed,
                    output_data_feed, final_feed_name, final_feed_id, final_pub_funnel_name, final_pub_funnel_id, "", "",
                    "", cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf,summary_input_Token, summary_thought_Token, summary_cache_Token, summary_output_Token, summary_total_Token, summary_llm_model, output_data_if_input_Token, output_data_if_thought_Token, output_data_if_cache_Token, output_data_if_output_Token, output_data_if_total_Token, output_data_if_llm_model, validation_if_input_Token, validation_if_thought_Token, validation_if_cache_Token, validation_if_output_Token, validation_if_total_Token, validation_if_llm_model, output_data_feed_input_Token, output_data_feed_thought_Token, output_data_feed_cache_Token, output_data_feed_output_Token, output_data_feed_total_Token, output_data_feed_llm_model]
    # if EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME :

    #     output_data = [final_raw_data, base_prompt_for_summery, input_data_summery, output_data_summery, base_prompt_for_if,
    #                     input_data_for_if, output_data_if, output_data_if_name, output_data_if_id,
    #                     input_sumry_for_if_validation, base_prompt_for_if_validation, input_data_for_if_validation,
    #                     output_data_if_validation, input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed,
    #                     output_data_feed, final_feed_name, final_feed_id, final_pub_funnel_name, final_pub_funnel_id,cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf]


    #     final_map[domain] = [final_raw_data, base_prompt_for_summery, input_data_summery, output_data_summery, base_prompt_for_if,
    #                     input_data_for_if, output_data_if, output_data_if_name, output_data_if_id,
    #                     input_sumry_for_if_validation, base_prompt_for_if_validation, input_data_for_if_validation,
    #                     output_data_if_validation, input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed,
    #                     output_data_feed, final_feed_name, final_feed_id, final_pub_funnel_name, final_pub_funnel_id, "", "",
    #                     "", cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf]
        
    # elif EXTRACTING_SHEET_NAME == IF_EXTRACTING_SHEET_NAME :

    #     output_data = [final_raw_data, base_prompt_for_summery, input_data_summery, output_data_summery, 
    #                    base_prompt_for_if,input_data_for_if, output_data_if, output_data_if_name, output_data_if_id,
    #                     input_sumry_for_if_validation, base_prompt_for_if_validation, input_data_for_if_validation,                       output_data_if_validation, 
    #                     input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed,
    #                     output_data_feed, final_feed_name, final_feed_id, final_pub_funnel_name, final_pub_funnel_id,
    #                     cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf]


    #     final_map[domain] = [final_raw_data, base_prompt_for_summery, input_data_summery, output_data_summery, base_prompt_for_if,
    #                     input_data_for_if, output_data_if, output_data_if_name, output_data_if_id,
    #                     input_sumry_for_if_validation, base_prompt_for_if_validation, input_data_for_if_validation,
    #                     output_data_if_validation, input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed,
    #                     output_data_feed, final_feed_name, final_feed_id, final_pub_funnel_name, final_pub_funnel_id, "", "",
    #                     "", cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf]

    
    return output_data




#**************************************************************************************************
async def Integrated_Process_domain(
    session, domain, row, client, semaphore, rowData, SHEET_ID, 
    EXTRACTING_SHEET_NAME, cache_id_IF_Prediction, token_IF_Prediction, 
    date_IF_Prediction, status_IF_Prediction, exp_date_IF_Prediction, 
    config_funnel_prompt, config_funnel_id_prompt, config_funnel_name_prompt, 
    config_funnel_name_Feed_Cached_prompt, config_funnel_name_Feed_Cached_API_Token, 
    config_funnel_name_Feed_Cached_Model, config_funnel_name_Feed_Cached_Gen_Date, 
    config_funnel_name_Feed_Cached_Final_Status, token_rotator,
    created_timestamp=None,
    mode="full",
    write_to_sheet=True,
    precomputed_summary=None
):
    async with semaphore:
        try:
            # Safe index check to prevent IndexError if rowData is shorter than expected
            min_len = 70
            if len(rowData) < min_len:
                rowData = rowData + [""] * (min_len - len(rowData))

            if is_5am_ist_cutoff():
                logging.warning(f"⏰ 5:00 AM IST cutoff reached before processing {domain}. Halting.")
                return None

            # 0. Local CSV Redundancy Cache Hydration & Direct Write Check
            rowData, can_direct_write = LOCAL_CACHE.hydrate_row_from_cache(domain, rowData)
            if can_direct_write and mode == "full":
                logging.info(f"✨ [Local CSV Cache Hit] {domain} is fully completed in local cache and missing in sheet. Writing directly to sheet!")
                write_data = LOCAL_CACHE.build_full_write_row(domain, rowData, is_cf_sheet=(EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME))
                if write_to_sheet:
                    await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [write_data], row, 20)
                return {
                    "tc_status": "SUCCESS",
                    "domain": domain,
                    "row": row,
                    "tc_job_id": rowData[23],
                    "rowData": rowData,
                    "write_data": write_data,
                    "start_col": 20
                }

            tc_job_id = safe_str(rowData[23])
            tc_job_gen_time = safe_str(rowData[24])
            tc_status = safe_str(rowData[25])
            tc_final_url = safe_str(rowData[26])
            tc_s3_html = safe_str(rowData[27])
            tc_s3_markdown = safe_str(rowData[28])
            tc_http_status = safe_str(rowData[29])
            tc_signals = safe_str(rowData[30])

            if created_timestamp is None and tc_job_gen_time:
                try:
                    created_timestamp = datetime.datetime.strptime(tc_job_gen_time, "%d/%b/%Y %H:%M:%S").timestamp()
                except Exception:
                    created_timestamp = time.time()
            elif created_timestamp is None:
                created_timestamp = time.time()

            scrappingBeautySoup = safe_str(rowData[31])
            existing_raw_data = scrappingBeautySoup or safe_str(rowData[37])
            home_body = ""

            if existing_raw_data:
                home_body = existing_raw_data
                status_code = ""
            else:
                tc_client = TechCrawlerClient(token=Tracxn_API_Token)

                # 1. Generate Job ID if missing
                if not tc_job_id and domain:
                    tc_job_id = await tc_client.generate_job_id(session, domain)
                    if tc_job_id:
                        tc_job_gen_time = datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                        created_timestamp = time.time()
                        tc_status = "PROCESSING"

                # 2. Track Job once if in progress
                if tc_job_id and (tc_status in ("", "PROCESSING") or not home_body):
                    track_res = await tc_client.track_job_id(session, tc_job_id)
                    tc_status = track_res.get("status") or tc_status
                    if track_res.get("final_url"):
                        tc_final_url = track_res["final_url"]
                    if track_res.get("s3_html"):
                        tc_s3_html = track_res["s3_html"]
                    if track_res.get("s3_markdown"):
                        tc_s3_markdown = track_res["s3_markdown"]
                    if track_res.get("http_status"):
                        tc_http_status = track_res["http_status"]
                    if track_res.get("signals"):
                        tc_signals = track_res["signals"]

                # 3. If Tech Crawler succeeded, fetch signed S3 Markdown BEFORE checking timeout!
                if tc_status == "SUCCESS" and tc_s3_markdown:
                    signed_url = await tc_client.get_signed_s3_url(session, tc_s3_markdown)
                    if signed_url:
                        home_body = await tc_client.fetch_raw_markdown(session, signed_url) or ""
                        if home_body:
                            scrappingBeautySoup = home_body

                # Check 3-hour timeout rule ONLY if still PROCESSING after real-time check
                is_timeout = (time.time() - created_timestamp) >= (3 * 3600)
                if is_timeout and tc_status == "PROCESSING":
                    logging.warning(
                        f"Tech Crawler job {tc_job_id} for {domain} has been PROCESSING for >= 3 hours "
                        f"({(time.time() - created_timestamp)/3600:.1f}h). Switching to Waterfall Scraper fallback."
                    )
                    tc_status = "FAILED"

                # If still PROCESSING after initial check, DO NOT call fallback scraper!
                if tc_status == "PROCESSING":
                    LOCAL_CACHE.save_record(domain, {
                        "tc_job_id": tc_job_id,
                        "tc_job_gen_time": tc_job_gen_time,
                        "tc_status": tc_status,
                        "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                    })
                    if write_to_sheet:
                        try:
                            await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [[tc_job_id, tc_job_gen_time, tc_status]], row, 24)
                        except Exception as ex:
                            logging.warning(f"Could not write intermediate PROCESSING state to sheet for {domain}: {ex}")

                    logging.info(f"Tech Crawler job {tc_job_id} for {domain} is PROCESSING. Skipping fallback scraper and yielding to background poller.")
                    return {
                        "tc_status": "PROCESSING",
                        "domain": domain,
                        "row": row,
                        "tc_job_id": tc_job_id,
                        "tc_job_gen_time": tc_job_gen_time,
                        "created_timestamp": created_timestamp,
                        "rowData": [
                            *rowData[:23], tc_job_id, tc_job_gen_time, tc_status, tc_final_url,
                            tc_s3_html, tc_s3_markdown, tc_http_status, tc_signals, scrappingBeautySoup,
                            *rowData[32:]
                        ],
                        "write_data": [tc_job_id, tc_job_gen_time, tc_status],
                        "start_col": 24
                    }

                # 3. If Tech Crawler succeeded, fetch signed S3 Markdown
                if tc_status == "SUCCESS" and tc_s3_markdown:
                    signed_url = await tc_client.get_signed_s3_url(session, tc_s3_markdown)
                    if signed_url:
                        home_body = await tc_client.fetch_raw_markdown(session, signed_url) or ""

                # 4. Fallback Rule: Switch to Waterfall Scraper ONLY if Tech Crawler FAILED or unable to scrape
                if not home_body and tc_status in ("FAILED", ""):
                    waterfall_scraper = WaterfallScraper()
                    home_body, scrape_method = await waterfall_scraper.scrape_domain(session, domain)
                    if home_body:
                        scrappingBeautySoup = home_body
                    logging.info(f"Fallback waterfall scraper for {domain} result: {scrape_method} (length: {len(home_body)})")

            body_results = home_body if home_body else ["Website is not working."]

            if mode == "phase1":
                phase1_data = [
                    tc_job_id, tc_job_gen_time, tc_status, tc_final_url,
                    tc_s3_html, tc_s3_markdown, tc_http_status, tc_signals,
                    scrappingBeautySoup
                ]
                LOCAL_CACHE.save_record(domain, {
                    "tc_job_id": tc_job_id,
                    "tc_job_gen_time": tc_job_gen_time,
                    "tc_status": tc_status,
                    "tc_final_url": tc_final_url,
                    "tc_s3_html": tc_s3_html,
                    "tc_s3_markdown": tc_s3_markdown,
                    "tc_http_status": tc_http_status,
                    "tc_signals": tc_signals,
                    "scrappingBeautySoup": scrappingBeautySoup,
                    "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                })
                if write_to_sheet:
                    await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [phase1_data], row, 24)
                return {
                    "tc_status": tc_status,
                    "domain": domain,
                    "row": row,
                    "tc_job_id": tc_job_id,
                    "rowData": [
                        *rowData[:23], tc_job_id, tc_job_gen_time, tc_status, tc_final_url,
                        tc_s3_html, tc_s3_markdown, tc_http_status, tc_signals, scrappingBeautySoup,
                        *rowData[32:]
                    ],
                    "body": body_results,
                    "write_data": phase1_data,
                    "start_col": 24
                }

            AI_Engine_Result_Array = []
            Domain = rowData[0]
            skip_done = False
            Final_Scrapped_Raw_data = rowData[37]

            domain_profile_id = rowData[10]
            filter_group_id = rowData[4]
            filter_id = rowData[6]
            crawl_status = rowData[21]

            enginePickedTime = safe_str(rowData[19])
            engineClearedTime = safe_str(rowData[20])
            engineClearedStatus = safe_str(rowData[21])
            engineTimeTaken = rowData[22]
            currentFunnelName = rowData[24]
            currentFunnelID = rowData[25]
            currentFunnelStatus = rowData[26]
            movementBy = rowData[27]
            movementTime = rowData[28]
            movementStatus = rowData[29]
            scrappingBeautySoup = safe_str(rowData[31])
            scrappingPlaywright = safe_str(rowData[32])
            scrappingSelenium = safe_str(rowData[33])
            scrappingGoogleAPI = safe_str(rowData[34])
            scrappingTracxnPF = safe_str(rowData[35])
            geminiRawDataInput = safe_str(rowData[37])
            geminiBasePromptSum = safe_str(rowData[38])
            geminiRevQuerySum = safe_str(rowData[39])

            # Initialize times safely
            if not enginePickedTime:
                picking_time_Format = datetime.datetime.now()
                enginePickedTime = picking_time_Format.strftime("%d/%b/%Y %H:%M:%S")
            else:
                try:
                    picking_time_Format = datetime.datetime.strptime(enginePickedTime, "%d/%b/%Y %H:%M:%S")
                except ValueError:
                    picking_time_Format = datetime.datetime.now()

            # Main row processing logic
            if rowData[20] == "" and rowData[21] != "DONE" and rowData[29] != "DONE":
                if not Domain:
                    skip_done = True
                    # FunnelRecheck = "Failed"
                else:
                    # FunnelRecheck = "Passed"
                    scraped_text_bs = body_results
                    if scraped_text_bs:
                        Final_Scrapped_Raw_data = scraped_text_bs
                        scrappingBeautySoup = safe_str(scraped_text_bs)

                    # if not Final_Scrapped_Raw_data:
                    #     status_code_pw, scraped_text_pw = scrape_page_playwright(domain)
                    #     if scraped_text_pw:
                    #         Final_Scrapped_Raw_data = scraped_text_pw
                    #         scrappingPlaywright = scraped_text_bs

                    # if not Final_Scrapped_Raw_data:
                    #     scraped_text_se = scrape_with_selenium(domain)
                    #     if scraped_text_se:
                    #         Final_Scrapped_Raw_data = scraped_text_se
                    #         scrappingSelenium = scraped_text_bs

                    if Final_Scrapped_Raw_data and not skip_done:
                        text = safe_str(Final_Scrapped_Raw_data).strip()
                        if text == CHECK_EXACT or len(text) < 100 or (any(cs in text for cs in CHECK_STRINGS) and len(text) < 300):
                            skip_done = True

                    if not skip_done:
                        Flag_Scrap_Failure = Mark_Error_Domains(Final_Scrapped_Raw_data)

                        if Flag_Scrap_Failure == "To be checked":
                            engineClearedStatus = "To be checked"
                        else:
                            engineClearedStatus = "DONE"

            # Truncate strings safely to prevent API overhead or Google Sheets character limits
            scrappingBeautySoup = scrappingBeautySoup[:49500]
            scrappingPlaywright = scrappingPlaywright[:49500]
            scrappingSelenium = scrappingSelenium[:49500]
            scrappingGoogleAPI = scrappingGoogleAPI[:49500]
            scrappingTracxnPF = scrappingTracxnPF[:49500]
            geminiRawDataInput = geminiRawDataInput[:49500]
            geminiRevQuerySum = geminiRevQuerySum[:49500]

            AI_Engine_Output_Result = await Extract_AI_Engine_Output(
                rowData, scrappingBeautySoup, SHEET_ID, EXTRACTING_SHEET_NAME,
                cache_id_IF_Prediction, token_IF_Prediction, date_IF_Prediction,
                status_IF_Prediction, exp_date_IF_Prediction, config_funnel_prompt,
                config_funnel_id_prompt, config_funnel_name_prompt,
                config_funnel_name_Feed_Cached_prompt, config_funnel_name_Feed_Cached_API_Token,
                config_funnel_name_Feed_Cached_Model, config_funnel_name_Feed_Cached_Gen_Date,
                config_funnel_name_Feed_Cached_Final_Status,token_rotator,
                mode=mode,
                session=session,
                precomputed_summary=precomputed_summary
            )

            # Unpack AI Engine Outputs (Col AN receives summary_status, Col AO receives output_data_summery)
            final_raw_data = AI_Engine_Output_Result[0]
            base_prompt_for_summery = "" #AI_Engine_Output_Result[1]
            summary_status = AI_Engine_Output_Result[2]
            output_data_summery = AI_Engine_Output_Result[3]

            if mode == "phase2":
                phase2_data = [summary_status, output_data_summery]
                LOCAL_CACHE.save_record(domain, {
                    "summary_status": summary_status,
                    "output_data_summery": output_data_summery,
                    "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                })
                if write_to_sheet:
                    await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [phase2_data], row, 40)
                return {
                    "tc_status": tc_status,
                    "domain": domain,
                    "row": row,
                    "summary_status": summary_status,
                    "summary": output_data_summery,
                    "rowData": rowData,
                    "write_data": phase2_data,
                    "start_col": 40
                }

            base_prompt_for_if = ""  # Col AP write back skipped per requirement
            input_data_for_if = AI_Engine_Output_Result[5]
            output_data_if = AI_Engine_Output_Result[6]
            output_data_if_name = AI_Engine_Output_Result[7]
            output_data_if_id = AI_Engine_Output_Result[8]

            input_sumry_for_if_validation = AI_Engine_Output_Result[9]
            base_prompt_for_if_validation = ""  # Col AV write back skipped per requirement
            input_data_for_if_validation = AI_Engine_Output_Result[11]
            output_data_if_validation = AI_Engine_Output_Result[12]

            input_sumry_for_feed = AI_Engine_Output_Result[13]
            base_prompt_for_feed = AI_Engine_Output_Result[14]
            input_data_for_feed = AI_Engine_Output_Result[15]
            output_data_feed = AI_Engine_Output_Result[16]

            final_feed_name = AI_Engine_Output_Result[17]
            final_feed_id = AI_Engine_Output_Result[18]
            final_pub_funnel_name = AI_Engine_Output_Result[19]
            final_pub_funnel_id = AI_Engine_Output_Result[20]

            cf_if_status = AI_Engine_Output_Result[21]
            cf_if_time = AI_Engine_Output_Result[22]
            if_pf_status = AI_Engine_Output_Result[23]
            if_pf_time = AI_Engine_Output_Result[24]
            tag_status_if = AI_Engine_Output_Result[25]
            tag_status_pf = AI_Engine_Output_Result[26]
            tag_status_discovery = AI_Engine_Output_Result[27] if len(AI_Engine_Output_Result) > 27 else ""
            discovery_bq = "yes" if str(tag_status_discovery).upper() in ("YES", "TRUE") else ""

            # Once write API (last step) is completed, update Col U with current date and time (format: 12/Oct/2026 15:00:14)
            if mode == "full":
                finishing_time_Format = datetime.datetime.now()
                engineClearedTime = finishing_time_Format.strftime("%d/%b/%Y %H:%M:%S")
                engineTimeTaken = (finishing_time_Format - picking_time_Format).total_seconds()

                if not engineClearedStatus:
                    engineClearedStatus = "To be checked"
            else:
                engineClearedTime = safe_str(rowData[20])
                engineTimeTaken = rowData[22] if len(rowData) > 22 else ""

            # Prepare sheet write array (Col AN receives summary_status, Col AO receives output_data_summery)
            if EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME:
                AI_Engine_Result_Array.append([
                    enginePickedTime, engineClearedTime, engineClearedStatus, engineTimeTaken, 
                    tc_job_id, tc_job_gen_time, tc_status, tc_final_url, tc_s3_html, tc_s3_markdown, 
                    tc_http_status, tc_signals, scrappingBeautySoup, scrappingPlaywright, scrappingSelenium, 
                    scrappingGoogleAPI, scrappingTracxnPF, rowData[36], "", base_prompt_for_summery, 
                    summary_status, output_data_summery, base_prompt_for_if, input_data_for_if, 
                    output_data_if, output_data_if_name, output_data_if_id, input_sumry_for_if_validation, 
                    base_prompt_for_if_validation, input_data_for_if_validation, output_data_if_validation, 
                    input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed, output_data_feed, 
                    final_feed_name, final_feed_id, final_pub_funnel_name, final_pub_funnel_id, "", "", "", 
                    cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf, "", discovery_bq
                ])
            else:
                AI_Engine_Result_Array.append([
                    enginePickedTime, engineClearedTime, engineClearedStatus, engineTimeTaken, 
                    tc_job_id, tc_job_gen_time, tc_status, tc_final_url, tc_s3_html, tc_s3_markdown, 
                    tc_http_status, tc_signals, scrappingBeautySoup, scrappingPlaywright, scrappingSelenium, 
                    scrappingGoogleAPI, scrappingTracxnPF, rowData[36], "", base_prompt_for_summery, 
                    summary_status, output_data_summery, base_prompt_for_if_validation, 
                    input_data_for_if_validation, output_data_if_validation, input_sumry_for_if_validation, 
                    base_prompt_for_if, input_data_for_if, output_data_if, output_data_if_name, 
                    output_data_if_id, input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed, 
                    output_data_feed, final_feed_name, final_feed_id, final_pub_funnel_name, 
                    final_pub_funnel_id, "", "", "", cf_if_status, cf_if_time, if_pf_status, 
                    if_pf_time, tag_status_if, tag_status_pf, "", discovery_bq
                ])

            # Save completed row to local CSV cache for redundancy & future runs
            LOCAL_CACHE.save_record(domain, {
                "enginePickedTime": enginePickedTime,
                "engineClearedTime": engineClearedTime,
                "engineClearedStatus": engineClearedStatus,
                "engineTimeTaken": engineTimeTaken,
                "tc_job_id": tc_job_id,
                "tc_job_gen_time": tc_job_gen_time,
                "tc_status": tc_status,
                "tc_final_url": tc_final_url,
                "tc_s3_html": tc_s3_html,
                "tc_s3_markdown": tc_s3_markdown,
                "tc_http_status": tc_http_status,
                "tc_signals": tc_signals,
                "scrappingBeautySoup": scrappingBeautySoup,
                "summary_status": summary_status,
                "output_data_summery": output_data_summery,
                "output_data_if": output_data_if,
                "output_data_if_name": output_data_if_name,
                "output_data_if_id": output_data_if_id,
                "output_data_if_validation": output_data_if_validation,
                "output_data_feed": output_data_feed,
                "final_feed_name": final_feed_name,
                "final_feed_id": final_feed_id,
                "final_pub_funnel_name": final_pub_funnel_name,
                "final_pub_funnel_id": final_pub_funnel_id,
                "cf_if_status": cf_if_status,
                "cf_if_time": cf_if_time,
                "if_pf_status": if_pf_status,
                "if_pf_time": if_pf_time,
                "tag_status_if": tag_status_if,
                "tag_status_pf": tag_status_pf,
                "discovery_bq": discovery_bq,
                "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
            })

            full_write_row = AI_Engine_Result_Array[0] if AI_Engine_Result_Array else []
            if write_to_sheet and full_write_row:
                await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [full_write_row], row, 20)

            return {
                "tc_status": tc_status,
                "domain": domain,
                "row": row,
                "tc_job_id": tc_job_id,
                "rowData": [
                    *rowData[:23], tc_job_id, tc_job_gen_time, tc_status, tc_final_url,
                    tc_s3_html, tc_s3_markdown, tc_http_status, tc_signals, scrappingBeautySoup,
                    *rowData[32:]
                ],
                "body": body_results,
                "write_data": full_write_row,
                "start_col": 20
            }

        except Exception as e:
            logging.error(f"Error processing {domain}: {e}")
            return None
        
async def reconcile_existing_tech_crawler_jobs(session, rows, client, SHEET_ID, sheet_name, start_row_num=4):
    """
    When resuming a run (especially if the script stopped >3 hours ago):
    Finds all rows that already have a tc_job_id but where scraping is not yet completed.
    First checks the status of all these job IDs with Tech Crawler API.
    For any job returning SUCCESS, retrieves the signed S3 markdown and updates the row/cache/sheet.
    Only genuinely non-successful cases remain to be picked up by the fallback scraper.
    """
    candidates = []
    for idx, r in enumerate(rows):
        if not r or not r[0]:
            continue
        min_len = 38
        if len(r) < min_len:
            r.extend([""] * (min_len - len(r)))
        tc_job_id = safe_str(r[23]).strip()
        tc_status = safe_str(r[25]).strip()
        scraped = safe_str(r[31]).strip()
        summary = safe_str(r[40]).strip() if len(r) > 40 else ""
        if tc_job_id and tc_status != "SUCCESS" and not scraped and not summary:
            candidates.append((start_row_num + idx, r))

    if not candidates:
        return

    logging.info(f"🔄 Resumption check: Reconciling status of {len(candidates)} existing Tech Crawler jobs before proceeding...")
    tc_client = TechCrawlerClient(token=Tracxn_API_Token)
    reconcile_sem = asyncio.Semaphore(25)

    async def _check_one_candidate(r_num, row_data):
        async with reconcile_sem:
            domain = row_data[0]
            job_id = row_data[23]
            try:
                track_res = await tc_client.track_job_id(session, job_id)
                status = track_res.get("status")
                if status == "SUCCESS" and track_res.get("s3_markdown"):
                    signed_url = await tc_client.get_signed_s3_url(session, track_res["s3_markdown"])
                    if signed_url:
                        md = await tc_client.fetch_raw_markdown(session, signed_url)
                        if md:
                            row_data[25] = "SUCCESS"
                            if track_res.get("final_url"): row_data[26] = track_res["final_url"]
                            if track_res.get("s3_html"): row_data[27] = track_res["s3_html"]
                            row_data[28] = track_res["s3_markdown"]
                            if track_res.get("http_status"): row_data[29] = str(track_res["http_status"])
                            if track_res.get("signals"): row_data[30] = str(track_res["signals"])
                            row_data[31] = md
                            LOCAL_CACHE.save_record(domain, {
                                "tc_job_id": job_id,
                                "tc_status": "SUCCESS",
                                "tc_final_url": row_data[26],
                                "tc_s3_html": row_data[27],
                                "tc_s3_markdown": row_data[28],
                                "tc_http_status": row_data[29],
                                "tc_signals": row_data[30],
                                "scrappingBeautySoup": md,
                                "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                            })
                            sheet_slice = [
                                row_data[23], row_data[24], row_data[25], row_data[26],
                                row_data[27], row_data[28], row_data[29], row_data[30],
                                row_data[31][:49500]
                            ]
                            return (r_num, sheet_slice)
                elif status == "FAILED":
                    row_data[25] = "FAILED"
            except Exception as e:
                logging.debug(f"Error checking candidate {job_id} for {domain}: {e}")
            return None

    tasks = [_check_one_candidate(r_num, r) for r_num, r in candidates]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    success_count = 0
    for res in results:
        if res and not isinstance(res, Exception):
            r_num, sheet_slice = res
            success_count += 1
            try:
                await write_sheet_data(client, SHEET_ID, sheet_name, [sheet_slice], r_num, 24)
            except Exception as we:
                logging.warning(f"Could not write reconciled Tech Crawler slice to sheet for row {r_num}: {we}")

    logging.info(f"✅ Reconciled {success_count}/{len(candidates)} existing Tech Crawler jobs to SUCCESS. Markdown harvested.")


async def Integrated_Extract_domains(
    client,
    SHEET_ID,
    EXTRACTING_SHEET_NAME,
    cache_id_IF_Prediction,
    token_IF_Prediction,
    date_IF_Prediction,
    status_IF_Prediction,
    exp_date_IF_Prediction,
    config_funnel_prompt,
    config_funnel_id_prompt,
    config_funnel_name_prompt,
    config_funnel_name_Feed_Cached_prompt,
    config_funnel_name_Feed_Cached_API_Token,
    config_funnel_name_Feed_Cached_Model,
    config_funnel_name_Feed_Cached_Gen_Date,
    config_funnel_name_Feed_Cached_Final_Status,
    token_rotator,
    start_row_num=4,
    mode="full",
):

    pf_data = await read_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, start_row=start_row_num)

    total_domains = len(pf_data)
    processed = 0
    success_count = 0
    fail_count = 0
    phase_title = f"Phase 1: Initial Crawl & Predictions ({mode})" if mode == "full" else f"Mode: {mode.upper()}"
    update_progress_file(processed, total_domains, success_count, fail_count, phase=phase_title, stage_details=f"Starting batch processing of {total_domains} rows...", pending_tc=0, mode=mode)

    semaphore = asyncio.Semaphore(MAX_WORKERS)
    pending_tech_crawler_jobs = []

    batch_size = BATCH_SIZE
    domains_batches = [
        pf_data[i : i + batch_size] for i in range(0, len(pf_data), batch_size)
    ]

    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=30,
        ttl_dns_cache=300
    )
    FLUSH_FILE = os.path.join(os.path.dirname(__file__), ".flush_requested")
    async with aiohttp.ClientSession(headers=mainheaders, connector=connector) as session:
        # Each 100-row batch automatically tracks its own existing Tech Crawler jobs and harvests S3 markdowns progressively in Integrated_Process_domain

        for batch_index, batch in enumerate(domains_batches):
            if is_5am_ist_cutoff():
                logging.warning("⏰ 5:00 AM IST daily cutoff reached! Halting execution so Apps Script can refresh daily domains.")
                break
            if os.path.exists(STOP_FILE):
                logging.info("Stop requested (.stop_requested found). Halting Integrated_Extract_domains.")
                break
            if is_pre_cutoff_window() or os.path.exists(FLUSH_FILE):
                logging.warning("⏰ 04:30 AM IST Pre-Cutoff Flush active! Halting new scraping ingestion to drain summarized domains.")
                if pending_tech_crawler_jobs:
                    BACKLOG_MANAGER.save_scraped_backlog([
                        {"domain": it.get("domain"), "raw_content": it.get("raw_content", ""), "tc_job_id": it.get("tc_job_id", "")}
                        for it in pending_tech_crawler_jobs if it.get("domain")
                    ])
                break

            await SYSTEM_HEALTH.wait_for_resources()

            # Cycle-based NIM summarization for the batch (up to 10 cycles, retrying only after all domains in cycle attempted)
            batch_summaries = {}
            if mode != "phase1":
                to_summarize = {}
                for row_index, row in enumerate(batch):
                    if not row or not row[0]:
                        continue
                    domain = row[0]
                    
                    # 1. Existing summary check from sheet
                    existing_sum = ""
                    if len(row) > 41 and safe_str(row[41]).strip() and safe_str(row[41]).strip() != safe_str(if_pred_base_prompt).strip():
                        existing_sum = safe_str(row[41]).strip()
                    elif len(row) > 40 and safe_str(row[40]).strip() and safe_str(row[40]).strip() != safe_str(summery_base_prompt).strip() and not safe_str(row[40]).strip().startswith("http"):
                        existing_sum = safe_str(row[40]).strip()
                    
                    if existing_sum:
                        batch_summaries[domain] = existing_sum
                        continue
                    
                    cached = LOCAL_CACHE.get(domain) if LOCAL_CACHE else None
                    if cached and cached.get("output_data_summery"):
                        batch_summaries[domain] = cached["output_data_summery"]
                        continue

                    # 2. Check raw data in row
                    raw_content = ""
                    for c_idx in [31, 37, 32, 33, 34, 35]:
                        if len(row) > c_idx and safe_str(row[c_idx]).strip():
                            raw_content = safe_str(row[c_idx]).strip()
                            break
                    
                    # 3. Check S3 markdown if Tech Crawler succeeded
                    if not raw_content and len(row) > 28 and safe_str(row[28]).strip() and len(row) > 25 and safe_str(row[25]).strip() == "SUCCESS":
                        s3_md = safe_str(row[28]).strip()
                        try:
                            tc_client = TechCrawlerClient(token=Tracxn_API_Token)
                            signed_url = await tc_client.get_signed_s3_url(session, s3_md)
                            if signed_url:
                                raw_content = await tc_client.fetch_raw_markdown(session, signed_url) or ""
                        except Exception as s3_err:
                            logging.warning(f"Could not fetch S3 markdown for {domain}: {s3_err}")

                    if raw_content:
                        if len(raw_content) > 48000:
                            raw_content = raw_content[:48000]
                        prompt_msg = Utility_Replace_Response(raw_content, summery_base_prompt, replacable_char)
                        prompt_msg = Utility_Replace_Response(domain, prompt_msg, replacable_char_Domain)
                        if "YYYY" in prompt_msg:
                            prompt_msg = prompt_msg.replace("YYYY", raw_content)
                        if "XXXX" in prompt_msg:
                            prompt_msg = prompt_msg.replace("XXXX", domain)
                        to_summarize[domain] = prompt_msg

                # 10-Cycle Retry loop for all domains needing NIM summary in this batch
                if to_summarize:
                    nim_sem = asyncio.Semaphore(15)  # Scaled for 60% resource utilization across 31 NIM keys
                    async def _single_nim(d, p):
                        async with nim_sem:
                            return await Nvidia_API_ASYNC(p, token_rotator=token_rotator, max_attempts=1, session=session)

                    pending_domains = list(to_summarize.keys())
                    for cycle in range(1, 11):
                        if not pending_domains:
                            break
                        stage_msg = f"Batch {batch_index + 1}/{len(domains_batches)}: NIM Cycle {cycle}/10 ({len(pending_domains)} domains pending)..."
                        logging.info(f"🔄 {stage_msg}")
                        update_progress_file(processed, total_domains, success_count, fail_count, phase=phase_title, stage_details=stage_msg, pending_tc=len(pending_tech_crawler_jobs), mode=mode)

                        cycle_tasks = [_single_nim(d, to_summarize[d]) for d in pending_domains]
                        cycle_results = await asyncio.gather(*cycle_tasks, return_exceptions=True)

                        next_pending = []
                        for d, res in zip(pending_domains, cycle_results):
                            if isinstance(res, str) and res.strip():
                                batch_summaries[d] = res.strip()
                            else:
                                next_pending.append(d)

                        successes = len(pending_domains) - len(next_pending)
                        logging.info(f"✅ NIM Cycle {cycle}/10: {successes} succeeded, {len(next_pending)} pending for next cycle.")
                        pending_domains = next_pending

                    # Fallback to Gemini for any domain that failed all 10 cycles
                    if pending_domains:
                        logging.warning(f"⚠️ {len(pending_domains)} domains exhausted all 10 NIM cycles. Falling back to Gemini...")
                        async def _gemini_fb(d, p):
                            try:
                                g_res = await Gemini_API_ASYNC(p, session=session)
                                return g_res.get("output", "")
                            except Exception as ex:
                                logging.error(f"Gemini fallback failed for {d}: {ex}")
                                return ""
                        fb_tasks = [_gemini_fb(d, to_summarize[d]) for d in pending_domains]
                        fb_results = await asyncio.gather(*fb_tasks, return_exceptions=True)
                        for d, res in zip(pending_domains, fb_results):
                            if isinstance(res, str) and res.strip():
                                batch_summaries[d] = res.strip()

            tasks = []
            start_row = start_row_num + batch_index * batch_size  # Adjust row index for writing

            for row_index, row in enumerate(batch):
                if row and row[0]:  # Ensure the row is valid
                    domain = row[0]
                    task = asyncio.create_task(
                        Integrated_Process_domain(
                            session,
                            domain,
                            start_row + row_index,
                            client,
                            semaphore,
                            row,
                            SHEET_ID,
                            EXTRACTING_SHEET_NAME,
                            cache_id_IF_Prediction,
                            token_IF_Prediction,
                            date_IF_Prediction,
                            status_IF_Prediction,
                            exp_date_IF_Prediction,
                            config_funnel_prompt,
                            config_funnel_id_prompt,
                            config_funnel_name_prompt,
                            config_funnel_name_Feed_Cached_prompt,
                            config_funnel_name_Feed_Cached_API_Token,
                            config_funnel_name_Feed_Cached_Model,
                            config_funnel_name_Feed_Cached_Gen_Date,
                            config_funnel_name_Feed_Cached_Final_Status,
                            token_rotator,
                            mode=mode,
                            write_to_sheet=False,
                            precomputed_summary=batch_summaries.get(domain),
                        )
                    )
                    tasks.append(task)
                else:
                    async def _noop():
                        return None
                    tasks.append(asyncio.create_task(_noop()))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            if mode == "phase1":
                target_start_col = 24
                expected_cols = 9
            elif mode == "phase2":
                target_start_col = 40
                expected_cols = 2
            else:
                target_start_col = 20
                expected_cols = 50

            batch_write_rows = []
            for idx, res in enumerate(results):
                orig_row = batch[idx] if idx < len(batch) else []

                if isinstance(res, Exception) or res is None:
                    processed += 1
                    fail_count += 1
                    slice_start = target_start_col - 1
                    existing_slice = orig_row[slice_start : slice_start + expected_cols]
                    if len(existing_slice) < expected_cols:
                        existing_slice = existing_slice + [""] * (expected_cols - len(existing_slice))
                    batch_write_rows.append(existing_slice)
                else:
                    if isinstance(res, dict) and res.get("tc_status") == "PROCESSING":
                        pending_tech_crawler_jobs.append(res)
                        if mode == "phase1":
                            write_data = res.get("write_data") or [res.get("tc_job_id", ""), res.get("tc_job_gen_time", ""), "PROCESSING"]
                            if len(write_data) < expected_cols:
                                write_data = write_data + [""] * (expected_cols - len(write_data))
                            batch_write_rows.append(write_data)
                        elif mode == "phase2":
                            processed += 1
                            success_count += 1
                            slice_start = target_start_col - 1
                            existing_slice = orig_row[slice_start : slice_start + expected_cols]
                            if len(existing_slice) < expected_cols:
                                existing_slice = existing_slice + [""] * (expected_cols - len(existing_slice))
                            batch_write_rows.append(existing_slice)
                        else:
                            slice_start = target_start_col - 1
                            row_slice = list(orig_row[slice_start : slice_start + expected_cols])
                            if len(row_slice) < expected_cols:
                                row_slice = row_slice + [""] * (expected_cols - len(row_slice))
                            row_slice[4] = res.get("tc_job_id", "")
                            row_slice[5] = res.get("tc_job_gen_time", "")
                            row_slice[6] = "PROCESSING"
                            batch_write_rows.append(row_slice)
                    else:
                        processed += 1
                        success_count += 1
                        write_data = res.get("write_data") if isinstance(res, dict) else None
                        if write_data and len(write_data) == expected_cols:
                            batch_write_rows.append(write_data)
                        elif write_data:
                            if len(write_data) < expected_cols:
                                write_data = write_data + [""] * (expected_cols - len(write_data))
                            else:
                                write_data = write_data[:expected_cols]
                            batch_write_rows.append(write_data)
                        else:
                            slice_start = target_start_col - 1
                            existing_slice = orig_row[slice_start : slice_start + expected_cols]
                            if len(existing_slice) < expected_cols:
                                existing_slice = existing_slice + [""] * (expected_cols - len(existing_slice))
                            batch_write_rows.append(existing_slice)

            if batch_write_rows:
                await write_sheet_data(
                    client, SHEET_ID, EXTRACTING_SHEET_NAME,
                    batch_write_rows, start_row, target_start_col
                )

            # High-Throughput Real-Time Finalization: Execute Ingestion Write APIs & Review APIs
            # immediately per batch so domains are marked DONE in Google Sheets in real-time!
            if mode == "full":
                try:
                    review_tasks = []
                    for r_offset, r_data in enumerate(batch):
                        if r_data and r_data[0]:
                            review_tasks.append(
                                Process_Review_domain(
                                    session, r_data, start_row + r_offset, client, semaphore, SHEET_ID, EXTRACTING_SHEET_NAME, write_to_sheet=False
                                )
                            )
                    rev_results = await asyncio.gather(*review_tasks, return_exceptions=True)
                    rev_batch_rows = []
                    for idx, r_res in enumerate(rev_results):
                        orig_r = batch[idx] if idx < len(batch) else []
                        if isinstance(r_res, Exception) or r_res is None:
                            existing_slice = orig_r[19:68] if len(orig_r) >= 68 else (orig_r[19:] + [""] * max(0, 49 - len(orig_r[19:])))
                            rev_batch_rows.append(existing_slice)
                        else:
                            rev_batch_rows.append(r_res)
                    if rev_batch_rows:
                        await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, rev_batch_rows, start_row, 20)
                except Exception as rev_err:
                    logging.warning(f"Batch real-time review execution warning: {rev_err}")

            phase_label = "Phase 1: Initial Crawl & Predictions" if mode == "full" else f"Mode: {mode.upper()}"
            update_progress_file(
                processed, total_domains, success_count, fail_count,
                phase=phase_label,
                stage_details=f"Batch {batch_index + 1}/{len(domains_batches)}: {processed}/{total_domains} rows processed",
                pending_tc=len(pending_tech_crawler_jobs),
                mode=mode
            )
            logging.info(
                f"Completed batch {batch_index + 1}/{len(domains_batches)} with {len(batch)} domains. Progress: {processed}/{total_domains} (Pending TC: {len(pending_tech_crawler_jobs)})"
            )
            trim_memory()

        if pending_tech_crawler_jobs:
            logging.info(
                f"{len(pending_tech_crawler_jobs)} domains still PROCESSING in Tech Crawler. "
                f"Starting 15-minute cadence polling loop (up to 3 hours)..."
            )
            tc_client = TechCrawlerClient(token_rotator=TRACXN_KEY_MANAGER)
            retry_wait_sec = 900  # Constant 15 minutes (900s) per user directive
            MAX_WAIT_SEC = 900

            while pending_tech_crawler_jobs:
                if is_5am_ist_cutoff():
                    logging.warning("⏰ 5:00 AM IST cutoff reached during Tech Crawler polling. Halting.")
                    break
                if os.path.exists(STOP_FILE):
                    logging.info("Stop requested (.stop_requested found). Exiting Tech Crawler polling loop.")
                    break

                logging.info(
                    f"Waiting {retry_wait_sec // 60} minutes ({retry_wait_sec}s) before re-checking "
                    f"{len(pending_tech_crawler_jobs)} pending Tech Crawler jobs..."
                )
                
                elapsed = 0
                while elapsed < retry_wait_sec:
                    if is_5am_ist_cutoff() or os.path.exists(STOP_FILE):
                        break
                    remaining_sec = retry_wait_sec - elapsed
                    update_progress_file(
                        processed, total_domains, success_count, fail_count,
                        phase="Tech Crawler Background Polling",
                        stage_details=f"Waiting for {len(pending_tech_crawler_jobs)} crawls to complete (re-check in {remaining_sec}s)",
                        pending_tc=len(pending_tech_crawler_jobs),
                        mode=mode
                    )
                    chunk = min(5, remaining_sec)
                    await asyncio.sleep(chunk)
                    elapsed += chunk

                if os.path.exists(STOP_FILE):
                    logging.info("Stop requested (.stop_requested found). Exiting Tech Crawler polling loop.")
                    break

                update_progress_file(
                    processed, total_domains, success_count, fail_count,
                    phase="Tech Crawler Background Polling",
                    stage_details=f"Checking {len(pending_tech_crawler_jobs)} pending Tech Crawler jobs...",
                    pending_tc=len(pending_tech_crawler_jobs),
                    mode=mode
                )

                still_pending = []
                for item in pending_tech_crawler_jobs:
                    if os.path.exists(STOP_FILE):
                        break
                    d = item["domain"]
                    r_num = item["row"]
                    j_id = item["tc_job_id"]
                    orig_row = item.get("rowData") or []

                    try:
                        c_ts = item.get("created_timestamp", time.time())
                        track_res = await tc_client.check_job_status_with_cadence(session, j_id, c_ts)
                        curr_status = track_res.get("status")

                        if curr_status in ("SUCCESS", "FAILED"):
                            logging.info(f"Tech Crawler job {j_id} for {d} resolved with status: {curr_status}")
                            if len(orig_row) > 30:
                                orig_row[23] = j_id
                                orig_row[25] = curr_status
                                orig_row[26] = track_res.get("final_url", "")
                                orig_row[27] = track_res.get("s3_html", "")
                                orig_row[28] = track_res.get("s3_markdown", "")
                                orig_row[29] = track_res.get("http_status", "")
                                orig_row[30] = track_res.get("signals", "")
                            await Integrated_Process_domain(
                                session, d, r_num, client, semaphore, orig_row,
                                SHEET_ID, EXTRACTING_SHEET_NAME,
                                cache_id_IF_Prediction, token_IF_Prediction,
                                date_IF_Prediction, status_IF_Prediction,
                                exp_date_IF_Prediction, config_funnel_prompt,
                                config_funnel_id_prompt, config_funnel_name_prompt,
                                config_funnel_name_Feed_Cached_prompt,
                                config_funnel_name_Feed_Cached_API_Token,
                                config_funnel_name_Feed_Cached_Model,
                                config_funnel_name_Feed_Cached_Gen_Date,
                                config_funnel_name_Feed_Cached_Final_Status,
                                token_rotator,
                                created_timestamp=c_ts,
                                mode=mode
                            )
                            processed += 1
                            success_count += 1
                            remaining_pending = len(still_pending) + (len(pending_tech_crawler_jobs) - 1)
                            update_progress_file(
                                processed, total_domains, success_count, fail_count,
                                phase="Tech Crawler Polling & Processing",
                                stage_details=f"Resolved job for {d} ({remaining_pending} pending)",
                                pending_tc=remaining_pending,
                                mode=mode
                            )
                        else:
                            still_pending.append(item)
                    except Exception as err:
                        logging.error(f"Error polling job {j_id} for {d}: {err}")
                        still_pending.append(item)

                pending_tech_crawler_jobs = still_pending
                if pending_tech_crawler_jobs:
                    retry_wait_sec = 900  # Constant 15 minutes per user directive

#**************************** Domain Rejection + Tagging *********************************

SNOOZE_DURATION = 10
To_Review_TAG_NAME = "domainformanualreview"
COMMENT = "This is a Write API Movement by Gemini"

# Helper: IST time
def get_ist_timestamp():
    try:
        return datetime.datetime.now(IST).strftime("%d/%b/%Y  %H:%M:%S")
    except Exception:
        return datetime.datetime.utcnow().strftime("%d/%b/%Y  %H:%M:%S")

# Async version of your domain logic
async def Process_Review_domain(session, row, row_number, client, semaphore, SHEET_ID, EXTRACTING_SHEET_NAME, write_to_sheet=False):
    try:
        async with semaphore:
            token = TRACXN_KEY_MANAGER.get_next_token()
            TOKEN = token
            domain = row[0]
            domain_profile_id = row[10]
            filter_group_id = row[4]
            filter_id = row[6]
            source_funnel_id = row[2]
            output_data_if = row[43] if len(row) > 43 else ""
            industry_funnel_id = output_data_if

            tagStatusForIF      = row[65]
            tagStatusForPF      = row[66]
            tagStatusForReview  = row[67]

            cf_if_status = row[61]
            cf_if_time = row[62]
            tag_status_review = tagStatusForReview

            if tagStatusForIF == "YES" or tagStatusForPF == "YES" or tagStatusForReview == "YES":
                pass
            else :
                # print("Domain Picked for review - ",domain)
                
                if source_funnel_id and ("22" in output_data_if or "MARKED_IRRELEVANT" in output_data_if):
                    cf_if_status = await WriteAPI_Domain_Funnel_async( domain_profile_id, source_funnel_id, filter_group_id, filter_id,source_funnel_id, "MARKED_IRRELEVANT", COMMENT, TOKEN, session=session)
                    cf_if_time = get_ist_timestamp()
                    if cf_if_status == "TRUE":
                        cf_if_status = "DONE"

                elif source_funnel_id and ("23" in output_data_if or "ESCALATED" in output_data_if):
                    cf_if_status = await WriteAPI_Domain_Funnel_async( domain_profile_id, source_funnel_id, filter_group_id, filter_id,source_funnel_id, "ESCALATED", COMMENT, TOKEN, session=session)
                    cf_if_time = get_ist_timestamp()
                    if cf_if_status == "TRUE":
                        cf_if_status = "DONE"

                else:
                    tag_status_review = await Domain_HashTag_async(domain, To_Review_TAG_NAME, TOKEN)
                    cf_if_status = await WriteAPI_Domain_Funnel_async( domain_profile_id, source_funnel_id, filter_group_id, filter_id,source_funnel_id, "SNOOZED", COMMENT, TOKEN, session=session)
                    cf_if_time = get_ist_timestamp()
                    if cf_if_status == "TRUE":
                        cf_if_status = "DONE"

            tag_status_discovery = ""
            if str(row[63]).strip().upper() == "DONE":
                tag_status_discovery = await Domain_HashTag_async(domain, Discovery_Processing_TAG_NAME, TOKEN)

            discovery_bq = "yes" if str(tag_status_discovery).upper() in ("YES", "TRUE") else (row[68] if len(row) > 68 else "")

            # Prepare output data
            output_row = [
                    get_ist_timestamp(),  # Status Date
                    get_ist_timestamp(),  # Manual Status
                    "To be checked",      # Some default value
                    *row[22:61],          # Data range
                    cf_if_status,
                    cf_if_time,
                    row[63],  # IF_PF_Status
                    row[64],  # IF_PF_Time
                    tagStatusForIF,  # TagStatusIF
                    tagStatusForPF,  # TagStatusPF
                    tag_status_review,
                    discovery_bq
                ]

            if write_to_sheet:
                await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [output_row], row_number, 20)
            return output_row
    except Exception as e:
        logging.error(f"Error processing row {row_number}: {e}")
        return None


# Entry point
async def Process_Review_Caller(client, mainheaders, SHEET_ID, EXTRACTING_SHEET_NAME, start_row_num=4, mode="full"):
    if os.path.exists(STOP_FILE):
        logging.info("Stop requested before Process_Review_Caller. Skipping.")
        return

    pf_data = await read_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, start_row=start_row_num)
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    batch_size = BATCH_SIZE
    domains_batches = [pf_data[i:i + batch_size] for i in range(0, len(pf_data), batch_size)]

    total_review_domains = len(pf_data)
    review_processed = 0
    review_success = 0
    review_fail = 0

    update_progress_file(
        review_processed, total_review_domains, review_success, review_fail,
        phase="Phase 4: Review Write APIs",
        stage_details=f"Starting review processing of {total_review_domains} domains...",
        pending_tc=0,
        mode=mode
    )

    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=25,
        ttl_dns_cache=300
    )
    async with aiohttp.ClientSession(headers=mainheaders, connector=connector) as session:
        for batch_index, batch in enumerate(domains_batches):
            if is_5am_ist_cutoff():
                logging.warning("⏰ 5:00 AM IST cutoff reached during Process_Review_Caller. Halting.")
                break
            if os.path.exists(STOP_FILE):
                logging.info("Stop requested during Process_Review_Caller. Halting.")
                break

            await SYSTEM_HEALTH.wait_for_resources()

            tasks = []
            start_row = start_row_num + batch_index * batch_size  # Adjust for writing

            for row_index, row in enumerate(batch):
                if row and row[0]:
                    task = asyncio.create_task(
                        Process_Review_domain(
                            session, row, start_row + row_index, client, semaphore, SHEET_ID, EXTRACTING_SHEET_NAME, write_to_sheet=False
                        )
                    )
                    tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_output_rows = []
            for idx, res in enumerate(results):
                orig_row = batch[idx] if idx < len(batch) else []
                if isinstance(res, Exception) or res is None:
                    review_fail += 1
                    # Accidental data loss prevention fallback: preserve existing row data
                    existing_slice = orig_row[19:68] if len(orig_row) >= 68 else (orig_row[19:] + [""] * max(0, 49 - len(orig_row[19:])))
                    batch_output_rows.append(existing_slice)
                else:
                    review_success += 1
                    batch_output_rows.append(res)
                review_processed += 1

            if batch_output_rows:
                await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, batch_output_rows, start_row, 20)

            update_progress_file(
                review_processed, total_review_domains, review_success, review_fail,
                phase="Phase 4: Review Write APIs",
                stage_details=f"Batch {batch_index + 1}/{len(domains_batches)}: {review_processed}/{total_review_domains} domains checked",
                pending_tc=0,
                mode=mode
            )
            logging.info(f"Completed review batch {batch_index + 1}/{len(domains_batches)} with {len(batch)} domains.")
            trim_memory()

#********************************************************************************************

async def Execute_Phase4_Ingestion_Write_APIs(client, mainheaders, SHEET_ID, EXTRACTING_SHEET_NAME, start_row_num=4):
    """Executes CF->IF and IF->PF Write APIs + Hashtags for rows that have predictions but uncommitted write APIs."""
    if os.path.exists(STOP_FILE):
        return

    pf_data = await read_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, start_row=start_row_num)
    total = len(pf_data)
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    batch_size = BATCH_SIZE
    batches = [pf_data[i:i + batch_size] for i in range(0, total, batch_size)]

    processed = 0
    success = 0
    fail = 0

    update_progress_file(
        0, total, 0, 0,
        phase="Phase 4: Ingestion Write APIs",
        stage_details=f"Executing Tracxn Ingestion Write APIs for {total} rows...",
        pending_tc=0,
        mode="phase4"
    )

    connector = aiohttp.TCPConnector(limit=100, limit_per_host=25, ttl_dns_cache=300)
    async with aiohttp.ClientSession(headers=mainheaders, connector=connector) as session:
        for b_idx, batch in enumerate(batches):
            if is_5am_ist_cutoff():
                logging.warning("⏰ 5:00 AM IST cutoff reached during Phase 4 Ingestion. Halting.")
                break
            if os.path.exists(STOP_FILE):
                break
            tasks = []
            start_row = start_row_num + b_idx * batch_size

            async def _process_row_ingestion(row, r_num):
                async with semaphore:
                    if not row or not row[0]:
                        return None
                    domain = row[0]
                    row, _ = LOCAL_CACHE.hydrate_row_from_cache(domain, row)
                    domain_profile_id = row[10] if len(row) > 10 else ""
                    filter_group_id = row[4] if len(row) > 4 else ""
                    filter_id = row[6] if len(row) > 6 else ""
                    source_funnel_id = row[2] if len(row) > 2 else ""

                    # Predictions in row
                    output_data_if = row[44] if len(row) > 44 else ""
                    industry_funnel_id = row[46] if len(row) > 46 else ""
                    output_data_if_validation = row[50] if len(row) > 50 else ""
                    publish_funnel_id = row[60] if len(row) > 60 else ""
                    cf_if_status = row[61] if len(row) > 61 else ""
                    cf_if_time = row[62] if len(row) > 62 else ""
                    if_pf_status = row[63] if len(row) > 63 else ""
                    if_pf_time = row[64] if len(row) > 64 else ""
                    tag_status_if = row[65] if len(row) > 65 else ""
                    tag_status_pf = row[66] if len(row) > 66 else ""
                    tag_status_discovery = row[68] if len(row) > 68 else ""

                    token = Tracxn_API_Token
                    comment = "This is a Write API Movement by Gemini"
                    tag_name_if = "bu_llm_industry_prediction"
                    tag_name_pf = "bu_llm_feed_prediction"
                    if_pf_action = "MOVED_OUT"

                    # If already done, skip calling Tracxn APIs
                    if cf_if_status == "DONE" and if_pf_status == "DONE":
                        return [cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf, "", tag_status_discovery]

                    # CF -> IF Write API & Tagging
                    if EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME and output_data_if_validation and "Yes" not in output_data_if_validation:
                        tag_status_if = await Domain_HashTag_async(domain, Wrong_Validated_IF_Tag_Name, token)
                        cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")
                        if source_funnel_id:
                            cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id, filter_id, source_funnel_id, "MARKED_FOR_LATER", Wrong_Validation_COMMENT, token, session=session)
                            cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")
                            if cf_if_status == "TRUE":
                                cf_if_status = "DONE"
                    elif (EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME and "Yes" in output_data_if_validation) or (EXTRACTING_SHEET_NAME != CF_EXTRACTING_SHEET_NAME):
                        if source_funnel_id and industry_funnel_id:
                            tag_status_if = await Domain_HashTag_async(domain, tag_name_if, token)
                            cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id, filter_id, industry_funnel_id, "MOVED_OUT", comment, token, session=session)
                            cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")
                            if cf_if_status == "TRUE":
                                cf_if_status = "DONE"
                        elif source_funnel_id and (not industry_funnel_id and (output_data_if == "22" or "MARKED_IRRELEVANT" in output_data_if)):
                            cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id, filter_id, source_funnel_id, "MARKED_IRRELEVANT", comment, token, session=session)
                            cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")
                            if cf_if_status == "TRUE":
                                cf_if_status = "DONE"
                        elif source_funnel_id and (not industry_funnel_id and (output_data_if == "23" or "ESCALATED" in output_data_if)):
                            cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id, filter_id, source_funnel_id, "ESCALATED", comment, token, session=session)
                            cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")
                            if cf_if_status == "TRUE":
                                cf_if_status = "DONE"
                        elif source_funnel_id:
                            cf_if_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, source_funnel_id, filter_group_id, filter_id, source_funnel_id, "SNOOZED", comment, token, session=session)
                            cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")
                            if cf_if_status == "TRUE":
                                cf_if_status = "DONE"

                    # IF -> PF Write API & Tagging
                    if industry_funnel_id and publish_funnel_id:
                        tag_status_pf = await Domain_HashTag_async(domain, tag_name_pf, token)
                        if_pf_status = await WriteAPI_Domain_Funnel_async(domain_profile_id, industry_funnel_id, filter_group_id, filter_id, publish_funnel_id, if_pf_action, comment, token, session=session)
                        if_pf_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")
                        if if_pf_status == "TRUE":
                            if_pf_status = "DONE"

                    if if_pf_status == "DONE":
                        tag_status_discovery = await Domain_HashTag_async(domain, Discovery_Processing_TAG_NAME, token, session=session)

                    discovery_bq = "yes" if str(tag_status_discovery).upper() in ("YES", "TRUE") else ""
                    res_write = [cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf, "", discovery_bq]
                    LOCAL_CACHE.save_record(domain, {
                        "cf_if_status": cf_if_status,
                        "cf_if_time": cf_if_time,
                        "if_pf_status": if_pf_status,
                        "if_pf_time": if_pf_time,
                        "tag_status_if": tag_status_if,
                        "tag_status_pf": tag_status_pf,
                        "discovery_bq": discovery_bq,
                        "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                    })
                    return res_write

            for r_idx, row in enumerate(batch):
                tasks.append(asyncio.create_task(_process_row_ingestion(row, start_row + r_idx)))

            b_results = await asyncio.gather(*tasks, return_exceptions=True)
            batch_write = []
            for r in b_results:
                processed += 1
                if isinstance(r, Exception) or r is None:
                    fail += 1
                    batch_write.append([""] * 8)
                else:
                    success += 1
                    batch_write.append(r)

            if batch_write:
                # Write to Cols 62 to 69 (start col 62 in 1-indexed)
                await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, batch_write, start_row, 62)

            update_progress_file(
                processed, total, success, fail,
                phase="Phase 4: Ingestion Write APIs",
                stage_details=f"Batch {b_idx + 1}/{len(batches)}: {processed}/{total} rows processed",
                pending_tc=0,
                mode="phase4"
            )

# Async task wrapper
def task():
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Scheduled task error: {e}")


async def main():
    try:
        start_row_arg = 4
        mode_arg = "full"
        custom_sheet_id = None

        if len(sys.argv) > 1 and sys.argv[1].isdigit():
            start_row_arg = int(sys.argv[1])
        if len(sys.argv) > 2 and not sys.argv[2].startswith("--"):
            mode_arg = sys.argv[2]
        if "--sheet_id" in sys.argv:
            idx = sys.argv.index("--sheet_id")
            if idx + 1 < len(sys.argv):
                custom_sheet_id = sys.argv[idx + 1]

        active_sheet_id = custom_sheet_id or CF_SHEET_ID
        logging.info(f"Discovery Engine Pipeline starting: start_row={start_row_arg}, mode={mode_arg}, sheet_id={active_sheet_id}")

        try:
            curr_state = EXEC_TRACKER.get_state()
            if curr_state.get("active_pid") != os.getpid():
                EXEC_TRACKER.record_start(
                    sheet=os.path.basename(BASE_DIR),
                    sheet_id=active_sheet_id,
                    start_row=start_row_arg,
                    mode=mode_arg,
                    pid=os.getpid()
                )
        except Exception:
            pass

        if is_5am_ist_cutoff():
            logging.warning("⏰ 5:00 AM IST daily cutoff active. Pipeline execution halted during daily domain refresh window.")
            try:
                EXEC_TRACKER.record_5am_cutoff()
            except Exception:
                pass
            sys.exit(0)

        client = await get_gspread_client()
        gc = get_gspread_client_General()

        if gc is None:
            gc = get_gspread_client()

        # Initialize Once-Per-Run Local Config Cache (downloads config once from Google Sheets)
        run_cfg = load_or_refresh_run_config(gc, force_refresh=True)

        cache_id_IF_Prediction = run_cfg.get("cache_id_IF_Prediction")
        token_IF_Prediction = run_cfg.get("token_IF_Prediction")
        date_IF_Prediction = run_cfg.get("date_IF_Prediction")
        status_IF_Prediction = run_cfg.get("status_IF_Prediction")
        exp_date_IF_Prediction = run_cfg.get("exp_date_IF_Prediction")

        logging.info("***************** IF Prediction - Cache ID Retrieved *******************")
        logging.info(f"Cache ID: {cache_id_IF_Prediction}")
        
        if not cache_id_IF_Prediction:
            logging.warning("Cache ID not found. Stopping pipeline execution.")
            return

        # Config - Funnel Wise Prompts
        config_funnel_prompt = Return_Base_Prompt_FunnelWise(gc)
        config_funnel_id_prompt = config_funnel_prompt["FunnelIdPromptMap"]
        config_funnel_name_prompt = config_funnel_prompt["FunnelNamePromptMap"]
        config_funnel_name_Feed_Cached_prompt = config_funnel_prompt["Feed_cache_Id"]
        config_funnel_name_Feed_Cached_API_Token = config_funnel_prompt["Feed_cache_Info_API_Token"]
        config_funnel_name_Feed_Cached_Model = config_funnel_prompt["Feed_cache_Info_Model"]
        config_funnel_name_Feed_Cached_Gen_Date = config_funnel_prompt["Feed_cache_Info_Gen_Date"]
        config_funnel_name_Feed_Cached_Final_Status = config_funnel_prompt["Feed_cache_Info_Final_Status"]

        # Config - Feed Wise Funnel Info
        config_feed_wise_funnel = Return_Funnel_FeedWise(gc)
        config_feed_num_funnel_map = config_feed_wise_funnel["FeedNumFunnelMap"]
        config_feed_name_funnel_map = config_feed_wise_funnel["FeedNameFunnelMap"]

        # Caller - NIM Model Detail & Tokens (Direct from .env, sheet fetch skipped)
        unique_tokens = list(settings.NIM_API_KEYS)
        model_to_pick = settings.Nvidia_Base_Model

        token_rotator = NIMKeyManager(unique_tokens)
        logging.info(f"NIMKeyManager initialized with {token_rotator.total_tokens} keys from environment.")

        if model_to_pick:
            global Nvidia_Base_Model
            Nvidia_Base_Model = model_to_pick

        # Extraction & Prediction Phase
        if mode_arg != "phase4":
            try:
                await Integrated_Extract_domains(
                    client,
                    active_sheet_id,
                    CF_EXTRACTING_SHEET_NAME,
                    cache_id_IF_Prediction,
                    token_IF_Prediction,
                    date_IF_Prediction,
                    status_IF_Prediction,
                    exp_date_IF_Prediction,
                    config_funnel_prompt,
                    config_funnel_id_prompt,
                    config_funnel_name_prompt,
                    config_funnel_name_Feed_Cached_prompt,
                    config_funnel_name_Feed_Cached_API_Token,
                    config_funnel_name_Feed_Cached_Model,
                    config_funnel_name_Feed_Cached_Gen_Date,
                    config_funnel_name_Feed_Cached_Final_Status,
                    token_rotator=token_rotator,
                    start_row_num=start_row_arg,
                    mode=mode_arg
                )
                logging.info(f"Integrated_Extract_domains phase ({mode_arg}) completed.")
            except Exception as e:
                logging.error(f"Error in Integrated_Extract_domains: {e}")
                raise
        else:
            logging.info("Phase 4 mode: skipping Integrated_Extract_domains phase.")

        # Phase 4 Ingestion Write APIs
        if mode_arg == "phase4" and not os.path.exists(STOP_FILE):
            logging.info("***************** Phase 4: Ingestion Write APIs Started *******************")
            try:
                await Execute_Phase4_Ingestion_Write_APIs(client, mainheaders, active_sheet_id, CF_EXTRACTING_SHEET_NAME, start_row_num=start_row_arg)
                logging.info("Execute_Phase4_Ingestion_Write_APIs completed.")
            except Exception as e:
                logging.error(f"Error in Execute_Phase4_Ingestion_Write_APIs: {e}")
                raise

        # Review & Rejection Write API Phase
        if mode_arg in ("full", "phase4") and not os.path.exists(STOP_FILE):
            logging.info("***************** Review & Write API Phase Started *******************")
            try:
                await Process_Review_Caller(client, mainheaders, active_sheet_id, CF_EXTRACTING_SHEET_NAME, start_row_num=start_row_arg, mode=mode_arg)
                logging.info("Process_Review_Caller phase completed.")
            except Exception as e:
                logging.error(f"Error in Process_Review_Caller: {e}")
                raise

        update_progress_file(
            100, 100, 100, 0,
            phase="Completed",
            stage_details="All pipeline phases completed successfully.",
            pending_tc=0,
            mode=mode_arg
        )
        try:
            EXEC_TRACKER.record_completion()
        except Exception:
            pass
        logging.info("Discovery Engine Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Fatal error in main(): {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
