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
    get_sharded_nim_keys,
    TechCrawlerClient,
    WaterfallScraper,
    LocalSheetCache,
    ExecutionTracker,
    is_5am_ist_cutoff,
    is_pre_cutoff_window,
    is_before_4am_ist,
    get_now_ist,
    get_ist_timestamp,
    safe_str,
    BacklogManager,
    TracxnClient,
    PipelineCoordinator,
    trim_memory,
    SystemHealthMonitor,
    GSheetCircuitBreaker,
    is_doc_too_large_error,
    is_nim_dead_domain,
    is_57_col_schema,
    is_valid_s3_url,
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
GSHEET_CIRCUIT_BREAKER = GSheetCircuitBreaker(name="Sheet2")
PENDING_SYNC_FILE = os.path.join(_SHEET_DIR, "pending_gsheet_sync.csv")
SYSTEM_HEALTH = SystemHealthMonitor(
    cpu_threshold=float(os.getenv("SYSTEM_HEALTH_CPU_THRESHOLD_5321", os.getenv("SYSTEM_HEALTH_CPU_THRESHOLD", "95.0"))),
    mem_threshold=float(os.getenv("SYSTEM_HEALTH_MEM_THRESHOLD_5321", os.getenv("SYSTEM_HEALTH_MEM_THRESHOLD", "88.0"))),
    cache_ttl=1.0
)

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

def update_progress_file(current: int, total: int, success: int, fail: int, phase: str = "", stage_details: str = "", pending_tc: int = 0, mode: str = "", scraped_count: int = None, nim_count: int = None, gemini_count: int = None, tracxn_count: int = None, tc_success: int = None):
    prev_prog = {}
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                prev_prog = json.load(f)
        except Exception:
            pass

    live_tc_success = max(tc_success if tc_success is not None else 0, prev_prog.get('tc_success', 0), success if success is not None else 0)
    live_scraped_count = max(scraped_count if scraped_count is not None else 0, prev_prog.get('scraped_count', 0), live_tc_success)
    live_nim_count = max(nim_count if nim_count is not None else 0, prev_prog.get('nim_count', 0))
    live_gemini_count = max(gemini_count if gemini_count is not None else 0, prev_prog.get('gemini_count', 0))
    live_tracxn_count = max(tracxn_count if tracxn_count is not None else 0, prev_prog.get('tracxn_count', 0), current if current is not None else 0)

    prog_data = {
        'current': live_tracxn_count,
        'total': total,
        'success': live_tc_success,
        'fail': fail,
        'phase': phase,
        'stage_details': stage_details,
        'pending_tc': pending_tc,
        'mode': mode,
        'scraped_count': live_scraped_count,
        'tc_success': live_tc_success,
        'nim_count': live_nim_count,
        'gemini_count': live_gemini_count,
        'tracxn_count': live_tracxn_count,
        'active_tc_keys': TRACXN_KEY_MANAGER.get_active_tokens_count() if 'TRACXN_KEY_MANAGER' in globals() else prev_prog.get('active_tc_keys', 30),
        'total_tc_keys': len(getattr(TRACXN_KEY_MANAGER, 'tokens', [])) if 'TRACXN_KEY_MANAGER' in globals() else prev_prog.get('total_tc_keys', 30),
        'active_nim_keys': NIM_KEY_MANAGER.get_active_keys_count() if ('NIM_KEY_MANAGER' in globals() and hasattr(NIM_KEY_MANAGER, 'get_active_keys_count')) else (len(getattr(NIM_KEY_MANAGER, 'keys', [])) if 'NIM_KEY_MANAGER' in globals() else prev_prog.get('active_nim_keys', 8)),
        'total_nim_keys': len(getattr(NIM_KEY_MANAGER, 'keys', [])) if 'NIM_KEY_MANAGER' in globals() else prev_prog.get('total_nim_keys', 8)
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

def is_row_fully_completed(row: list) -> bool:
    """
    Returns True if full end-to-end processing INCLUDING Tracxn movement is completed.
    A domain is fully completed only when:
    1. It has a non-empty domain name (row[0]).
    2. Tracxn movement has completed via any of the terminal conditions:
       - 57-Column Schema:
         * Col 50 (idx 49, Col AX) cf_if_status in terminal statuses
         * Col 51 (idx 50, Col AY) cf_if_time contains a valid movement timestamp
         * Col 52 (idx 51, Col AZ) if_pf_status in terminal statuses
         * Col 53 (idx 52, Col BA) if_pf_time contains a valid movement timestamp
       - Legacy 70/93-Column Schema:
         * Col 62 (idx 61) or Col 63 (idx 62) cf_if_status in terminal statuses
           (DONE, YES, TRUE, MOVED, MOVED_OUT, SNOOZED, MARKED_IRRELEVANT, MARKED_FOR_LATER, ESCALATED, SKIPPED_UNASSIGNABLE)
         * Col 63 (idx 62) or Col 64 (idx 63) cf_if_time contains a valid movement timestamp
         * Col 64 (idx 63) or Col 65 (idx 64) if_pf_status in valid statuses or if_pf_time timestamp
    """
    if not row or not row[0] or not str(row[0]).strip():
        return False

    valid_statuses = {
        "DONE", "YES", "TRUE", "MOVED", "MOVED_OUT",
        "SNOOZED", "MARKED_IRRELEVANT", "MARKED_FOR_LATER", "ESCALATED", "SKIPPED_UNASSIGNABLE"
    }

    is_57 = is_57_col_schema(row)
    tc_idx = 23 if is_57 else 25
    nim_idx = 34 if is_57 else 40

    # Non-retryable terminal exit: Tech Crawler permanently failed
    if len(row) > tc_idx and str(row[tc_idx]).strip().upper() == "FAILED":
        return True

    # Non-retryable terminal exit: Dead domain / no content for NIM
    if len(row) > nim_idx and str(row[nim_idx]).strip().upper() in ("NO_CONTENT", "FAILED_NO_CONTENT"):
        # Guard against false NO_CONTENT if Tech Crawler actually succeeded and S3 link exists but raw data wasn't saved
        raw_idx = 29 if is_57 else 31
        s3_idx = 26 if is_57 else 28
        tc_stat = str(row[tc_idx]).strip().upper() if len(row) > tc_idx else ""
        has_s3 = (len(row) > s3_idx and str(row[s3_idx]).strip().startswith("http"))
        has_raw = (len(row) > raw_idx and bool(str(row[raw_idx]).strip()))
        if tc_stat == "SUCCESS" and has_s3 and not has_raw:
            return False
        return True

    def _is_valid_timestamp(s: str) -> bool:
        s = s.strip()
        if len(s) >= 8 and (":" in s or "/" in s or "-" in s) and s.upper() not in valid_statuses and not s.startswith("http"):
            return True
        return False

    # Check 57-column schema Write API movement columns (Cols AX-BA / indices 49-52)
    for col_idx in range(49, min(53, len(row))):
        val = str(row[col_idx]).strip()
        if val.upper() in valid_statuses or _is_valid_timestamp(val):
            return True

    # Check Legacy 70/93-column schema (indices 61-65)
    for col_idx in range(61, min(65, len(row))):
        val = str(row[col_idx]).strip()
        if val.upper() in valid_statuses or _is_valid_timestamp(val):
            return True

    return False


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
MAX_WORKERS = int(os.getenv("MAX_WORKERS_SHEET2", os.getenv("MAX_WORKERS_5080", getattr(settings, "MAX_WORKERS", 10))))
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

# Dedicated sharded NIM API keys for Sheet 2 (sharded across 4 sheets fleet-wide = zero key collision)
NIM_HARDCODED_KEYS = get_sharded_nim_keys(2, total_sheets=4, keys=settings.NIM_API_KEYS)
NIM_KEY_MANAGER = NIMKeyManager(list(NIM_HARDCODED_KEYS))

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


async def read_sheet_data(client, SHEET_ID, sheet_name, start_row=3, start_col=1):
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

    if GSHEET_CIRCUIT_BREAKER.should_skip_gsheet():
        return

    # Critical: safely cap all cell values to 49,000 chars to avoid Google Sheets 50,000 char per cell HTTP 400 fatal crash
    data = [
        [
            val[:49000] if isinstance(val, str) and len(val) > 49000 else val
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
        if is_doc_too_large_error(update_err):
            GSHEET_CIRCUIT_BREAKER.trip(str(update_err))
            return

        logging.warning(f"Worksheet update failed ({update_err}). Invalidating cache and retrying...")
        _WORKSHEET_CACHE.pop(cache_key, None)
        sheet = None
        gc = await (second_mgr or first_mgr).authorize() if second_mgr else await first_mgr.authorize()
        sheet = await gc.open_by_key(SHEET_ID)
        worksheet = await sheet.worksheet(sheet_name)
        _WORKSHEET_CACHE[cache_key] = worksheet
        try:
            try:
                await worksheet.update(values=data, range_name=cell_range, value_input_option='USER_ENTERED')
            except TypeError:
                await worksheet.update(cell_range, data, value_input_option='USER_ENTERED')
        except Exception as retry_err:
            if is_doc_too_large_error(retry_err):
                GSHEET_CIRCUIT_BREAKER.trip(str(retry_err))
                return
            raise


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

# NIMKeyManager is imported from common (common.key_manager.NIMKeyManager)
# with automatic preflight validation, dynamic permanent sleep, and token rotation.
TokenRotator = NIMKeyManager

#********************************** Getting NVDIA Tokens ************************************

def Return_NIM_Config_Details(client=None):
    """Retrieve NIM model name and API tokens directly from environment settings, skipping sheet fetch."""
    global _RUN_CACHE
    if _RUN_CACHE.get("nim_config"):
        return _RUN_CACHE["nim_config"]

    tokens = [["", "", "", "", k] for k in NIM_HARDCODED_KEYS]
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
    if is_pre_cutoff_window() or is_5am_ist_cutoff():
        logging.info("⏰ [Pre-Cutoff Wind-Down] Skipping outbound NVIDIA NIM API call during 04:30-05:00 AM window.")
        return ""
    if token_rotator is None and 'NIM_KEY_MANAGER' in globals() and NIM_KEY_MANAGER is not None:
        token_rotator = NIM_KEY_MANAGER
    system_instruction = (
        "You are a professional website information extractor and summarizer. "
        "Output ONLY the final single continuous summary paragraph directly. "
        "Do NOT output any thinking process, reasoning steps, analysis, planning notes, or bullet points."
    )
    if max_attempts is None:
        tt = getattr(token_rotator, "total_tokens", None)
        max_attempts = min(4, int(tt)) if isinstance(tt, (int, float)) and int(tt) > 0 else 4

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
    timeout = aiohttp.ClientTimeout(total=40.0, sock_connect=5.0, sock_read=35.0)

    attempt = 0
    while attempt < max_attempts and (token_rotator is None or token_rotator.keys):
        attempt += 1
        api_key = None

        try:
            # Get key with in-flight concurrency isolation and cooldown awareness
            if token_rotator:
                api_key, wait_time = await token_rotator.get_next_key_async()
                if wait_time > 0:
                    logging.info(f"NIM keys cooling down / in-flight. Throttling {wait_time:.1f}s (attempt {attempt})...")
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

                    # 200 OK
                    if resp.status == 200:
                        data = await resp.json()
                        choices = data.get("choices", [])
                        if choices:
                            msg = choices[0].get("message", {})
                            content = msg.get("content") or msg.get("reasoning_content") or ""
                            cleaned = sanitize_summary_output(content)
                            if cleaned and "below is the website extraction" not in cleaned.lower():
                                return cleaned
                        # Empty/refused response -> retry with same temperature (always 0.0)
                        logging.warning(f"Empty/incomplete NIM response (attempt {attempt}/{max_attempts}), key ...{api_key[-6:]}")
                        await asyncio.sleep(0.5)

                    # 202 Accepted (NVCF queued/in-progress)
                    elif resp.status == 202:
                        logging.info(f"NIM 202 Accepted on key ...{api_key[-6:]} (queued on NVCF). Waiting 2.0s...")
                        await asyncio.sleep(2.0)

                    # 401/403 Unauthorized / Forbidden
                    elif resp.status in (401, 403):
                        if token_rotator:
                            token_rotator.mark_invalid(api_key)
                        await asyncio.sleep(0.1)

                    # 429 Rate Limited
                    elif resp.status == 429:
                        if token_rotator:
                            token_rotator.mark_rate_limited(api_key, cooldown_seconds=3.0)
                        logging.info(f"NIM 429 rate limit on key ...{api_key[-6:]}. Rotating (attempt {attempt})...")
                        await asyncio.sleep(0.3)

                    # 400 Bad Request (context length overflow)
                    elif resp.status == 400:
                        resp_text = await resp.text()
                        logging.warning(f"NIM 400 Bad Request: {resp_text[:120]}. Shortening context and retrying...")
                        shorter = original_user_content[:15000] if len(original_user_content) > 15000 else original_user_content[:5000]
                        payload["messages"][1]["content"] = shorter
                        await asyncio.sleep(0.5)

                    # 404 Not Found (account deactivated / model endpoint issue)
                    elif resp.status == 404:
                        resp_text = await resp.text()
                        logging.warning(f"NIM 404 on key ...{api_key[-6:]}: {resp_text[:120]}. Deactivating key...")
                        if token_rotator:
                            token_rotator.mark_invalid(api_key)
                        await asyncio.sleep(0.1)

                    # 5xx Server Errors
                    elif resp.status in (500, 502, 503, 504):
                        backoff = min(15.0, 1.5 ** min(attempt, 4))
                        logging.warning(f"NIM server error {resp.status} on key ...{api_key[-6:]}. Backing off {backoff:.1f}s...")
                        await asyncio.sleep(backoff)

                    # Other status codes
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
                logging.warning(f"NIM API timeout on key ...{api_key[-6:]} (>35s, attempt {attempt}/{max_attempts}). Applying brief 2s pause...")
                if token_rotator and api_key:
                    token_rotator.mark_rate_limited(api_key, penalty_sec=2.0)
                await asyncio.sleep(min(1.0, 0.2 + attempt * 0.2))

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
            finally:
                if close_sess and active_sess:
                    await active_sess.close()

        finally:
            if token_rotator and api_key:
                token_rotator.release_key(api_key)

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
        return resultMap
    except Exception as e:
        print(f"Unexpected error: {e}")
        return resultMap
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
    active_sess = session
    close_sess = False
    if active_sess is None or active_sess.closed:
        active_sess = aiohttp.ClientSession(timeout=timeout)
        close_sess = True
    try:
        try:
            # The 'await' here ensures the process holds until completion
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

                    return resultMap
                else:
                    print(f"? Call Error: {await response.text()}")
                    return resultMap
        except Exception as e:
            print(f"Gemini_API_WITH_CACHE error: {e}")
            return resultMap
    finally:
        if close_sess and active_sess:
            await active_sess.close()


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
            tokens = [["", "", "", "", k] for k in NIM_HARDCODED_KEYS]
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

            # Sanitize secrets before writing fallback cache to disk
            sanitized_disk_cache = {
                "basic_config": basic_config,
                "funnel_prompts": {
                    "FunnelIdPromptMap": funnel_id_map,
                    "FunnelNamePromptMap": funnel_name_map,
                    "Feed_cache_Id": feed_cache_id_map,
                    "Feed_cache_Info_API_Token": {k: "" for k in feed_api_token_map},
                    "Feed_cache_Info_Model": feed_model_map,
                    "Feed_cache_Info_Gen_Date": feed_gen_date_map,
                    "Feed_cache_Info_Final_Status": feed_status_map,
                    "inputResult": []
                },
                "feed_mappings": feed_mappings,
                "nim_config": {
                    "NIM_Model_To_Pick": settings.Nvidia_Base_Model,
                    "tokenArray": []
                },
                "cache_id_IF_Prediction": cache_id_IF,
                "token_IF_Prediction": "",
                "date_IF_Prediction": date_IF,
                "status_IF_Prediction": status_IF,
                "exp_date_IF_Prediction": exp_date_IF,
                "timestamp": time.time()
            }

            for cf in [candidate_cache_files[0], candidate_cache_files[1]]:
                try:
                    with open(cf, "w") as f:
                        json.dump(sanitized_disk_cache, f)
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


def get_api_result(payload=None, api_url="https://platform.tracxn.com/api/2.2/domains", method='POST', token=None, max_retries=3):
    if method == '':
        method = 'POST'
    if payload is None:
        payload = {}
    if 'dataset' not in payload:
        payload['dataset'] = 'internal'

    for attempt in range(max_retries):
        if token and str(token).strip() and token != Tracxn_API_Token:
            tok = str(token).strip()
        elif 'TRACXN_KEY_MANAGER' in globals() and TRACXN_KEY_MANAGER is not None:
            tok = TRACXN_KEY_MANAGER.acquire_sync() if hasattr(TRACXN_KEY_MANAGER, "acquire_sync") else TRACXN_KEY_MANAGER.get_token()
        else:
            tok = token or Tracxn_API_Token

        if not tok:
            print("Please pass the Token as it is mandatory for this project")
            return {'error': 'Missing Tracxn token'}

        headers = {
            "X-Request-Source": "Discovery Team",
            "accessToken": tok,
            "cache-control": "no-cache",
            "Content-Type": "application/json"
        }
        options = {
            'headers': headers,
            'allow_redirects': True
        }
        if 'blank' not in payload:
            options['json'] = payload

        try:
            if method.upper() == 'POST':
                response = requests.post(api_url, **options)
            elif method.upper() == 'PUT':
                response = requests.put(api_url, headers=headers, json=payload)
            else:
                raise ValueError("Invalid HTTP method. Use 'POST' or 'PUT'.")

            if response.status_code in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                TRACXN_KEY_MANAGER.handle_http_error(tok, response.status_code)
                if attempt < max_retries - 1:
                    continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            status_code = getattr(getattr(e, 'response', None), 'status_code', None)
            if status_code in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                TRACXN_KEY_MANAGER.handle_http_error(tok, status_code)
                if attempt < max_retries - 1:
                    continue
            print(f"Error in getting API result (attempt {attempt+1}): {e}")
            if attempt == max_retries - 1:
                return {'error': str(e)}
    return {'error': 'Max retries exceeded'}


async def get_api_result_async(payload=None, api_url="https://platform.tracxn.com/api/2.2/domains", method='POST', token=None, session: aiohttp.ClientSession = None, max_retries: int = 3):
    if method == '':
        method = 'POST'
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
        for attempt in range(max_retries):
            if token and str(token).strip() and token != Tracxn_API_Token:
                tok = str(token).strip()
            elif 'TRACXN_KEY_MANAGER' in globals() and TRACXN_KEY_MANAGER is not None:
                tok = await TRACXN_KEY_MANAGER.acquire() if hasattr(TRACXN_KEY_MANAGER, "acquire") else TRACXN_KEY_MANAGER.get_token()
            else:
                tok = token or Tracxn_API_Token

            if not tok:
                print("Please pass the Token as it is mandatory for this project")
                return {'error': 'Missing Tracxn token'}

            headers = {
                "X-Request-Source": "Discovery Team",
                "accessToken": tok,
                "cache-control": "no-cache",
                "Content-Type": "application/json"
            }

            try:
                if method.upper() == 'POST':
                    async with active_sess.post(api_url, json=payload, headers=headers) as response:
                        if response.status in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                            TRACXN_KEY_MANAGER.handle_http_error(tok, response.status)
                            if attempt < max_retries - 1:
                                continue
                        response.raise_for_status()
                        return await response.json()
                elif method.upper() == 'PUT':
                    async with active_sess.put(api_url, json=payload, headers=headers) as response:
                        if response.status in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                            TRACXN_KEY_MANAGER.handle_http_error(tok, response.status)
                            if attempt < max_retries - 1:
                                continue
                        response.raise_for_status()
                        return await response.json()
                else:
                    raise ValueError("Invalid HTTP method. Use 'POST' or 'PUT'.")
            except aiohttp.ClientResponseError as e:
                if e.status in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                    TRACXN_KEY_MANAGER.handle_http_error(tok, e.status)
                    if attempt < max_retries - 1:
                        continue
                print(f"Error in getting API result (attempt {attempt+1}): {e}")
                if attempt == max_retries - 1:
                    return {'error': str(e)}
            except aiohttp.ClientError as e:
                print(f"Network error in getting API result (attempt {attempt+1}): {e}")
                if attempt == max_retries - 1:
                    return {'error': str(e)}
        return {'error': 'Max retries exceeded'}
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass

    

#**************************** Tag Addition ***********************************

# @timeout(20)
def Domain_HashTag(domain, newtag='NO', token=None):
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

async def Domain_HashTag_async(domain, newtag='NO', token=None, session: aiohttp.ClientSession = None):
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

async def Domain_HashTag_Remove_async(domain, remove_tag='NO', token=None, session: aiohttp.ClientSession = None):
    payload = {"filter": {"domain": [domain]}, "dataset": "internal"}
    api_url = "https://platform.tracxn.com/api/2.2/domains"

    response = await get_api_result_async(payload, api_url, 'POST', token, session=session)

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
#************************************************************************# ******************************* Funnel Movement - Caller  **********************************

def _resolve_tracxn_sync_token(token=None) -> str:
    if token and str(token).strip() and token != Tracxn_API_Token:
        return str(token).strip()
    if 'TRACXN_KEY_MANAGER' in globals() and TRACXN_KEY_MANAGER is not None:
        return TRACXN_KEY_MANAGER.acquire_sync() if hasattr(TRACXN_KEY_MANAGER, "acquire_sync") else TRACXN_KEY_MANAGER.get_token()
    return token or Tracxn_API_Token

async def _resolve_tracxn_async_token(token=None) -> str:
    if token and str(token).strip() and token != Tracxn_API_Token:
        return str(token).strip()
    if 'TRACXN_KEY_MANAGER' in globals() and TRACXN_KEY_MANAGER is not None:
        return await TRACXN_KEY_MANAGER.acquire() if hasattr(TRACXN_KEY_MANAGER, "acquire") else TRACXN_KEY_MANAGER.get_token()
    return token or Tracxn_API_Token

def WriteAPI_Domain_Funnel(domainProfileId, source_funnelID, filterGroupId, filterId, dest_Funnel, status, comments, token=None):
    if not domainProfileId or not source_funnelID or not dest_Funnel or not status:
        return 'FALSE'
    tok = _resolve_tracxn_sync_token(token)
    ret = 'NO'
    if status in ('SNOOZED', 'MARKED_IRRELEVANT', 'MARKED_FOR_LATER', 'ESCALATED'):
        Domain_Funnel_Assign(domainProfileId, source_funnelID, filterGroupId, filterId, comments, tok)
        time.sleep(0.5)
        ret = Change_Domain_Funnel_Status(domainProfileId, source_funnelID, filterGroupId, filterId, status, comments, tok)
    elif status == 'MOVED_OUT':
        Domain_Funnel_Assign(domainProfileId, source_funnelID, filterGroupId, filterId, comments, tok)
        time.sleep(0.5)
        ret = Change_Domain_Funnel_Moved_Out(domainProfileId, source_funnelID, filterGroupId, filterId, dest_Funnel, comments, tok)
    return 'TRUE' if ret == 'YES' else 'FALSE'

async def WriteAPI_Domain_Funnel_async(domainProfileId, source_funnelID, filterGroupId, filterId, dest_Funnel, status, comments, token=None, session: aiohttp.ClientSession = None):
    if not domainProfileId or not source_funnelID or not dest_Funnel or not status:
        return 'FALSE'

    close_sess = False
    active_sess = session
    if active_sess is None:
        active_sess = aiohttp.ClientSession()
        close_sess = True

    try:
        tok = str(token).strip() if (token and str(token).strip() and token != Tracxn_API_Token) else None
        if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
            # Bind single user token across all actions for this domain to preserve assignment identity
            tok = await TRACXN_CLIENT._acquire_token(tok)
            if status == 'MOVED_OUT':
                # Safely unsnoozes (if snoozed), force-assigns, and moves under the SAME user token
                move_res = await TRACXN_CLIENT.unsnooze_and_move_domain(
                    active_sess, domainProfileId, source_funnelID, filterGroupId, filterId, dest_Funnel, comments, token=tok, is_snoozed=True
                )
                if move_res == "UNASSIGNABLE":
                    return 'SKIPPED_UNASSIGNABLE'
                return 'TRUE' if move_res in ("YES", "ALREADY_MOVED") else 'FALSE'
            else:
                assign_res = await TRACXN_CLIENT.force_assign_domain(
                    active_sess, domainProfileId, source_funnelID, filterGroupId, filterId, comments, token=tok
                )
                if assign_res == "UNASSIGNABLE":
                    return 'SKIPPED_UNASSIGNABLE'
                ret = "NO"
                if status == 'SNOOZED':
                    ret = await TRACXN_CLIENT.snooze_domain(
                        active_sess, domainProfileId, source_funnelID, filterGroupId, filterId, comments, token=tok
                    )
                elif status == 'ESCALATED':
                    ret = await TRACXN_CLIENT.escalate_domain(
                        active_sess, domainProfileId, source_funnelID, filterGroupId, filterId, comments, token=tok
                    )
                elif status == 'MARKED_FOR_LATER':
                    ret = await TRACXN_CLIENT.mark_domain_for_later(
                        active_sess, domainProfileId, source_funnelID, filterGroupId, filterId, comments, token=tok
                    )
                elif status == 'MARKED_IRRELEVANT':
                    ret = await TRACXN_CLIENT.mark_domain_irrelevant(
                        active_sess, domainProfileId, source_funnelID, filterGroupId, filterId, comments, token=tok
                    )
                return 'TRUE' if ret == 'YES' else 'FALSE'
        else:
            await Domain_Funnel_Assign_async(domainProfileId, source_funnelID, filterGroupId, filterId, comments, tok, session=active_sess)
            await asyncio.sleep(0.5)
            if status in ('SNOOZED', 'MARKED_IRRELEVANT', 'MARKED_FOR_LATER', 'ESCALATED'):
                ret = await Change_Domain_Funnel_Status_async(domainProfileId, source_funnelID, filterGroupId, filterId, status, comments, tok, session=active_sess)
            elif status == 'MOVED_OUT':
                ret = await Change_Domain_Funnel_Moved_Out_async(domainProfileId, source_funnelID, filterGroupId, filterId, dest_Funnel, comments, tok, session=active_sess)
            else:
                ret = "NO"
            return 'TRUE' if ret == 'YES' else 'FALSE'
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass


# ********************* Funnel Movement - Status Change call ****************************

def Change_Domain_Funnel_Status(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token=None):
    ret = "NO"
    tok = _resolve_tracxn_sync_token(token)
    if status == 'SNOOZED':
        ret = Domain_Snoozed(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, tok)
    elif status == 'MARKED_IRRELEVANT':
        ret = Domain_MARKED_IRRELEVANT(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, tok)
    elif status == "ESCALATED":
        ret = asyncio.run(Domain_ESCALATED_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, tok))
    elif status == "MARKED_FOR_LATER":
        ret = asyncio.run(Domain_MARKED_FOR_LATER_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, tok))
    return ret

async def Change_Domain_Funnel_Status_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token=None, session: aiohttp.ClientSession = None):
    ret = "NO"
    if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
        close_sess = False
        active_sess = session
        if active_sess is None:
            active_sess = aiohttp.ClientSession()
            close_sess = True
        try:
            tok = str(token).strip() if (token and str(token).strip() and token != Tracxn_API_Token) else None
            if status == 'SNOOZED':
                ret = await TRACXN_CLIENT.snooze_domain(active_sess, domainProfileId, currentfunnelID, filterGroupId, filterId, comments, token=tok)
            elif status == 'MARKED_IRRELEVANT':
                ret = await TRACXN_CLIENT.mark_domain_irrelevant(active_sess, domainProfileId, currentfunnelID, filterGroupId, filterId, comments, token=tok)
            elif status == "ESCALATED":
                ret = await TRACXN_CLIENT.escalate_domain(active_sess, domainProfileId, currentfunnelID, filterGroupId, filterId, comments, token=tok)
            elif status == "MARKED_FOR_LATER":
                ret = await TRACXN_CLIENT.mark_domain_for_later(active_sess, domainProfileId, currentfunnelID, filterGroupId, filterId, comments, token=tok)
            return ret
        finally:
            if close_sess:
                try:
                    await active_sess.close()
                except Exception:
                    pass

    if status == 'SNOOZED':
        ret = await Domain_Snoozed_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token, session=session)
    elif status == 'MARKED_IRRELEVANT':
        ret = await Domain_MARKED_IRRELEVANT_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token, session=session)
    elif status == "ESCALATED":
        ret = await Domain_ESCALATED_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token, session=session)
    elif status == "MARKED_FOR_LATER":
        ret = await Domain_MARKED_FOR_LATER_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token, session=session)
    return ret

# ************************* Funnel Movement - MOVED_OUT *************************

def Change_Domain_Funnel_Moved_Out(domainProfileId, currentfunnelID, filterGroupId, filterId, dest_Funnel, comments, token=None):
    tok = _resolve_tracxn_sync_token(token)
    url = "https://platform.tracxn.com/data/funnel-action/move"
    headers = {"accessToken": tok, "Content-Type": 'application/json', "X-Request-Source": '123'}
    data = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "movedTo": [dest_Funnel],
        "comment": comments,
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
    time.sleep(1)
    response = requests.put(url, headers=headers, json=data)
    if response.status_code in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
        TRACXN_KEY_MANAGER.handle_http_error(tok, response.status_code)
    return "YES" if response.status_code == 200 else "NO"

async def Change_Domain_Funnel_Moved_Out_async(domainProfileId, currentfunnelID, filterGroupId, filterId, dest_Funnel, comments, token=None, session: aiohttp.ClientSession = None):
    if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
        close_sess = False
        active_sess = session
        if active_sess is None:
            active_sess = aiohttp.ClientSession()
            close_sess = True
        try:
            tok = str(token).strip() if (token and str(token).strip() and token != Tracxn_API_Token) else None
            return await TRACXN_CLIENT.move_domain_funnel(
                active_sess, domainProfileId, currentfunnelID, filterGroupId, filterId, dest_Funnel, comments, token=tok
            )
        finally:
            if close_sess:
                try:
                    await active_sess.close()
                except Exception:
                    pass

    tok = await _resolve_tracxn_async_token(token)
    url = "https://platform.tracxn.com/data/funnel-action/move"
    headers = {
        "accessToken": tok,
        "Content-Type": "application/json",
        "X-Request-Source": "123"
    }
    data = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "movedTo": [dest_Funnel],
        "comment": comments,
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
    close_sess = False
    active_sess = session
    if active_sess is None:
        active_sess = aiohttp.ClientSession()
        close_sess = True
    try:
        async with active_sess.put(url, headers=headers, json=data) as response:
            if response.status in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                TRACXN_KEY_MANAGER.handle_http_error(tok, response.status)
            return "YES" if response.status == 200 else "NO"
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass


# ******************************* Funnel Movement - SNOOZED **********************************

def Domain_Snoozed(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token=None):
    tok = _resolve_tracxn_sync_token(token)
    headers = {"accessToken": tok, "Content-Type": "application/json"}
    url = "https://platform.tracxn.com/data/funnel-action/snooze"
    data = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
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
    if response.status_code in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
        TRACXN_KEY_MANAGER.handle_http_error(tok, response.status_code)
    return "YES" if response.status_code == 200 else "NO"

async def Domain_Snoozed_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token=None, session: aiohttp.ClientSession = None):
    if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
        close_sess = False
        active_sess = session
        if active_sess is None:
            active_sess = aiohttp.ClientSession()
            close_sess = True
        try:
            tok = str(token).strip() if (token and str(token).strip() and token != Tracxn_API_Token) else None
            return await TRACXN_CLIENT.snooze_domain(
                active_sess, domainProfileId, currentfunnelID, filterGroupId, filterId, comments, token=tok
            )
        finally:
            if close_sess:
                try:
                    await active_sess.close()
                except Exception:
                    pass

    tok = await _resolve_tracxn_async_token(token)
    headers = {"accessToken": tok, "Content-Type": "application/json"}
    url = "https://platform.tracxn.com/data/funnel-action/snooze"
    data = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
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
    close_sess = False
    active_sess = session
    if active_sess is None:
        active_sess = aiohttp.ClientSession()
        close_sess = True
    try:
        async with active_sess.put(url, headers=headers, json=data) as response:
            if response.status in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                TRACXN_KEY_MANAGER.handle_http_error(tok, response.status)
            return "YES" if response.status == 200 else "NO"
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass


# *********************** Funnel Movement - ESCALATED *************************

async def Domain_ESCALATED_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token=None, session: aiohttp.ClientSession = None):
    if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
        close_sess = False
        active_sess = session
        if active_sess is None:
            active_sess = aiohttp.ClientSession()
            close_sess = True
        try:
            tok = str(token).strip() if (token and str(token).strip() and token != Tracxn_API_Token) else None
            return await TRACXN_CLIENT.escalate_domain(
                active_sess, domainProfileId, currentfunnelID, filterGroupId, filterId, comments, token=tok
            )
        finally:
            if close_sess:
                try:
                    await active_sess.close()
                except Exception:
                    pass

    tok = await _resolve_tracxn_async_token(token)
    url = "https://platform.tracxn.com/data/funnel-action/escalate"
    payload = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
        "sourceDetails": {"source": "Write API", "sourceData": {}}
    }
    if filterGroupId:
        payload["sourceDetails"]["sourceData"] = {
            "view": "Card",
            "tab": "Funnel Filter Group",
            "tabData": {"id": filterGroupId}
        }
        if filterId:
            payload["sourceDetails"]["sourceData"]["tabData"]["funnelFilterId"] = filterId

    headers = {"accessToken": tok, "Content-Type": "application/json"}
    close_sess = False
    active_sess = session
    if active_sess is None:
        active_sess = aiohttp.ClientSession()
        close_sess = True
    try:
        async with active_sess.put(url, headers=headers, json=payload) as response:
            if response.status in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                TRACXN_KEY_MANAGER.handle_http_error(tok, response.status)
            return "YES" if response.status == 200 else "NO"
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass


# ****************** Funnel Movement - MARKED_FOR_LATER *******************

async def Domain_MARKED_FOR_LATER_With_Tab_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token=None, session: aiohttp.ClientSession = None):
    if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
        close_sess = False
        active_sess = session
        if active_sess is None:
            active_sess = aiohttp.ClientSession()
            close_sess = True
        try:
            tok = str(token).strip() if (token and str(token).strip() and token != Tracxn_API_Token) else None
            return await TRACXN_CLIENT.mark_domain_for_later(
                active_sess, domainProfileId, currentfunnelID, filterGroupId, filterId, comments, token=tok
            )
        finally:
            if close_sess:
                try:
                    await active_sess.close()
                except Exception:
                    pass

    tok = await _resolve_tracxn_async_token(token)
    url = "https://platform.tracxn.com/data/funnel-action/mark-for-later"
    payload = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
        "sourceDetails": {"source": "Write API", "sourceData": {}}
    }
    if filterGroupId:
        payload["sourceDetails"]["sourceData"] = {
            "view": "Card",
            "tab": "Funnel Filter Group",
            "tabData": {"id": filterGroupId}
        }
        if filterId:
            payload["sourceDetails"]["sourceData"]["tabData"]["funnelFilterId"] = filterId

    headers = {"accessToken": tok, "Content-Type": "application/json"}
    close_sess = False
    active_sess = session
    if active_sess is None:
        active_sess = aiohttp.ClientSession()
        close_sess = True
    try:
        async with active_sess.put(url, headers=headers, json=payload) as response:
            if response.status in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                TRACXN_KEY_MANAGER.handle_http_error(tok, response.status)
            return "YES" if response.status == 200 else "NO"
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass


# *********************** Funnel Movement - MARKED_IRRELEVANT *****************

def Domain_MARKED_IRRELEVANT(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token=None):
    tok = _resolve_tracxn_sync_token(token)
    headers = {"accessToken": tok, "Content-Type": "application/json"}
    url = "https://platform.tracxn.com/data/funnel-action/mark-irrelevant"
    data = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
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
    if response.status_code in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
        TRACXN_KEY_MANAGER.handle_http_error(tok, response.status_code)
    return "YES" if response.status_code == 200 else "NO"

async def Domain_MARKED_IRRELEVANT_async(domainProfileId, currentfunnelID, filterGroupId, filterId, status, comments, token=None, session: aiohttp.ClientSession = None):
    if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
        close_sess = False
        active_sess = session
        if active_sess is None:
            active_sess = aiohttp.ClientSession()
            close_sess = True
        try:
            tok = str(token).strip() if (token and str(token).strip() and token != Tracxn_API_Token) else None
            return await TRACXN_CLIENT.mark_domain_irrelevant(
                active_sess, domainProfileId, currentfunnelID, filterGroupId, filterId, comments, token=tok
            )
        finally:
            if close_sess:
                try:
                    await active_sess.close()
                except Exception:
                    pass

    tok = await _resolve_tracxn_async_token(token)
    headers = {"accessToken": tok, "Content-Type": "application/json"}
    url = "https://platform.tracxn.com/data/funnel-action/mark-irrelevant"
    data = {
        "funnelId": currentfunnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
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
    close_sess = False
    active_sess = session
    if active_sess is None:
        active_sess = aiohttp.ClientSession()
        close_sess = True
    try:
        async with active_sess.put(url, headers=headers, json=data) as response:
            if response.status in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                TRACXN_KEY_MANAGER.handle_http_error(tok, response.status)
            return "YES" if response.status == 200 else "NO"
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass


# *******************************  Funnel Movement - Assign  **********************************

def Domain_Funnel_Assign(domainProfileId, funnelID, filterGroupId, filterId, comments, token=None):
    tok = _resolve_tracxn_sync_token(token)
    url = "https://platform.tracxn.com/data/funnel-action/force-assign"
    headers = {"accessToken": tok, "Content-Type": "application/json"}
    data = {
        "funnelId": funnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
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
    time.sleep(1)
    response = requests.put(url, headers=headers, json=data)
    if response.status_code in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
        TRACXN_KEY_MANAGER.handle_http_error(tok, response.status_code)
    return "YES" if response.status_code == 200 else "NO"

async def Domain_Funnel_Assign_async(domainProfileId, funnelID, filterGroupId, filterId, comments, token=None, session: aiohttp.ClientSession = None):
    if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
        close_sess = False
        active_sess = session
        if active_sess is None:
            active_sess = aiohttp.ClientSession()
            close_sess = True
        try:
            tok = str(token).strip() if (token and str(token).strip() and token != Tracxn_API_Token) else None
            return await TRACXN_CLIENT.force_assign_domain(
                active_sess, domainProfileId, funnelID, filterGroupId, filterId, comments, token=tok
            )
        finally:
            if close_sess:
                try:
                    await active_sess.close()
                except Exception:
                    pass

    tok = await _resolve_tracxn_async_token(token)
    url = "https://platform.tracxn.com/data/funnel-action/force-assign"
    headers = {"accessToken": tok, "Content-Type": "application/json"}
    data = {
        "funnelId": funnelID,
        "domainProfileId": domainProfileId,
        "comment": comments,
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
    await asyncio.sleep(1)
    close_sess = False
    active_sess = session
    if active_sess is None:
        active_sess = aiohttp.ClientSession()
        close_sess = True
    try:
        async with active_sess.put(url, headers=headers, json=data) as response:
            if response.status in (401, 403, 429) and 'TRACXN_KEY_MANAGER' in globals():
                TRACXN_KEY_MANAGER.handle_http_error(tok, response.status)
            return "YES" if response.status == 200 else "NO"
    finally:
        if close_sess:
            try:
                await active_sess.close()
            except Exception:
                pass


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
from common.industry_funnel_map import build_default_funnel_maps, resolve_industry_funnel
funnel_name_map, funnel_id_map, funnel_num_map = build_default_funnel_maps()
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

    is_57col = is_57_col_schema(row)
    final_raw_data = ""
    if is_57col:
        base_prompt_for_summery = summery_base_prompt
        if raw_data_beat_soup and safe_str(raw_data_beat_soup).strip():
            final_raw_data = safe_str(raw_data_beat_soup).strip()
        elif len(row) > 29 and safe_str(row[29]).strip():
            final_raw_data = safe_str(row[29]).strip()
    else:
        base_prompt_for_summery = row[38] if len(row) > 38 else summery_base_prompt
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

    if len(final_raw_data) > 15000:
        final_raw_data = final_raw_data[:15000]

    input_data_summery = final_raw_data  # sheet.getRange(i+1,1).getValue();
    output_data_summery = ""


    #Skip the Logic if domain is empty or finalRawData is empty or DONE
    # if not domain and len(row) >= 61 and row[61] == "DONE": # or not final_raw_data or row[61] == "DONE" or row[21] == "To be checked" or (row[44] and row[49]): #Check if domain exists or not,if not then skip to the next loop
    #     continue

    # logging.info(f"Started For :- {domain}")

    
    # ******************************* Gemini output for - Summery ***********************

    # Summary status: SUCCESS, NO_RAW_DATA, or FAILED (written to Col AN / 40)
    summary_status = ""

    # Smart auto-resume: prioritize Column AJ (idx 35) in 57-col schema or Column AO (idx 40) in legacy schema
    existing_summary = ""
    if is_57col:
        # 57-column schema: Col AI (idx 34) is NIM Status, Col AJ (idx 35) is NIM Summary
        if len(row) > 35 and safe_str(row[35]).strip() and not safe_str(row[35]).strip().startswith("http"):
            existing_summary = safe_str(row[35]).strip()
        elif len(row) > 36 and safe_str(row[36]).strip() and not safe_str(row[36]).strip().startswith("http"):
            existing_summary = safe_str(row[36]).strip()
    else:
        # Legacy 70/93-column schema: Col AO (idx 40) is NIM Summary, Col AP (idx 41) is base prompt
        if len(row) > 40 and safe_str(row[40]).strip() and safe_str(row[40]).strip() != safe_str(base_prompt_for_summery).strip() and not safe_str(row[40]).strip().startswith("http"):
            existing_summary = safe_str(row[40]).strip()
        elif len(row) > 41 and safe_str(row[41]).strip() and safe_str(row[41]).strip() != safe_str(if_pred_base_prompt).strip():
            existing_summary = safe_str(row[41]).strip()

    if precomputed_summary:
        output_data_summery = precomputed_summary
        summary_status = "SUCCESS"
        logging.info(f"Using precomputed cycle NIM summary for {domain} (length {len(output_data_summery)}).")
    elif existing_summary:
        output_data_summery = existing_summary
        if is_57col:
            summary_status = safe_str(row[34]).strip() if (len(row) > 34 and safe_str(row[34]).strip()) else "SUCCESS"
        else:
            summary_status = safe_str(row[39]).strip() if (len(row) > 39 and safe_str(row[39]).strip()) else "SUCCESS"
        logging.info(f"Smart auto-resume: reusing existing summary for {domain} (length {len(output_data_summery)}) with status {summary_status}")
    elif not final_raw_data:
        output_data_summery = ""
        summary_status = "NO_CONTENT"
        logging.info(f"No raw data found in Col AF or Col AL for {domain}. Marking summary status NO_CONTENT.")
    elif is_nim_dead_domain(final_raw_data):
        output_data_summery = ""
        summary_status = "NO_CONTENT"
        logging.info(f"Dead domain content detected for {domain}. Marking summary status NO_CONTENT.")
    elif input_data_summery:
        input_data_summery = Utility_Replace_Response(input_data_summery, base_prompt_for_summery, replacable_char)
        input_data_summery = Utility_Replace_Response(domain, input_data_summery, replacable_char_Domain)

        try:
            # 1. Primary Attempt: NVIDIA API Call (4 retries with key rotation)
            output_data_summery = await Nvidia_API_ASYNC(input_data_summery, token_rotator=token_rotator, max_attempts=4, session=session)
            
            # 2. Fallback Attempt: Gemini API Call if NVIDIA failed or returned empty result
            if not output_data_summery:
                logging.info(f"Nvidia API returned empty result for {domain}. Falling back to Gemini API...")
                
                try:
                    gemini_res = await Gemini_API_ASYNC(input_data_summery, session=session)
                    
                    # Extract Gemini metadata and output
                    if isinstance(gemini_res, dict):
                        summary_input_Token = gemini_res.get("input_Token", "")
                        summary_thought_Token = gemini_res.get("thinking_Token", "")
                        summary_cache_Token = gemini_res.get("cached_Token", "")
                        summary_output_Token = gemini_res.get("output_Token", "")
                        summary_total_Token = gemini_res.get("total_Token", "")
                        summary_llm_model = gemini_res.get("model", "")
                        output_data_summery = gemini_res.get("output", "")
                    else:
                        output_data_summery = ""

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

        if (is_57col and len(row) > 37 and safe_str(row[37]).strip()) or (not is_57col and len(row) > 44 and safe_str(row[44]).strip()):
            if is_57col:
                # 57-column schema: Col AL (idx 37) is IF Status, Col AM (idx 38) is IF Name, Col AN (idx 39) is IF ID
                output_data_if = safe_str(row[37]).strip()
                if len(row) > 38 and safe_str(row[38]).strip():
                    output_data_if_name = safe_str(row[38]).strip()
                if len(row) > 39 and safe_str(row[39]).strip():
                    output_data_if_id = safe_str(row[39]).strip()
            else:
                # Legacy 70/93-column schema: Col AS (idx 44) is IF Status, Col AT (idx 45) is IF Name, Col AU (idx 46) is IF ID
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
                    
                    if isinstance(output_data_if, dict):
                        output_data_if_input_Token = output_data_if.get("input_Token","")
                        output_data_if_thought_Token = output_data_if.get("thinking_Token","")
                        output_data_if_cache_Token = output_data_if.get("cached_Token","")
                        output_data_if_output_Token = output_data_if.get("output_Token","")
                        output_data_if_total_Token = output_data_if.get("total_Token","")
                        output_data_if_llm_model = output_data_if.get("model","")
                        output_data_if = output_data_if.get("output","")
                    else:
                        output_data_if = ""

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

                res_name, res_id = resolve_industry_funnel(output_data_if)
                if res_name and res_id:
                    output_data_if_name = res_name
                    output_data_if_id = res_id
                elif output_data_if in funnel_num_map:
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

    if is_57col and len(row) > 43 and safe_str(row[43]).strip():
        output_data_feed = safe_str(row[43]).strip()
        logging.info(f"Smart resume: reusing existing Feed prediction for {domain}: {output_data_feed}")
    elif not is_57col and len(row) > 54 and safe_str(row[54]).strip():
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

                if isinstance(output_data_feed, dict):
                    output_data_feed_input_Token = output_data_feed.get("input_Token","")
                    output_data_feed_thought_Token = output_data_feed.get("thinking_Token","")
                    output_data_feed_cache_Token = output_data_feed.get("cached_Token","")
                    output_data_feed_output_Token = output_data_feed.get("output_Token","")
                    output_data_feed_total_Token = output_data_feed.get("total_Token","")
                    output_data_feed_llm_model = output_data_feed.get("model","")
                    output_data_feed = output_data_feed.get("output","")
                else:
                    output_data_feed = ""

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
    is_57 = is_57_col_schema(row)
    if is_57:
        cf_if_status = safe_str(row[49]).strip() if len(row) > 49 else ""
        cf_if_time = safe_str(row[50]).strip() if len(row) > 50 else ""
        if_pf_status = safe_str(row[51]).strip() if len(row) > 51 else ""
        if_pf_time = safe_str(row[52]).strip() if len(row) > 52 else ""
        tag_status_if = safe_str(row[53]).strip() if len(row) > 53 else ""
        tag_status_pf = safe_str(row[54]).strip() if len(row) > 54 else ""
        tag_status_discovery = safe_str(row[56]).strip() if len(row) > 56 else ""
    else:
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
        if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
            token = await TRACXN_CLIENT._acquire_token()
        elif 'TRACXN_KEY_MANAGER' in globals() and TRACXN_KEY_MANAGER is not None:
            token = await TRACXN_KEY_MANAGER.acquire() if hasattr(TRACXN_KEY_MANAGER, "acquire") else TRACXN_KEY_MANAGER.get_token()

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
                # User directive: DO NOT SNOOZE unmapped/unresolved predictions.
                # Keep for retry when there are no more new domains to process.
                cf_if_status = "RETRY_UNMAPPED_IF"
                cf_if_time = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")
                logging.warning(f"Domain {domain} predicted IF '{output_data_if}' unmapped. Preserved as RETRY_UNMAPPED_IF (DO NOT SNOOZE).")

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

        # User rule: ONLY for cases where Publish Funnel (bu_llm_feed_prediction) is YES should Hashtag (bu_llm_discovery_processing) be added
        tag_status_discovery = ""
        if str(tag_status_pf).strip().upper() in ("YES", "TRUE"):
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
            is_57col = is_57_col_schema(rowData)
            min_len = 57 if is_57col else 70
            if len(rowData) < min_len:
                rowData = rowData + [""] * (min_len - len(rowData))

            if is_5am_ist_cutoff():
                logging.warning(f"⏰ 5:00 AM IST cutoff reached before processing {domain}. Halting.")
                return None

            # 0. Local CSV Redundancy Cache Hydration & Direct Write Check
            rowData, can_direct_write = LOCAL_CACHE.hydrate_row_from_cache(domain, rowData)
            if can_direct_write and mode == "full":
                logging.info(f"✨ [Local CSV Cache Hit] {domain} is fully completed in local cache and missing in sheet. Writing directly to sheet!")
                if is_57col:
                    full_57 = LOCAL_CACHE.build_57col_write_row(domain, rowData)
                    write_data = full_57[17:]
                    write_start_col = 18
                else:
                    write_data = LOCAL_CACHE.build_full_write_row(domain, rowData, is_cf_sheet=(EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME))
                    write_start_col = 20
                if write_to_sheet:
                    await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [write_data], row, write_start_col)
                return {
                    "tc_status": "SUCCESS",
                    "domain": domain,
                    "row": row,
                    "tc_job_id": rowData[21] if is_57col else (rowData[23] if len(rowData) > 23 else ""),
                    "rowData": rowData,
                    "write_data": write_data,
                    "start_col": write_start_col
                }

            if is_57col:
                engineClearedStatus = safe_str(rowData[20])
                tc_job_id = safe_str(rowData[21])
                tc_job_gen_time = safe_str(rowData[22])
                tc_status = safe_str(rowData[23])
                tc_final_url = safe_str(rowData[24])
                tc_s3_html = safe_str(rowData[25])
                tc_s3_markdown = safe_str(rowData[26])
                tc_http_status = safe_str(rowData[27])
                tc_signals = safe_str(rowData[28])
                scrappingBeautySoup = safe_str(rowData[29])
                existing_raw_data = scrappingBeautySoup
            else:
                engineClearedStatus = safe_str(rowData[21])
                tc_job_id = safe_str(rowData[23])
                tc_job_gen_time = safe_str(rowData[24])
                tc_status = safe_str(rowData[25])
                tc_final_url = safe_str(rowData[26])
                tc_s3_html = safe_str(rowData[27])
                tc_s3_markdown = safe_str(rowData[28])
                tc_http_status = safe_str(rowData[29])
                tc_signals = safe_str(rowData[30])
                scrappingBeautySoup = safe_str(rowData[31])
                existing_raw_data = scrappingBeautySoup or safe_str(rowData[37])

            if created_timestamp is None and tc_job_gen_time:
                try:
                    created_timestamp = datetime.datetime.strptime(tc_job_gen_time, "%d/%b/%Y %H:%M:%S").timestamp()
                except Exception:
                    created_timestamp = time.time()
            elif created_timestamp is None:
                created_timestamp = time.time()
            home_body = ""

            if existing_raw_data:
                home_body = existing_raw_data
                status_code = ""
            else:
                tc_client = TechCrawlerClient(token_rotator=TRACXN_KEY_MANAGER) if 'TRACXN_KEY_MANAGER' in globals() else TechCrawlerClient(token=Tracxn_API_Token)

                # 1. Generate Job ID if missing
                if not tc_job_id and domain:
                    if is_pre_cutoff_window() or is_5am_ist_cutoff():
                        logging.info(f"⏰ [Pre-Cutoff Wind-Down] Skipping new Tech Crawler job generation for {domain}")
                    else:
                        tc_job_id = await tc_client.generate_job_id(session, domain)
                        if tc_job_id:
                            tc_job_gen_time = datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                            created_timestamp = time.time()
                            tc_status = "PROCESSING"

                # 2. Track Job once if in progress
                if tc_job_id and tc_status in ("", "PROCESSING") and not home_body:
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
                if tc_status == "SUCCESS" and tc_s3_markdown and is_valid_s3_url(tc_s3_markdown):
                    signed_url = await tc_client.get_signed_s3_url(session, tc_s3_markdown)
                    if signed_url:
                        home_body = await tc_client.fetch_raw_markdown(session, signed_url) or ""
                        if home_body:
                            scrappingBeautySoup = home_body

                # Check 3-hour timeout rule ONLY if still PROCESSING after real-time check
                c_ts = created_timestamp if created_timestamp is not None else time.time()
                is_timeout = (time.time() - c_ts) >= (3 * 3600)
                if is_timeout and tc_status == "PROCESSING":
                    logging.warning(
                        f"Tech Crawler job {tc_job_id} for {domain} has been PROCESSING for >= 3 hours "
                        f"({(time.time() - c_ts)/3600:.1f}h). Switching to Waterfall Scraper fallback."
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
                    start_col_proc = 22 if is_57col else 24
                    if write_to_sheet:
                        try:
                            await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [[tc_job_id, tc_job_gen_time, tc_status]], row, start_col_proc)
                        except Exception as ex:
                            logging.warning(f"Could not write intermediate PROCESSING state to sheet for {domain}: {ex}")

                    logging.info(f"Tech Crawler job {tc_job_id} for {domain} is PROCESSING. Skipping fallback scraper and yielding to background poller.")
                    if is_57col:
                        new_row_data = list(rowData)
                        new_row_data[21] = tc_job_id
                        new_row_data[22] = tc_job_gen_time
                        new_row_data[23] = tc_status
                    else:
                        new_row_data = [
                            *rowData[:23], tc_job_id, tc_job_gen_time, tc_status, tc_final_url,
                            tc_s3_html, tc_s3_markdown, tc_http_status, tc_signals, scrappingBeautySoup,
                            *rowData[32:]
                        ]
                    return {
                        "tc_status": "PROCESSING",
                        "domain": domain,
                        "row": row,
                        "tc_job_id": tc_job_id,
                        "tc_job_gen_time": tc_job_gen_time,
                        "created_timestamp": created_timestamp,
                        "rowData": new_row_data,
                        "write_data": [tc_job_id, tc_job_gen_time, tc_status],
                        "start_col": start_col_proc
                    }

                # 3. If Tech Crawler succeeded, fetch signed S3 Markdown
                if tc_status == "SUCCESS" and tc_s3_markdown and is_valid_s3_url(tc_s3_markdown) and not home_body:
                    signed_url = await tc_client.get_signed_s3_url(session, tc_s3_markdown)
                    if signed_url:
                        home_body = await tc_client.fetch_raw_markdown(session, signed_url) or ""

                # 4. Fallback Rule: Switch to Waterfall Scraper ONLY if Tech Crawler FAILED or unable to scrape
                if not home_body and tc_status in ("FAILED", ""):
                    if is_pre_cutoff_window() or is_5am_ist_cutoff():
                        logging.info(f"⏰ [Pre-Cutoff Wind-Down] Skipping fallback waterfall scraper for {domain}")
                    else:
                        waterfall_scraper = WaterfallScraper()
                        home_body, scrape_method = await waterfall_scraper.scrape_domain(session, domain)
                        if home_body:
                            scrappingBeautySoup = home_body
                        logging.info(f"Fallback waterfall scraper for {domain} result: {scrape_method} (length: {len(home_body)})")

            body_results = home_body if home_body else ["Website is not working."]

            if mode == "phase1":
                if is_57col:
                    phase1_data = [
                        engineClearedStatus or "DONE", tc_job_id, tc_job_gen_time, tc_status,
                        tc_final_url, tc_s3_html, tc_s3_markdown, tc_http_status or "200",
                        tc_signals or "HTTP_OK", str(scrappingBeautySoup)[:49000],
                        tc_job_gen_time, tc_final_url, "", ""
                    ]
                    write_start_p1 = 21
                    new_row_data = list(rowData)
                    new_row_data[20:34] = phase1_data
                else:
                    phase1_data = [
                        tc_job_id, tc_job_gen_time, tc_status, tc_final_url,
                        tc_s3_html, tc_s3_markdown, tc_http_status, tc_signals,
                        scrappingBeautySoup
                    ]
                    write_start_p1 = 24
                    new_row_data = [
                        *rowData[:23], tc_job_id, tc_job_gen_time, tc_status, tc_final_url,
                        tc_s3_html, tc_s3_markdown, tc_http_status, tc_signals, scrappingBeautySoup,
                        *rowData[32:]
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
                    await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [phase1_data], row, write_start_p1)
                return {
                    "tc_status": tc_status,
                    "domain": domain,
                    "row": row,
                    "tc_job_id": tc_job_id,
                    "rowData": new_row_data,
                    "body": body_results,
                    "write_data": phase1_data,
                    "start_col": write_start_p1
                }

            AI_Engine_Result_Array = []
            Domain = rowData[0]
            skip_done = False
            domain_profile_id = rowData[10] if len(rowData) > 10 else ""
            filter_group_id = rowData[4] if len(rowData) > 4 else ""
            filter_id = rowData[6] if len(rowData) > 6 else ""
            if is_57col:
                crawl_status = safe_str(rowData[20]) if len(rowData) > 20 else ""
                enginePickedTime = safe_str(rowData[18]) if len(rowData) > 18 else ""
                engineClearedTime = safe_str(rowData[19]) if len(rowData) > 19 else ""
                engineClearedStatus = safe_str(rowData[20]) if len(rowData) > 20 else ""
                engineTimeTaken = ""
                # 57-column master schema: Col AD (index 29) is Raw Scraped Data
                Final_Scrapped_Raw_data = rowData[29] if (len(rowData) > 29 and safe_str(rowData[29]).strip()) else (scrappingBeautySoup or home_body)
                scrappingBeautySoup = safe_str(Final_Scrapped_Raw_data)
            else:
                crawl_status = safe_str(rowData[21]) if len(rowData) > 21 else ""
                enginePickedTime = safe_str(rowData[19]) if len(rowData) > 19 else ""
                engineClearedTime = safe_str(rowData[20]) if len(rowData) > 20 else ""
                engineClearedStatus = safe_str(rowData[21]) if len(rowData) > 21 else ""
                engineTimeTaken = rowData[22] if len(rowData) > 22 else ""
                Final_Scrapped_Raw_data = rowData[37] if (len(rowData) > 37 and safe_str(rowData[37]).strip()) else (scrappingBeautySoup or home_body)
                scrappingBeautySoup = safe_str(Final_Scrapped_Raw_data)

            currentFunnelName = rowData[24] if len(rowData) > 24 else ""
            currentFunnelID = rowData[25] if len(rowData) > 25 else ""
            currentFunnelStatus = rowData[26] if len(rowData) > 26 else ""
            movementBy = rowData[27] if len(rowData) > 27 else ""
            movementTime = rowData[28] if len(rowData) > 28 else ""
            movementStatus = rowData[29] if len(rowData) > 29 else ""
            scrappingPlaywright = safe_str(rowData[32]) if len(rowData) > 32 else ""
            scrappingSelenium = safe_str(rowData[33]) if len(rowData) > 33 else ""
            scrappingGoogleAPI = safe_str(rowData[34]) if len(rowData) > 34 else ""
            scrappingTracxnPF = safe_str(rowData[35]) if len(rowData) > 35 else ""
            geminiRawDataInput = safe_str(rowData[37]) if len(rowData) > 37 else ""
            geminiBasePromptSum = safe_str(rowData[38]) if len(rowData) > 38 else ""
            geminiRevQuerySum = safe_str(rowData[39]) if len(rowData) > 39 else ""

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
            scrappingBeautySoup = scrappingBeautySoup[:49000]
            scrappingPlaywright = scrappingPlaywright[:49000]
            scrappingSelenium = scrappingSelenium[:49000]
            scrappingGoogleAPI = scrappingGoogleAPI[:49000]
            scrappingTracxnPF = scrappingTracxnPF[:49000]
            geminiRawDataInput = geminiRawDataInput[:49000]
            geminiRevQuerySum = geminiRevQuerySum[:49000]

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
                raw_soup_49k = str(scrappingBeautySoup)[:49000]
                if is_57col:
                    phase2_data = [
                        raw_soup_49k,
                        tc_job_gen_time,
                        tc_final_url,
                        "",
                        "",
                        summary_status,
                        output_data_summery,
                    ]
                    write_start_p2 = 30
                    new_row_data = list(rowData)
                    new_row_data[29] = raw_soup_49k
                    new_row_data[34] = summary_status
                    new_row_data[35] = output_data_summery
                else:
                    phase2_data = [summary_status, output_data_summery]
                    write_start_p2 = 40
                    new_row_data = rowData
                LOCAL_CACHE.save_record(domain, {
                    "summary_status": summary_status,
                    "output_data_summery": output_data_summery,
                    "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                })
                if write_to_sheet:
                    await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [phase2_data], row, write_start_p2)
                return {
                    "tc_status": tc_status,
                    "domain": domain,
                    "row": row,
                    "tc_job_id": tc_job_id,
                    "summary_status": summary_status,
                    "summary": output_data_summery,
                    "rowData": new_row_data,
                    "write_data": phase2_data,
                    "start_col": write_start_p2
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
            industry_funnel_id = AI_Engine_Output_Result[8] if len(AI_Engine_Output_Result) > 8 else ""
            publish_funnel_id = AI_Engine_Output_Result[20] if len(AI_Engine_Output_Result) > 20 else ""
            out_if_check = str(AI_Engine_Output_Result[6] if len(AI_Engine_Output_Result) > 6 else "").strip().upper()

            valid_movement_terminals = {"DONE", "YES", "TRUE", "MOVED", "MOVED_OUT", "SNOOZED", "MARKED_IRRELEVANT", "MARKED_FOR_LATER", "ESCALATED", "SKIPPED_UNASSIGNABLE"}
            def _is_valid_ts_movement(s: str) -> bool:
                s = str(s).strip()
                return len(s) >= 8 and (":" in s or "/" in s or "-" in s) and s.upper() not in valid_movement_terminals and not s.startswith("http")

            is_moved = (
                str(cf_if_status).strip().upper() in valid_movement_terminals or
                str(if_pf_status).strip().upper() in valid_movement_terminals or
                _is_valid_ts_movement(cf_if_time) or
                _is_valid_ts_movement(if_pf_time)
            )

            # User rule: ONLY for cases where Publish Funnel (bu_llm_feed_prediction) is YES should Hashtag (bu_llm_discovery_processing) be added
            if str(tag_status_pf).strip().upper() in ("YES", "TRUE"):
                discovery_bq = "yes" if str(tag_status_discovery).upper() in ("YES", "TRUE") else "no"
            elif is_moved:
                # SNOOZED, MARKED_IRRELEVANT, ESCALATED, or PF not YES -> mark 'no'
                discovery_bq = "no"
            else:
                discovery_bq = ""

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

            # Prepare sheet write array
            is_57col = is_57_col_schema(rowData)
            raw_soup_49k = str(scrappingBeautySoup)[:49000]

            if is_57col:
                # 57-column optimized layout: Cols 18-57 (40 columns)
                # Raw data (scrappingBeautySoup) is capped at 49,000 chars per user specification
                full_write_row = [
                    safe_str(rowData[17]) or enginePickedTime, # Col 18 (R): Database Added Time
                    enginePickedTime,                           # Col 19 (S): Engine Pick Time
                    engineClearedTime,                          # Col 20 (T): Engine Clear Time
                    engineClearedStatus or "DONE",              # Col 21 (U): Crawling Status
                    tc_job_id,                                  # Col 22 (V): Job ID
                    tc_job_gen_time,                            # Col 23 (W): TimeStamp
                    tc_status,                                  # Col 24 (X): Status
                    tc_final_url,                               # Col 25 (Y): Final URL
                    tc_s3_html,                                 # Col 26 (Z): S3 HTML
                    tc_s3_markdown,                             # Col 27 (AA): S3 Markdown
                    tc_http_status or "200",                    # Col 28 (AB): HTTP Status
                    tc_signals or "HTTP_OK",                    # Col 29 (AC): Signals
                    raw_soup_49k,                               # Col 30 (AD): Raw Data (capped at 49,000 chars)
                    tc_job_gen_time,                            # Col 31 (AE): TimeStamp
                    tc_final_url,                               # Col 32 (AF): URL
                    "",                                         # Col 33 (AG): Error Code
                    "",                                         # Col 34 (AH): Error Reason
                    summary_status,                             # Col 35 (AI): NIM Status
                    output_data_summery,                        # Col 36 (AJ): NIM Summary
                    output_data_summery,                        # Col 37 (AK): Domain Summary Query
                    output_data_if,                             # Col 38 (AL): Gemini IF Number
                    output_data_if_name,                        # Col 39 (AM): IF Name
                    output_data_if_id,                          # Col 40 (AN): IF ID
                    output_data_if_name,                        # Col 41 (AO): IF Context
                    output_data_if_validation,                  # Col 42 (AP): IF Validation Flag
                    output_data_feed,                           # Col 43 (AQ): Feed Context
                    output_data_feed,                           # Col 44 (AR): Feed Number
                    final_feed_name,                            # Col 45 (AS): Feed Name
                    final_feed_id,                              # Col 46 (AT): Feed ID
                    final_pub_funnel_name,                      # Col 47 (AU): Publish Funnel Name
                    final_pub_funnel_id,                        # Col 48 (AV): Publish Funnel ID
                    "",                                         # Col 49 (AW): Funnel Recheck
                    cf_if_status,                               # Col 50 (AX): Write API Status CF -> IF
                    cf_if_time,                                 # Col 51 (AY): Movement Time
                    if_pf_status,                               # Col 52 (AZ): Write API Status Publish Funnel
                    if_pf_time,                                 # Col 53 (BA): Movement Time
                    tag_status_if,                              # Col 54 (BB): Tag Status IF
                    tag_status_pf,                              # Col 55 (BC): Tag Status PF
                    "",                                         # Col 56 (BD): Review Tag
                    discovery_bq                                # Col 57 (BE): Hashtag bu_llm_discovery_processing
                ]
                AI_Engine_Result_Array.append(full_write_row)
                write_start_col = 18
            elif EXTRACTING_SHEET_NAME == CF_EXTRACTING_SHEET_NAME:
                full_write_row = [
                    enginePickedTime, engineClearedTime, engineClearedStatus, engineTimeTaken, 
                    tc_job_id, tc_job_gen_time, tc_status, tc_final_url, tc_s3_html, tc_s3_markdown, 
                    tc_http_status, tc_signals, raw_soup_49k, scrappingPlaywright, scrappingSelenium, 
                    scrappingGoogleAPI, scrappingTracxnPF, rowData[36] if len(rowData) > 36 else "", "", base_prompt_for_summery, 
                    summary_status, output_data_summery, base_prompt_for_if, input_data_for_if, 
                    output_data_if, output_data_if_name, output_data_if_id, input_sumry_for_if_validation, 
                    base_prompt_for_if_validation, input_data_for_if_validation, output_data_if_validation, 
                    input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed, output_data_feed, 
                    final_feed_name, final_feed_id, final_pub_funnel_name, final_pub_funnel_id, "", "", "", 
                    cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf, "", discovery_bq
                ]
                AI_Engine_Result_Array.append(full_write_row)
                write_start_col = 20
            else:
                full_write_row = [
                    enginePickedTime, engineClearedTime, engineClearedStatus, engineTimeTaken, 
                    tc_job_id, tc_job_gen_time, tc_status, tc_final_url, tc_s3_html, tc_s3_markdown, 
                    tc_http_status, tc_signals, raw_soup_49k, scrappingPlaywright, scrappingSelenium, 
                    scrappingGoogleAPI, scrappingTracxnPF, rowData[36] if len(rowData) > 36 else "", "", base_prompt_for_summery, 
                    summary_status, output_data_summery, base_prompt_for_if_validation, 
                    input_data_for_if_validation, output_data_if_validation, input_sumry_for_if_validation, 
                    base_prompt_for_if, input_data_for_if, output_data_if, output_data_if_name, 
                    output_data_if_id, input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed, 
                    output_data_feed, final_feed_name, final_feed_id, final_pub_funnel_name, 
                    final_pub_funnel_id, "", "", "", cf_if_status, cf_if_time, if_pf_status, 
                    if_pf_time, tag_status_if, tag_status_pf, "", discovery_bq
                ]
                AI_Engine_Result_Array.append(full_write_row)
                write_start_col = 20

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
                "scrappingBeautySoup": raw_soup_49k,
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

            if write_to_sheet and full_write_row:
                await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [full_write_row], row, write_start_col)

            return {
                "tc_status": tc_status,
                "domain": domain,
                "row": row,
                "tc_job_id": tc_job_id,
                "rowData": [
                    *rowData[:23], tc_job_id, tc_job_gen_time, tc_status, tc_final_url,
                    tc_s3_html, tc_s3_markdown, tc_http_status, tc_signals, raw_soup_49k,
                    *rowData[32:]
                ],
                "body": body_results,
                "write_data": full_write_row,
                "start_col": write_start_col
            }

        except Exception as e:
            logging.error(f"Error processing {domain}: {e}")
            return None
        
async def reconcile_existing_tech_crawler_jobs(
    session,
    rows,
    client,
    SHEET_ID,
    sheet_name,
    start_row_num=3,
    sheet_write_queue=None,
    domain_queue=None,
    batch_size=50
):
    """
    When resuming/starting a run:
    Finds all rows that already have a tc_job_id but where scraping is not yet completed.
    Sweeps them in batches of 50 domains:
    - Retries each candidate once with tc_client.track_job_id.
    - If SUCCESS and s3_markdown exists: retrieves markdown, updates row/cache/sheet to SUCCESS.
    - If status is FAILED or still PROCESSING after >= 3 hours from creation: marks as FAILED immediately (no retry).
      When normal workers reach this row, they immediately run the Waterfall Scraper fallback.
    - If still PROCESSING and < 3 hours old: retains in pending_tech_crawler_jobs to be monitored
      in the background 15-minute retry loop.
    Returns the list of remaining pending jobs (< 3 hours old) for the background poller.
    """
    candidates = []
    for idx, r in enumerate(rows):
        if not r or not r[0]:
            continue
        if is_row_fully_completed(r):
            continue
        is_57_row = is_57_col_schema(r)
        min_len = 57 if is_57_row else 70
        if len(r) < min_len:
            r = r + [""] * (min_len - len(r))
        tc_job_id = safe_str(r[21] if is_57_row else r[23]).strip()
        tc_status = safe_str(r[23] if is_57_row else r[25]).strip().upper()
        scraped = safe_str(r[29] if is_57_row else r[31]).strip()
        summary = safe_str(r[35] if is_57_row else r[40]).strip() if len(r) > (35 if is_57_row else 40) else ""
        if not scraped and tc_status != "SUCCESS" and not summary:
            candidates.append((start_row_num + idx, r))

    if not candidates:
        return []

    total_candidates = len(candidates)
    logging.info(f"🔄 Startup Sweep: Reconciling {total_candidates} pending Tech Crawler jobs in batches of {batch_size}...")

    tc_client = TechCrawlerClient(token_rotator=TRACXN_KEY_MANAGER) if 'TRACXN_KEY_MANAGER' in globals() else TechCrawlerClient(token=Tracxn_API_Token)
    reconcile_sem = asyncio.Semaphore(10)
    pending_retained = []
    resolved_success = 0
    resolved_failed = 0

    num_batches = (total_candidates + batch_size - 1) // batch_size

    for b_idx in range(num_batches):
        if is_5am_ist_cutoff() or os.path.exists(STOP_FILE):
            logging.warning("Cutoff or stop requested during Tech Crawler bulk sweep. Halting sweep.")
            break

        batch = candidates[b_idx * batch_size : (b_idx + 1) * batch_size]
        batch_num = b_idx + 1

        async def _check_one_candidate(r_num, row_data):
            async with reconcile_sem:
                await asyncio.sleep(0.05)
                domain = row_data[0]
                rec = LOCAL_CACHE.get_record(domain)
                is_57_row = is_57_col_schema(row_data)

                # If already scraped or completed by a concurrent foreground worker, skip
                already_raw = safe_str(row_data[29]).strip() if is_57_row else safe_str(row_data[31]).strip()
                if already_raw or (isinstance(rec, dict) and (rec.get("scrappingBeautySoup") or rec.get("tc_status") in ("SUCCESS", "FAILED"))):
                    return ("SKIP", r_num, None, None)

                job_id = row_data[21] if is_57_row else row_data[23]
                c_ts = None
                tc_job_gen_time = safe_str(row_data[22] if is_57_row else row_data[24]).strip()

                # If Tech Crawler job is not yet created, generate it now
                if not job_id and domain:
                    if is_pre_cutoff_window() or is_5am_ist_cutoff():
                        logging.info(f"⏰ [Pre-Cutoff Wind-Down] Skipping new Tech Crawler job generation for {domain}")
                        return ("SKIP", r_num, None, None)
                    job_id = await tc_client.generate_job_id(session, domain)
                    if not job_id:
                        logging.warning(f"Failed to generate Tech Crawler job for {domain}")
                        return ("SKIP", r_num, None, None)
                    tc_job_gen_time = datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                    c_ts = time.time()
                    if is_57_row:
                        row_data[20] = "DONE"
                        row_data[21] = job_id
                        row_data[22] = tc_job_gen_time
                        row_data[23] = "PROCESSING"
                        initial_slice = [
                            "DONE", job_id, tc_job_gen_time, "PROCESSING", "", "", "", "", "", "", tc_job_gen_time, "", "", ""
                        ]
                    else:
                        row_data[21] = "DONE"
                        row_data[23] = job_id
                        row_data[24] = tc_job_gen_time
                        row_data[25] = "PROCESSING"
                        initial_slice = [
                            job_id, tc_job_gen_time, "PROCESSING", "", "", "", "", "", ""
                        ]
                    LOCAL_CACHE.save_record(domain, {
                        "tc_job_id": job_id,
                        "tc_job_gen_time": tc_job_gen_time,
                        "tc_status": "PROCESSING",
                        "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                    })
                    # Track once in case it resolved instantaneously
                    try:
                        track_res = await tc_client.track_job_id(session, job_id)
                        if track_res.get("status") == "SUCCESS" and track_res.get("s3_markdown"):
                            signed_url = await tc_client.get_signed_s3_url(session, track_res["s3_markdown"])
                            if signed_url:
                                md = await tc_client.fetch_raw_markdown(session, signed_url)
                                if md:
                                    if is_57_row:
                                        row_data[23] = "SUCCESS"
                                        if track_res.get("final_url"): row_data[24] = track_res["final_url"]
                                        if track_res.get("s3_html"): row_data[25] = track_res["s3_html"]
                                        row_data[26] = track_res["s3_markdown"]
                                        if track_res.get("http_status"): row_data[27] = str(track_res["http_status"])
                                        if track_res.get("signals"): row_data[28] = str(track_res["signals"])
                                        row_data[29] = md
                                        sheet_slice = [
                                            "DONE", job_id, tc_job_gen_time, "SUCCESS", row_data[24],
                                            row_data[25], row_data[26], row_data[27], row_data[28],
                                            md[:49000], tc_job_gen_time, row_data[24], "", ""
                                        ]
                                    else:
                                        row_data[25] = "SUCCESS"
                                        if track_res.get("final_url"): row_data[26] = track_res["final_url"]
                                        if track_res.get("s3_html"): row_data[27] = track_res["s3_html"]
                                        row_data[28] = track_res["s3_markdown"]
                                        if track_res.get("http_status"): row_data[29] = str(track_res["http_status"])
                                        if track_res.get("signals"): row_data[30] = str(track_res["signals"])
                                        row_data[31] = md
                                        sheet_slice = [
                                            row_data[23], row_data[24], "SUCCESS", row_data[26],
                                            row_data[27], row_data[28], row_data[29], row_data[30],
                                            row_data[31][:49000]
                                        ]
                                    LOCAL_CACHE.save_record(domain, {
                                        "tc_job_id": job_id,
                                        "tc_status": "SUCCESS",
                                        "tc_final_url": row_data[24 if is_57_row else 26],
                                        "tc_s3_html": row_data[25 if is_57_row else 27],
                                        "tc_s3_markdown": row_data[26 if is_57_row else 28],
                                        "tc_http_status": row_data[27 if is_57_row else 29],
                                        "tc_signals": row_data[28 if is_57_row else 30],
                                        "scrappingBeautySoup": md,
                                        "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                                    })
                                    return ("SUCCESS", r_num, sheet_slice, row_data)
                    except Exception:
                        pass
                    return ("NEW_JOB", r_num, initial_slice, {
                        "domain": domain,
                        "row": r_num,
                        "tc_job_id": job_id,
                        "tc_job_gen_time": tc_job_gen_time,
                        "created_timestamp": c_ts,
                        "rowData": row_data
                    })

                if tc_job_gen_time:
                    try:
                        c_ts = datetime.datetime.strptime(tc_job_gen_time, "%d/%b/%Y %H:%M:%S").timestamp()
                    except Exception:
                        c_ts = None
                if c_ts is None:
                    c_ts = time.time()

                age_sec = time.time() - c_ts
                is_over_3hr = age_sec >= (3 * 3600)

                try:
                    track_res = await tc_client.track_job_id(session, job_id)
                    status = track_res.get("status")

                    if status == "SUCCESS" and track_res.get("s3_markdown"):
                        signed_url = await tc_client.get_signed_s3_url(session, track_res["s3_markdown"])
                        if signed_url:
                            md = await tc_client.fetch_raw_markdown(session, signed_url)
                            if md:
                                if is_57_row:
                                    row_data[20] = "DONE"
                                    row_data[23] = "SUCCESS"
                                    if track_res.get("final_url"): row_data[24] = track_res["final_url"]
                                    if track_res.get("s3_html"): row_data[25] = track_res["s3_html"]
                                    row_data[26] = track_res["s3_markdown"]
                                    if track_res.get("http_status"): row_data[27] = str(track_res["http_status"])
                                    if track_res.get("signals"): row_data[28] = str(track_res["signals"])
                                    row_data[29] = md
                                    LOCAL_CACHE.save_record(domain, {
                                        "tc_job_id": job_id,
                                        "tc_status": "SUCCESS",
                                        "tc_final_url": row_data[24],
                                        "tc_s3_html": row_data[25],
                                        "tc_s3_markdown": row_data[26],
                                        "tc_http_status": row_data[27],
                                        "tc_signals": row_data[28],
                                        "scrappingBeautySoup": md,
                                        "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                                    })
                                    sheet_slice = [
                                        "DONE", job_id, tc_job_gen_time, "SUCCESS", row_data[24],
                                        row_data[25], row_data[26], row_data[27], row_data[28],
                                        md[:49000], tc_job_gen_time, row_data[24], "", ""
                                    ]
                                else:
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
                                        row_data[23], row_data[24], "SUCCESS", row_data[26],
                                        row_data[27], row_data[28], row_data[29], row_data[30],
                                        row_data[31][:49000]
                                    ]
                                return ("SUCCESS", r_num, sheet_slice, row_data)

                    if status == "FAILED" or is_over_3hr:
                        LOCAL_CACHE.save_record(domain, {
                            "tc_job_id": job_id,
                            "tc_status": "FAILED",
                            "updated_at": datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                        })
                        if is_57_row:
                            row_data[20] = "DONE"
                            row_data[23] = "FAILED"
                            sheet_slice = [
                                "DONE", job_id, tc_job_gen_time, "FAILED", "", "", "", "", "", "", tc_job_gen_time, "", "", ""
                            ]
                        else:
                            row_data[25] = "FAILED"
                            sheet_slice = [
                                row_data[23], row_data[24], "FAILED", row_data[26] if len(row_data) > 26 else "",
                                row_data[27] if len(row_data) > 27 else "", row_data[28] if len(row_data) > 28 else "",
                                row_data[29] if len(row_data) > 29 else "", row_data[30] if len(row_data) > 30 else "",
                                ""
                            ]
                        return ("FAILED", r_num, sheet_slice, None)

                    # Still PROCESSING and < 3 hours old: retain for 15-min background loop
                    return ("RETAIN", r_num, None, {
                        "domain": domain,
                        "row": r_num,
                        "tc_job_id": job_id,
                        "tc_job_gen_time": tc_job_gen_time,
                        "created_timestamp": c_ts,
                        "rowData": row_data
                    })

                except Exception as e:
                    logging.debug(f"Error checking candidate {job_id} for {domain}: {e}")
                    if is_over_3hr:
                        if is_57_row:
                            row_data[20] = "DONE"
                            row_data[23] = "FAILED"
                            sheet_slice = [
                                "DONE", job_id, tc_job_gen_time, "FAILED", "", "", "", "", "", "", tc_job_gen_time, "", "", ""
                            ]
                        else:
                            row_data[25] = "FAILED"
                            sheet_slice = [
                                row_data[23], row_data[24], "FAILED", "", "", "", "", "", ""
                            ]
                        return ("FAILED", r_num, sheet_slice, None)
                    return ("RETAIN", r_num, None, {
                        "domain": domain,
                        "row": r_num,
                        "tc_job_id": job_id,
                        "tc_job_gen_time": tc_job_gen_time,
                        "created_timestamp": c_ts,
                        "rowData": row_data
                    })

        tasks = [_check_one_candidate(r_num, r) for r_num, r in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if res and not isinstance(res, Exception):
                outcome, r_num, sheet_slice, item = res
                if outcome == "SKIP":
                    continue
                is_57_c = (len(sheet_slice) == 14) if sheet_slice else False
                target_col_cand = 21 if is_57_c else 24
                if outcome == "SUCCESS":
                    resolved_success += 1
                    if sheet_write_queue:
                        await sheet_write_queue.put((r_num, target_col_cand, sheet_slice))
                    elif client:
                        try: await write_sheet_data(client, SHEET_ID, sheet_name, [sheet_slice], r_num, target_col_cand)
                        except Exception: pass
                    if domain_queue is not None and item:
                        idx = r_num - start_row_num
                        await domain_queue.put((idx, item))
                        logging.info(f"⚡ [Auto Re-Queue] Newly scraped domain '{item[0]}' from row {r_num} fed into NIM pipeline")
                elif outcome == "FAILED":
                    resolved_failed += 1
                    if sheet_write_queue:
                        await sheet_write_queue.put((r_num, target_col_cand, sheet_slice))
                    elif client:
                        try: await write_sheet_data(client, SHEET_ID, sheet_name, [sheet_slice], r_num, target_col_cand)
                        except Exception: pass
                elif outcome in ("RETAIN", "NEW_JOB") and item:
                    if outcome == "NEW_JOB" and sheet_slice:
                        if sheet_write_queue:
                            await sheet_write_queue.put((r_num, target_col_cand, sheet_slice))
                        elif client:
                            try: await write_sheet_data(client, SHEET_ID, sheet_name, [sheet_slice], r_num, target_col_cand)
                            except Exception: pass
                    pending_retained.append(item)

        processed_so_far = min((b_idx + 1) * batch_size, total_candidates)
        try:
            if os.path.exists(PROGRESS_FILE):
                with open(PROGRESS_FILE, 'r') as pf:
                    pdata = json.load(pf)
                pdata['pending_tc'] = len(pending_retained)
                pdata['stage_details'] = f"Streaming active | Reconciling TC jobs: batch {batch_num}/{num_batches} ({processed_so_far}/{total_candidates} checked | {resolved_success} SUCCESS, {resolved_failed} FAILED)"
                pdata['tc_success'] = pre_tc_scraped + resolved_success
                pdata['scraped_count'] = pre_tc_completed + resolved_success + resolved_failed
                pdata['success'] = pdata['tc_success']
                tmp_path = f"{PROGRESS_FILE}.tmp.{os.getpid()}"
                with open(tmp_path, 'w') as pf:
                    json.dump(pdata, pf)
                os.replace(tmp_path, PROGRESS_FILE)
        except Exception:
            pass
        logging.info(
            f"🔄 Batch {batch_num}/{num_batches} ({len(batch)} jobs) sweep complete: "
            f"+{resolved_success} SUCCESS, +{resolved_failed} FAILED, {len(pending_retained)} pending (<3h)"
        )
        # Yield briefly between batches to prevent CPU spikes and prioritize streaming workers
        await asyncio.sleep(0.5)

    logging.info(
        f"✅ Startup Tech Crawler sweep complete! Total checked: {total_candidates} | "
        f"Resolved to SUCCESS: {resolved_success} | Marked FAILED (>3h): {resolved_failed} | "
        f"Retained for 15m polling loop (<3h): {len(pending_retained)}"
    )
    return pending_retained



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
    start_row_num=3,
    mode="full",
):

    pf_data = await read_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, start_row=start_row_num)

    total_domains = sum(1 for r in pf_data if r and safe_str(r[0]).strip())
    is_57 = is_57_col_schema(pf_data[0] if pf_data else None)
    tc_col_idx = 23 if is_57 else 25
    nim_col_idx = 34 if is_57 else 40
    gemini_col_idx = 43 if is_57 else 48
    tracxn_col_idx = 49 if is_57 else 61

    pre_tc_scraped = sum(1 for r in pf_data if (len(r) > tc_col_idx and safe_str(r[tc_col_idx]).strip().upper() == "SUCCESS") or (len(r) > (29 if is_57 else 31) and safe_str(r[29 if is_57 else 31]).strip()))
    pre_tc_failed = sum(1 for r in pf_data if len(r) > tc_col_idx and safe_str(r[tc_col_idx]).strip().upper() == "FAILED")
    pre_tc_completed = min(total_domains, pre_tc_scraped + pre_tc_failed)

    pre_nim_success = sum(1 for r in pf_data if len(r) > nim_col_idx and safe_str(r[nim_col_idx]).strip().upper() == "SUCCESS")
    pre_nim_no_content = sum(1 for r in pf_data if len(r) > nim_col_idx and safe_str(r[nim_col_idx]).strip().upper() in ("NO_CONTENT", "FAILED_NO_CONTENT"))
    pre_nim_completed = min(pre_tc_scraped, pre_nim_success + pre_nim_no_content)

    pre_gemini_if = sum(1 for r in pf_data if len(r) > (37 if is_57 else 44) and safe_str(r[37 if is_57 else 44]).strip())
    pre_gemini_feed = sum(1 for r in pf_data if len(r) > gemini_col_idx and safe_str(r[gemini_col_idx]).strip())
    pre_gemini_completed = min(pre_nim_success, max(pre_gemini_if, pre_gemini_feed))

    pre_completed = min(pre_gemini_completed, sum(1 for r in pf_data if is_row_fully_completed(r)))

    processed = 0
    success_count = pre_tc_scraped
    fail_count = 0
    tc_completed_count = pre_tc_completed
    nim_completed_count = pre_nim_completed
    gemini_completed_count = pre_gemini_completed
    tracxn_completed_count = pre_completed
    phase_title = f"Phase 1: Initial Crawl & Predictions ({mode})" if mode == "full" else f"Mode: {mode.upper()}"
    update_progress_file(
        tracxn_completed_count, total_domains, success_count, fail_count,
        phase=phase_title,
        stage_details=f"Starting batch processing of {total_domains} rows ({pre_completed} already completed)...",
        pending_tc=0, mode=mode,
        scraped_count=tc_completed_count,
        tc_success=pre_tc_scraped,
        nim_count=nim_completed_count,
        gemini_count=gemini_completed_count,
        tracxn_count=tracxn_completed_count
    )

    semaphore = asyncio.Semaphore(MAX_WORKERS)
    pending_tech_crawler_jobs = []

    is_57col_sheet = is_57_col_schema(pf_data[0] if pf_data else None)
    if mode == "phase1":
        target_start_col = 21 if is_57col_sheet else 24
        expected_cols = 14 if is_57col_sheet else 9
    elif mode == "phase2":
        target_start_col = 35 if is_57col_sheet else 40
        expected_cols = 2
    else:
        target_start_col = 18 if is_57col_sheet else 20
        expected_cols = 40 if is_57col_sheet else 50

    sheet_write_queue = asyncio.Queue()

    connector = aiohttp.TCPConnector(
        limit=100,
        limit_per_host=30,
        ttl_dns_cache=300
    )
    FLUSH_FILE = os.path.join(os.path.dirname(__file__), ".flush_requested")
    async with aiohttp.ClientSession(headers=mainheaders, connector=connector) as session:
        # Background non-blocking Google Sheets writer
        def _sanitize_val(v):
            if isinstance(v, str) and len(v) > 49000:
                return v[:49000]
            return v

        async def _flush_sheet_buffer(buf):
            if not buf:
                return

            # If Google Sheets is in document-too-large circuit breaker state, skip GSheet writes and record to pending sync
            if GSHEET_CIRCUIT_BREAKER.should_skip_gsheet():
                for r_num, c_num, val in buf:
                    GSHEET_CIRCUIT_BREAKER.record_pending_sync(PENDING_SYNC_FILE, "unknown", r_num, c_num, val)
                return

            # Deduplicate buffer keeping latest update per (row_number, col_number)
            deduped = {}
            for r_num, c_num, val in buf:
                deduped[(r_num, c_num)] = val

            by_col = {}
            for (r_num, c_num), val in deduped.items():
                sanitized_val = [_sanitize_val(x) for x in val] if isinstance(val, list) else _sanitize_val(val)
                by_col.setdefault(c_num, []).append((r_num, sanitized_val))

            for c_num, items in by_col.items():
                items.sort(key=lambda x: x[0])
                groups = []
                curr_grp = [items[0]]
                for it in items[1:]:
                    if it[0] == curr_grp[-1][0] + 1:
                        curr_grp.append(it)
                    else:
                        groups.append(curr_grp)
                        curr_grp = [it]
                groups.append(curr_grp)

                for grp in groups:
                    start_r = grp[0][0]
                    chunk_rows = [x[1] for x in grp]
                    try:
                        await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, chunk_rows, start_r, c_num)
                        await asyncio.sleep(0.5)
                    except Exception as ex:
                        if is_doc_too_large_error(ex):
                            GSHEET_CIRCUIT_BREAKER.trip(str(ex))
                            for r_num, val in grp:
                                GSHEET_CIRCUIT_BREAKER.record_pending_sync(PENDING_SYNC_FILE, "unknown", r_num, c_num, val)
                            return

                        logging.warning(f"Batch write failed for rows {start_r}-{start_r + len(chunk_rows)-1} col {c_num}: {ex}. Trying chunked fallback (5 rows)...")
                        # Chunked fallback in batches of 5 to isolate rogue rows (inspired by publishing-sr)
                        chunk_size = 5
                        for ci in range(0, len(chunk_rows), chunk_size):
                            sub_rows = chunk_rows[ci : ci + chunk_size]
                            sub_start = start_r + ci
                            try:
                                await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, sub_rows, sub_start, c_num)
                                await asyncio.sleep(0.2)
                            except Exception as sub_ex:
                                if is_doc_too_large_error(sub_ex):
                                    GSHEET_CIRCUIT_BREAKER.trip(str(sub_ex))
                                    for r_num, val in grp:
                                        GSHEET_CIRCUIT_BREAKER.record_pending_sync(PENDING_SYNC_FILE, "unknown", r_num, c_num, val)
                                    break
                                logging.error(f"Chunked fallback failed for rows {sub_start}-{sub_start + len(sub_rows)-1} col {c_num}: {sub_ex}")

        async def sheet_writer_worker():
            buffer = []
            last_flush = time.time()
            while True:
                try:
                    try:
                        item = await asyncio.wait_for(sheet_write_queue.get(), timeout=1.0)
                        if item is None:
                            break
                        buffer.append(item)
                        sheet_write_queue.task_done()
                    except asyncio.TimeoutError:
                        pass

                    now = time.time()
                    in_pre_cutoff = is_pre_cutoff_window() or is_5am_ist_cutoff()
                    flush_threshold = 15 if in_pre_cutoff else 50
                    flush_interval = 5.0 if in_pre_cutoff else 30.0

                    if buffer and (len(buffer) >= flush_threshold or (now - last_flush) >= flush_interval):
                        await _flush_sheet_buffer(buffer)
                        buffer.clear()
                        last_flush = time.time()
                except Exception as e:
                    logging.error(f"Error in sheet_writer_worker: {e}")

            if buffer:
                await _flush_sheet_buffer(buffer)
                buffer.clear()

        writer_task = asyncio.create_task(sheet_writer_worker())

        # Decoupled SEDA Two-Stage Architecture
        # Stage 1: Fast Dedicated NIM Summarizer Producer Pool (leases keys at maximum 35-40 RPM rate)
        # Stage 2: Paced Gemini Predictions & Tracxn Movements Consumer Pool (consumes precomputed summaries)
        sharded_key_count = token_rotator.get_total_keys() if token_rotator else 16
        default_nim_workers = max(20, sharded_key_count)
        num_nim_workers = min(int(os.getenv("MAX_NIM_WORKERS", str(default_nim_workers))), max(1, len(pf_data)))
        num_downstream_workers = min(MAX_WORKERS, max(1, len(pf_data)))
        nim_semaphore = asyncio.Semaphore(num_nim_workers)
        downstream_semaphore = asyncio.Semaphore(num_downstream_workers)

        domain_queue = asyncio.Queue()
        # Fast-Track Prioritization:
        # Tier 0: Uncompleted domains with existing NIM summary ready for immediate Gemini & Tracxn movement
        # Tier 1: Uncompleted domains with existing scraped markdown ready for immediate NIM summarization
        # Tier 2: Uncompleted domains needing initial Tech Crawler job generation or crawling
        # Tier 3: Completed or empty domains (handled quickly by skip)
        presummarized_items = []
        priority_items = []
        regular_items = []
        completed_items = []

        for idx, r in enumerate(pf_data):
            if not r or not r[0] or is_row_fully_completed(r):
                completed_items.append((idx, r))
                continue

            is_57 = is_57_col_schema(r)
            if is_57:
                tc_status = safe_str(r[23]).strip().upper() if len(r) > 23 else ""
                raw_scraped = safe_str(r[29]).strip() if len(r) > 29 else ""
                nim_txt = safe_str(r[35]).strip() if len(r) > 35 else ""
            else:
                tc_status = safe_str(r[25]).strip().upper() if len(r) > 25 else ""
                raw_scraped = (safe_str(r[31]).strip() or safe_str(r[37]).strip()) if len(r) > 31 else ""
                nim_txt = safe_str(r[40]).strip() if len(r) > 40 else ""

            if len(nim_txt) > 10:
                presummarized_items.append((idx, r))
            elif (raw_scraped or tc_status in ("SUCCESS", "FAILED")):
                priority_items.append((idx, r))
            else:
                regular_items.append((idx, r))

        logging.info(
            f"🚀 [Queue Partitioning] {len(presummarized_items)} pre-summarized domains ready for instant Gemini/Tracxn | "
            f"{len(priority_items)} pre-scraped domains fast-tracked for NIM | "
            f"{len(regular_items)} domains queued for Tech Crawler | {len(completed_items)} completed/empty"
        )
        for item in presummarized_items:
            await domain_queue.put(item)
        for item in priority_items:
            await domain_queue.put(item)
        for item in regular_items:
            await domain_queue.put(item)
        for item in completed_items:
            await domain_queue.put(item)

        # Large decoupled queue (maxsize 10000) allows producers to continuously process all domains without blocking on consumer
        summarized_queue = asyncio.Queue(maxsize=10000)
        sweep_task = None

        # Stage 1: Dedicated NIM Summarizer Producer
        async def nim_producer_worker():
            nonlocal nim_completed_count, tc_completed_count
            while True:
                if is_5am_ist_cutoff() or os.path.exists(STOP_FILE) or is_pre_cutoff_window() or os.path.exists(FLUSH_FILE):
                    break
                try:
                    item = await asyncio.wait_for(domain_queue.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if sweep_task is not None and sweep_task.done() and domain_queue.empty():
                        break
                    continue
                except asyncio.QueueEmpty:
                    if sweep_task is not None and sweep_task.done():
                        break
                    await asyncio.sleep(0.5)
                    continue

                if item is None:
                    domain_queue.task_done()
                    break

                idx, row = item
                row_number = start_row_num + idx
                if not row or not row[0]:
                    await summarized_queue.put((idx, row, None, True))
                    domain_queue.task_done()
                    continue

                if is_row_fully_completed(row):
                    await summarized_queue.put((idx, row, None, True))
                    domain_queue.task_done()
                    continue

                # Guard RAM to prevent OOM without blocking pure outbound async network I/O on CPU spikes
                if hasattr(SYSTEM_HEALTH, "_psutil"):
                    try:
                        if SYSTEM_HEALTH._psutil.virtual_memory().percent > 85.0:
                            trim_memory()
                            await asyncio.sleep(1.0)
                    except Exception:
                        pass
                domain = row[0]

                try:
                    # ⚡ Fast-forward optimization: if row already has NIM summary, skip
                    # Integrated_Process_domain entirely and pass summary directly to consumer.
                    # This eliminates the ~2.5s/row "Smart auto-resume" overhead that stalls
                    # the producer when scanning thousands of pre-summarized rows on resume.
                    is_57_row = is_57_col_schema(row)
                    existing_nim_summary = ""
                    existing_nim_status = ""
                    if is_57_row:
                        existing_nim_summary = safe_str(row[35]).strip() if len(row) > 35 else ""
                        existing_nim_status = safe_str(row[34]).strip() if len(row) > 34 else ""
                    else:
                        existing_nim_summary = safe_str(row[40]).strip() if len(row) > 40 else ""
                        existing_nim_status = safe_str(row[39]).strip() if len(row) > 39 else ""

                    if len(existing_nim_summary) > 10 and not existing_nim_summary.startswith("http"):
                        # Row has a valid existing NIM summary — fast-forward to consumer
                        if not existing_nim_status:
                            existing_nim_status = "SUCCESS"
                        stage1_res = {
                            "tc_status": safe_str(row[23 if is_57_row else 25]).strip().upper() if len(row) > (23 if is_57_row else 25) else "",
                            "domain": domain,
                            "row": row_number,
                            "tc_job_id": safe_str(row[21 if is_57_row else 23]) if len(row) > (21 if is_57_row else 23) else "",
                            "summary_status": existing_nim_status,
                            "summary": existing_nim_summary,
                            "rowData": row,
                            "write_data": None,
                            "start_col": None,
                        }
                    else:
                        stage1_mode = "phase1" if mode == "phase1" else "phase2"
                        stage1_res = await Integrated_Process_domain(
                            session,
                            domain,
                            row_number,
                            client,
                            nim_semaphore,
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
                            mode=stage1_mode,
                            write_to_sheet=False,
                            precomputed_summary=None,
                        )
                    had_nim_at_start = (len(row) > nim_col_idx and safe_str(row[nim_col_idx]).strip().upper() in ("SUCCESS", "NO_CONTENT", "FAILED_NO_CONTENT"))
                    if not had_nim_at_start:
                        nim_completed_count = min(total_domains, nim_completed_count + 1)
                    if isinstance(stage1_res, dict) and stage1_res.get("tc_status") in ("SUCCESS", "FAILED"):
                        had_tc_at_start = (len(row) > tc_col_idx and safe_str(row[tc_col_idx]).strip().upper() in ("SUCCESS", "FAILED")) or (len(row) > (29 if is_57 else 31) and bool(safe_str(row[29 if is_57 else 31]).strip()))
                        if not had_tc_at_start:
                            tc_completed_count = min(total_domains, tc_completed_count + 1)
                    # Persist NIM summary or crawler status to sheet immediately if newly generated
                    if not had_nim_at_start and isinstance(stage1_res, dict) and stage1_res.get("write_data") and stage1_res.get("start_col"):
                        await sheet_write_queue.put((row_number, stage1_res["start_col"], stage1_res["write_data"]))
                    if nim_completed_count % 10 == 0:
                        logging.info(f"⚡ [NIM Producer] {nim_completed_count}/{total_domains} summarized (Buffer: {summarized_queue.qsize()})")
                        update_progress_file(
                            tracxn_completed_count, total_domains, success_count, fail_count,
                            phase=phase_title,
                            stage_details=f"Pipelined: {nim_completed_count}/{total_domains} summarized | {tracxn_completed_count}/{total_domains} fully completed ({tc_completed_count} resolved)",
                            pending_tc=len(pending_tech_crawler_jobs),
                            mode=mode,
                            scraped_count=tc_completed_count,
                            tc_success=success_count,
                            nim_count=nim_completed_count,
                            gemini_count=gemini_completed_count,
                            tracxn_count=tracxn_completed_count
                        )
                    await summarized_queue.put((idx, row, stage1_res, False))
                except Exception as ex:
                    logging.error(f"Error in NIM producer for {domain} at row {row_number}: {ex}")
                    await summarized_queue.put((idx, row, ex, False))
                finally:
                    domain_queue.task_done()

        # Stage 2: Paced Gemini Predictions & Tracxn Movements Consumer
        async def downstream_consumer_worker():
            nonlocal processed, success_count, fail_count, gemini_completed_count, tracxn_completed_count
            while True:
                if is_5am_ist_cutoff() or os.path.exists(STOP_FILE) or (is_pre_cutoff_window() and summarized_queue.empty()):
                    break

                item = await summarized_queue.get()
                if item is None:
                    summarized_queue.task_done()
                    break

                idx, row, stage1_res, is_skip = item
                row_number = start_row_num + idx

                try:
                    if is_skip:
                        continue

                    if is_5am_ist_cutoff() or os.path.exists(STOP_FILE):
                        continue

                    await SYSTEM_HEALTH.wait_for_resources()
                    domain = row[0]

                    if isinstance(stage1_res, Exception) or stage1_res is None:
                        fail_count += 1
                        slice_start = target_start_col - 1
                        existing_slice = row[slice_start : slice_start + expected_cols]
                        if len(existing_slice) < expected_cols:
                            existing_slice = existing_slice + [""] * (expected_cols - len(existing_slice))
                        await sheet_write_queue.put((row_number, target_start_col, existing_slice))
                    elif isinstance(stage1_res, dict) and stage1_res.get("tc_status") == "PROCESSING":
                        pending_tech_crawler_jobs.append(stage1_res)
                        if mode == "phase1":
                            write_data = stage1_res.get("write_data") or [stage1_res.get("tc_job_id", ""), stage1_res.get("tc_job_gen_time", ""), "PROCESSING"]
                            if len(write_data) < expected_cols:
                                write_data = write_data + [""] * (expected_cols - len(write_data))
                            await sheet_write_queue.put((row_number, target_start_col, write_data))
                        elif mode == "phase2":
                            success_count += 1
                            slice_start = target_start_col - 1
                            existing_slice = row[slice_start : slice_start + expected_cols]
                            if len(existing_slice) < expected_cols:
                                existing_slice = existing_slice + [""] * (expected_cols - len(existing_slice))
                            await sheet_write_queue.put((row_number, target_start_col, existing_slice))
                        else:
                            slice_start = target_start_col - 1
                            row_slice = list(row[slice_start : slice_start + expected_cols])
                            if len(row_slice) < expected_cols:
                                row_slice = row_slice + [""] * (expected_cols - len(row_slice))
                            row_slice[4] = stage1_res.get("tc_job_id", "")
                            row_slice[5] = stage1_res.get("tc_job_gen_time", "")
                            row_slice[6] = "PROCESSING"
                            await sheet_write_queue.put((row_number, target_start_col, row_slice))
                    elif mode in ("phase1", "phase2"):
                        success_count += 1
                        write_data = stage1_res.get("write_data") if isinstance(stage1_res, dict) else None
                        if write_data:
                            if len(write_data) < expected_cols:
                                write_data = write_data + [""] * (expected_cols - len(write_data))
                            await sheet_write_queue.put((row_number, target_start_col, write_data))
                    else:
                        downstream_row = stage1_res.get("rowData") or row
                        precomputed_sum = stage1_res.get("summary")

                        if is_pre_cutoff_window() and not precomputed_sum:
                            # Pre-cutoff wind-down: skip un-summarized rows to avoid blocking consumer; only finish predictions on summarized rows
                            logging.info(f"⏰ [Pre-Cutoff Wind-Down] Skipping prediction for un-summarized domain {domain} to fast-track buffer drain.")
                            continue

                        full_res = await Integrated_Process_domain(
                            session,
                            domain,
                            row_number,
                            client,
                            downstream_semaphore,
                            downstream_row,
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
                            mode="full",
                            write_to_sheet=False,
                            precomputed_summary=precomputed_sum,
                        )

                        if isinstance(full_res, Exception) or full_res is None:
                            fail_count += 1
                            slice_start = target_start_col - 1
                            existing_slice = row[slice_start : slice_start + expected_cols]
                            if len(existing_slice) < expected_cols:
                                existing_slice = existing_slice + [""] * (expected_cols - len(existing_slice))
                            await sheet_write_queue.put((row_number, target_start_col, existing_slice))
                        else:
                            success_count += 1
                            final_write_data = full_res.get("write_data") if isinstance(full_res, dict) else None
                            actual_start_col = full_res.get("start_col", target_start_col) if isinstance(full_res, dict) else target_start_col

                            if final_write_data:
                                try:
                                    updated_row = (list(downstream_row[:17]) + list(final_write_data)) if len(final_write_data) == 40 else (list(downstream_row[:19]) + list(final_write_data))
                                    rev_res = await Process_Review_domain(
                                        session, updated_row, row_number, client, downstream_semaphore,
                                        SHEET_ID, EXTRACTING_SHEET_NAME, write_to_sheet=False
                                    )
                                    if rev_res and not isinstance(rev_res, Exception):
                                        final_write_data = rev_res
                                    elif rev_res is not None and not isinstance(rev_res, Exception):
                                        final_write_data = rev_res
                                except Exception as rev_err:
                                    logging.warning(f"Domain real-time review warning for {domain}: {rev_err}")

                            if final_write_data and len(final_write_data) == expected_cols:
                                await sheet_write_queue.put((row_number, actual_start_col, final_write_data))
                            elif final_write_data:
                                if len(final_write_data) < expected_cols:
                                    final_write_data = final_write_data + [""] * (expected_cols - len(final_write_data))
                                else:
                                    final_write_data = final_write_data[:expected_cols]
                                await sheet_write_queue.put((row_number, actual_start_col, final_write_data))
                            else:
                                slice_start = target_start_col - 1
                                existing_slice = row[slice_start : slice_start + expected_cols]
                                if len(existing_slice) < expected_cols:
                                    existing_slice = existing_slice + [""] * (expected_cols - len(existing_slice))
                                await sheet_write_queue.put((row_number, target_start_col, existing_slice))

                            # Live telemetry counter update for resolved domains
                            if final_write_data:
                                if is_57col_sheet:
                                    feed_val = final_write_data[26] if len(final_write_data) > 26 else ""
                                    if_val = final_write_data[20] if len(final_write_data) > 20 else ""
                                    if str(feed_val).strip() or str(if_val).strip():
                                        had_gem_at_start = bool(safe_str(row[37 if is_57 else 44]).strip() or safe_str(row[gemini_col_idx]).strip())
                                        if not had_gem_at_start:
                                            gemini_completed_count = min(nim_completed_count, gemini_completed_count + 1)
                                else:
                                    gemini_completed_count = min(nim_completed_count, gemini_completed_count + 1)
                                tracxn_completed_count = min(total_domains, tracxn_completed_count + 1)

                except Exception as d_err:
                    logging.error(f"Error in downstream consumer for {row[0] if row else 'unknown'} at row {row_number}: {d_err}")
                    fail_count += 1
                finally:
                    processed += 1
                    if processed % 15 == 0 or processed == total_domains:
                        update_progress_file(
                            tracxn_completed_count, total_domains, success_count, fail_count,
                            phase=phase_title,
                            stage_details=f"Pipelined: {nim_completed_count}/{total_domains} summarized | {tracxn_completed_count}/{total_domains} fully completed ({tc_completed_count} resolved, {success_count} success)",
                            pending_tc=len(pending_tech_crawler_jobs),
                            mode=mode,
                            scraped_count=tc_completed_count,
                            tc_success=success_count,
                            nim_count=nim_completed_count,
                            gemini_count=gemini_completed_count,
                            tracxn_count=tracxn_completed_count
                        )
                        trim_memory()
                    summarized_queue.task_done()

        # Concurrent Background Sweep: Reconcile all pending Tech Crawler jobs in batches of 50
        async def _run_background_sweep():
            try:
                initial_pending = await reconcile_existing_tech_crawler_jobs(
                    session=session,
                    rows=pf_data,
                    client=client,
                    SHEET_ID=SHEET_ID,
                    sheet_name=EXTRACTING_SHEET_NAME,
                    start_row_num=start_row_num,
                    sheet_write_queue=sheet_write_queue,
                    domain_queue=domain_queue,
                    batch_size=50
                )
                if initial_pending:
                    pending_tech_crawler_jobs.extend(initial_pending)
            except Exception as e:
                logging.error(f"Error in background Tech Crawler sweep: {e}")

        sweep_task = asyncio.create_task(_run_background_sweep())

        # Launch decoupled Stage 1 (NIM Producer) and Stage 2 (Downstream Consumer) worker pools
        producer_tasks = [asyncio.create_task(nim_producer_worker()) for _ in range(num_nim_workers)]
        consumer_tasks = [asyncio.create_task(downstream_consumer_worker()) for _ in range(num_downstream_workers)]

        async def _close_queue_when_producers_done():
            await asyncio.gather(*producer_tasks)
            for _ in range(num_downstream_workers):
                await summarized_queue.put(None)

        close_task = asyncio.create_task(_close_queue_when_producers_done())
        await asyncio.gather(close_task, *consumer_tasks)

        # Ensure background sweep finishes before closing sheet write queue
        if not sweep_task.done():
            logging.info("Domain workers completed; waiting for background Tech Crawler sweep to finish...")
            try:
                await sweep_task
            except Exception as se:
                logging.error(f"Error awaiting background sweep task: {se}")

        # Signal sheet writer worker to flush remaining writes and exit
        await sheet_write_queue.put(None)
        await writer_task

        update_progress_file(
            processed, total_domains, success_count, fail_count,
            phase=phase_title,
            stage_details=f"Streaming completed: {processed}/{total_domains} rows processed",
            pending_tc=len(pending_tech_crawler_jobs),
            mode=mode
        )
        logging.info(
            f"Streaming completed for {processed}/{total_domains} domains (Success: {success_count}, Fail: {fail_count}, Pending TC: {len(pending_tech_crawler_jobs)})"
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
                    if not isinstance(item, dict):
                        continue
                    d = item.get("domain", "")
                    r_num = item.get("row", 0)
                    j_id = item.get("tc_job_id", "")
                    if not d or not j_id:
                        continue
                    orig_row = item.get("rowData") or []

                    try:
                        c_ts = item.get("created_timestamp", time.time())
                        track_res = await tc_client.check_job_status_with_cadence(session, j_id, c_ts)
                        curr_status = track_res.get("status")

                        if curr_status in ("SUCCESS", "FAILED"):
                            logging.info(f"Tech Crawler job {j_id} for {d} resolved with status: {curr_status}")
                            is_57_orig = is_57_col_schema(orig_row)
                            if is_57_orig:
                                if len(orig_row) < 57:
                                    orig_row.extend([""] * (57 - len(orig_row)))
                                orig_row[20] = "DONE"
                                orig_row[21] = j_id
                                orig_row[23] = curr_status
                                orig_row[24] = track_res.get("final_url", "")
                                orig_row[25] = track_res.get("s3_html", "")
                                orig_row[26] = track_res.get("s3_markdown", "")
                                orig_row[27] = str(track_res.get("http_status", ""))
                                orig_row[28] = str(track_res.get("signals", ""))
                            else:
                                if len(orig_row) < 70:
                                    orig_row.extend([""] * (70 - len(orig_row)))
                                orig_row[21] = "DONE"
                                orig_row[23] = j_id
                                orig_row[25] = curr_status
                                orig_row[26] = track_res.get("final_url", "")
                                orig_row[27] = track_res.get("s3_html", "")
                                orig_row[28] = track_res.get("s3_markdown", "")
                                orig_row[29] = str(track_res.get("http_status", ""))
                                orig_row[30] = str(track_res.get("signals", ""))
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
            if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
                token = await TRACXN_CLIENT._acquire_token()
            elif 'TRACXN_KEY_MANAGER' in globals() and TRACXN_KEY_MANAGER is not None:
                token = await TRACXN_KEY_MANAGER.acquire() if hasattr(TRACXN_KEY_MANAGER, "acquire") else TRACXN_KEY_MANAGER.get_token()
            else:
                token = Tracxn_API_Token
            TOKEN = token
            domain = row[0]
            domain_profile_id = row[10]
            filter_group_id = row[4]
            filter_id = row[6]
            source_funnel_id = row[2]
            is_57 = is_57_col_schema(row)

            if is_57:
                output_data_if = row[37] if len(row) > 37 else ""
                industry_funnel_id = row[39] if len(row) > 39 else ""
                publish_funnel_id = row[47] if len(row) > 47 else ""
                tagStatusForIF = row[53] if len(row) > 53 else ""
                tagStatusForPF = row[54] if len(row) > 54 else ""
                tagStatusForReview = row[55] if len(row) > 55 else ""
                cf_if_status = row[49] if len(row) > 49 else ""
                cf_if_time = row[50] if len(row) > 50 else ""
                if_pf_status = row[51] if len(row) > 51 else ""
                if_pf_time = row[52] if len(row) > 52 else ""
            else:
                output_data_if = row[43] if len(row) > 43 else ""
                industry_funnel_id = output_data_if
                publish_funnel_id = row[60] if len(row) > 60 else ""
                tagStatusForIF = row[65] if len(row) > 65 else ""
                tagStatusForPF = row[66] if len(row) > 66 else ""
                tagStatusForReview = row[67] if len(row) > 67 else ""
                cf_if_status = row[61] if len(row) > 61 else ""
                cf_if_time = row[62] if len(row) > 62 else ""
                if_pf_status = row[63] if len(row) > 63 else ""
                if_pf_time = row[64] if len(row) > 64 else ""

            tc_idx = 23 if is_57 else 25
            raw_idx = 29 if is_57 else 31
            tc_status_val = str(row[tc_idx]).strip().upper() if len(row) > tc_idx else ""
            raw_data_val = str(row[raw_idx]).strip() if len(row) > raw_idx else ""
            is_truly_unscrapable = (tc_status_val == "FAILED" and not raw_data_val)

            tag_status_review = ""
            if is_row_fully_completed(row):
                pass
            elif tagStatusForIF == "YES" or tagStatusForPF == "YES" or tagStatusForReview == "YES":
                pass
            elif is_truly_unscrapable:
                # User directive: Domains should be snoozed ONLY on valid failure cases:
                # unable to scrape website when both TC and waterfall scraper fails.
                tag_status_review = await Domain_HashTag_async(domain, To_Review_TAG_NAME, TOKEN)
                cf_if_status = await WriteAPI_Domain_Funnel_async( domain_profile_id, source_funnel_id, filter_group_id, filter_id,source_funnel_id, "SNOOZED", COMMENT, TOKEN, session=session)
                cf_if_time = get_ist_timestamp()
                if cf_if_status == "TRUE":
                    cf_if_status = "DONE"
            elif source_funnel_id and ("22" in output_data_if or "MARKED_IRRELEVANT" in output_data_if):
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
                # Domain was scraped successfully, in-flight, or had transient write API error:
                # DO NOT SNOOZE! Preserve for retry.
                logging.info(f"Process_Review_Caller: Preserving {domain} for retry (not unscrapable). Bypassing snooze.")

            tag_status_discovery = ""
            valid_movement_terminals = {"DONE", "YES", "TRUE", "MOVED", "MOVED_OUT", "SNOOZED", "MARKED_IRRELEVANT", "MARKED_FOR_LATER", "ESCALATED", "SKIPPED_UNASSIGNABLE"}
            def _is_valid_ts_movement(s: str) -> bool:
                s = str(s).strip()
                return len(s) >= 8 and (":" in s or "/" in s or "-" in s) and s.upper() not in valid_movement_terminals and not s.startswith("http")

            pf_val = str(if_pf_status).strip().upper()
            cf_val = str(cf_if_status).strip().upper()
            is_moved_row = (
                cf_val in valid_movement_terminals or
                pf_val in valid_movement_terminals or
                _is_valid_ts_movement(cf_if_time) or
                _is_valid_ts_movement(if_pf_time)
            )

            ind_id = industry_funnel_id
            pub_id = publish_funnel_id
            out_check = str(output_data_if).strip().upper()

            # User rule: ONLY for cases where Publish Funnel (bu_llm_feed_prediction) is YES should Hashtag (bu_llm_discovery_processing) be added
            if str(tagStatusForPF).strip().upper() in ("YES", "TRUE"):
                tag_status_discovery = await Domain_HashTag_async(domain, Discovery_Processing_TAG_NAME, TOKEN)
                discovery_bq = "yes" if str(tag_status_discovery).upper() in ("YES", "TRUE") else "no"
            elif is_moved_row:
                discovery_bq = "no"
            else:
                discovery_bq = (row[56] if is_57 else row[68]) if len(row) > (56 if is_57 else 68) else ""

            # Prepare output data
            if is_57:
                existing_prefix = list(row[17:49]) if len(row) > 17 else []
                if len(existing_prefix) < 32:
                    existing_prefix = existing_prefix + [""] * (32 - len(existing_prefix))
                output_row = existing_prefix + [
                    cf_if_status,
                    cf_if_time,
                    if_pf_status,
                    if_pf_time,
                    tagStatusForIF,
                    tagStatusForPF,
                    tag_status_review,
                    discovery_bq
                ]
                write_start = 18
            else:
                picked_time = row[19] if len(row) > 19 and row[19] else get_ist_timestamp()
                cleared_time = row[20] if len(row) > 20 and row[20] else get_ist_timestamp()
                output_status = row[21] if len(row) > 21 and row[21] else "To be checked"
                output_row = [
                    picked_time,  # Engine Picked Time
                    cleared_time,  # Engine Cleared Time
                    output_status,  # Engine Cleared Status
                    *row[22:61],          # Data range
                    cf_if_status,
                    cf_if_time,
                    if_pf_status,
                    if_pf_time,
                    tagStatusForIF,
                    tagStatusForPF,
                    tag_status_review,
                    discovery_bq
                ]
                write_start = 20

            if write_to_sheet:
                await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, [output_row], row_number, write_start)
            return output_row
    except Exception as e:
        logging.error(f"Error processing row {row_number}: {e}")
        return None


# Entry point
async def Process_Review_Caller(client, mainheaders, SHEET_ID, EXTRACTING_SHEET_NAME, start_row_num=3, mode="full"):
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

            tasks_with_index = []
            start_row = start_row_num + batch_index * batch_size  # Adjust for writing

            for row_index, row in enumerate(batch):
                if row and row[0]:
                    is_57_chk = is_57_col_schema(row)
                    tc_chk = safe_str(row[23 if is_57_chk else 25]).strip().upper() if len(row) > (23 if is_57_chk else 25) else ""
                    raw_chk = safe_str(row[29 if is_57_chk else 31]).strip() if len(row) > (29 if is_57_chk else 31) else ""
                    if not raw_chk and tc_chk != "SUCCESS":
                        tasks_with_index.append((row_index, None))
                        continue
                    task = asyncio.create_task(
                        Process_Review_domain(
                            session, row, start_row + row_index, client, semaphore, SHEET_ID, EXTRACTING_SHEET_NAME, write_to_sheet=False
                        )
                    )
                    tasks_with_index.append((row_index, task))
                else:
                    tasks_with_index.append((row_index, None))

            active_tasks = [t for _, t in tasks_with_index if t is not None]
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)

            batch_output_rows = []
            is_57_rev = is_57_col_schema(pf_data[0] if pf_data else None)
            expected_rev_cols = 8 if is_57_rev else 49

            for row_index, task in tasks_with_index:
                orig_row = batch[row_index] if row_index < len(batch) else []
                is_57_b = is_57_col_schema(orig_row)
                if is_57_b:
                    orig_slice = orig_row[49:57] if len(orig_row) >= 57 else (orig_row[49:] + [""] * max(0, 8 - len(orig_row[49:])))
                else:
                    orig_slice = orig_row[19:68] if len(orig_row) >= 68 else (orig_row[19:] + [""] * max(0, 49 - len(orig_row[19:])))

                if task is None:
                    batch_output_rows.append(orig_slice)
                else:
                    try:
                        res = task.result() if task.done() and not task.cancelled() else None
                    except Exception as err:
                        res = err

                    if isinstance(res, Exception) or res is None:
                        review_fail += 1
                        batch_output_rows.append(orig_slice)
                    else:
                        review_success += 1
                        # Process_Review_domain returns 40 columns (Cols 18-57) for 57-col, or 49 columns (Cols 20-68) for legacy
                        slice_to_write = res[-8:] if is_57_b else res
                        if len(slice_to_write) < expected_rev_cols:
                            slice_to_write = slice_to_write + [""] * (expected_rev_cols - len(slice_to_write))
                        elif len(slice_to_write) > expected_rev_cols:
                            slice_to_write = slice_to_write[:expected_rev_cols]
                        batch_output_rows.append(slice_to_write)
                review_processed += 1

            if batch_output_rows:
                write_col_rev = 50 if is_57_rev else 20
                await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, batch_output_rows, start_row, write_col_rev)

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

async def Execute_Phase4_Ingestion_Write_APIs(client, mainheaders, SHEET_ID, EXTRACTING_SHEET_NAME, start_row_num=3):
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
                    is_57 = is_57_col_schema(row)
                    if is_57:
                        output_data_if = row[37] if len(row) > 37 else ""
                        industry_funnel_id = row[39] if len(row) > 39 else ""
                        output_data_if_validation = row[41] if len(row) > 41 else ""
                        publish_funnel_id = row[47] if len(row) > 47 else ""
                        cf_if_status = row[49] if len(row) > 49 else ""
                        cf_if_time = row[50] if len(row) > 50 else ""
                        if_pf_status = row[51] if len(row) > 51 else ""
                        if_pf_time = row[52] if len(row) > 52 else ""
                        tag_status_if = row[53] if len(row) > 53 else ""
                        tag_status_pf = row[54] if len(row) > 54 else ""
                        tag_status_discovery = row[56] if len(row) > 56 else ""
                    else:
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

                    token = None
                    if 'TRACXN_CLIENT' in globals() and TRACXN_CLIENT is not None:
                        token = await TRACXN_CLIENT._acquire_token()
                    elif 'TRACXN_KEY_MANAGER' in globals() and TRACXN_KEY_MANAGER is not None:
                        token = await TRACXN_KEY_MANAGER.acquire() if hasattr(TRACXN_KEY_MANAGER, "acquire") else TRACXN_KEY_MANAGER.get_token()
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

                    valid_movement_terminals = {"DONE", "YES", "TRUE", "MOVED", "MOVED_OUT", "SNOOZED", "MARKED_IRRELEVANT", "MARKED_FOR_LATER", "ESCALATED", "SKIPPED_UNASSIGNABLE"}
                    def _is_valid_ts_movement(s: str) -> bool:
                        s = str(s).strip()
                        return len(s) >= 8 and (":" in s or "/" in s or "-" in s) and s.upper() not in valid_movement_terminals and not s.startswith("http")

                    cf_val = str(cf_if_status).strip().upper()
                    pf_val = str(if_pf_status).strip().upper()
                    is_moved_ingest = (
                        cf_val in valid_movement_terminals or
                        pf_val in valid_movement_terminals or
                        _is_valid_ts_movement(cf_if_time) or
                        _is_valid_ts_movement(if_pf_time)
                    )

                    out_check = str(output_data_if).strip().upper()
                    # User rule: ONLY for cases where Publish Funnel (bu_llm_feed_prediction) is YES should Hashtag (bu_llm_discovery_processing) be added
                    if str(tag_status_pf).strip().upper() in ("YES", "TRUE"):
                        tag_status_discovery = await Domain_HashTag_async(domain, Discovery_Processing_TAG_NAME, token, session=session)
                        discovery_bq = "yes" if str(tag_status_discovery).upper() in ("YES", "TRUE") else "no"
                    elif is_moved_ingest:
                        discovery_bq = "no"
                    else:
                        discovery_bq = ""
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
                is_57_ing = is_57_col_schema(pf_data[0] if pf_data else None)
                write_col_ing = 50 if is_57_ing else 62
                await write_sheet_data(client, SHEET_ID, EXTRACTING_SHEET_NAME, batch_write, start_row, write_col_ing)

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
        start_row_arg = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 3
        mode_arg = "full"
        custom_sheet_id = None
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

        # Caller - NIM Model Detail & Tokens (Direct from sharded pool)
        unique_tokens = list(NIM_HARDCODED_KEYS)
        model_to_pick = settings.Nvidia_Base_Model

        token_rotator = NIM_KEY_MANAGER or NIMKeyManager(unique_tokens)
        logging.info(f"NIMKeyManager initialized with {token_rotator.total_tokens} keys from environment.")

        # Pre-flight validate and prune invalid/expired tokens for Tracxn and NIM APIs
        logging.info("Starting pre-flight validation of Tracxn API tokens...")
        await TRACXN_KEY_MANAGER.validate_and_prune_all_tokens()
        logging.info("Starting pre-flight validation of NVIDIA NIM API keys...")
        await token_rotator.validate_and_prune_all_keys()
        if 'NIM_KEY_MANAGER' in globals() and NIM_KEY_MANAGER is not None:
            NIM_KEY_MANAGER.keys = list(token_rotator.keys)
            NIM_KEY_MANAGER.invalid_keys = set(token_rotator.invalid_keys)

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

        # Pre-4:00 AM IST Retry Loop: Retries all previously failed phases EXCEPT Tech Crawler
        retry_round = 0
        while is_before_4am_ist() and not os.path.exists(STOP_FILE) and mode_arg in ("full", "phase2", "phase3", "phase4"):
            retry_round += 1
            logging.info(f"Checking for previously failed phase domains to retry before 4:00 AM IST (Round {retry_round})...")
            try:
                pf_retry = await read_sheet_data(client, active_sheet_id, CF_EXTRACTING_SHEET_NAME, start_row=start_row_arg)
            except Exception as e_rf:
                logging.warning(f"Could not read sheet data for retry loop: {e_rf}")
                break

            if not pf_retry:
                break

            is_57_ret = is_57_col_schema(pf_retry[0] if pf_retry else None)
            failed_candidates = 0
            for r in pf_retry:
                if not r or not r[0] or is_row_fully_completed(r):
                    continue
                tc_st = safe_str(r[23 if is_57_ret else 25]).strip().upper()
                raw_d = safe_str(r[29 if is_57_ret else 31]).strip()
                # Strictly skip Tech Crawler failures per explicit requirement ("except TC")
                if tc_st != "SUCCESS" and not raw_d:
                    continue

                nim_st = safe_str(r[34 if is_57_ret else 40]).strip().upper()
                nim_sum = safe_str(r[35 if is_57_ret else 40]).strip()
                gemini_feed = safe_str(r[43 if is_57_ret else 48]).strip()
                write_st = safe_str(r[49 if is_57_ret else 61]).strip().upper()

                if nim_st == "FAILED" or len(nim_sum) <= 10 or not gemini_feed or gemini_feed in ("FAILED", "ERROR") or write_st in ("FALSE", "ERROR", ""):
                    failed_candidates += 1

            if failed_candidates == 0:
                logging.info(f"No previously failed phase rows remaining to retry before 4:00 AM IST.")
                break

            logging.info(f"🔄 [Pre-4AM Retry Round {retry_round}] Found {failed_candidates} eligible previously failed rows. Executing retry pass...")
            try:
                if mode_arg != "phase4":
                    await Integrated_Extract_domains(
                        client, active_sheet_id, CF_EXTRACTING_SHEET_NAME,
                        cache_id_IF_Prediction, token_IF_Prediction, date_IF_Prediction,
                        status_IF_Prediction, exp_date_IF_Prediction, config_funnel_prompt,
                        config_funnel_id_prompt, config_funnel_name_prompt,
                        config_funnel_name_Feed_Cached_prompt, config_funnel_name_Feed_Cached_API_Token,
                        config_funnel_name_Feed_Cached_Model, config_funnel_name_Feed_Cached_Gen_Date,
                        config_funnel_name_Feed_Cached_Final_Status, token_rotator=token_rotator,
                        start_row_num=start_row_arg, mode=mode_arg
                    )
                if not os.path.exists(STOP_FILE) and not is_5am_ist_cutoff():
                    await Process_Review_Caller(client, mainheaders, active_sheet_id, CF_EXTRACTING_SHEET_NAME, start_row_num=start_row_arg, mode=mode_arg)
            except Exception as e_ret:
                logging.error(f"Error during pre-4AM retry pass: {e_ret}")
                break

            await asyncio.sleep(5)

        # Dynamic Completion Metric Calculation — Zero Hardcoded Values
        try:
            pf_final = await read_sheet_data(client, active_sheet_id, CF_EXTRACTING_SHEET_NAME, start_row=start_row_arg)
            is_57_fin = is_57_col_schema(pf_final[0] if pf_final else None)
            fin_tc_scraped = sum(1 for r in pf_final if (len(r) > (23 if is_57_fin else 25) and safe_str(r[23 if is_57_fin else 25]).strip().upper() == "SUCCESS") or (len(r) > (29 if is_57_fin else 31) and safe_str(r[29 if is_57_fin else 31]).strip()))
            fin_tc_failed = sum(1 for r in pf_final if len(r) > (23 if is_57_fin else 25) and safe_str(r[23 if is_57_fin else 25]).strip().upper() == "FAILED")
            fin_tc = min(len(pf_final), fin_tc_scraped + fin_tc_failed)

            fin_nim_succ = sum(1 for r in pf_final if len(r) > (34 if is_57_fin else 40) and safe_str(r[34 if is_57_fin else 40]).strip().upper() == "SUCCESS")
            fin_nim_nocontent = sum(1 for r in pf_final if len(r) > (34 if is_57_fin else 40) and safe_str(r[34 if is_57_fin else 40]).strip().upper() in ("NO_CONTENT", "FAILED_NO_CONTENT"))
            fin_nim = min(fin_tc_scraped, fin_nim_succ + fin_nim_nocontent)

            fin_gem_if = sum(1 for r in pf_final if len(r) > (37 if is_57_fin else 44) and safe_str(r[37 if is_57_fin else 44]).strip())
            fin_gem_feed = sum(1 for r in pf_final if len(r) > (43 if is_57_fin else 48) and safe_str(r[43 if is_57_fin else 48]).strip())
            fin_gem = min(fin_nim_succ, max(fin_gem_if, fin_gem_feed))

            fin_tracxn = min(fin_gem, sum(1 for r in pf_final if is_row_fully_completed(r)))
            fin_total = len(pf_final)
        except Exception:
            fin_tc = tc_completed_count if 'tc_completed_count' in locals() else 0
            fin_nim = nim_completed_count if 'nim_completed_count' in locals() else 0
            fin_gem = gemini_completed_count if 'gemini_completed_count' in locals() else 0
            fin_tracxn = tracxn_completed_count if 'tracxn_completed_count' in locals() else 0
            fin_total = total_domains if 'total_domains' in locals() else 0

        update_progress_file(
            fin_tracxn, fin_total, fin_tc_scraped if 'fin_tc_scraped' in locals() else fin_tc, 0,
            phase="Completed",
            stage_details=f"All pipeline phases completed ({fin_tracxn}/{fin_total} domains moved).",
            pending_tc=0,
            mode=mode_arg,
            scraped_count=fin_tc,
            tc_success=fin_tc_scraped if 'fin_tc_scraped' in locals() else fin_tc,
            nim_count=fin_nim,
            gemini_count=fin_gem,
            tracxn_count=fin_tracxn
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
