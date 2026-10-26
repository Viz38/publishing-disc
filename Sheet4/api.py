import asyncio
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from .models import RunRequest
    from .config import settings
except (ImportError, ValueError):
    from models import RunRequest
    from config import settings

LOGS_DIR = os.path.join(BASE_DIR, 'Logs')
PROGRESS_FILE = os.path.join(BASE_DIR, ".progress.json")
STOP_FILE = os.path.join(BASE_DIR, ".stop_requested")
ACTIVE_FILE = os.path.join(BASE_DIR, ".active_state.json")
CACHE_FILE = os.path.join(BASE_DIR, "discovery_cache.csv")
ARCHIVE_CACHE_DIR = os.path.join(LOGS_DIR, "Cache")
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(ARCHIVE_CACHE_DIR, exist_ok=True)
api_log_path = os.path.join(LOGS_DIR, 'api.logs')

from common import (
    LocalSheetCache,
    is_5am_ist_cutoff,
    is_pre_cutoff_window,
    get_now_ist,
    ExecutionTracker,
    BacklogManager,
)
from common.local_cache import is_pid_alive

EXEC_STATE_FILE = os.path.join(BASE_DIR, ".execution_state.json")
EXEC_TRACKER = ExecutionTracker(EXEC_STATE_FILE)
BACKLOG_MANAGER = BacklogManager()

try:
    LOCAL_CACHE = LocalSheetCache(CACHE_FILE)
except Exception as _ce:
    LOCAL_CACHE = None

if not os.path.exists(api_log_path):
    with open(api_log_path, 'w') as f:
        pass
try:
    os.chmod(api_log_path, 0o666)
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(api_log_path, mode="a"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("api_cf-1")

app = FastAPI(title="Discovery Engine - Discovery AI - CF-1 (Domains DB)")

@app.middleware("http")
async def log_requests(request: Request, call_next):
    client_ip = request.headers.get("x-forwarded-for") or (request.client.host if request.client else "unknown")
    logger.info(f"REQ FROM {client_ip}: {request.method} {request.url.path}")
    return await call_next(request)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

async def _daily_5am_ist_watchdog():
    """
    Background watchdog checking every 15 seconds:
    At 05:00 AM IST daily (05:00:00 - 05:05:00 IST), Google Apps Script clears the sheet and loads fresh domains.
    1. Immediately terminates any active pipeline running for this sheet to prevent writing stale results.
    2. Resets state to 'idle' and cleans up lock/progress/stop/active files.
    3. Archives the previous day's discovery_cache.csv into Logs/Cache/discovery_cache_YYYY-MM-DD.csv
       and initializes a fresh discovery_cache.csv for the new daily domain set (once per day).
    """
    last_rotated_date = None
    last_cleared_date = None
    FLUSH_FILE = os.path.join(BASE_DIR, ".flush_requested")
    logger.info("🕒 Daily 5:00 AM IST Watchdog initialized (04:30 AM Pre-Cutoff Flush enabled).")
    while True:
        try:
            now_ist = get_now_ist()
            current_date_str = now_ist.strftime("%Y-%m-%d")

            if is_pre_cutoff_window(now_ist):
                # 04:30 AM IST Pre-Cutoff Flush: Signal active pipeline to stop taking new scrapes
                if not os.path.exists(FLUSH_FILE):
                    try:
                        with open(FLUSH_FILE, "w") as f:
                            f.write(f"flush_requested_at={now_ist.isoformat()}")
                        logger.warning("🚨 [04:30 AM IST Watchdog] Created .flush_requested to trigger pre-cutoff flush!")
                    except Exception as fe:
                        logger.error(f"Error creating .flush_requested: {fe}")

            # At 05:05 AM IST: Clear .flush_requested
            if now_ist.hour == 5 and now_ist.minute >= 5:
                if os.path.exists(FLUSH_FILE):
                    try:
                        os.remove(FLUSH_FILE)
                        logger.info("Cleared .flush_requested after 5:00 AM cutoff window.")
                    except Exception:
                        pass

            if is_5am_ist_cutoff(now_ist):
                # 1. Terminate any running pipeline
                active_pid = state.get("active_pid")
                if active_pid or state.get("status") in ("running", "stopping"):
                    logger.warning(
                        f"⏰ 5:00 AM IST Daily Cutoff reached! Terminating active pipeline PID {active_pid} "
                        "so Apps Script can refresh daily domains without conflict."
                    )
                    if active_pid:
                        try:
                            os.kill(active_pid, signal.SIGTERM)
                            await asyncio.sleep(1)
                            try:
                                os.kill(active_pid, 0)
                                os.kill(active_pid, signal.SIGKILL)
                            except OSError:
                                pass
                        except Exception as e:
                            logger.error(f"Error terminating PID {active_pid}: {e}")

                    async with state_lock:
                        state["status"] = "idle"
                        state["is_stopping"] = False
                        state["active_pid"] = None
                        state["current_task"] = None

                    EXEC_TRACKER.record_5am_cutoff()

                    if os.path.exists(STOP_FILE):
                        try: os.remove(STOP_FILE)
                        except Exception: pass
                    if os.path.exists(ACTIVE_FILE):
                        try: os.remove(ACTIVE_FILE)
                        except Exception: pass

                # 2. When not in CSV-only mode, clear sheet keeping only row 1 and 2
                if last_cleared_date != current_date_str:
                    try:
                        from main import get_gspread_client
                        from common.bulk_pusher import handle_5am_sheet_cleanup
                        target_sheet_id = settings.CF_SHEET_ID
                        target_sheet_name = getattr(settings, "CF_EXTRACTING_SHEET_NAME", "CF_IF_Engine_Database")
                        primary_mgr = await get_gspread_client()
                        if primary_mgr and target_sheet_id:
                            gc = await primary_mgr.authorize()
                            cleared, reason = await handle_5am_sheet_cleanup(
                                sheet_dir=BASE_DIR,
                                client=gc,
                                sheet_id=target_sheet_id,
                                sheet_name=target_sheet_name,
                                keep_rows=2
                            )
                            if cleared:
                                logger.info(f"🧹 [5:00 AM Cutoff] Sheet {target_sheet_id} cleared (kept rows 1 & 2).")
                            else:
                                logger.info(f"ℹ️ [5:00 AM Cutoff] Sheet cleanup skipped: {reason}")
                        last_cleared_date = current_date_str
                    except Exception as ce:
                        logger.error(f"Error during 5:00 AM sheet cleanup: {ce}")

                # 3. Rotate daily cache once per day during the 5:00 AM window
                if last_rotated_date != current_date_str and LOCAL_CACHE:
                    try:
                        archived = LOCAL_CACHE.rotate_daily_cache(ARCHIVE_CACHE_DIR, current_dt=now_ist)
                        last_rotated_date = current_date_str
                        logger.info(f"📦 5:00 AM IST Daily Cache Rotation complete. Archived to {archived}, fresh cache active.")
                    except Exception as re:
                        logger.error(f"Error during daily cache rotation: {re}")

            # Supervisor: check if active_pid died unexpectedly while state was running
            active_pid = state.get("active_pid")
            if active_pid and not is_pid_alive(active_pid) and state.get("status") == "running" and not state.get("is_stopping"):
                logger.warning(f"🚨 Supervisor watchdog detected active PID {active_pid} died unexpectedly.")
                EXEC_TRACKER.record_external_failure(-1)
                async with state_lock:
                    state["status"] = "failed"
                    state["active_pid"] = None
                    state["current_task"] = None
                should_resume, saved_state = EXEC_TRACKER.should_auto_resume()
                if should_resume and saved_state:
                    cnt = EXEC_TRACKER.increment_retry_count()
                    logger.warning(
                        f"Autonomous Smart Resume triggered by supervisor in 15s (attempt {cnt}/{EXEC_TRACKER.MAX_AUTO_RETRIES})..."
                    )
                    req = RunRequest(
                        start_row=3,
                        mode=saved_state.get("mode", "full"),
                        sheet_id=saved_state.get("sheet_id", settings.CF_SHEET_ID)
                    )
                    asyncio.create_task(_delayed_smart_resume(req, 15))

            await asyncio.sleep(15)
        except Exception as e:
            logger.error(f"Exception in _daily_5am_ist_watchdog: {e}")
            await asyncio.sleep(15)

async def _delayed_smart_resume(req: RunRequest, delay_sec: int):
    """Waits for a buffer, then autonomously smart-resumes the pipeline where it left off."""
    logger.info(f"⏳ Waiting {delay_sec}s buffer before autonomous smart resume...")
    await asyncio.sleep(delay_sec)
    if is_5am_ist_cutoff():
        logger.warning("5:00 AM IST cutoff reached during resume delay. Postponing resume.")
        return
    if state["status"] in ("running", "stopping"):
        logger.info("Pipeline already running or stopping. Skipping auto-resume.")
        return
    logger.info(f"🚀 Autonomously resuming pipeline: start_row={req.start_row}, mode={req.mode}, sheet_id={req.sheet_id}")
    await run_pipeline_task(req)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(_daily_5am_ist_watchdog())

    # Check if previous execution died due to desktop shutdown / power cut / reboot
    should_resume, saved_state = EXEC_TRACKER.should_auto_resume()
    if should_resume and saved_state:
        cnt = EXEC_TRACKER.increment_retry_count()
        logger.warning(
            f"🚨 System Startup: Detected external termination (desktop shutdown/infra failure) "
            f"from previous execution (PID {saved_state.get('active_pid')}, started {saved_state.get('start_time_ist')}). "
            f"Autonomous Smart Resume initiating in 10 seconds (attempt {cnt}/{EXEC_TRACKER.MAX_AUTO_RETRIES})..."
        )
        req = RunRequest(
            start_row=3,
            mode=saved_state.get("mode", "full"),
            sheet_id=saved_state.get("sheet_id", settings.CF_SHEET_ID)
        )
        asyncio.create_task(_delayed_smart_resume(req, 10))

security = HTTPBearer()

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != settings.SERVICE_AUTH_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

state_lock = asyncio.Lock()
state = {
    "status": "idle",
    "is_stopping": False,
    "current_task": None,
    "start_time": None,
    "active_pid": None,
    "progress": {"current": 0, "total": 0, "success": 0, "fail": 0}
}

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_device_active_sheets() -> int:
    """Calculates active running sheets on this local device across Sheet1..Sheet4."""
    active_count = 0
    root_dir = os.path.dirname(BASE_DIR)
    current_folder = os.path.basename(BASE_DIR)

    for folder in ["Sheet1", "Sheet2", "Sheet3", "Sheet4"]:
        if folder == current_folder:
            if state.get("status") in ("running", "stopping"):
                active_count += 1
            continue

        f_dir = os.path.join(root_dir, folder)
        act_file = os.path.join(f_dir, ".active_state.json")
        prog_file = os.path.join(f_dir, ".progress.json")
        is_folder_active = False

        if os.path.exists(act_file):
            try:
                with open(act_file, "r") as f:
                    adata = json.load(f)
                pid = adata.get("pid")
                if pid:
                    try:
                        os.kill(pid, 0)
                        is_folder_active = True
                    except OSError:
                        try: os.remove(act_file)
                        except Exception: pass
            except Exception:
                pass

        if not is_folder_active and os.path.exists(prog_file):
            try:
                mtime = os.path.getmtime(prog_file)
                if (time.time() - mtime) < 45:
                    with open(prog_file, "r") as f:
                        pdata = json.load(f)
                    if pdata.get("current", 0) < pdata.get("total", 0):
                        is_folder_active = True
            except Exception:
                pass

        if is_folder_active:
            active_count += 1

    return active_count

async def run_pipeline_task(request: RunRequest):
    existing_prog = {"current": 0, "total": 0, "success": 0, "fail": 0}
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as pf:
                data = json.load(pf)
                if isinstance(data, dict):
                    existing_prog = data
        except Exception:
            pass

    async with state_lock:
        state["status"] = "running"
        state["is_stopping"] = False
        state["start_time"] = utc_now()
        state["progress"] = existing_prog

    if os.path.exists(STOP_FILE):
        try: os.remove(STOP_FILE)
        except Exception: pass

    try:

        effective_start_row = 3
        cmd = [sys.executable, "main.py", str(effective_start_row), request.mode]
        target_sheet = request.sheet_id or settings.CF_SHEET_ID
        if target_sheet:
            cmd.extend(["--sheet_id", target_sheet])

        logger.info(f"Starting Filter-V2 pipeline (always from row {effective_start_row}): {' '.join(cmd)}")

        stdout_path = os.path.join(LOGS_DIR, "stdout.log")
        stderr_path = os.path.join(LOGS_DIR, "stderr.log")
        with open(stdout_path, "a") as out_f, open(stderr_path, "a") as err_f:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=out_f,
                stderr=err_f,
                cwd=BASE_DIR
            )
            state["active_pid"] = process.pid
            logger.info(f"Filter-V2 pipeline running with PID {process.pid}")

            EXEC_TRACKER.record_start(
                sheet=settings.SERVICE_NAME,
                sheet_id=target_sheet,
                start_row=effective_start_row,
                mode=request.mode,
                pid=process.pid
            )

            try:
                with open(ACTIVE_FILE, "w") as f:
                    json.dump({
                        "sheet": settings.SERVICE_NAME,
                        "active": True,
                        "pid": process.pid,
                        "start_time": state["start_time"]
                    }, f)
            except Exception:
                pass

            await process.wait()

        if state.get("is_stopping"):
            logger.info("CF-1 pipeline stopped gracefully by user")
            EXEC_TRACKER.record_user_stop()
            state["status"] = "stopped"
        elif is_5am_ist_cutoff():
            logger.warning("CF-1 pipeline stopped due to 5:00 AM IST daily cutoff")
            EXEC_TRACKER.record_5am_cutoff()
            state["status"] = "idle"
        elif process.returncode == 0:
            logger.info("CF-1 pipeline completed successfully")
            EXEC_TRACKER.record_completion()
            state["status"] = "succeeded"
        else:
            logger.error(f"CF-1 pipeline exited with code {process.returncode}")
            EXEC_TRACKER.record_external_failure(process.returncode)
            state["status"] = "failed"
            should_resume, saved_state = EXEC_TRACKER.should_auto_resume()
            if should_resume and saved_state:
                cnt = EXEC_TRACKER.increment_retry_count()
                logger.warning(
                    f"🚨 Pipeline terminated abnormally (code {process.returncode}). "
                    f"Autonomous Smart Resume scheduled in 15 seconds (attempt {cnt}/{EXEC_TRACKER.MAX_AUTO_RETRIES})..."
                )
                asyncio.create_task(_delayed_smart_resume(request, 15))

    except Exception as e:
        logger.error(f"Pipeline execution error: {e}")
        EXEC_TRACKER.record_external_failure(-1)
        state["status"] = "failed"
    finally:
        state["current_task"] = None
        state["active_pid"] = None
        state["is_stopping"] = False
        if os.path.exists(STOP_FILE):
            try: os.remove(STOP_FILE)
            except Exception: pass
        if os.path.exists(ACTIVE_FILE):
            try: os.remove(ACTIVE_FILE)
            except Exception: pass

def _get_status_response():
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r") as f:
                state["progress"] = json.load(f)
    except Exception: pass

    is_stopping = state.get("is_stopping", False)
    current_status = "stopping" if (is_stopping and state["status"] in ("running", "stopping")) else state["status"]
    worker_identity = os.environ.get("WORKER_IDENTITY", os.uname().nodename)
    is_vishnu = "vishnu" in worker_identity.lower() or "vishnu" in os.getenv("USER", "").lower()

    active_sheets = get_device_active_sheets()
    max_sheets = 999 if is_vishnu else 2

    prog = state.get("progress", {})
    curr_phase = prog.get("phase", "Running" if state["status"] in ("running", "stopping") else "Idle")
    
    active_tc = prog.get("active_tc_keys", 0)
    if not active_tc and state["status"] in ("running", "stopping"):
        raw_tokens = [t.strip() for t in os.getenv("TRACXN_API_TOKENS", "").split(",") if t.strip()]
        if not raw_tokens and getattr(settings, "Tracxn_API_Token", None):
            raw_tokens = [settings.Tracxn_API_Token]
        active_tc = len(raw_tokens) if raw_tokens else 30

    active_nim = prog.get("active_nim_keys", 0)
    if not active_nim and state["status"] in ("running", "stopping"):
        nim_keys = [k.strip() for k in os.getenv("NIM_API_KEYS", "").split(",") if k.strip()]
        from common import get_sharded_nim_keys
        svc_name = getattr(settings, "SERVICE_NAME", "")
        shard_idx = 2 if ("CF-2" in svc_name or "Sheet2" in BASE_DIR) else (4 if ("Filter-V2" in svc_name or "Sheet4" in BASE_DIR) else (3 if ("CF-3" in svc_name or "Sheet3" in BASE_DIR) else 1))
        sharded = get_sharded_nim_keys(shard_idx, total_sheets=4, keys=nim_keys)
        active_nim = len(sharded) if sharded else 8
    
    if "Phase 1" in curr_phase or "Crawl" in curr_phase or state["status"] in ("running", "stopping"):
        workers_phase = "16 workers (14 domain + 1 TC sweep + 1 writer)"
    elif "Review" in curr_phase or "Phase 3" in curr_phase:
        workers_phase = "14 review workers"
    elif "Phase 4" in curr_phase or "Ingestion" in curr_phase:
        workers_phase = "Batch writer (size 100)"
    else:
        workers_phase = "0 (Idle)"

    try:
        import psutil
        cpu_pct = round(psutil.cpu_percent(interval=None), 1)
        mem_pct = round(psutil.virtual_memory().percent, 1)
        dev_health = "HEALTHY" if cpu_pct < 85 and mem_pct < 85 else "THROTTLED"
    except Exception:
        cpu_pct = 0.0
        mem_pct = 0.0
        dev_health = "UNKNOWN"

    stage_details = prog.get("stage_details", "")
    current_val = prog.get("current", 0)
    total_val = prog.get("total", 0)
    success_val = prog.get("success", 0)
    fail_val = prog.get("fail", 0)

    # 1. First priority: Exact phase metrics recorded in progress
    scraped_count = prog.get("scraped_count")
    nim_count = prog.get("nim_count")
    gemini_count = prog.get("gemini_count")
    tracxn_count = prog.get("tracxn_count")

    # 2. Telemetry heuristics fallback if not in progress
    if tracxn_count is None:
        m_already = re.search(r"(\d+)\s+already completed", stage_details)
        m_comp = re.search(r"(\d+)/\d+\s+fully completed", stage_details)
        if m_comp:
            tracxn_count = int(m_comp.group(1))
        elif m_already:
            tracxn_count = int(m_already.group(1))
        else:
            tracxn_count = current_val

    if nim_count is None:
        m_nim = re.search(r"(\d+)/\d+\s+summarized", stage_details)
        m_already_sum = re.search(r"(\d+)\s+already summarized", stage_details)
        if m_nim:
            nim_count = int(m_nim.group(1))
        elif m_already_sum:
            nim_count = int(m_already_sum.group(1))
        else:
            nim_count = 0

    if gemini_count is None:
        gemini_count = 0

    tc_success = prog.get("tc_success")
    if tc_success is None:
        m_tc_succ = re.search(r"(\d+)\s+success", stage_details, re.IGNORECASE)
        if m_tc_succ:
            tc_success = int(m_tc_succ.group(1))
        else:
            tc_success = success_val if (success_val is not None and success_val != scraped_count) else scraped_count

    if scraped_count is None:
        m_tc = re.search(r"(\d+)\s+resolved", stage_details, re.IGNORECASE)
        if m_tc:
            scraped_count = int(m_tc.group(1))
        else:
            scraped_count = tc_success if tc_success is not None else (success_val if success_val is not None else 0)

    completed_count = tracxn_count if tracxn_count is not None else current_val

    # Any fully completed tracxn domain must have completed Gemini predictions
    if tracxn_count is not None and (gemini_count is None or tracxn_count > gemini_count):
        gemini_count = tracxn_count

    # Bounding: each stage count must not exceed total_val
    if total_val >= 1000:
        scraped_count = min(scraped_count if scraped_count is not None else 0, total_val)
        tc_success = min(tc_success if tc_success is not None else 0, total_val)
        nim_count = min(nim_count if nim_count is not None else 0, total_val)
        gemini_count = min(gemini_count if gemini_count is not None else 0, total_val)
        tracxn_count = min(tracxn_count if tracxn_count is not None else 0, total_val)
        completed_count = tracxn_count

    return {
        "status": current_status,
        "active": state["status"] in ("running", "stopping"),
        "is_stopping": is_stopping,
        "progress_current": completed_count,
        "progress_total": total_val,
        "progress_success": tc_success,
        "progress_fail": fail_val,
        "scraped_count": scraped_count,
        "tc_success": tc_success,
        "nim_count": nim_count,
        "gemini_count": gemini_count,
        "tracxn_count": completed_count,
        "phase": curr_phase,
        "stage_details": stage_details,
        "pending_tc": prog.get("pending_tc", 0),
        "pipeline_mode": prog.get("mode", ""),
        "active_tc_keys": active_tc,
        "total_tc_keys": 30,
        "active_nim_keys": active_nim,
        "total_nim_keys": 35,
        "workers_per_phase": workers_phase,
        "cpu_percent": cpu_pct,
        "memory_percent": mem_pct,
        "device_health": dev_health,
        "workerName": f"{worker_identity}-Filter-V2-Pipeline",
        "sheet_id": settings.CF_SHEET_ID,
        "device_active_sheets": active_sheets,
        "device_max_sheets": max_sheets
    }

async def _cancel():
    if state["active_pid"]:
        if state.get("is_stopping"):
            try:
                os.kill(state["active_pid"], signal.SIGKILL)
                logger.info(f"Force killed PID {state['active_pid']}")
            except Exception as e:
                logger.error(f"Failed to kill: {e}")
            state["status"] = "idle"
            state["is_stopping"] = False
            state["active_pid"] = None
            EXEC_TRACKER.record_user_stop()
            if os.path.exists(STOP_FILE):
                try: os.remove(STOP_FILE)
                except Exception: pass
            if os.path.exists(ACTIVE_FILE):
                try: os.remove(ACTIVE_FILE)
                except Exception: pass
            return {"status": "ok", "message": "Force terminated"}

        state["is_stopping"] = True
        state["status"] = "stopping"
        EXEC_TRACKER.record_user_stop()
        try:
            with open(STOP_FILE, "w") as f:
                f.write("stop")
        except Exception: pass

        try:
            os.kill(state["active_pid"], signal.SIGTERM)
            logger.info(f"Sent SIGTERM to PID {state['active_pid']}")
        except Exception: pass

        return {"status": "stopping", "message": "Graceful stop requested."}

    state["status"] = "idle"
    state["is_stopping"] = False
    return {"status": "ok"}

def tail_file(filepath: str, max_lines: int = 200) -> str:
    if not os.path.exists(filepath):
        return ""
    try:
        from collections import deque
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            return "".join(deque(f, maxlen=max_lines))
    except Exception as e:
        return f"[Error reading log: {e}]"

def _get_logs_response(lines: int = 200, log_type: str = "all"):
    worker_identity = os.environ.get("WORKER_IDENTITY", os.uname().nodename)
    result = {
        "status": "ok",
        "service": settings.SERVICE_NAME,
        "worker": f"{worker_identity}-{settings.SERVICE_NAME}-Pipeline",
        "timestamp": utc_now(),
        "lines_requested": lines,
        "logs": {},
        "available_files": []
    }

    api_log_file = os.path.join(LOGS_DIR, "api.logs") if os.path.exists(os.path.join(LOGS_DIR, "api.logs")) else os.path.join(LOGS_DIR, "api.log")
    log_mapping = {
        "discovery": os.path.join(LOGS_DIR, "discovery.log"),
        "stdout": os.path.join(LOGS_DIR, "stdout.log"),
        "stderr": os.path.join(LOGS_DIR, "stderr.log"),
        "api": api_log_file,
    }

    if os.path.exists(LOGS_DIR):
        result["available_files"] = [f for f in os.listdir(LOGS_DIR) if os.path.isfile(os.path.join(LOGS_DIR, f))]

    if log_type == "all":
        for k, path in log_mapping.items():
            result["logs"][k] = tail_file(path, lines)
    elif log_type in log_mapping:
        result["logs"][log_type] = tail_file(log_mapping[log_type], lines)
    else:
        custom_path = os.path.join(LOGS_DIR, log_type)
        if os.path.exists(custom_path) and os.path.commonpath([LOGS_DIR, custom_path]) == LOGS_DIR:
            result["logs"][log_type] = tail_file(custom_path, lines)
        else:
            result["logs"][log_type] = f"Log type '{log_type}' not found."

    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                result["progress"] = json.load(f)
        except Exception:
            pass

    return result

for prefix in ["/discovery", "/sheet1"]:
    app.add_api_route(f"{prefix}/health", lambda: {"status": "ok", "service": "CF-1", "timestamp": utc_now()}, methods=["GET"], dependencies=[Depends(verify_token)])
    app.add_api_route(f"{prefix}/status", _get_status_response, methods=["GET"], dependencies=[Depends(verify_token)])
    app.add_api_route(f"{prefix}/logs", _get_logs_response, methods=["GET"], dependencies=[Depends(verify_token)])
    app.add_api_route(f"{prefix}/cancel", _cancel, methods=["POST"], dependencies=[Depends(verify_token)])

@app.post("/discovery/start", dependencies=[Depends(verify_token)])
@app.post("/sheet1/start", dependencies=[Depends(verify_token)])
async def start_pipeline(req: RunRequest, background_tasks: BackgroundTasks):
    if is_5am_ist_cutoff():
        return {
            "status": "error",
            "message": "Cannot start pipeline between 05:00 - 05:05 AM IST. Apps Script daily domain refresh in progress."
        }

    if state["status"] in ("running", "stopping"):
        return {"status": "error", "message": "Pipeline already running or stopping for this sheet"}

    worker_identity = os.environ.get("WORKER_IDENTITY", os.uname().nodename)
    is_vishnu = "vishnu" in worker_identity.lower() or "vishnu" in os.getenv("USER", "").lower()
    active_sheets = get_device_active_sheets()

    if not is_vishnu and active_sheets >= 2:
        return {
            "status": "busy",
            "message": f"Device capacity reached ({active_sheets}/2 active sheets). Maximum 2 concurrent sheets allowed on this host."
        }

    EXEC_TRACKER.reset_retry_count()
    background_tasks.add_task(run_pipeline_task, req)
    return {"status": "accepted", "sheet_id": req.sheet_id or settings.CF_SHEET_ID}

async def run_clear_and_fetch_task(sheet_id: str):
    try:
        if "unittest" not in sys.modules:
            try:
                import importlib
                import common.tracxn_domain_ingestor
                import common.advance_precrawl
                importlib.reload(common.tracxn_domain_ingestor)
                importlib.reload(common.advance_precrawl)
            except Exception:
                pass

        precrawl_csv = os.path.join(BASE_DIR, "next_day_pre_crawl.csv")
        folder_name = os.path.basename(BASE_DIR)
        count = 0

        if os.path.exists(precrawl_csv):
            logger.info(f"📂 Found advance pre-crawl CSV ({precrawl_csv}). Loading pre-crawled domains with Job IDs into sheet {sheet_id}...")
            from common.advance_precrawl import load_precrawl_to_sheet
            count = await asyncio.to_thread(load_precrawl_to_sheet, sheet_id, folder_name)
            logger.info(f"✅ Loaded {count} pre-crawled domains with Job IDs to sheet {sheet_id}.")
        else:
            logger.info(f"ℹ️ No advance pre-crawl CSV found at {precrawl_csv}. Falling back to live Tracxn ingestion...")
            from common.tracxn_domain_ingestor import TracxnDomainIngestor
            ingestor = TracxnDomainIngestor()
            sheet_cfg = {
                "name": f"Sheet-{sheet_id[:8]}",
                "sheet_id": sheet_id
            }
            count = await asyncio.to_thread(ingestor.ingest_domains_for_sheet, sheet_cfg, clear_first=True)
            logger.info(f"✅ Live Ingestion completed for sheet {sheet_id}: {count} domains ingested.")

        if count and count > 0:
            init_prog = {
                "current": 0,
                "total": count,
                "success": 0,
                "fail": 0,
                "scraped_count": 0,
                "tc_success": 0,
                "nim_count": 0,
                "gemini_count": 0,
                "tracxn_count": 0,
                "phase": "Starting fresh daily batch",
                "stage_details": f"Ingested {count} fresh domains",
                "pending_tc": 0,
                "mode": "full"
            }
            try:
                with open(PROGRESS_FILE, "w") as pf:
                    json.dump(init_prog, pf)
            except Exception:
                pass
            state["progress"] = init_prog

            if is_5am_ist_cutoff():
                dt = get_now_ist()
                wait_secs = max(5, (5 - dt.minute) * 60 - dt.second + 2)
                logger.info(f"⏳ Clear & Fetch completed ({count} domains). Waiting {wait_secs}s for 05:05 AM IST cutoff window to pass before starting pipeline...")
                await asyncio.sleep(wait_secs)

            logger.info(f"🚀 Auto-starting discovery engine pipeline for sheet {sheet_id} (start_row=3, mode=full)...")
            EXEC_TRACKER.reset_retry_count()
            pipeline_req = RunRequest(start_row=3, mode="full", sheet_id=sheet_id)
            await run_pipeline_task(pipeline_req)

            # Auto-spawn tomorrow's Day T-1 pre-crawl in detached background process (24-hour rendering)
            try:
                from common.advance_precrawl import spawn_advance_precrawl_subprocess
                sheet_num_map = {"Sheet1": 1, "Sheet2": 2, "Sheet3": 3, "Sheet4": 4}
                s_num = sheet_num_map.get(folder_name, 4)
                spawn_advance_precrawl_subprocess(folder=folder_name, sheet_num=s_num, budget=15000)
                logger.info(f"🚀 Spawned tomorrow's advance pre-crawl subprocess for {folder_name} (24-hour S3 rendering).")
            except Exception as pe:
                logger.warning(f"Could not spawn advance pre-crawl for tomorrow: {pe}")
        else:
            logger.warning(f"⚠️ Clear & Fetch returned {count} domains for sheet {sheet_id}. Downstream pipeline auto-start skipped to prevent running on an empty sheet.")
    except Exception as e:
        logger.error(f"❌ Clear & Fetch failed for sheet {sheet_id}: {e}", exc_info=True)

@app.post("/discovery/clear-and-fetch", dependencies=[Depends(verify_token)])
@app.post("/sheet4/clear-and-fetch", dependencies=[Depends(verify_token)])
@app.post("/discovery/fetch-domains", dependencies=[Depends(verify_token)])
@app.post("/sheet4/fetch-domains", dependencies=[Depends(verify_token)])
async def clear_and_fetch_domains_route(background_tasks: BackgroundTasks, req: Optional[RunRequest] = None):
    target_sheet_id = (req.sheet_id if req else None) or settings.CF_SHEET_ID
    background_tasks.add_task(run_clear_and_fetch_task, target_sheet_id)
    return {
        "status": "accepted",
        "message": "Clear and fetch domains initiated in background",
        "sheet_id": target_sheet_id
    }

@app.post("/discovery/advance-precrawl", dependencies=[Depends(verify_token)])
@app.post("/sheet4/advance-precrawl", dependencies=[Depends(verify_token)])
async def advance_precrawl_route(req: Optional[RunRequest] = None):
    target_sheet_id = (req.sheet_id if req else None) or settings.CF_SHEET_ID
    from common.advance_precrawl import spawn_advance_precrawl_subprocess
    folder_name = os.path.basename(BASE_DIR)
    sheet_num = int(folder_name.replace("Sheet", "") if "Sheet" in folder_name else 4)
    pid = spawn_advance_precrawl_subprocess(folder_name, sheet_num=sheet_num, budget=15000)
    return {
        "status": "accepted",
        "message": f"Advance pre-crawl for tomorrow launched as isolated background process (PID {pid}) for {folder_name} (15,000 budget)",
        "sheet_id": target_sheet_id,
        "pid": pid
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.DISCOVERY_PORT)
