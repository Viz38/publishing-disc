import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from models import RunRequest
from config import settings

LOGS_DIR = os.path.join(BASE_DIR, 'Logs')
PROGRESS_FILE = os.path.join(BASE_DIR, ".progress.json")
STOP_FILE = os.path.join(BASE_DIR, ".stop_requested")
ACTIVE_FILE = os.path.join(BASE_DIR, ".active_state.json")
os.makedirs(LOGS_DIR, exist_ok=True)
api_log_path = os.path.join(LOGS_DIR, 'api.logs')

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
logger = logging.getLogger("api_cf-3")

app = FastAPI(title="Discovery Engine - Discovery AI - CF-3 (Nandan Domains DB)")

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
    async with state_lock:
        state["status"] = "running"
        state["is_stopping"] = False
        state["start_time"] = utc_now()
        state["progress"] = {"current": 0, "total": 0, "success": 0, "fail": 0}

    if os.path.exists(STOP_FILE):
        try: os.remove(STOP_FILE)
        except Exception: pass

    try:
        if os.path.exists(PROGRESS_FILE):
            try: os.remove(PROGRESS_FILE)
            except Exception: pass

        cmd = [sys.executable, "main.py", str(request.start_row), request.mode]
        target_sheet = request.sheet_id or settings.CF_SHEET_ID
        if target_sheet:
            cmd.extend(["--sheet_id", target_sheet])

        logger.info(f"Starting CF-3 pipeline: {' '.join(cmd)}")

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
            logger.info(f"CF-3 pipeline running with PID {process.pid}")

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
            logger.info("CF-3 pipeline stopped gracefully")
            state["status"] = "stopped"
        elif process.returncode == 0:
            logger.info("CF-3 pipeline completed successfully")
            state["status"] = "succeeded"
        else:
            logger.error(f"CF-3 pipeline exited with code {process.returncode}")
            state["status"] = "failed"

    except Exception as e:
        logger.error(f"Pipeline execution error: {e}")
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

    return {
        "status": current_status,
        "active": state["status"] in ("running", "stopping"),
        "is_stopping": is_stopping,
        "progress_current": state["progress"].get("current", 0),
        "progress_total": state["progress"].get("total", 0),
        "progress_success": state["progress"].get("success", 0),
        "progress_fail": state["progress"].get("fail", 0),
        "workerName": f"{worker_identity}-CF-3-Pipeline",
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
            if os.path.exists(STOP_FILE):
                try: os.remove(STOP_FILE)
                except Exception: pass
            if os.path.exists(ACTIVE_FILE):
                try: os.remove(ACTIVE_FILE)
                except Exception: pass
            return {"status": "ok", "message": "Force terminated"}

        state["is_stopping"] = True
        state["status"] = "stopping"
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

for prefix in ["/discovery", "/sheet3"]:
    app.add_api_route(f"{prefix}/health", lambda: {"status": "ok", "service": "CF-3", "timestamp": utc_now()}, methods=["GET"], dependencies=[Depends(verify_token)])
    app.add_api_route(f"{prefix}/status", _get_status_response, methods=["GET"], dependencies=[Depends(verify_token)])
    app.add_api_route(f"{prefix}/cancel", _cancel, methods=["POST"], dependencies=[Depends(verify_token)])

@app.post("/discovery/start", dependencies=[Depends(verify_token)])
@app.post("/sheet3/start", dependencies=[Depends(verify_token)])
async def start_pipeline(req: RunRequest, background_tasks: BackgroundTasks):
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

    background_tasks.add_task(run_pipeline_task, req)
    return {"status": "accepted", "sheet_id": req.sheet_id or settings.CF_SHEET_ID}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=settings.DISCOVERY_PORT)
