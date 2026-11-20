"""Shared daily midnight cutoff, eligible export, CSV-only status, and 5k backlog carryover lifecycle."""

import asyncio
import datetime
import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from common.backlog_manager import BacklogManager
from common.bulk_pusher import handle_5am_sheet_cleanup
from common.daily_eligible_exporter import (
    DAILY_PROCESSING_GID,
    DAILY_PROCESSING_SHEET_ID,
    collect_fleet_eligible_records,
    collect_fleet_gemini_ran_count,
    count_gemini_ran_from_rows_and_cache,
    export_eligible_domains_to_gsheet,
    export_fleet_eligible_sequential,
    load_sheet_daily_metrics_snapshot,
    load_sheet_eligible_snapshot,
    save_sheet_eligible_snapshot,
    update_daily_processed_counts_sheet,
)
from common.dashboard_metrics import (
    extract_eligible_domain_records,
    load_cache_map,
    load_local_sheet_rows,
)
from common.gsheet_circuit_breaker import is_csv_only_mode
from common.local_cache import (
    IST_TZ,
    get_now_ist,
    is_5am_ist_cutoff,
    is_pre_cutoff_window,
)

logger = logging.getLogger(__name__)


def get_effective_budget(
    base_budget: Union[int, float],
    target_date: Optional[Union[datetime.date, datetime.datetime]] = None,
) -> Union[int, float]:
    """
    Calculates the effective daily budget applying the fleet weekend 50% policy.
    - Weekdays (Monday through Friday in IST): returns 100% of base_budget.
    - Weekends (Saturday and Sunday in IST): returns 50% of base_budget (max(1, round(base_budget * 0.5))).
    - Zero or infinite budgets are preserved as-is.
    """
    if base_budget == float("inf") or base_budget <= 0:
        return base_budget
    if target_date is None:
        target_date = get_now_ist().date()
    elif isinstance(target_date, datetime.datetime):
        target_date = target_date.date()

    # weekday(): Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4, Saturday=5, Sunday=6
    if target_date.weekday() in (5, 6):
        return max(1, round(base_budget * 0.5))
    return base_budget


def should_trigger_daily_guardian(
    saved_state: Optional[Dict[str, Any]],
    current_state: Dict[str, Any],
    current_dt: Optional[datetime.datetime] = None,
) -> Tuple[bool, str]:
    """
    State-driven decision engine determining whether the autonomous daily guardian
    should trigger a fresh daily domain ingestion and pipeline start.

    Returns (True, reason) if:
      1. Not inside the daily maintenance cutoff window (23:30 - 00:05 IST).
      2. No pipeline or task is actively running or stopping.
      3. A new day's cycle has arrived (or no previous run exists).
      4. Today's run has not already completed or been user-cancelled today.
    """
    if current_dt is None:
        current_dt = get_now_ist()
    elif current_dt.tzinfo is None:
        current_dt = current_dt.replace(tzinfo=IST_TZ)
    else:
        current_dt = current_dt.astimezone(IST_TZ)

    # 1. Maintenance cutoff window check (23:30 - 00:05 IST)
    if is_pre_cutoff_window(current_dt) or is_5am_ist_cutoff(current_dt):
        return (False, "maintenance_cutoff_window")

    # 2. Active pipeline check
    active_pid = current_state.get("active_pid")
    if active_pid is not None:
        return (False, f"pipeline_running_pid_{active_pid}")
    if current_state.get("status") in ("running", "stopping"):
        return (False, f"pipeline_status_{current_state.get('status')}")
    if current_state.get("current_task") is not None:
        return (False, "task_already_active")

    # 3. State evaluation
    if not saved_state:
        return (True, "no_previous_state")

    current_date_str = current_dt.strftime("%Y-%m-%d")
    last_run_date = saved_state.get("date_ist")
    last_status = saved_state.get("status")
    last_term = saved_state.get("termination_reason")

    if last_run_date != current_date_str:
        return (True, f"new_day_cycle_last_run_{last_run_date}")

    # Same date checks
    if last_status == "completed" or last_term == "completed":
        return (False, "today_run_already_completed")
    if last_status == "user_cancelled" or last_term == "user_cancelled":
        return (False, "today_run_user_cancelled")
    if last_status in ("failed", "interrupted"):
        return (True, f"today_run_retry_{last_status}")
    if last_status in ("idle", "stopped"):
        return (True, f"today_run_unstarted_{last_status}")
    if last_status == "running":
        # Stale run recorded as running but no active PID in current_state
        return (True, "today_crashed_run_recovery")

    return (False, f"unhandled_state_{last_status}")


def reload_all_common_modules() -> List[str]:
    """
    Dynamically reloads all cached common modules in sys.modules to prevent
    stale in-memory bytecode or ImportErrors after git updates on persistent daemon runtimes.
    """
    import importlib
    import sys

    reloaded = []
    MODULES_TO_RELOAD = [
        "common.daily_lifecycle",
        "common.local_cache",
        "common.fleet_deduplicator",
        "common.tracxn_domain_ingestor",
        "common.advance_precrawl",
        "common.key_manager",
        "common.tech_crawler",
        "common.gsheet_circuit_breaker",
        "common.bulk_pusher",
        "common.daily_eligible_exporter",
        "common.dashboard_metrics",
    ]
    for mod_name in MODULES_TO_RELOAD:
        if mod_name in sys.modules:
            try:
                importlib.reload(sys.modules[mod_name])
                reloaded.append(mod_name)
            except Exception as e:
                logger.warning(f"Failed to reload {mod_name}: {e}")
    return reloaded




def get_sheet_num_from_dir(base_dir: str) -> int:
    m = re.search(r"Sheet(\d+)", os.path.basename(os.path.abspath(base_dir)), re.IGNORECASE)
    return int(m.group(1)) if m else 1


def _read_rows_for_snapshot(
    base_dir: str,
    sync_gc: Any,
    sheet_id: Optional[str],
    sheet_name: str,
) -> List[List[str]]:
    rows: List[List[str]] = []
    if not is_csv_only_mode(base_dir) and sync_gc is not None and sheet_id:
        try:
            ss = sync_gc.open_by_key(sheet_id)
            ws = ss.worksheet(sheet_name)
            vals = ws.get_all_values()
            if len(vals) > 2:
                rows = vals[2:]
        except Exception as e:
            logger.warning(f"GSheet read failed during midnight snapshot ({sheet_id}); falling back to local_sheet.csv: {e}")

    if not rows:
        rows = load_local_sheet_rows(base_dir)
    return rows


def snapshot_sheet_before_clear(
    base_dir: str,
    sync_gc: Any,
    sheet_id: str,
    sheet_name: str,
    local_cache: Any,
    backlog_manager: BacklogManager,
    snapshot_date: str,
) -> List[List[str]]:
    """
    Captures BOTH eligible next-department rows AND up to 1,250 uncompleted/scraped carryover rows
    BEFORE midnight sheet clearing and cache rotation wipe daily state.
    """
    cache_map = load_cache_map(cache=local_cache, base_dir=base_dir)
    rows = _read_rows_for_snapshot(base_dir, sync_gc=sync_gc, sheet_id=sheet_id, sheet_name=sheet_name)
    if not rows and cache_map:
        rows = [[dom] + [""] * 56 for dom in cache_map.keys()]

    eligible_recs = extract_eligible_domain_records(rows, cache_map=cache_map)
    gemini_ran_count = count_gemini_ran_from_rows_and_cache(rows, cache_map=cache_map)
    save_sheet_eligible_snapshot(
        base_dir,
        eligible_recs,
        snapshot_date=snapshot_date,
        gemini_ran_count=gemini_ran_count,
    )

    try:
        backlog_items = backlog_manager.extract_uncompleted_items_from_rows(
            rows,
            cache_map=cache_map,
            max_cap=BacklogManager.MAX_PER_SHEET_BACKLOG,
        )
        if backlog_items:
            backlog_manager.save_scraped_backlog(backlog_items)
            logger.info(
                f"📌 [12:00 AM Carryover] Saved {len(backlog_items)} uncompleted domains "
                f"for tomorrow's backfill ({os.path.basename(base_dir)})."
            )
    except Exception as be:
        logger.error(f"Error saving uncompleted carryover backlog: {be}")

    return eligible_recs


async def coordinate_daily_eligible_export(
    base_dir: str,
    sync_gc: Any,
    sheet_num: int,
    snapshot_date: str,
    local_records: List[List[str]],
    now_ist: Any,
    local_gemini_ran: Optional[int] = None,
) -> bool:
    """
    Coordinates consolidated 4-sheet export to Discovery Daily Processing (1DCKv...)
    AND updates the daily summary counts (Col F Accepted + Col G Processing/Gemini) on
    Processed Counts (1GMojL6BkqqTZv3rkcQ19b20y4JYT9gkucPRE7h6vYmo) for the completed run date.
    Sheet 1 is primary coordinator; Sheet 3 is HA fallback after 00:01:30 if B1 wasn't updated yet.
    """
    if sync_gc is None or sheet_num not in (1, 3):
        return False

    logs_dir = os.path.join(base_dir, "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    done_marker = os.path.join(logs_dir, f".daily_eligible_exported_{snapshot_date}.done")
    if os.path.exists(done_marker):
        return True

    expected_b1 = now_ist.strftime("%d %b %Y")
    if sheet_num == 1:
        # Give peer sheets ~20s right at 00:00:00 so all 4 save their midnight snapshots first
        if now_ist.hour == 0 and now_ist.minute == 0 and now_ist.second < 22:
            await asyncio.sleep(22 - now_ist.second)
    elif sheet_num == 3:
        if now_ist.hour == 0 and (now_ist.minute < 1 or (now_ist.minute == 1 and now_ist.second < 30)):
            return False
        try:
            def _is_already_exported():
                ss = sync_gc.open_by_key(DAILY_PROCESSING_SHEET_ID)
                try:
                    ws = ss.get_worksheet_by_id(DAILY_PROCESSING_GID)
                except Exception:
                    ws = ss.worksheet("Sheet1")
                val = (ws.acell("B1").value or "").strip()
                return val == expected_b1

            if await asyncio.to_thread(_is_already_exported):
                with open(done_marker, "w") as mf:
                    mf.write(expected_b1)
                return True
        except Exception as ce:
            logger.warning(f"Sheet3 HA fallback check failed: {ce}")

    try:
        exported_cnt = await asyncio.to_thread(
            export_fleet_eligible_sequential,
            gc=sync_gc,
            snapshot_date=snapshot_date,
            local_sheet_num=sheet_num,
            local_records=local_records,
            now_ist=now_ist,
        )
        with open(done_marker, "w") as mf:
            mf.write(f"exported={exported_cnt},date={expected_b1}")
        logger.info(
            f"✅ [12:00 AM Export] Sheet{sheet_num} executed sequential export of {exported_cnt} eligible fleet domains "
            f"to {DAILY_PROCESSING_SHEET_ID} (B1={expected_b1})."
        )

        # Update Daily Processed Counts (Col F Accepted & Col G Processing/Gemini Ran)
        try:
            report_dt = (now_ist - datetime.timedelta(days=1)) if now_ist.hour < 6 else now_ist
            report_date_str = report_dt.strftime("%Y-%m-%d")
            counts_done_marker = os.path.join(logs_dir, f".daily_processed_counts_{report_date_str}.done")
            if not os.path.exists(counts_done_marker):
                eff_local_gemini = local_gemini_ran
                if eff_local_gemini is None:
                    snap_meta = load_sheet_daily_metrics_snapshot(base_dir, snapshot_date)
                    if snap_meta is not None:
                        eff_local_gemini = snap_meta.get("gemini_ran_count", 0)
                total_gemini_ran = await asyncio.to_thread(
                    collect_fleet_gemini_ran_count,
                    snapshot_date=snapshot_date,
                    local_sheet_num=sheet_num,
                    local_gemini_ran=eff_local_gemini,
                )
                await asyncio.to_thread(
                    update_daily_processed_counts_sheet,
                    gc=sync_gc,
                    report_date_str=report_date_str,
                    accepted_count=exported_cnt,
                    gemini_processing_count=total_gemini_ran,
                )
                with open(counts_done_marker, "w") as cf:
                    cf.write(f"date={report_date_str},accepted={exported_cnt},gemini={total_gemini_ran}")
        except Exception as pe:
            logger.error(f"Error updating Daily Processed Counts sheet: {pe}")

        return True
    except Exception as ee:
        logger.error(f"Error during 12:00 AM daily eligible export: {ee}")
        return False


def get_eligible_domains_payload(
    base_dir: str,
    sync_gc: Any = None,
    sheet_id: Optional[str] = None,
    sheet_name: str = "CF_IF_Engine_Database",
    local_cache: Any = None,
    date_param: Optional[str] = None,
) -> Dict[str, Any]:
    target_date = date_param or get_now_ist().strftime("%Y-%m-%d")
    snap_meta = load_sheet_daily_metrics_snapshot(base_dir, target_date) if date_param else None
    if date_param and snap_meta is not None:
        return {
            "sheet": os.path.basename(os.path.abspath(base_dir)),
            "date": target_date,
            "count": len(snap_meta["records"]),
            "gemini_ran_count": int(snap_meta.get("gemini_ran_count") or 0),
            "records": snap_meta["records"],
        }

    if snap_meta is None:
        today_str = get_now_ist().strftime("%Y-%m-%d")
        snap_meta = load_sheet_daily_metrics_snapshot(base_dir, today_str)

    snap_recs = snap_meta["records"] if snap_meta is not None else []
    snap_gemini = int(snap_meta.get("gemini_ran_count") or 0) if snap_meta is not None else 0

    cache_map = load_cache_map(cache=local_cache, base_dir=base_dir)
    live_rows = _read_rows_for_snapshot(base_dir, sync_gc=sync_gc, sheet_id=sheet_id, sheet_name=sheet_name)
    live_recs = extract_eligible_domain_records(live_rows, cache_map=cache_map) if live_rows else []
    live_gemini = count_gemini_ran_from_rows_and_cache(live_rows, cache_map=cache_map) if (live_rows or cache_map) else 0

    records = live_recs if len(live_recs) >= len(snap_recs) else snap_recs
    gemini_ran_count = max(live_gemini, snap_gemini)
    return {
        "sheet": os.path.basename(os.path.abspath(base_dir)),
        "date": target_date,
        "count": len(records),
        "gemini_ran_count": gemini_ran_count,
        "records": records,
    }


def append_carryover_backlog_sync(
    backlog_manager: BacklogManager,
    sync_gc: Any,
    sheet_id: str,
    sheet_name: str,
    base_dir: str,
    local_cache: Any = None,
) -> int:
    return backlog_manager.append_backlog_to_sheet_or_csv(
        gspread_client=sync_gc,
        sheet_id=sheet_id,
        sheet_name=sheet_name,
        sheet_dir=base_dir,
        cache=local_cache,
        max_cap=BacklogManager.MAX_PER_SHEET_BACKLOG,
    )

