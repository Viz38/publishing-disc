"""Shared daily midnight cutoff, eligible export, CSV-only status, and 5k backlog carryover lifecycle."""

import asyncio
import logging
import os
import re
from typing import Any, Callable, Dict, List, Optional

from common.backlog_manager import BacklogManager
from common.bulk_pusher import handle_5am_sheet_cleanup
from common.daily_eligible_exporter import (
    DAILY_PROCESSING_GID,
    DAILY_PROCESSING_SHEET_ID,
    collect_fleet_eligible_records,
    export_eligible_domains_to_gsheet,
    load_sheet_eligible_snapshot,
    save_sheet_eligible_snapshot,
)
from common.dashboard_metrics import (
    extract_eligible_domain_records,
    load_cache_map,
    load_local_sheet_rows,
)
from common.gsheet_circuit_breaker import is_csv_only_mode
from common.local_cache import get_now_ist

logger = logging.getLogger(__name__)


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
    save_sheet_eligible_snapshot(base_dir, eligible_recs, snapshot_date=snapshot_date)

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
) -> bool:
    """
    Coordinates consolidated 4-sheet export to Discovery Daily Processing (1DCKv...).
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
        fleet_recs = await asyncio.to_thread(
            collect_fleet_eligible_records,
            snapshot_date=snapshot_date,
            local_sheet_num=sheet_num,
            local_records=local_records,
        )
        exported_cnt = await asyncio.to_thread(
            export_eligible_domains_to_gsheet,
            sync_gc,
            fleet_recs,
            updated_dt=now_ist,
        )
        with open(done_marker, "w") as mf:
            mf.write(f"exported={exported_cnt},date={expected_b1}")
        logger.info(
            f"✅ [12:00 AM Export] Sheet{sheet_num} exported {exported_cnt} eligible fleet domains "
            f"to {DAILY_PROCESSING_SHEET_ID} (B1={expected_b1})."
        )
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
    snap = load_sheet_eligible_snapshot(base_dir, target_date) if date_param else None
    if snap is None:
        # Also check today's snapshot if cutoff just cleared sheet
        today_str = get_now_ist().strftime("%Y-%m-%d")
        snap = load_sheet_eligible_snapshot(base_dir, today_str)

    cache_map = load_cache_map(cache=local_cache, base_dir=base_dir)
    live_rows = _read_rows_for_snapshot(base_dir, sync_gc=sync_gc, sheet_id=sheet_id, sheet_name=sheet_name)
    live_recs = extract_eligible_domain_records(live_rows, cache_map=cache_map) if live_rows else []

    records = live_recs if len(live_recs) >= len(snap or []) else (snap or [])
    return {
        "sheet": os.path.basename(os.path.abspath(base_dir)),
        "date": target_date,
        "count": len(records),
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

