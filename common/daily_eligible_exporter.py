"""Daily consolidated exporter for eligible next-department domains (`B1` date + `B4:D` list)."""

import datetime
import json
import logging
import os
import tempfile
import time
from typing import Any, Dict, List, Optional, Sequence

from common.dashboard_metrics import (
    extract_eligible_domain_records,
    load_cache_map,
    load_local_sheet_rows,
    normalize_domain,
)
from common.domain_filter_validator import filter_unpublished_non_sameas_domains
from common.gsheet_circuit_breaker import is_csv_only_mode
from common.local_cache import IST_TZ, get_now_ist

logger = logging.getLogger(__name__)

DAILY_PROCESSING_SHEET_ID = "1DCKvQcAQgTKGnWquYNOH6SlGgCun8yq8HD8Zj0AIJ-Y"
DAILY_PROCESSING_GID = 0


def get_snapshot_filepath(sheet_dir: str, snapshot_date: str) -> str:
    logs_dir = os.path.join(sheet_dir, "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    return os.path.join(logs_dir, f"daily_eligible_{snapshot_date}.json")


def save_sheet_eligible_snapshot(
    sheet_dir: str,
    records: List[List[str]],
    snapshot_date: str,
) -> str:
    """Atomically persists per-sheet eligible [domain, profile_id, pub_funnel_name, pub_funnel_id] before midnight clear."""
    path = get_snapshot_filepath(sheet_dir, snapshot_date)
    payload = {
        "date": snapshot_date,
        "saved_at": get_now_ist().isoformat(),
        "count": len(records),
        "records": records,
    }
    dir_name = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, prefix=".eligible_tmp_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise
    logger.info(f"💾 Saved daily eligible snapshot ({len(records)} domains) -> {path}")
    return path


def load_sheet_eligible_snapshot(
    sheet_dir: str,
    snapshot_date: str,
) -> Optional[List[List[str]]]:
    path = get_snapshot_filepath(sheet_dir, snapshot_date)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        recs = data.get("records")
        return recs if isinstance(recs, list) else []
    except Exception as e:
        logger.warning(f"Failed loading snapshot {path}: {e}")
        return None


def get_eligible_records_for_sheet_dir(
    sheet_dir: str,
    gspread_client: Any = None,
    sheet_id: Optional[str] = None,
    sheet_name: str = "CF_IF_Engine_Database",
    cache: Any = None,
) -> List[List[str]]:
    """
    Collects current eligible rows ([domain, profile_id, publish_funnel_name]) from
    Google Sheet (when online) + local_sheet.csv + discovery_cache.csv (irrespective of CSV-only mode).
    """
    cache_map = load_cache_map(cache=cache, base_dir=sheet_dir)
    rows: List[List[str]] = []

    use_csv_only = is_csv_only_mode(sheet_dir)
    if not use_csv_only and gspread_client is not None and sheet_id:
        try:
            ss = gspread_client.open_by_key(sheet_id)
            ws = ss.worksheet(sheet_name)
            all_vals = ws.get_all_values()
            if len(all_vals) > 2:
                rows = all_vals[2:]
        except Exception as e:
            logger.warning(f"GSheet read fallback to local_sheet.csv during eligible scan ({sheet_id}): {e}")

    if not rows:
        rows = load_local_sheet_rows(sheet_dir)

    if not rows and cache_map:
        # Synthesize rows purely from cache map (missing profile_id if local_sheet absent)
        rows = [[dom] + [""] * 56 for dom in cache_map.keys()]

    return extract_eligible_domain_records(rows, cache_map=cache_map)


def export_eligible_domains_to_gsheet(
    gc: Any,
    records: Sequence[Sequence[str]],
    updated_dt: Optional[datetime.datetime] = None,
    spreadsheet_id: str = DAILY_PROCESSING_SHEET_ID,
    gid: int = DAILY_PROCESSING_GID,
    verify_live_tracxn: bool = True,
) -> int:
    """
    Clears B4:E on Discovery Daily Processing sheet, updates B1 with DD MMM YYYY in red text,
    ensures B3:E3 headers, runs live Tracxn API check to exclude published & sameAs domains,
    and writes [Domain, Domain Profile ID, Publish Funnel Name, Publish Funnel ID] starting at Row 4 (B4:E).
    """
    if updated_dt is None:
        dt = get_now_ist()
    elif updated_dt.tzinfo is None:
        dt = updated_dt.replace(tzinfo=IST_TZ)
    else:
        dt = updated_dt.astimezone(IST_TZ)

    date_str = dt.strftime("%d %b %Y")

    # Live Tracxn API validation: verify published & sameAs status directly from platform API
    cand_domains = [str(r[0]).strip() for r in records if r and str(r[0] or "").strip()]
    accepted_domains = None
    if verify_live_tracxn and cand_domains:
        try:
            accepted_domains, rejected = filter_unpublished_non_sameas_domains(cand_domains)
            pub_cnt = sum(1 for r in rejected.values() if r == "published")
            same_cnt = sum(1 for r in rejected.values() if r == "sameAs")
            logger.info(
                f"🛡️ [Live Tracxn API Check] Evaluated {len(cand_domains)} candidate domains: "
                f"{len(accepted_domains)} accepted, {len(rejected)} excluded ({pub_cnt} published, {same_cnt} sameAs)"
            )
        except Exception as e:
            logger.error(f"Error during live Tracxn API validation in daily export: {e}")

    clean_rows: List[List[str]] = []
    seen = set()
    for r in records:
        if not r or not str(r[0] or "").strip():
            continue
        dom = str(r[0]).strip()
        nd = normalize_domain(dom)
        if not nd or nd in seen:
            continue
        if accepted_domains is not None and nd not in accepted_domains:
            continue
        seen.add(nd)
        prof_id = str(r[1]).strip() if len(r) > 1 and r[1] is not None else ""
        funnel_name = str(r[2]).strip() if len(r) > 2 and r[2] is not None else ""
        funnel_id = str(r[3]).strip() if len(r) > 3 and r[3] is not None else ""
        clean_rows.append([dom, prof_id, funnel_name, funnel_id])

    ss = gc.open_by_key(spreadsheet_id)
    try:
        ws = ss.get_worksheet_by_id(gid)
    except Exception:
        ws = ss.worksheet("Sheet1")

    needed_rows = max(1000, len(clean_rows) + 10)
    cur_rows = getattr(ws, "row_count", 0)
    cur_cols = getattr(ws, "col_count", 5)
    if isinstance(cur_rows, int) and 0 < cur_rows < needed_rows:
        ws.resize(rows=needed_rows, cols=max(5, cur_cols if isinstance(cur_cols, int) else 5))

    ws.batch_clear(["B4:E"])

    # Ensure header row 3 has all 4 columns
    ws.update(
        range_name="B3:E3",
        values=[["Domains", "Domain Profile ID", "Publish Funnel Name", "Publish Funnel ID"]],
        value_input_option="USER_ENTERED",
    )

    # Update B1 date in DD MMM YYYY with red text (#FF0000)
    ws.update(range_name="B1", values=[[date_str]], value_input_option="USER_ENTERED")
    ws.format(
        "B1",
        {
            "textFormat": {
                "foregroundColor": {"red": 1.0, "green": 0.0, "blue": 0.0},
                "bold": True,
            },
            "numberFormat": {
                "type": "DATE",
                "pattern": "dd mmm yyyy",
            },
        },
    )

    if clean_rows:
        CHUNK = 2500
        for idx in range(0, len(clean_rows), CHUNK):
            chunk = clean_rows[idx : idx + CHUNK]
            start_r = 4 + idx
            end_r = start_r + len(chunk) - 1
            ws.update(range_name=f"B{start_r}:E{end_r}", values=chunk, value_input_option="RAW")
            if idx + CHUNK < len(clean_rows):
                time.sleep(0.5)

    logger.info(f"📤 Exported {len(clean_rows)} eligible domains to Daily Processing sheet ({spreadsheet_id}), B1={date_str}")
    return len(clean_rows)


def collect_fleet_eligible_records(
    snapshot_date: Optional[str] = None,
    local_sheet_num: Optional[int] = None,
    local_records: Optional[List[List[str]]] = None,
    auth_token: Optional[str] = None,
) -> List[List[str]]:
    """
    Gathers eligible domain records across all 4 sheets (Sheet1-Sheet4) via their `/fleet/eligible-domains`
    API endpoints (with optional pre-collected local_records for the coordinating sheet).
    """
    import requests
    from common.fleet_orchestrator import FLEET_NODES, get_node_url

    token = auth_token or os.getenv("SERVICE_AUTH_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    combined: List[List[str]] = []
    seen = set()

    def _extend(recs: Sequence[Sequence[str]]):
        for r in recs:
            if not r or not str(r[0] or "").strip():
                continue
            nd = normalize_domain(r[0])
            if nd and nd not in seen:
                seen.add(nd)
                combined.append([
                    str(r[0]).strip(),
                    str(r[1]).strip() if len(r) > 1 and r[1] is not None else "",
                    str(r[2]).strip() if len(r) > 2 and r[2] is not None else "",
                    str(r[3]).strip() if len(r) > 3 and r[3] is not None else "",
                ])

    if local_records is not None:
        _extend(local_records)

    for s_num in sorted(FLEET_NODES.keys()):
        if local_sheet_num == s_num and local_records is not None:
            continue
        url = f"{get_node_url(s_num).rstrip('/')}/fleet/eligible-domains"
        if snapshot_date:
            url = f"{url}?date={snapshot_date}"
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                _extend(data.get("records", []))
            else:
                logger.warning(f"Peer Sheet{s_num} /fleet/eligible-domains returned HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"Failed fetching eligible domains from Sheet{s_num} ({url}): {e}")

    return combined
