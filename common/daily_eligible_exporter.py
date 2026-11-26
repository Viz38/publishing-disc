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

PROCESSED_COUNTS_SHEET_ID = "1GMojL6BkqqTZv3rkcQ19b20y4JYT9gkucPRE7h6vYmo"
PROCESSED_COUNTS_GID = 0


def count_gemini_ran_from_rows_and_cache(
    rows: Sequence[Sequence[Any]],
    cache_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> int:
    """Counts unique domains where Gemini ran (IF status, IF name, or Feed number non-empty)."""
    seen_gemini = set()
    for r in rows or []:
        if not r or not str(r[0] or "").strip():
            continue
        nd = normalize_domain(str(r[0]))
        if not nd:
            continue
        if_st = str(r[37]).strip() if len(r) > 37 and r[37] is not None else ""
        if_nm = str(r[38]).strip() if len(r) > 38 and r[38] is not None else ""
        feed_no = str(r[43]).strip() if len(r) > 43 and r[43] is not None else ""
        if if_st or if_nm or feed_no:
            seen_gemini.add(nd)

    for dom_key, c_rec in (cache_map or {}).items():
        nd = normalize_domain(dom_key)
        if not nd or nd in seen_gemini or not isinstance(c_rec, dict):
            continue
        if_st = str(c_rec.get("output_data_if") or "").strip()
        if_nm = str(c_rec.get("output_data_if_name") or "").strip()
        feed_no = str(c_rec.get("output_data_feed") or "").strip()
        if if_st or if_nm or feed_no:
            seen_gemini.add(nd)

    return len(seen_gemini)


def get_snapshot_filepath(sheet_dir: str, snapshot_date: str) -> str:
    logs_dir = os.path.join(sheet_dir, "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    return os.path.join(logs_dir, f"daily_eligible_{snapshot_date}.json")


def save_sheet_eligible_snapshot(
    sheet_dir: str,
    records: List[List[str]],
    snapshot_date: str,
    gemini_ran_count: Optional[int] = None,
) -> str:
    """Atomically persists per-sheet eligible records + daily gemini_ran_count before midnight clear."""
    path = get_snapshot_filepath(sheet_dir, snapshot_date)
    payload: Dict[str, Any] = {
        "date": snapshot_date,
        "saved_at": get_now_ist().isoformat(),
        "count": len(records),
        "records": records,
    }
    if gemini_ran_count is not None:
        payload["gemini_ran_count"] = int(gemini_ran_count)
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
    logger.info(
        f"💾 Saved daily eligible snapshot ({len(records)} eligible, gemini_ran={gemini_ran_count}) -> {path}"
    )
    return path


def load_sheet_daily_metrics_snapshot(
    sheet_dir: str,
    snapshot_date: str,
) -> Optional[Dict[str, Any]]:
    path = get_snapshot_filepath(sheet_dir, snapshot_date)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        recs = data.get("records")
        recs_list = recs if isinstance(recs, list) else []
        return {
            "date": data.get("date", snapshot_date),
            "count": len(recs_list),
            "gemini_ran_count": int(data.get("gemini_ran_count") or 0),
            "records": recs_list,
        }
    except Exception as e:
        logger.warning(f"Failed loading snapshot {path}: {e}")
        return None


def load_sheet_eligible_snapshot(
    sheet_dir: str,
    snapshot_date: str,
) -> Optional[List[List[str]]]:
    snap = load_sheet_daily_metrics_snapshot(sheet_dir, snapshot_date)
    return snap["records"] if snap is not None else None


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
    local_rows = load_local_sheet_rows(sheet_dir)
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

    # Prioritize local_rows if remote Google Sheet returned a truncated subset
    if len(local_rows) > len(rows):
        rows = local_rows
    elif not rows:
        rows = local_rows

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
    name_to_id = load_funnel_name_to_id_map(gc)
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
        if not funnel_id and funnel_name and funnel_name in name_to_id:
            funnel_id = name_to_id[funnel_name]
        clean_rows.append([dom, prof_id, funnel_name, funnel_id])

    ss = gc.open_by_key(spreadsheet_id)
    try:
        ws = ss.get_worksheet_by_id(gid)
    except Exception:
        ws = ss.worksheet("Sheet1")

    needed_rows = max(1000, len(clean_rows) + 10)
    cur_rows = getattr(ws, "row_count", 0)
    cur_cols = getattr(ws, "col_count", 5)
    max_clear_row = max(cur_rows if isinstance(cur_rows, int) else 0, needed_rows, 50000)
    ws.batch_clear([f"B4:E{max_clear_row}"])

    if isinstance(cur_rows, int) and cur_rows != needed_rows:
        ws.resize(rows=needed_rows, cols=max(5, cur_cols if isinstance(cur_cols, int) else 5))

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


def load_funnel_name_to_id_map(gc: Any = None) -> Dict[str, str]:
    if gc is None:
        return {}
    try:
        from common.pipeline_engine import Return_Funnel_FeedWise
        cfg = Return_Funnel_FeedWise(gc)
        feed_map = cfg.get("FeedNumFunnelMap", {})
        mapping = {}
        for k, row in feed_map.items():
            if len(row) > 6:
                pf_name = str(row[5]).strip()
                pf_id = str(row[6]).strip()
                if pf_name and pf_id:
                    mapping[pf_name] = pf_id
        return mapping
    except Exception as e:
        logger.warning(f"Failed loading name_to_id map: {e}")
        return {}


def fetch_peer_sheet_records(
    sheet_num: int,
    snapshot_date: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> List[List[str]]:
    import requests
    from common.fleet_orchestrator import get_node_url

    token = auth_token or os.getenv("SERVICE_AUTH_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    url = f"{get_node_url(sheet_num).rstrip('/')}/fleet/eligible-domains"
    if snapshot_date:
        url = f"{url}?date={snapshot_date}"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            recs = resp.json().get("records", [])
            if len(recs) >= 500:
                return recs
            # If peer returned 0 records and snapshot_date is provided, try querying peer with adjacent date
            if snapshot_date and len(recs) == 0:
                try:
                    cur_dt = datetime.datetime.strptime(snapshot_date, "%Y-%m-%d")
                    for delta_days in (-1, 1):
                        alt_date = (cur_dt + datetime.timedelta(days=delta_days)).strftime("%Y-%m-%d")
                        alt_url = f"{get_node_url(sheet_num).rstrip('/')}/fleet/eligible-domains?date={alt_date}"
                        alt_resp = requests.get(alt_url, headers=headers, timeout=15)
                        if alt_resp.status_code == 200:
                            alt_recs = alt_resp.json().get("records", [])
                            if len(alt_recs) > len(recs):
                                logger.info(f"Loaded {len(alt_recs)} records from peer Sheet{sheet_num} using alternate date {alt_date}")
                                recs = alt_recs
                                break
                except Exception:
                    pass
            if recs:
                return recs
        else:
            logger.warning(f"Peer Sheet{sheet_num} /fleet/eligible-domains returned HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"Failed fetching eligible domains from Sheet{sheet_num} ({url}): {e}")

    # Fallback to local disk snapshot file if accessible (e.g. co-located sheet or shared storage)
    if snapshot_date:
        try:
            cur_dt = datetime.datetime.strptime(snapshot_date, "%Y-%m-%d")
            date_candidates = [snapshot_date, (cur_dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d"), (cur_dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
        except Exception:
            date_candidates = [snapshot_date]
        for dc in date_candidates:
            snap = load_sheet_eligible_snapshot(f"Sheet{sheet_num}", dc)
            if snap and len(snap) > 0:
                logger.info(f"Loaded {len(snap)} records from local disk snapshot for Sheet{sheet_num} (date {dc})")
                return snap
    return []


def export_fleet_eligible_sequential(
    gc: Any,
    snapshot_date: Optional[str] = None,
    local_sheet_num: Optional[int] = None,
    local_records: Optional[List[List[str]]] = None,
    now_ist: Optional[datetime.datetime] = None,
    spreadsheet_id: str = DAILY_PROCESSING_SHEET_ID,
    gid: int = DAILY_PROCESSING_GID,
    verify_live_tracxn: bool = True,
    auth_token: Optional[str] = None,
    peer_fetcher: Optional[Any] = None,
    name_to_id_map: Optional[Dict[str, str]] = None,
) -> int:
    """
    Exports eligible domains to Discovery Daily Processing sheet ONE SHEET AT A TIME:
      1. Sheet 1: Clears B4:E completely to the bottom, sets B1 date (red), ensures B3:E3 headers,
         and writes Sheet 1's eligible domains starting at row 4.
      2. Sheet 2: Fetches Sheet 2's eligible domains, deduplicates against Sheet 1, and appends below Sheet 1.
      3. Sheet 3: Fetches Sheet 3's eligible domains, deduplicates, and appends below Sheet 2.
      4. Sheet 4: Fetches Sheet 4's eligible domains, deduplicates, and appends below Sheet 3.
    """
    if now_ist is None:
        dt = get_now_ist()
    elif now_ist.tzinfo is None:
        dt = now_ist.replace(tzinfo=IST_TZ)
    else:
        dt = now_ist.astimezone(IST_TZ)

    date_str = dt.strftime("%d %b %Y")

    if name_to_id_map is None:
        name_to_id_map = load_funnel_name_to_id_map(gc)

    fetch_fn = peer_fetcher or (lambda s_num: fetch_peer_sheet_records(s_num, snapshot_date=snapshot_date, auth_token=auth_token))

    ss = gc.open_by_key(spreadsheet_id)
    try:
        ws = ss.get_worksheet_by_id(gid)
    except Exception:
        ws = ss.worksheet("Sheet1")

    # Step 1: Sheet 1 initial setup & clearing full sheet
    cur_rows = getattr(ws, "row_count", 0)
    cur_cols = getattr(ws, "col_count", 5)
    max_clear_row = max(cur_rows if isinstance(cur_rows, int) else 0, 50000)
    ws.batch_clear([f"B4:E{max_clear_row}"])

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

    seen = set()
    current_row = 4

    from common.fleet_orchestrator import FLEET_NODES
    active_sheet_nums = sorted(FLEET_NODES.keys()) or [1]
    for sheet_num in active_sheet_nums:
        raw_recs: List[List[str]] = []
        if local_sheet_num == sheet_num and local_records is not None:
            raw_recs = local_records
        else:
            raw_recs = fetch_fn(sheet_num)

        if not raw_recs:
            logger.info(f"ℹ️ Sheet {sheet_num}: 0 eligible records found.")
            continue

        cand_domains = [str(r[0]).strip() for r in raw_recs if r and str(r[0] or "").strip()]
        accepted_domains = None
        if verify_live_tracxn and cand_domains:
            try:
                accepted_domains, rejected = filter_unpublished_non_sameas_domains(cand_domains)
            except Exception as e:
                logger.error(f"Error during live Tracxn API check for Sheet {sheet_num}: {e}")

        sheet_clean_rows: List[List[str]] = []
        for r in raw_recs:
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
            if not funnel_id and funnel_name and funnel_name in name_to_id_map:
                funnel_id = name_to_id_map[funnel_name]
            sheet_clean_rows.append([dom, prof_id, funnel_name, funnel_id])

        if not sheet_clean_rows:
            logger.info(f"ℹ️ Sheet {sheet_num}: 0 clean records to append after deduplication/filtering.")
            continue

        needed_total_rows = current_row + len(sheet_clean_rows) + 10
        cur_ws_rows = getattr(ws, "row_count", 0)
        if isinstance(cur_ws_rows, int) and cur_ws_rows < needed_total_rows:
            ws.resize(rows=needed_total_rows, cols=max(5, cur_cols if isinstance(cur_cols, int) else 5))

        CHUNK = 2500
        for idx in range(0, len(sheet_clean_rows), CHUNK):
            chunk = sheet_clean_rows[idx : idx + CHUNK]
            start_r = current_row + idx
            end_r = start_r + len(chunk) - 1
            ws.update(range_name=f"B{start_r}:E{end_r}", values=chunk, value_input_option="RAW")
            if idx + CHUNK < len(sheet_clean_rows):
                time.sleep(0.5)

        logger.info(f"📤 Sheet {sheet_num}: Wrote {len(sheet_clean_rows)} domains to B{current_row}:E{current_row + len(sheet_clean_rows) - 1}")
        current_row += len(sheet_clean_rows)

    total_exported = current_row - 4
    logger.info(f"🎉 Completed sequential export: Total {total_exported} domains written to {spreadsheet_id}, B1={date_str}")
    return total_exported


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


def fetch_peer_sheet_gemini_ran_count(
    sheet_num: int,
    snapshot_date: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> int:
    import requests
    from common.fleet_orchestrator import get_node_url

    token = auth_token or os.getenv("SERVICE_AUTH_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    url = f"{get_node_url(sheet_num).rstrip('/')}/fleet/eligible-domains"
    if snapshot_date:
        url = f"{url}?date={snapshot_date}"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            cnt = int(data.get("gemini_ran_count") or 0)
            if cnt > 0:
                return cnt
            if snapshot_date and cnt == 0:
                try:
                    cur_dt = datetime.datetime.strptime(snapshot_date, "%Y-%m-%d")
                    for delta_days in (-1, 1):
                        alt_date = (cur_dt + datetime.timedelta(days=delta_days)).strftime("%Y-%m-%d")
                        alt_url = f"{get_node_url(sheet_num).rstrip('/')}/fleet/eligible-domains?date={alt_date}"
                        alt_resp = requests.get(alt_url, headers=headers, timeout=15)
                        if alt_resp.status_code == 200:
                            alt_cnt = int(alt_resp.json().get("gemini_ran_count") or 0)
                            if alt_cnt > cnt:
                                return alt_cnt
                except Exception:
                    pass
        else:
            logger.warning(f"Peer Sheet{sheet_num} /fleet/eligible-domains returned HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"Failed fetching gemini_ran_count from Sheet{sheet_num} ({url}): {e}")

    if snapshot_date:
        try:
            cur_dt = datetime.datetime.strptime(snapshot_date, "%Y-%m-%d")
            date_candidates = [snapshot_date, (cur_dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d"), (cur_dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
        except Exception:
            date_candidates = [snapshot_date]
        for dc in date_candidates:
            snap = load_sheet_daily_metrics_snapshot(f"Sheet{sheet_num}", dc)
            if snap and int(snap.get("gemini_ran_count") or 0) > 0:
                return int(snap.get("gemini_ran_count") or 0)
    return 0


def collect_fleet_gemini_ran_count(
    snapshot_date: Optional[str] = None,
    local_sheet_num: Optional[int] = None,
    local_gemini_ran: Optional[int] = None,
    auth_token: Optional[str] = None,
    peer_fetcher: Optional[Any] = None,
) -> int:
    """Sums daily Gemini-ran domain counts across Sheets 1..4."""
    fetch_fn = peer_fetcher or (
        lambda s_num: fetch_peer_sheet_gemini_ran_count(s_num, snapshot_date=snapshot_date, auth_token=auth_token)
    )
    total = 0
    from common.fleet_orchestrator import FLEET_NODES
    active_sheets = sorted(FLEET_NODES.keys()) or [1]
    for s_num in active_sheets:
        if local_sheet_num == s_num and local_gemini_ran is not None:
            total += int(local_gemini_ran)
        else:
            total += int(fetch_fn(s_num) or 0)
    return total


def update_daily_processed_counts_sheet(
    gc: Any,
    report_date_str: str,
    accepted_count: int,
    gemini_processing_count: int,
    spreadsheet_id: str = PROCESSED_COUNTS_SHEET_ID,
    gid: int = PROCESSED_COUNTS_GID,
) -> Dict[str, Any]:
    """
    Upserts Col F (`Accepted`) and Col G (`Processing` / Gemini Ran) for `report_date_str` (YYYY-MM-DD)
    on the daily tracking sheet (`Processed Counts`, gid=0), inserting above `Total (Daily Sum)` if needed
    and keeping the footer `=SUM(...)` formulas aligned.
    """
    ss = gc.open_by_key(spreadsheet_id)
    try:
        ws = ss.worksheet("Processed Counts")
    except Exception:
        try:
            ws = ss.get_worksheet_by_id(gid)
        except Exception:
            ws = ss.get_worksheet(0)

    rows = ws.get_all_values()
    target_row_idx: Optional[int] = None
    total_row_idx: Optional[int] = None

    for idx, r in enumerate(rows, start=1):
        if idx == 1:
            continue
        col_a = str(r[0]).strip() if r else ""
        if col_a == report_date_str:
            target_row_idx = idx
        elif col_a.lower().startswith("total"):
            total_row_idx = idx

    if target_row_idx is not None:
        ws.update(
            range_name=f"F{target_row_idx}:G{target_row_idx}",
            values=[[int(accepted_count), int(gemini_processing_count)]],
            value_input_option="USER_ENTERED",
        )
        action = "updated"
        written_row = target_row_idx
    else:
        new_row = [report_date_str, "", "", "", "", int(accepted_count), int(gemini_processing_count)]
        if total_row_idx is not None:
            ws.insert_row(new_row, index=total_row_idx, value_input_option="USER_ENTERED")
            written_row = total_row_idx
            total_row_idx += 1
        else:
            written_row = len(rows) + 1
            ws.update(
                range_name=f"A{written_row}:G{written_row}",
                values=[new_row],
                value_input_option="USER_ENTERED",
            )
        action = "inserted"

    # Keep Total (Daily Sum) formulas aligned through the last data row above it
    if total_row_idx is not None and total_row_idx > 2:
        last_data_row = total_row_idx - 1
        ws.update(
            range_name=f"B{total_row_idx}:G{total_row_idx}",
            values=[[
                f"=SUM(B2:B{last_data_row})",
                f"=SUM(C2:C{last_data_row})",
                f"=SUM(D2:D{last_data_row})",
                f"=SUM(E2:E{last_data_row})",
                f"=SUM(F2:F{last_data_row})",
                f"=SUM(G2:G{last_data_row})",
            ]],
            value_input_option="USER_ENTERED",
        )

    logger.info(
        f"📊 [Daily Processed Counts] {action.upper()} {report_date_str} (Row {written_row}): "
        f"Col F (Accepted)={accepted_count:,}, Col G (Processing/Gemini)={gemini_processing_count:,}"
    )
    return {
        "action": action,
        "date": report_date_str,
        "row": written_row,
        "accepted": int(accepted_count),
        "processing": int(gemini_processing_count),
    }
