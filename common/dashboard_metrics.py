"""Shared dashboard metrics & eligibility helpers across Sheet1-4 engines and APIs."""

import csv
import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence

from common.gsheet_circuit_breaker import is_csv_only_mode

logger = logging.getLogger(__name__)

# 57-column 0-based indices
IDX_DOMAIN = 0                 # Col A
IDX_PROFILE_ID = 10            # Col K
IDX_TC_JOB_ID = 21             # Col V
IDX_TC_STATUS = 23             # Col X
IDX_RAW_DATA = 29              # Col AD
IDX_NIM_STATUS = 34            # Col AI
IDX_NIM_SUMMARY = 35           # Col AJ
IDX_GEMINI_IF = 37             # Col AL
IDX_GEMINI_FEED = 43           # Col AR
IDX_PUB_FUNNEL_NAME = 46       # Col AU
IDX_PUB_FUNNEL_ID = 47         # Col AV
IDX_CF_IF_STATUS = 49          # Col AX
IDX_IF_PF_STATUS = 51          # Col AZ

_DONE_STATUSES = {
    "DONE", "TRUE", "YES", "MOVED", "MOVED_OUT",
    "SNOOZED", "MARKED_IRRELEVANT", "MARKED_FOR_LATER",
    "ESCALATED", "SKIPPED_UNASSIGNABLE", "SKIPPED", "SUCCESS",
}


def normalize_domain(domain: Any) -> str:
    if not domain:
        return ""
    d = str(domain).strip().lower()
    if d.startswith("http://"):
        d = d[7:]
    elif d.startswith("https://"):
        d = d[8:]
    d = d.strip("/")
    if d.startswith("www."):
        d = d[4:]
    return d


def _pad_57(row: Sequence[Any]) -> List[str]:
    out = [str(x) if x is not None else "" for x in row[:57]]
    if len(out) < 57:
        out.extend([""] * (57 - len(out)))
    return out


def hydrate_row_from_cache(
    row: Sequence[Any],
    cache_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[str]:
    """Pads row to 57 cols and fills empty/stale phase fields from discovery cache."""
    padded = _pad_57(row)
    if not cache_map:
        return padded

    norm_d = normalize_domain(padded[IDX_DOMAIN])
    rec = cache_map.get(norm_d)
    if not rec:
        return padded

    field_map = (
        ("tc_job_id", IDX_TC_JOB_ID),
        ("tc_status", IDX_TC_STATUS),
        ("summary_status", IDX_NIM_STATUS),
        ("output_data_summery", IDX_NIM_SUMMARY),
        ("output_data_if", IDX_GEMINI_IF),
        ("output_data_feed", IDX_GEMINI_FEED),
        ("final_pub_funnel_name", IDX_PUB_FUNNEL_NAME),
        ("final_pub_funnel_id", IDX_PUB_FUNNEL_ID),
        ("cf_if_status", IDX_CF_IF_STATUS),
        ("if_pf_status", IDX_IF_PF_STATUS),
    )
    for key, idx in field_map:
        cval = str(rec.get(key, "") or "").strip()
        if cval and not padded[idx].strip():
            padded[idx] = cval
        elif key in ("final_pub_funnel_name", "final_pub_funnel_id", "cf_if_status", "if_pf_status") and cval:
            # Prefer terminal/DONE cache status when cache has progressed past an in-progress row value
            c_up = cval.upper()
            p_up = padded[idx].strip().upper()
            if not p_up or (c_up == "DONE" and p_up != "DONE") or (
                key == "final_pub_funnel_name" and cval.lower().startswith("publishing funnel")
            ) or (
                key == "final_pub_funnel_id" and cval
            ):
                padded[idx] = cval
    return padded


def is_row_eligible_for_next_dept(row: Sequence[Any]) -> bool:
    """
    Returns True iff:
      - Col AX (idx 49) == 'Done' (case-insensitive)
      - Col AZ (idx 51) == 'Done' (case-insensitive)
      - Col AU (idx 46) starts with 'Publishing Funnel' (case-insensitive)
    """
    if not row or len(row) <= IDX_IF_PF_STATUS:
        return False
    ax = str(row[IDX_CF_IF_STATUS] or "").strip().upper()
    az = str(row[IDX_IF_PF_STATUS] or "").strip().upper()
    au = str(row[IDX_PUB_FUNNEL_NAME] or "").strip().lower()
    return ax == "DONE" and az == "DONE" and au.startswith("publishing funnel")


def extract_eligible_domain_records(
    rows: Iterable[Sequence[Any]],
    cache_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[List[str]]:
    """
    Returns deduplicated [[domain, domain_profile_id, publish_funnel_name, publish_funnel_id], ...]
    for all rows eligible to move to the next department.
    """
    seen = set()
    records: List[List[str]] = []
    for raw_row in rows:
        if not raw_row or not str(raw_row[0] or "").strip():
            continue
        row = hydrate_row_from_cache(raw_row, cache_map=cache_map)
        if not is_row_eligible_for_next_dept(row):
            continue
        dom = row[IDX_DOMAIN].strip()
        norm_d = normalize_domain(dom)
        if not norm_d or norm_d in seen:
            continue
        seen.add(norm_d)
        records.append([
            dom,
            row[IDX_PROFILE_ID].strip(),
            row[IDX_PUB_FUNNEL_NAME].strip(),
            row[IDX_PUB_FUNNEL_ID].strip() if len(row) > IDX_PUB_FUNNEL_ID else "",
        ])
    return records


def load_local_sheet_rows(base_dir: str) -> List[List[str]]:
    path = os.path.join(base_dir, "local_sheet.csv")
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []
    rows: List[List[str]] = []
    try:
        with open(path, mode="r", encoding="utf-8", newline="", errors="ignore") as f:
            reader = csv.reader(f)
            for r in reader:
                if r and str(r[0] or "").strip():
                    rows.append(_pad_57(r))
    except Exception as e:
        logger.warning(f"Failed reading {path}: {e}")
    return rows


def load_cache_map(
    cache: Any = None,
    base_dir: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    if cache is not None and hasattr(cache, "_cache"):
        lock = getattr(cache, "_lock", None)
        if lock is not None:
            with lock:
                return {k: dict(v) for k, v in cache._cache.items()}
        return {k: dict(v) for k, v in cache._cache.items()}

    if not base_dir:
        return {}
    csv_path = os.path.join(base_dir, "discovery_cache.csv")
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    try:
        with open(csv_path, mode="r", encoding="utf-8", newline="", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                nd = normalize_domain(row.get("domain", ""))
                if nd:
                    out[nd] = dict(row)
    except Exception as e:
        logger.warning(f"Failed reading discovery cache {csv_path}: {e}")
    return out


def compute_sheet_metrics_from_rows(
    rows: Iterable[Sequence[Any]],
    cache_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, int]:
    total = 0
    scraped = 0
    nim = 0
    gemini = 0
    tracxn = 0
    pf_completed = 0
    eligible_completed = 0
    fully_completed = 0
    eligible_domains = set()

    for raw_row in rows:
        if not raw_row or not str(raw_row[0] or "").strip():
            continue
        total += 1
        row = hydrate_row_from_cache(raw_row, cache_map=cache_map)
        tc_s = row[IDX_TC_STATUS].strip().upper()
        raw_d = row[IDX_RAW_DATA].strip()
        nim_s = row[IDX_NIM_STATUS].strip().upper()
        nim_txt = row[IDX_NIM_SUMMARY].strip()
        if_val = row[IDX_GEMINI_IF].strip().upper()
        feed_val = row[IDX_GEMINI_FEED].strip()
        ax = row[IDX_CF_IF_STATUS].strip().upper()
        az = row[IDX_IF_PF_STATUS].strip().upper()

        if tc_s == "SUCCESS" or (raw_d and not raw_d.startswith(("Error", "FAILED", "N/A"))):
            scraped += 1
        if nim_s == "SUCCESS":
            nim += 1
        if feed_val:
            gemini += 1
        if ax in _DONE_STATUSES or az in _DONE_STATUSES:
            tracxn += 1
        if az in ("DONE", "TRUE", "YES", "MOVED", "MOVED_OUT"):
            pf_completed += 1
        if is_row_eligible_for_next_dept(row):
            eligible_completed += 1
            eligible_domains.add(normalize_domain(row[IDX_DOMAIN]))

        has_nim_ok = (nim_s == "SUCCESS" and bool(nim_txt)) or nim_s == "NO_CONTENT"
        if (nim_s == "NO_CONTENT") or (has_nim_ok and bool(if_val) and (ax in _DONE_STATUSES or az in _DONE_STATUSES)):
            fully_completed += 1

    return {
        "total": total,
        "scraped_count": scraped,
        "tc_success": scraped,
        "nim_count": nim,
        "gemini_count": gemini,
        "tracxn_count": tracxn,
        "pf_completed": pf_completed,
        "eligible_completed": eligible_completed,
        "eligible_unique_domains": len(eligible_domains),
        "fully_completed": fully_completed,
    }


def build_sheet_status_payload(
    base_dir: str,
    raw_progress: Optional[Dict[str, Any]] = None,
    cache: Any = None,
    csv_only_override: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Merges `.progress.json` with live CSV + cache metrics so `eligible_completed`,
    `csv_only_mode`, and `tracxn_count` are accurate in both GSheet and CSV-only mode.
    """
    csv_only = is_csv_only_mode(base_dir) if csv_only_override is None else bool(csv_only_override)
    progress = dict(raw_progress) if isinstance(raw_progress, dict) else {}
    progress["csv_only_mode"] = csv_only

    if csv_only:
        rows = load_local_sheet_rows(base_dir)
        cache_map = load_cache_map(cache=cache, base_dir=base_dir)
        if rows:
            metrics = compute_sheet_metrics_from_rows(rows, cache_map=cache_map)
            progress["total"] = max(int(progress.get("total", 0) or 0), metrics["total"])
            progress["scraped_count"] = max(int(progress.get("scraped_count", 0) or 0), metrics["scraped_count"])
            progress["tc_success"] = max(int(progress.get("tc_success", 0) or 0), metrics["tc_success"])
            progress["nim_count"] = max(int(progress.get("nim_count", 0) or 0), metrics["nim_count"])
            progress["gemini_count"] = max(int(progress.get("gemini_count", 0) or 0), metrics["gemini_count"])
            progress["tracxn_count"] = max(int(progress.get("tracxn_count", 0) or 0), metrics["tracxn_count"])
            progress["pf_completed"] = max(int(progress.get("pf_completed", 0) or 0), metrics["pf_completed"])
            progress["fully_completed"] = max(int(progress.get("fully_completed", 0) or 0), metrics["fully_completed"])
            progress["current"] = max(int(progress.get("current", 0) or 0), metrics["fully_completed"])
            if "progress_total" in progress:
                progress["progress_total"] = max(int(progress.get("progress_total", 0) or 0), progress["total"])
            if "progress_current" in progress:
                progress["progress_current"] = max(int(progress.get("progress_current", 0) or 0), progress["current"])
            if "progress_success" in progress:
                progress["progress_success"] = max(int(progress.get("progress_success", 0) or 0), progress["tc_success"])
            progress["eligible_completed"] = metrics["eligible_completed"]
        elif cache_map:
            synth_rows = [[dom] + [""] * 56 for dom in cache_map.keys()]
            metrics = compute_sheet_metrics_from_rows(synth_rows, cache_map=cache_map)
            progress["eligible_completed"] = metrics["eligible_completed"]
            for key in ("scraped_count", "tc_success", "nim_count", "gemini_count", "tracxn_count", "pf_completed", "fully_completed"):
                progress[key] = max(int(progress.get(key, 0) or 0), metrics[key])
        else:
            progress["eligible_completed"] = int(progress.get("eligible_completed", 0) or 0)
    elif "eligible_completed" in progress and progress["eligible_completed"] is not None:
        progress["eligible_completed"] = int(progress["eligible_completed"])
    else:
        progress["eligible_completed"] = None

    return progress
