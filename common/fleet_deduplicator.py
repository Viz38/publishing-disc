#!/usr/bin/env python3
"""
Discovery Engine - Fleet Deduplicator & Daily Uniqueness Manager.
Guarantees:
1. Strict unique domain ingestion during fetch (intra-batch + active fleet + daily registry).
2. Cross-sheet and intra-sheet duplicate cleaning across all 4 production sheets.
3. Priority hierarchy: Sheet 1 > Sheet 2 > Sheet 3 > Sheet 4.
4. One domain processed only once per day (daily registry persistence).
5. No backfill policy: purges duplicates and retains remaining unique rows.
6. Pre-TC gate: strictly executed before Tech Crawler Job ID generation.
"""

import os
import sys
import json
import logging
import datetime
from typing import List, Dict, Tuple, Set, Optional, Any, Iterable

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

logger = logging.getLogger("FleetDeduplicator")

TARGET_SHEETS = [
    {"num": 1, "id": "1IWXqnR52IzpFewE0QS0tlJFT65nJE_fmIoJs8ps9nZo", "name": "Sheet 1 (CF-1)"},
    {"num": 2, "id": "1QGraMZOJjc-EIZlt_CieOVpr1jxw6tLa_QnWgWHou5I", "name": "Sheet 2 (CF-2)"},
    {"num": 3, "id": "1MHdkFCsoAY0V7ix_pJ5SpWFq3EFpJEug2ntJk1FkEl8", "name": "Sheet 3 (CF-3)"},
    {"num": 4, "id": "1z3wH--c2qHGlNqW0e-6Mc8IVqh_kN5_fbWjILM4FxQM", "name": "Sheet 4 (Filter-V2)"},
]

def get_today_ist() -> str:
    """Returns today's date formatted as YYYY-MM-DD in Asia/Kolkata timezone."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_ist = now_utc + datetime.timedelta(hours=5, minutes=30)
    return now_ist.strftime("%Y-%m-%d")


def retry_gspread_call(fn, max_retries: int = 5, initial_delay: int = 5):
    """Retries a Google Sheets API call with exponential backoff on 429 quota exhaustion."""
    import time
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            err_str = str(e)
            if attempt == max_retries:
                raise
            sleep_time = delay
            if "Quota exceeded" in err_str or "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                sleep_time = max(delay, 25)
                logger.warning(f"Google Sheets Rate Limit / 429 hit (attempt {attempt}/{max_retries}). Sleeping {sleep_time}s...")
            else:
                logger.warning(f"Google Sheets API call failed (attempt {attempt}/{max_retries}): {e}. Sleeping {sleep_time}s...")
            time.sleep(sleep_time)
            delay = min(delay * 2, 60)


class DailyProcessedRegistry:
    """
    Persistent daily registry of all domains processed across the fleet.
    Guarantees that a domain is processed at most once per calendar day.
    """
    def __init__(self, storage_dir: Optional[str] = None):
        self.storage_dir = storage_dir or os.path.join(BASE_DIR, "Logs", "daily_registry")
        os.makedirs(self.storage_dir, exist_ok=True)

    def _get_registry_path(self, date_str: Optional[str] = None) -> str:
        d = date_str or get_today_ist()
        return os.path.join(self.storage_dir, f"daily_processed_{d}.json")

    def get_daily_processed(self, date_str: Optional[str] = None) -> Set[str]:
        path = self._get_registry_path(date_str)
        if not os.path.exists(path):
            return set()
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(d.strip().lower() for d in data if d and d.strip())
        except Exception as e:
            logger.error(f"Error reading daily processed registry from {path}: {e}")
        return set()

    def record_daily_processed(self, domains: Iterable[str], date_str: Optional[str] = None) -> int:
        path = self._get_registry_path(date_str)
        current = self.get_daily_processed(date_str)
        for dom in domains:
            if dom:
                clean_d = dom.strip().lower()
                if clean_d:
                    current.add(clean_d)

        # Atomic write via temp file
        temp_path = f"{path}.tmp.{os.getpid()}"
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(sorted(list(current)), f, indent=2)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"Error writing daily processed registry to {path}: {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception:
                    pass
        return len(current)


def identify_fleet_duplicates(
    sheets_data: Dict[int, List[Tuple[int, str]]],
    daily_seen: Optional[Set[str]] = None
) -> Dict[int, List[Tuple[int, str, str]]]:
    """
    Evaluates domain occurrences across production sheets in strict priority order (Sheet 1 > 2 > 3 > 4).
    Returns a mapping: sheet_num -> list of (row_number, domain, reason) for duplicate rows to be purged.
    """
    prior_done: Set[str] = set()
    if daily_seen:
        prior_done.update(d.strip().lower() for d in daily_seen if d and d.strip())

    duplicates: Dict[int, List[Tuple[int, str, str]]] = {s: [] for s in [1, 2, 3, 4]}
    fleet_origin: Dict[str, Tuple[int, int]] = {}  # domain -> (sheet_num, row_num)

    for sheet_num in sorted(sheets_data.keys()):
        rows = sheets_data.get(sheet_num, [])
        for row_num, dom in rows:
            if not dom:
                continue
            clean_d = dom.strip().lower()
            if not clean_d:
                continue

            if clean_d in fleet_origin:
                orig_sheet, orig_row = fleet_origin[clean_d]
                if orig_sheet == sheet_num:
                    reason = f"Duplicate within Sheet {sheet_num} (Row {orig_row})"
                else:
                    reason = f"Duplicate of Sheet {orig_sheet} (Row {orig_row})"
                duplicates[sheet_num].append((row_num, clean_d, reason))
            elif clean_d in prior_done:
                reason = "Already processed in fleet earlier today"
                duplicates[sheet_num].append((row_num, clean_d, reason))
            else:
                fleet_origin[clean_d] = (sheet_num, row_num)

    return duplicates


def filter_unique_fetch_domains(
    incoming_items: List[Dict[str, Any]],
    existing_fleet: Optional[Set[str]] = None,
    daily_seen: Optional[Set[str]] = None,
    domain_key: str = "domain"
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Filters raw company items during ingestion fetch.
    Ensures domains are unique within the batch, not present in any existing sheet,
    and not present in the daily processed registry.
    """
    excluded = set()
    if existing_fleet:
        excluded.update(d.strip().lower() for d in existing_fleet if d and d.strip())
    if daily_seen:
        excluded.update(d.strip().lower() for d in daily_seen if d and d.strip())

    unique_items: List[Dict[str, Any]] = []
    skipped_items: List[Dict[str, Any]] = []
    batch_seen: Set[str] = set()

    for item in incoming_items:
        raw_dom = item.get(domain_key, "")
        if not raw_dom:
            skipped_items.append(item)
            continue

        clean_dom = raw_dom.strip().lower()
        if not clean_dom or clean_dom in excluded or clean_dom in batch_seen:
            skipped_items.append(item)
            continue

        batch_seen.add(clean_dom)
        unique_items.append(item)

    return unique_items, skipped_items


def load_all_active_fleet_domains(gc=None) -> Dict[int, List[Tuple[int, str]]]:
    """
    Connects to Google Sheets and reads Col A (Row 3+) across all 4 production sheets.
    Returns: {sheet_num: [(row_num, domain), ...]}
    """
    if gc is None:
        from common.advance_precrawl import get_gspread_client
        gc = get_gspread_client()

    fleet_data: Dict[int, List[Tuple[int, str]]] = {}
    for s_cfg in TARGET_SHEETS:
        s_num = s_cfg["num"]
        s_id = s_cfg["id"]
        try:
            sh = retry_gspread_call(lambda: gc.open_by_key(s_id))
            try:
                ws = sh.worksheet("CF_IF_Engine_Database")
            except Exception:
                ws = sh.get_worksheet(0)
            col_a = retry_gspread_call(lambda: ws.col_values(1))
            # Row 1 & 2 are headers, domains start at row 3 (1-indexed row 3)
            rows = []
            for idx, val in enumerate(col_a[2:], start=3):
                val_clean = val.strip().lower() if val else ""
                if val_clean:
                    rows.append((idx, val_clean))
            fleet_data[s_num] = rows
            logger.info(f"Loaded {len(rows)} domains from Sheet {s_num} ({s_cfg['name']})")
        except Exception as e:
            logger.error(f"Failed to read domains from Sheet {s_num} ({s_id}): {e}")
            fleet_data[s_num] = []

    return fleet_data


from common.advance_precrawl import get_gspread_client, fetch_domains_for_sheet

def clean_and_deduplicate_all_sheets(gc=None, dry_run: bool = False, backfill: bool = True) -> Dict[str, Any]:
    """
    Full cross-sheet cleaning operation across all 4 sheets.
    1. Reads active domains across Sheets 1-4.
    2. Identifies all duplicates (respecting priority Sheet 1 > 2 > 3 > 4) and daily registry.
    3. Purges duplicate rows from Google Sheets (blanks Col A so engine bypasses them).
    4. Automated Budget Backfill: Re-fetches replacement domains for purged rows so budget is 100% fulfilled.
    5. Returns audit report.
    """
    if gc is None:
        gc = get_gspread_client()

    registry = DailyProcessedRegistry()
    daily_seen = registry.get_daily_processed()
    logger.info(f"Daily registry has {len(daily_seen)} previously processed domains today.")

    sheets_data = load_all_active_fleet_domains(gc)
    duplicates = identify_fleet_duplicates(sheets_data, daily_seen=daily_seen)

    total_dups = sum(len(d_list) for d_list in duplicates.values())
    logger.info(f"Cross-sheet deduplication scan found {total_dups} duplicate instances.")

    report = {
        "timestamp": datetime.datetime.now().isoformat(),
        "dry_run": dry_run,
        "total_duplicates_found": total_dups,
        "sheet_details": {}
    }

    all_valid_unique_domains: List[str] = []

    for s_cfg in TARGET_SHEETS:
        s_num = s_cfg["num"]
        s_id = s_cfg["id"]
        s_dups = duplicates.get(s_num, [])
        report["sheet_details"][s_num] = {
            "name": s_cfg["name"],
            "total_rows": len(sheets_data.get(s_num, [])),
            "duplicates_count": len(s_dups),
            "duplicate_rows": s_dups
        }

        # Collect unique domains to update registry
        dup_row_set = set(row_num for row_num, _, _ in s_dups)
        for row_num, dom in sheets_data.get(s_num, []):
            if row_num not in dup_row_set:
                all_valid_unique_domains.append(dom)

        if not s_dups:
            logger.info(f"[{s_cfg['name']}] Clean! 0 duplicates detected.")
            continue

        logger.info(f"[{s_cfg['name']}] Purging {len(s_dups)} duplicate rows...")
        if not dry_run:
            try:
                sh = retry_gspread_call(lambda: gc.open_by_key(s_id))
                try:
                    ws = sh.worksheet("CF_IF_Engine_Database")
                except Exception:
                    ws = sh.get_worksheet(0)

                # Batch clear/blank duplicate rows
                # Blanks Col A and Col U so engine skips row and never creates TC job ID
                clear_ranges = []
                for row_num, dom, reason in s_dups:
                    clear_ranges.append(f"A{row_num}:BE{row_num}")
                    logger.info(f"  - Purging Row {row_num}: '{dom}' ({reason})")

                # In gspread, batch_clear accepts a list of range strings
                # Chunk into batches of 100 to prevent URL length limits
                CHUNK = 100
                for c_idx in range(0, len(clear_ranges), CHUNK):
                    sub_ranges = clear_ranges[c_idx : c_idx + CHUNK]
                    retry_gspread_call(lambda: ws.batch_clear(sub_ranges))

                logger.info(f"[{s_cfg['name']}] Successfully purged {len(s_dups)} duplicate rows in sheet.")

                # Automated Budget Backfill
                if backfill and len(s_dups) > 0:
                    missing_count = len(s_dups)
                    logger.info(f"[{s_cfg['name']}] Refetching {missing_count} fresh replacement domains to maintain full budget...")
                    exclusions = set(daily_seen) | set(all_valid_unique_domains)
                    try:
                        replacement_rows = fetch_domains_for_sheet(
                            sheet_id=s_id,
                            target_count=missing_count,
                            shared_exclusion_set=exclusions
                        )
                        if replacement_rows:
                            cleared_rows = sorted([r for r, _, _ in s_dups])
                            for idx, rep_row in enumerate(replacement_rows):
                                if idx < len(cleared_rows):
                                    target_r = cleared_rows[idx]
                                    retry_gspread_call(lambda: ws.update(range_name=f"A{target_r}:BE{target_r}", values=[rep_row], value_input_option="USER_ENTERED"))
                                    dom_clean = rep_row[0].strip().lower()
                                    all_valid_unique_domains.append(dom_clean)
                                    exclusions.add(dom_clean)
                            logger.info(f"[{s_cfg['name']}] Successfully backfilled {len(replacement_rows)}/{missing_count} replacement domains.")
                            report["sheet_details"][s_num]["backfilled_count"] = len(replacement_rows)
                    except Exception as bfe:
                        logger.error(f"[{s_cfg['name']}] Error during budget backfill refetch: {bfe}")
            except Exception as e:
                logger.error(f"[{s_cfg['name']}] Failed to purge duplicate rows in sheet: {e}")

    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    clean_and_deduplicate_all_sheets(dry_run=False)
