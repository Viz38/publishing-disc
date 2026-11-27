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
import csv
import glob
import time
import logging
import datetime
from typing import List, Dict, Tuple, Set, Optional, Any, Iterable, Union
import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

logger = logging.getLogger("FleetDeduplicator")

def build_target_sheets() -> List[Dict[str, Any]]:
    from common.key_manager import get_fleet_sheet_count
    fleet_count = get_fleet_sheet_count()
    defaults = [
        {"num": 1, "id": "1IWXqnR52IzpFewE0QS0tlJFT65nJE_fmIoJs8ps9nZo", "name": "Sheet 1 (CF-1)"},
        {"num": 2, "id": "1QGraMZOJjc-EIZlt_CieOVpr1jxw6tLa_QnWgWHou5I", "name": "Sheet 2 (CF-2)"},
        {"num": 3, "id": "1MHdkFCsoAY0V7ix_pJ5SpWFq3EFpJEug2ntJk1FkEl8", "name": "Sheet 3 (CF-3)"},
        {"num": 4, "id": "1z3wH--c2qHGlNqW0e-6Mc8IVqh_kN5_fbWjILM4FxQM", "name": "Sheet 4 (Filter-V2)"},
    ]
    out: List[Dict[str, Any]] = []
    for item in defaults[:fleet_count]:
        num = item["num"]
        env_id = os.getenv(f"SHEET{num}_ID", "").strip()
        out.append({"num": num, "id": env_id or item["id"], "name": item["name"]})
    return out


TARGET_SHEETS = build_target_sheets()

CUMULATIVE_DEDUP_SHEET_ID = "1p4wg5rlfpn8xPwVCWuDnyWdSCwha9pjXSIMISjF84Sg"

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
    Persistent registry of domains processed across the fleet.
    Supports rolling multi-day historical lookups (e.g. 7-day deduplication window),
    peer fleet synchronization across hosts, and automatic cache bootstrapping.
    """
    def __init__(self, storage_dir: Optional[str] = None, root_dir: Optional[str] = None):
        self.root_dir = root_dir or BASE_DIR
        self.storage_dir = storage_dir or os.path.join(self.root_dir, "Logs", "daily_registry")
        os.makedirs(self.storage_dir, exist_ok=True)

    def _get_registry_path(self, date_str: Optional[str] = None) -> str:
        d = date_str or get_today_ist()
        return os.path.join(self.storage_dir, f"daily_processed_{d}.json")

    def _bootstrap_from_local_cache(self, date_str: str) -> Set[str]:
        """
        Self-bootstraps historical domains for a given date from existing
        pre_crawl_loaded_*.csv, discovery_cache_*.csv, or daily_eligible_*.json on local disk.
        """
        found_domains: Set[str] = set()
        sheets = ["Sheet1", "Sheet2", "Sheet3", "Sheet4"]

        for s in sheets:
            cache_dir = os.path.join(self.root_dir, s, "Logs", "Cache")
            logs_dir = os.path.join(self.root_dir, s, "Logs")

            # 1. Check pre_crawl_loaded_YYYY-MM-DD*.csv (cleanest format)
            if os.path.exists(cache_dir):
                precrawl_matches = glob.glob(os.path.join(cache_dir, f"pre_crawl_loaded_{date_str}*.csv"))
                for p_file in precrawl_matches:
                    try:
                        with open(p_file, "r", encoding="utf-8", errors="ignore") as f:
                            for row in csv.reader(f):
                                if row and row[0].strip():
                                    dom = row[0].strip().strip('"').strip("'").lower()
                                    if "." in dom and " " not in dom and not dom.startswith("title") and dom != "domain":
                                        found_domains.add(dom)
                    except Exception as e:
                        logger.debug(f"Error bootstrapping from {p_file}: {e}")

            # 2. Check daily_eligible_YYYY-MM-DD.json
            eligible_file = os.path.join(logs_dir, f"daily_eligible_{date_str}.json")
            if os.path.exists(eligible_file):
                try:
                    with open(eligible_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        recs = data.get("records", [])
                        for r in recs:
                            if r and r[0]:
                                dom = str(r[0]).strip().lower()
                                if "." in dom and " " not in dom:
                                    found_domains.add(dom)
                except Exception as e:
                    logger.debug(f"Error bootstrapping from {eligible_file}: {e}")

            # 3. Check discovery_cache_YYYY-MM-DD.csv if still empty
            if not found_domains and os.path.exists(cache_dir):
                dcache_file = os.path.join(cache_dir, f"discovery_cache_{date_str}.csv")
                if os.path.exists(dcache_file):
                    try:
                        with open(dcache_file, "r", encoding="utf-8", errors="ignore") as f:
                            for row in csv.reader(f):
                                if row and row[0].strip():
                                    dom = row[0].strip().strip('"').strip("'").lower()
                                    if "." in dom and " " not in dom and dom != "domain":
                                        found_domains.add(dom)
                    except Exception as e:
                        logger.debug(f"Error bootstrapping from {dcache_file}: {e}")

        if found_domains:
            logger.info(f"✨ Self-bootstrapped {len(found_domains)} historical domains for {date_str} from disk.")
            self.record_daily_processed(found_domains, date_str=date_str)

        return found_domains

    def get_daily_processed(self, date_str: Optional[str] = None, allow_bootstrap: bool = True) -> Set[str]:
        d = date_str or get_today_ist()
        path = self._get_registry_path(d)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list) and data:
                        return set(dom.strip().lower() for dom in data if dom and dom.strip())
            except Exception as e:
                logger.error(f"Error reading daily processed registry from {path}: {e}")

        # If empty or not exists, attempt self-bootstrap from cache archives
        if allow_bootstrap:
            bootstrapped = self._bootstrap_from_local_cache(d)
            if bootstrapped:
                return bootstrapped

        return set()

    def get_historical_processed(
        self,
        days: int = 7,
        target_date: Optional[Union[datetime.date, datetime.datetime, str]] = None,
        include_peer_endpoints: bool = True,
        include_cumulative_sheet: bool = True
    ) -> Set[str]:
        """
        Retrieves all domains processed within a rolling window of `days` up to target_date (inclusive).
        Aggregates local daily registry files, bootstraps missing days, merges peer fleet domains,
        and pulls from the 7-day cumulative master sheet (1p4wg5rlfpn8xPwVCWuDnyWdSCwha9pjXSIMISjF84Sg).
        """
        if target_date is None:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_ist = now_utc + datetime.timedelta(hours=5, minutes=30)
            base_dt = now_ist.date()
        elif isinstance(target_date, str):
            base_dt = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
        elif isinstance(target_date, datetime.datetime):
            base_dt = target_date.date()
        else:
            base_dt = target_date

        historical_pool: Set[str] = set()

        for offset in range(days + 1):
            d_curr = base_dt - datetime.timedelta(days=offset)
            d_str = d_curr.strftime("%Y-%m-%d")
            daily_doms = self.get_daily_processed(d_str, allow_bootstrap=True)
            historical_pool.update(daily_doms)

        if include_cumulative_sheet:
            try:
                cum_doms = fetch_cumulative_sheet_domains(days=days, root_dir=self.root_dir)
                if cum_doms:
                    logger.info(f"Merged {len(cum_doms)} historical domains from 7-day cumulative sheet.")
                    historical_pool.update(cum_doms)
            except Exception as ce:
                logger.warning(f"Could not fetch cumulative sheet domains: {ce}")

        if include_peer_endpoints:
            peer_doms = fetch_fleet_peer_historical_domains(days=days)
            if peer_doms:
                logger.info(f"Merged {len(peer_doms)} historical domains from peer fleet endpoints.")
                historical_pool.update(peer_doms)

        logger.info(f"Loaded {len(historical_pool)} total historical domains across {days}-day rolling window.")
        return historical_pool

    def record_daily_processed(self, domains: Iterable[str], date_str: Optional[str] = None) -> int:
        d = date_str or get_today_ist()
        path = self._get_registry_path(d)
        current = self.get_daily_processed(d, allow_bootstrap=False)
        for dom in domains:
            if dom:
                clean_d = dom.strip().lower()
                if clean_d and "." in clean_d and " " not in clean_d:
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

        # Automatic 30-day retention pruning
        self._prune_old_registries(retention_days=30)
        return len(current)

    def _prune_old_registries(self, retention_days: int = 30):
        """Removes daily registry files older than retention_days."""
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_ist = (now_utc + datetime.timedelta(hours=5, minutes=30)).date()
            cutoff_date = now_ist - datetime.timedelta(days=retention_days)

            pattern = os.path.join(self.storage_dir, "daily_processed_*.json")
            for f in glob.glob(pattern):
                fname = os.path.basename(f)
                date_part = fname.replace("daily_processed_", "").replace(".json", "")
                try:
                    f_date = datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
                    if f_date < cutoff_date:
                        os.remove(f)
                        logger.info(f"🧹 Pruned expired historical registry file: {fname}")
                except Exception:
                    continue
        except Exception as pe:
            logger.debug(f"Error during registry pruning: {pe}")


def fetch_fleet_peer_historical_domains(days: int = 7) -> Set[str]:
    """
    Queries peer endpoints across Desktop-5080 and Desktop-5321 for their historical processed domains.
    """
    fleet_endpoints = [
        "https://5080-sheet1.rajath.world/fleet/historical-domains",
        "https://5080-sheet2.rajath.world/fleet/historical-domains",
        "https://5321-sheet3.rajath.world/fleet/historical-domains",
        "https://5321-sheet4.rajath.world/fleet/historical-domains",
    ]
    auth_header = {"Authorization": f"Bearer {os.getenv('SERVICE_AUTH_TOKEN', 'Tracxn@SR')}"}
    peer_domains: Set[str] = set()

    for ep in fleet_endpoints:
        try:
            resp = requests.get(ep, params={"days": days}, headers=auth_header, timeout=4)
            if resp.status_code == 200:
                data = resp.json()
                doms = data.get("domains", [])
                if isinstance(doms, list):
                    for d in doms:
                        if d and isinstance(d, str):
                            peer_domains.add(d.strip().lower())
        except Exception:
            continue

    return peer_domains


def fetch_cumulative_sheet_domains(
    days: int = 7,
    cache_ttl_seconds: int = 3600,
    force_refresh: bool = False,
    root_dir: Optional[str] = None
) -> Set[str]:
    """
    Fetches processed domains from the master 7-day cumulative Google Sheet
    (1p4wg5rlfpn8xPwVCWuDnyWdSCwha9pjXSIMISjF84Sg).
    Parses rolling date tabs (e.g. '27Nov2026', '26Nov2026') up to `days` days back.
    Maintains a local disk cache in Logs/daily_registry/cumulative_7day_cache.json
    with TTL to prevent excessive Google Sheets API calls across fleet nodes.
    """
    base_dir = root_dir or BASE_DIR
    cache_dir = os.path.join(base_dir, "Logs", "daily_registry")
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, "cumulative_7day_cache.json")

    now_ts = time.time()
    today_str = get_today_ist()

    # 1. Check local disk cache first
    if not force_refresh and os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            cached_ts = cached.get("timestamp", 0)
            cached_domains = cached.get("domains", [])
            if (now_ts - cached_ts < cache_ttl_seconds) and cached_domains:
                logger.info(f"Loaded {len(cached_domains)} cumulative domains from local disk cache ({cache_file}).")
                return set(d.strip().lower() for d in cached_domains if d and "." in d)
        except Exception as ce:
            logger.debug(f"Failed to read cumulative cache file: {ce}")

    # 2. Fetch from live Google Sheet
    domains: Set[str] = set()
    tab_counts: Dict[str, int] = {}
    try:
        from common.advance_precrawl import get_gspread_client
        gc = get_gspread_client()
        sh = retry_gspread_call(lambda: gc.open_by_key(CUMULATIVE_DEDUP_SHEET_ID))
        worksheets = retry_gspread_call(lambda: sh.worksheets())

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_ist = (now_utc + datetime.timedelta(hours=5, minutes=30)).date()
        cutoff_date = now_ist - datetime.timedelta(days=days)

        for ws in worksheets:
            title = ws.title.strip()
            # Ignore log/audit/config tabs (e.g. 'Daily Logs')
            if "log" in title.lower() or "config" in title.lower() or "audit" in title.lower():
                continue

            # Check if title matches standard date format
            tab_date = None
            for fmt in ["%d%b%Y", "%d%B%Y", "%d-%b-%Y", "%Y-%m-%d"]:
                try:
                    tab_date = datetime.datetime.strptime(title, fmt).date()
                    break
                except ValueError:
                    continue

            # Per operational design: exactly 7 rolling daily sheets are present in this spreadsheet.
            # If date parsed, check the cutoff window; if sheet names change or don't parse as date,
            # include all data tabs (Cols <= 10) so no day is ever missed.
            is_data_sheet = False
            if tab_date is not None:
                if cutoff_date <= tab_date <= now_ist:
                    is_data_sheet = True
            else:
                col_c = getattr(ws, "col_count", 0)
                if col_c <= 10:
                    is_data_sheet = True

            if is_data_sheet:
                try:
                    col_vals = retry_gspread_call(lambda: ws.col_values(1))
                    cnt = 0
                    for val in col_vals:
                        if val:
                            d = val.strip().lower()
                            if "." in d and " " not in d and d != "domains" and not d.startswith("title"):
                                domains.add(d)
                                cnt += 1
                    tab_counts[title] = cnt
                    logger.info(f"Loaded {cnt} domains from cumulative sheet tab '{title}'")
                except Exception as we:
                    logger.warning(f"Error fetching column A from tab '{title}': {we}")

        if domains:
            temp_cache = f"{cache_file}.tmp.{os.getpid()}"
            try:
                with open(temp_cache, "w", encoding="utf-8") as f:
                    json.dump({
                        "timestamp": now_ts,
                        "date": today_str,
                        "days": days,
                        "tab_counts": tab_counts,
                        "domains": sorted(list(domains))
                    }, f)
                os.replace(temp_cache, cache_file)
                logger.info(f"Cached {len(domains)} cumulative domains to {cache_file}.")
            except Exception as swe:
                logger.debug(f"Error caching cumulative domains: {swe}")
                if os.path.exists(temp_cache):
                    try:
                        os.remove(temp_cache)
                    except Exception:
                        pass
            return domains

    except Exception as e:
        logger.error(f"Failed to fetch cumulative sheet domains from Google Sheets: {e}")
        # Resilient fallback: return stale cache if available
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                cached_domains = cached.get("domains", [])
                if cached_domains:
                    logger.info(f"Fallback: using {len(cached_domains)} domains from existing cache.")
                    return set(d.strip().lower() for d in cached_domains if d and "." in d)
            except Exception:
                pass

    return domains


def identify_fleet_duplicates(
    sheets_data: Dict[int, List[Tuple[int, str]]],
    daily_seen: Optional[Set[str]] = None,
    historical_seen: Optional[Set[str]] = None
) -> Dict[int, List[Tuple[int, str, str]]]:
    """
    Evaluates domain occurrences across production sheets in strict priority order (Sheet 1 > 2 > 3 > 4).
    Flags intra-sheet duplicates, cross-sheet duplicates, and rolling 7-day historical duplicates.
    Returns a mapping: sheet_num -> list of (row_number, domain, reason) for duplicate rows to be purged.
    """
    prior_done: Set[str] = set()
    if daily_seen:
        prior_done.update(d.strip().lower() for d in daily_seen if d and d.strip())

    prior_historical: Set[str] = set()
    if historical_seen:
        prior_historical.update(d.strip().lower() for d in historical_seen if d and d.strip())

    active_nums = sorted(set(sheets_data.keys()) | {s["num"] for s in TARGET_SHEETS})
    duplicates: Dict[int, List[Tuple[int, str, str]]] = {s: [] for s in active_nums}
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
            elif clean_d in prior_historical:
                reason = "Already processed in fleet within last 7 days"
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

def clean_and_deduplicate_all_sheets(
    gc=None,
    dry_run: bool = False,
    backfill: bool = True,
    history_days: int = 7
) -> Dict[str, Any]:
    """
    Full cross-sheet cleaning operation across all 4 sheets.
    1. Reads active domains across Sheets 1-4.
    2. Identifies all duplicates respecting priority (Sheet 1 > 2 > 3 > 4) and rolling 7-day history.
    3. Purges duplicate rows from Google Sheets (blanks Col A so engine bypasses them).
    4. Automated Budget Backfill: Re-fetches replacement domains for purged rows excluding 7-day history.
    5. Persists valid unique domains into the daily registry.
    6. Returns audit report.
    """
    if gc is None:
        gc = get_gspread_client()

    registry = DailyProcessedRegistry()
    daily_seen = registry.get_daily_processed()
    historical_seen = registry.get_historical_processed(days=history_days)
    logger.info(f"Loaded {len(daily_seen)} domains from today's registry, {len(historical_seen)} from {history_days}-day history.")

    sheets_data = load_all_active_fleet_domains(gc)
    duplicates = identify_fleet_duplicates(sheets_data, daily_seen=daily_seen, historical_seen=historical_seen)

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
                    exclusions = set(historical_seen) | set(daily_seen) | set(all_valid_unique_domains)
                    try:
                        replacement_rows = fetch_domains_for_sheet(
                            sheet_id=s_id,
                            target_count=missing_count,
                            shared_exclusion_set=exclusions
                        )
                        if replacement_rows:
                            cleared_rows = sorted([r for r, _, _ in s_dups])
                            batch_updates = []
                            for idx, rep_row in enumerate(replacement_rows):
                                if idx < len(cleared_rows):
                                    target_r = cleared_rows[idx]
                                    batch_updates.append({
                                        "range": f"A{target_r}:BE{target_r}",
                                        "values": [rep_row]
                                    })
                                    dom_clean = rep_row[0].strip().lower()
                                    all_valid_unique_domains.append(dom_clean)
                                    exclusions.add(dom_clean)

                            # Batch update replacement rows in chunks of 100 to avoid single-row API rate limits
                            UPDATE_CHUNK = 100
                            for u_idx in range(0, len(batch_updates), UPDATE_CHUNK):
                                sub_batch = batch_updates[u_idx : u_idx + UPDATE_CHUNK]
                                retry_gspread_call(lambda: ws.batch_update(sub_batch, value_input_option="USER_ENTERED"))
                                time.sleep(0.5)

                            logger.info(f"[{s_cfg['name']}] Successfully backfilled {len(replacement_rows)}/{missing_count} replacement domains in {len(batch_updates)} batched ranges.")
                            report["sheet_details"][s_num]["backfilled_count"] = len(replacement_rows)
                    except Exception as bfe:
                        logger.error(f"[{s_cfg['name']}] Error during budget backfill refetch: {bfe}")
            except Exception as e:
                logger.error(f"[{s_cfg['name']}] Failed to purge duplicate rows in sheet: {e}")

    # Persist all valid unique domains in the daily registry
    if all_valid_unique_domains and not dry_run:
        try:
            registry.record_daily_processed(all_valid_unique_domains)
            logger.info(f"📝 Recorded {len(all_valid_unique_domains)} unique domains into daily processed registry.")
        except Exception as pe:
            logger.warning(f"Failed to record daily processed domains in registry: {pe}")

    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    clean_and_deduplicate_all_sheets(dry_run=False)
