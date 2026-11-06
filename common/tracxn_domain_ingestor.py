import os
import sys
import json
import csv
import time
import logging
import datetime
import asyncio
from typing import List, Dict, Any, Optional, Set, Tuple
import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

load_dotenv(os.path.join(BASE_DIR, ".env"))

from common.key_manager import TracxnKeyManager
from common.gsheet_circuit_breaker import is_csv_only_mode, set_csv_only_mode, is_doc_too_large_error
from common.domain_filter_validator import is_domain_leaked, batch_validate_domains

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("TracxnDomainIngestor")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

TARGET_SHEETS = [
    {
        "sheet_num": 1,
        "sheet_id": "1IWXqnR52IzpFewE0QS0tlJFT65nJE_fmIoJs8ps9nZo",
        "name": "Discovery Operations - Engine 1"
    },
    {
        "sheet_num": 2,
        "sheet_id": "1xH7HPdSYtj7J1AjycQ6pvwz5VJKFsNHngnWxtyKZNFQ",
        "name": "Discovery Operations - Engine 2"
    },
    {
        "sheet_num": 3,
        "sheet_id": "1MHdkFCsoAY0V7ix_pJ5SpWFq3EFpJEug2ntJk1FkEl8",
        "name": "Discovery Operations - Engine 3"
    },
    {
        "sheet_num": 4,
        "sheet_id": "1RYkvqgxMJpjMrD3x9D4VE8Zu4UJlf36gN-bfJ7T-bcA",
        "name": "Discovery Operations - Engine 4"
    }
]

FUNNELS = [
    {"name": "CENTRAL FUNNEL", "id": "5dc559138787070a80be21ba"},
    {"name": "Central Funnel (Backlog)", "id": "64197f01a6dcff6572453ead"}
]

TRACXN_API_URL = "https://platform.tracxn.com/api/2.2/domains"

def get_gspread_client():
    creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        creds_json = os.getenv("GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def retry_call(fn, max_retries=10, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            err_str = str(e)
            if attempt == max_retries:
                raise
            sleep_time = delay
            if "Quota exceeded" in err_str or "429" in err_str:
                sleep_time = max(delay, 25)
                logger.warning(f"Rate limit / 429 hit (attempt {attempt}/{max_retries}). Sleeping {sleep_time}s...")
            else:
                logger.warning(f"API call failed (attempt {attempt}/{max_retries}): {e}. Retrying in {sleep_time}s...")
            time.sleep(sleep_time)
            delay = min(delay * 2, 60)

class TracxnDomainIngestor:
    def __init__(self, key_manager: Optional[TracxnKeyManager] = None):
        self.key_manager = key_manager or TracxnKeyManager()
        self.gc = get_gspread_client()

    def fetch_tracxn_domains(self, payload: Dict[str, Any], max_results: int, start_offset: int = 0) -> Dict[str, Any]:
        """
        Calls Tracxn API using token rotation from TracxnKeyManager.
        Fetches up to max_results companies in pages of 100.
        """
        results = []
        total_coverage = 0
        from_offset = start_offset
        exc_retries = 0

        while len(results) < max_results:
            page_payload = dict(payload)
            page_size = min(100, max_results - len(results))
            page_payload["from"] = from_offset
            page_payload["size"] = page_size

            token = self.key_manager.get_next_token()
            if not token:
                logger.error("No active Tracxn token available!")
                break

            headers = {
                "Content-Type": "application/json",
                "accessToken": token,
                "X-Request-Source": "Discovery Team"
            }

            try:
                resp = requests.post(TRACXN_API_URL, json=page_payload, headers=headers, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    total_coverage = data.get("total_count", total_coverage)
                    page_items = data.get("result", [])
                    if not page_items:
                        break
                    results.extend(page_items)
                    from_offset += page_size
                    if from_offset >= total_coverage:
                        break
                elif resp.status_code == 403:
                    is_unauth = False
                    try:
                        err_data = resp.json()
                        if err_data.get("errorCode") == 403000000 or "unauthorized" in str(err_data.get("message", "")).lower():
                            is_unauth = True
                    except Exception:
                        if "unauthorized" in resp.text.lower():
                            is_unauth = True

                    if is_unauth and page_payload.get("dataset") != "search":
                        logger.info(f"Tracxn 403000000 unauthorized dataset error on '{page_payload.get('dataset')}'. Falling back to 'search' without penalizing token.")
                        payload["dataset"] = "search"
                        page_payload["dataset"] = "search"
                        continue

                    self.key_manager.handle_http_error(token, resp.status_code)
                    logger.warning(f"Tracxn token invalid/expired (status {resp.status_code}). Rotating token...")
                    continue
                elif resp.status_code == 401:
                    self.key_manager.handle_http_error(token, resp.status_code)
                    logger.warning(f"Tracxn token invalid/expired (status {resp.status_code}). Rotating token...")
                    continue
                elif resp.status_code == 429:
                    self.key_manager.handle_http_error(token, resp.status_code)
                    logger.warning("Tracxn rate limit 429 hit. Rotating token...")
                    time.sleep(2)
                    continue
                else:
                    self.key_manager.handle_http_error(token, resp.status_code)
                    logger.warning(f"Tracxn error status {resp.status_code}: {resp.text[:200]}")
                    break
            except Exception as e:
                exc_retries += 1
                logger.error(f"Tracxn request exception (attempt {exc_retries}/3): {e}")
                if "timeout" in str(e).lower() and payload.get("dataset") != "search":
                    logger.info("Tracxn timeout hit; falling back to dataset='search'...")
                    payload["dataset"] = "search"
                    page_payload["dataset"] = "search"
                if exc_retries >= 3:
                    logger.warning(f"Exceeded 3 retry attempts on Tracxn API. Breaking.")
                    break
                time.sleep(2)
                continue

        return {"total_coverage": total_coverage, "items": results[:max_results]}

    def parse_company_to_57col(
        self,
        company: Dict[str, Any],
        funnel_name: str,
        funnel_id: str,
        filter_group_name: str,
        filter_group_id: str,
        filter_name: str,
        filter_id: str,
        payload_str: str,
        added_time_str: str
    ) -> List[Any]:
        row = [""] * 57

        domain = company.get("domain", "")
        row[0] = domain
        row[1] = funnel_name
        row[2] = funnel_id
        row[3] = filter_group_name
        row[4] = filter_group_id
        row[5] = filter_name
        row[6] = filter_id
        row[7] = payload_str
        row[8] = company.get("tracxnUrl", "") or f"https://tracxn.com/d/companies/{company.get('id', '')}"
        row[9] = company.get("domainStatus", "")
        row[10] = company.get("domainProfileId", "") or company.get("id", "")
        row[11] = company.get("globalPublishedStatus", "")

        # Published date
        pub_date = company.get("companyPublishDate") or {}
        if pub_date and isinstance(pub_date, dict) and "year" in pub_date:
            try:
                d = datetime.date(pub_date["year"], pub_date.get("month", 1), pub_date.get("day", 1))
                row[12] = d.strftime("%d/%b/%Y")
            except Exception:
                row[12] = ""

        # Hashtags
        kws = company.get("keywords", {}) or {}
        hash_tags = kws.get("hashTags", [])
        row[13] = ", ".join(hash_tags) if isinstance(hash_tags, list) else str(hash_tags or "-")

        # PS2 Variable and Score
        ps_list = company.get("publishabilityScoreList", []) or []
        if ps_list and isinstance(ps_list, list) and len(ps_list) > 0:
            vars_list = ps_list[0].get("variables", []) or []
            if vars_list and len(vars_list) > 0:
                row[14] = vars_list[0].get("source", {}).get("variableName", "")
                row[15] = str(vars_list[0].get("score", ""))

        # Tracxn Platform (Descriptions + Titles + Headings)
        wk = company.get("websiteKeywords", {}).get("homePage", {}) or {}
        head = wk.get("head", {}) or {}
        body = wk.get("body", {}) or {}

        title = head.get("title", {}).get("currentValue", "")
        h1_list = body.get("h1", {}).get("value", []) or []
        h2_list = body.get("h2", {}).get("value", []) or []
        desc_list = head.get("description", {}).get("value", []) or []

        desc_info = (
            f"Title :- {title}\n"
            f"Heading 1 :- {', '.join(h1_list) if isinstance(h1_list, list) else str(h1_list)}\n"
            f"Heading 2 :- {', '.join(h2_list) if isinstance(h2_list, list) else str(h2_list)}\n"
            f"Description :- {', '.join(desc_list) if isinstance(desc_list, list) else str(desc_list)}"
        )
        row[16] = desc_info

        # Col 18 (R): Database Added Time
        row[17] = added_time_str

        # Cols 19-57 remain empty strings for downstream processing
        return row

    def repopulate_clean_domains(
        self,
        fetch_fn,
        budget: int,
        fleet_seen: Set[str],
        daily_seen: Set[str],
        filter_metadata: Dict[str, Any],
        max_loop_iterations: int = 20
    ) -> List[List[Any]]:
        """
        Executes the self-healing ingestion loop:
        1. Ingests domains from fetch_fn.
        2. Evaluates the 3 criteria (sameAs, status == PUBLISHED, feeds tagged) -> removes invalid.
        3. Evaluates duplicates (intra-batch, fleet_seen from prior sheets, daily_seen) -> removes duplicates.
        4. Repopulates the removed count from fetch_fn.
        5. Loops steps 2-4 until budget is fulfilled with 100% clean, unique domains.
        """
        clean_rows: List[List[Any]] = []
        clean_domains: Set[str] = set()
        offset = 0
        loop_count = 0

        while len(clean_rows) < budget and loop_count < max_loop_iterations:
            loop_count += 1
            needed = budget - len(clean_rows)
            page_size = max(needed, 100)
            items = fetch_fn(page_size, offset)
            if not items:
                break
            offset += len(items)

            for item in items:
                dom = (item.get("domain") or "").strip().lower()
                if not dom or "." not in dom:
                    continue

                # Step 2: Check 3 criteria
                is_leaked, reason = is_domain_leaked(item)
                if is_leaked:
                    logger.info(f"Purging leaked domain '{dom}' (Reason: {reason})")
                    continue

                # Step 3: Check duplicates
                if dom in clean_domains or dom in fleet_seen or dom in daily_seen:
                    logger.info(f"Purging duplicate domain '{dom}'")
                    continue

                # Step 4: Add clean row
                clean_domains.add(dom)
                row_57 = self.parse_company_to_57col(
                    company=item,
                    funnel_name=filter_metadata.get("funnel_name", ""),
                    funnel_id=filter_metadata.get("funnel_id", ""),
                    filter_group_name=filter_metadata.get("filter_group_name", ""),
                    filter_group_id=filter_metadata.get("filter_group_id", ""),
                    filter_name=filter_metadata.get("filter_name", ""),
                    filter_id=filter_metadata.get("filter_id", ""),
                    payload_str=filter_metadata.get("payload_str", ""),
                    added_time_str=filter_metadata.get("added_time_str", "")
                )
                clean_rows.append(row_57)
                if len(clean_rows) >= budget:
                    break

        return clean_rows

    def audit_and_repopulate_rows(
        self,
        candidate_rows: List[List[Any]],
        fetch_replacement_fn,
        target_budget: int,
        fleet_seen: Set[str],
        daily_seen: Set[str],
        filter_metadata: Dict[str, Any],
        max_loop_iterations: int = 20
    ) -> List[List[Any]]:
        """
        Post-ingestion validation, deduplication, and self-healing repopulation:
        1. Checks all candidate rows for the 3 criteria (via batch_validate_domains).
        2. Discards leaked rows and duplicate rows.
        3. Fetches replacement domains using fetch_replacement_fn.
        4. Validates replacements against the 3 criteria and duplicates.
        5. Loops until target_budget of 100% clean, verified, non-duplicate rows is reached.
        """
        # Step 1: Batch validate initial candidates
        candidate_domains = [r[0].strip().lower() for r in candidate_rows if r and r[0]]
        val_map = batch_validate_domains(candidate_domains)

        clean_rows: List[List[Any]] = []
        clean_domains: Set[str] = set()

        for r in candidate_rows:
            if not r or not r[0]:
                continue
            dom = r[0].strip().lower()
            is_leaked, reason = val_map.get(dom, (False, ""))
            if is_leaked:
                logger.info(f"Post-ingestion audit purged leaked domain '{dom}' (Reason: {reason})")
                continue
            if dom in clean_domains or dom in fleet_seen or dom in daily_seen:
                logger.info(f"Post-ingestion audit purged duplicate domain '{dom}'")
                continue
            clean_domains.add(dom)
            clean_rows.append(r)

        # Step 2: Repopulate if short of budget
        loop_count = 0
        while len(clean_rows) < target_budget and loop_count < max_loop_iterations:
            loop_count += 1
            needed = target_budget - len(clean_rows)
            rep_items = fetch_replacement_fn(needed, filter_metadata)
            if not rep_items:
                break

            potential_candidates = []
            for item in rep_items:
                dom = (item.get("domain") or "").strip().lower()
                if not dom or "." not in dom:
                    continue
                is_leaked, reason = is_domain_leaked(item)
                if is_leaked:
                    logger.info(f"Purging leaked replacement domain '{dom}' (Reason: {reason})")
                    continue
                if dom in clean_domains or dom in fleet_seen or dom in daily_seen:
                    logger.info(f"Purging duplicate replacement domain '{dom}'")
                    continue
                potential_candidates.append(item)

            if not potential_candidates:
                continue

            # Batch validate remaining candidates
            rep_doms = [(it.get("domain") or "").strip().lower() for it in potential_candidates]
            rep_val_map = batch_validate_domains(rep_doms)

            for item in potential_candidates:
                dom = (item.get("domain") or "").strip().lower()
                is_leaked, reason = rep_val_map.get(dom, (False, ""))
                if is_leaked:
                    logger.info(f"Purging leaked replacement domain '{dom}' after validation (Reason: {reason})")
                    continue
                if dom in clean_domains:
                    continue

                clean_domains.add(dom)
                row_57 = self.parse_company_to_57col(
                    company=item,
                    funnel_name=filter_metadata.get("funnel_name", ""),
                    funnel_id=filter_metadata.get("funnel_id", ""),
                    filter_group_name=filter_metadata.get("filter_group_name", ""),
                    filter_group_id=filter_metadata.get("filter_group_id", ""),
                    filter_name=filter_metadata.get("filter_name", ""),
                    filter_id=filter_metadata.get("filter_id", ""),
                    payload_str=filter_metadata.get("payload_str", ""),
                    added_time_str=filter_metadata.get("added_time_str", "")
                )
                clean_rows.append(row_57)
                if len(clean_rows) >= target_budget:
                    break

        return clean_rows

    def ingest_domains_for_sheet(self, sheet_cfg: Dict[str, Any], clear_first: bool = True, fleet_seen: Optional[Set[str]] = None) -> int:
        s_name = sheet_cfg["name"]
        s_id = sheet_cfg["sheet_id"]
        if fleet_seen is None:
            fleet_seen = set()
        logger.info(f"\n==========================================")
        logger.info(f"[{s_name}] Ingesting fresh domains (Sheet ID: {s_id}, fleet_seen: {len(fleet_seen)})...")
        logger.info(f"==========================================")

        sh = retry_call(lambda: self.gc.open_by_key(s_id))

        # 1. Read Payload Tab
        try:
            payload_ws = sh.worksheet("CF_IF Engine - Input Payload")
            payload_rows = retry_call(lambda: payload_ws.get_all_values())
        except Exception as e:
            logger.error(f"[{s_name}] Missing or unreadable 'CF_IF Engine - Input Payload' tab: {e}")
            return 0

        if len(payload_rows) < 3:
            logger.warning(f"[{s_name}] No payload definitions in 'CF_IF Engine - Input Payload'.")
            return 0

        # Get Database Worksheet
        try:
            db_ws = sh.worksheet("CF_IF_Engine_Database")
        except Exception:
            db_ws = sh.get_worksheet(0)

        # Resolve local sheet directory for CSV-only mode
        from common.fleet_deduplicator import TARGET_SHEETS as FLEET_TARGET_SHEETS
        folder = sheet_cfg.get("folder")
        if not folder:
            for ts in FLEET_TARGET_SHEETS:
                if ts.get("id") == s_id or ts.get("sheet_id") == s_id:
                    folder = ts.get("folder")
                    break
        if not folder and "sheet_num" in sheet_cfg:
            folder = f"Sheet{sheet_cfg['sheet_num']}"
        sheet_dir = os.path.join(BASE_DIR, folder) if folder else None

        use_csv_only = is_csv_only_mode(sheet_dir) if sheet_dir else False

        # 2. Clear Database if requested (Rows 3+)
        if clear_first and not use_csv_only:
            last_row = db_ws.row_count
            if last_row > 2:
                try:
                    logger.info(f"[{s_name}] Clearing old data from Row 3 to Row {last_row}...")
                    retry_call(lambda: db_ws.batch_clear([f"A3:BE{last_row}"]), max_retries=3, delay=2)
                    logger.info(f"[{s_name}] Database cleared successfully.")
                except Exception as ce:
                    logger.critical(f"[{s_name}] 🚨 Database clear failed ({ce})! Sheet appears read-only/too large. Switching to CSV-only mode.")
                    use_csv_only = True
                    if sheet_dir:
                        set_csv_only_mode(sheet_dir, True)

        # 3. Process each payload row
        # Strictly enforce unique domain ingestion: load today's daily processed registry
        # and active domains in other fleet sheets before fetching to avoid double processing
        from common.fleet_deduplicator import (
            DailyProcessedRegistry,
            TARGET_SHEETS as FLEET_TARGET_SHEETS,
            clean_and_deduplicate_all_sheets
        )
        registry = DailyProcessedRegistry()
        daily_seen = registry.get_daily_processed()
        logger.info(f"[{s_name}] Loaded {len(daily_seen)} domains from daily processed registry to exclude.")

        cross_sheet_seen = set()
        for other_s in FLEET_TARGET_SHEETS:
            if other_s["id"] != s_id:
                try:
                    other_sh = retry_call(lambda: self.gc.open_by_key(other_s["id"]))
                    try:
                        other_ws = other_sh.worksheet("CF_IF_Engine_Database")
                    except Exception:
                        other_ws = other_sh.get_worksheet(0)
                    col_a = other_ws.col_values(1)
                    for d in col_a[2:]:
                        if d and d.strip():
                            cross_sheet_seen.add(d.strip().lower())
                except Exception as ex:
                    logger.warning(f"[{s_name}] Could not load domains from {other_s['name']} for cross-sheet exclusion: {ex}")
        logger.info(f"[{s_name}] Loaded {len(cross_sheet_seen)} active domains from other fleet sheets to exclude.")

        new_rows_57 = []
        domain_seen = set(daily_seen) | cross_sheet_seen | fleet_seen
        added_time_str = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

        # Row 0 & 1 are headers, payload data starts at row index 2 (Row 3/4)
        for idx in range(2, len(payload_rows)):
            p_row = payload_rows[idx]
            if not p_row or not p_row[0]:
                continue

            filter_group_name = p_row[0]
            filter_name = p_row[1] if len(p_row) > 1 else ""
            filter_id = p_row[2] if len(p_row) > 2 else ""
            raw_payload = p_row[10] if len(p_row) > 10 and p_row[10] else (p_row[3] if len(p_row) > 3 else "")
            filter_group_id = p_row[4] if len(p_row) > 4 else ""
            
            custom_payload_template = None
            if raw_payload and raw_payload.strip():
                try:
                    parsed_p = json.loads(raw_payload.strip())
                    if isinstance(parsed_p, dict):
                        custom_payload_template = parsed_p
                        logger.info(f"[{s_name}] Using dynamic sheet payload from tab for '{filter_name}' ({len(raw_payload)} chars)")
                except Exception as pe:
                    logger.warning(f"[{s_name}] Could not parse JSON payload from sheet for '{filter_name}': {pe}")

            budget_str = p_row[5].strip() if len(p_row) > 5 else ""
            if budget_str:
                try:
                    budget = int(budget_str)
                except ValueError:
                    budget = 0

                if budget <= 0:
                    logger.info(f"[{s_name}] Skipping payload '{filter_name}' (explicit budget 0)")
                    continue
            else:
                budget = float("inf")
                logger.info(f"[{s_name}] No budget count specified for payload '{filter_name}'. Clearing all available domains.")

            funnel_selection = p_row[7] if len(p_row) > 7 else "All"

            active_funnels = []
            if "All" in funnel_selection or not funnel_selection:
                active_funnels = FUNNELS
            else:
                active_funnels = [f for f in FUNNELS if f["name"] == funnel_selection or f["id"] == funnel_selection]
                if not active_funnels:
                    active_funnels = FUNNELS

            logger.info(f"[{s_name}] Processing payload '{filter_name}' (Filter ID: {filter_id}, Budget: {budget})")

            cf_coverage = 0
            backlog_coverage = 0
            domain_added_count = 0

            for funnel in active_funnels:
                f_name = funnel["name"]
                f_id = funnel["id"]

                if custom_payload_template:
                    query_payload = json.loads(json.dumps(custom_payload_template))
                    payload_str = raw_payload.strip()
                else:
                    query_payload = {
                        "sort": [{"sortField": "funnelStatusUpdatedDate", "order": "ASC"}],
                        "filter": {
                            "funnelId": [f_id],
                            "funnelFilterId": [filter_id]
                        },
                        "from": 0,
                        "size": 100,
                        "datatype": "companies",
                        "dataset": "funnelassignment"
                    }
                    payload_str = json.dumps(query_payload)

                # If assigned budget is already satisfied from previous funnels,
                # do a lightweight coverage query (size=1) solely to update metadata
                if domain_added_count >= budget:
                    cov_res = self.fetch_tracxn_domains(query_payload, max_results=1)
                    cov = cov_res.get("total_coverage", 0)
                    if f_id == "5dc559138787070a80be21ba":
                        cf_coverage = cov
                    else:
                        backlog_coverage = cov
                    continue

                from_offset = 0
                funnel_coverage = 0
                exc_retries = 0

                while domain_added_count < budget:
                    page_size = 100
                    page_payload = dict(query_payload)
                    page_payload["from"] = from_offset
                    page_payload["size"] = page_size

                    token = self.key_manager.get_next_token()
                    if not token:
                        logger.error("No active Tracxn token available!")
                        break

                    headers = {
                        "Content-Type": "application/json",
                        "accessToken": token,
                        "X-Request-Source": "Discovery Team"
                    }

                    try:
                        resp = requests.post(TRACXN_API_URL, json=page_payload, headers=headers, timeout=30)
                        if resp.status_code == 200:
                            data = resp.json()
                            funnel_coverage = data.get("total_count", funnel_coverage)
                            page_items = data.get("result", [])
                            if not page_items:
                                break

                            for item in page_items:
                                dom = item.get("domain", "").strip().lower()
                                if not dom or "." not in dom:
                                    continue

                                # Step 2: Check 3 criteria in-flight
                                is_leaked, reason = is_domain_leaked(item)
                                if is_leaked:
                                    logger.info(f"[{s_name}] Purging leaked domain '{dom}' in-flight (Reason: {reason})")
                                    continue

                                # Step 3: Check duplicates (fleet, daily, intra-batch)
                                if dom in domain_seen:
                                    continue

                                domain_seen.add(dom)
                                row_57 = self.parse_company_to_57col(
                                    company=item,
                                    funnel_name=f_name,
                                    funnel_id=f_id,
                                    filter_group_name=filter_group_name,
                                    filter_group_id=filter_group_id,
                                    filter_name=filter_name,
                                    filter_id=filter_id,
                                    payload_str=payload_str,
                                    added_time_str=added_time_str
                                )
                                new_rows_57.append(row_57)
                                domain_added_count += 1
                                if domain_added_count >= budget:
                                    break

                            from_offset += page_size
                            if from_offset >= funnel_coverage:
                                break
                        elif resp.status_code == 403:
                            is_unauth = False
                            try:
                                err_data = resp.json()
                                if err_data.get("errorCode") == 403000000 or "unauthorized" in str(err_data.get("message", "")).lower():
                                    is_unauth = True
                            except Exception:
                                if "unauthorized" in resp.text.lower():
                                    is_unauth = True

                            if is_unauth and page_payload.get("dataset") != "search":
                                logger.info(f"Tracxn 403000000 unauthorized dataset error on '{page_payload.get('dataset')}'. Falling back to 'search' without penalizing token.")
                                query_payload["dataset"] = "search"
                                page_payload["dataset"] = "search"
                                continue

                            self.key_manager.handle_http_error(token, resp.status_code)
                            logger.warning(f"Tracxn token invalid/expired (status {resp.status_code}). Rotating token...")
                            continue
                        elif resp.status_code == 401:
                            self.key_manager.handle_http_error(token, resp.status_code)
                            logger.warning(f"Tracxn token invalid/expired (status {resp.status_code}). Rotating token...")
                            continue
                        elif resp.status_code == 429:
                            self.key_manager.handle_http_error(token, resp.status_code)
                            logger.warning("Tracxn rate limit 429 hit. Rotating token...")
                            time.sleep(2)
                            continue
                        else:
                            self.key_manager.handle_http_error(token, resp.status_code)
                            logger.warning(f"Tracxn error status {resp.status_code}: {resp.text[:200]}")
                            break
                    except Exception as e:
                        exc_retries += 1
                        logger.error(f"Tracxn request exception (attempt {exc_retries}/3): {e}")
                        if "timeout" in str(e).lower() and query_payload.get("dataset") != "search":
                            logger.info("Tracxn timeout hit; falling back to dataset='search'...")
                            query_payload["dataset"] = "search"
                            page_payload["dataset"] = "search"
                        if exc_retries >= 3:
                            logger.warning(f"Exceeded 3 retry attempts on payload '{filter_name}'. Skipping to next payload.")
                            break
                        time.sleep(2)
                        continue

                if f_id == "5dc559138787070a80be21ba":
                    cf_coverage = funnel_coverage
                else:
                    backlog_coverage = funnel_coverage

                if custom_payload_template and "funnelId" not in custom_payload_template.get("filter", {}):
                    break

            # Update coverage in payload tab: Col 15 (O), Col 16 (P), Col 17 (Q)
            # 1-indexed row: idx + 1
            payload_row_num = idx + 1
            update_range = f"O{payload_row_num}:Q{payload_row_num}"
            try:
                retry_call(lambda: payload_ws.update(
                    range_name=update_range,
                    values=[[cf_coverage, backlog_coverage, domain_added_count]]
                ))
            except Exception as e:
                logger.warning(f"[{s_name}] Could not update payload coverage cells: {e}")

        # 4. Post-Ingestion Audit & Criteria Verification across candidate rows
        if new_rows_57:
            logger.info(f"[{s_name}] Running post-ingestion verification across {len(new_rows_57)} candidate domains...")
            candidate_domains = [r[0].strip().lower() for r in new_rows_57 if r and r[0]]
            val_map = batch_validate_domains(candidate_domains)

            verified_rows = []
            leaked_purged = 0
            dups_purged = 0
            sheet_clean_doms = set()

            for r in new_rows_57:
                dom = r[0].strip().lower()
                is_leaked, reason = val_map.get(dom, (False, ""))
                if is_leaked:
                    logger.warning(f"[{s_name}] Post-ingestion audit purged leaked domain '{dom}' (Reason: {reason})")
                    leaked_purged += 1
                    continue
                if dom in sheet_clean_doms or dom in fleet_seen or dom in daily_seen:
                    logger.warning(f"[{s_name}] Post-ingestion audit purged duplicate domain '{dom}'")
                    dups_purged += 1
                    continue
                sheet_clean_doms.add(dom)
                verified_rows.append(r)

            logger.info(f"[{s_name}] Post-ingestion audit complete: {len(verified_rows)} verified clean, {leaked_purged} leaked purged, {dups_purged} duplicates purged.")
            new_rows_57 = verified_rows
            fleet_seen.update(sheet_clean_doms)

        # 5. Write new domains to CF_IF_Engine_Database starting at Row 3 (or local_sheet.csv if CSV-only mode)
        if new_rows_57:
            total_new = len(new_rows_57)
            logger.info(f"[{s_name}] Total unique domains collected: {total_new}. Writing rows...")

            if not use_csv_only:
                try:
                    # Ensure row capacity
                    target_needed_rows = total_new + 10
                    if db_ws.row_count < target_needed_rows:
                        retry_call(lambda: db_ws.resize(rows=target_needed_rows, cols=57), max_retries=3, delay=2)

                    # Push in chunks of 500
                    CHUNK_SIZE = 500
                    for start_idx in range(0, total_new, CHUNK_SIZE):
                        chunk = new_rows_57[start_idx : start_idx + CHUNK_SIZE]
                        write_start_row = 3 + start_idx
                        write_end_row = write_start_row + len(chunk) - 1
                        target_range = f"A{write_start_row}:BE{write_end_row}"
                        logger.info(f"[{s_name}] Writing chunk {target_range} ({len(chunk)} rows)...")
                        retry_call(lambda: db_ws.update(range_name=target_range, values=chunk), max_retries=3, delay=2)
                        time.sleep(1.5)

                    logger.info(f"[{s_name}] ✅ Ingestion complete! Added {total_new} domains to {db_ws.title}.")
                except Exception as we:
                    logger.critical(f"[{s_name}] 🚨 Sheet write failed ({we})! Automatically applying CSV-only mode.")
                    use_csv_only = True
                    if sheet_dir:
                        set_csv_only_mode(sheet_dir, True)

            if use_csv_only and sheet_dir:
                set_csv_only_mode(sheet_dir, True)
                local_sheet_csv = os.path.join(sheet_dir, "local_sheet.csv")
                logger.info(f"[{s_name}] 📂 [CSV-Only Mode] Staging {total_new} domains directly into {local_sheet_csv}...")
                with open(local_sheet_csv, "w", encoding="utf-8", newline="") as f_out:
                    writer = csv.writer(f_out)
                    writer.writerows(new_rows_57)
                logger.info(f"[{s_name}] ✅ Staged {total_new} domains into {local_sheet_csv}")

            # 5. Post-Ingestion Cross-Sheet Cleaning & Deduplication Operation
            # Strictly executed BEFORE any Tech Crawler Job ID is generated
            try:
                logger.info(f"[{s_name}] Running cross-sheet deduplication cleaning across all 4 production sheets...")
                dedup_report = clean_and_deduplicate_all_sheets(gc=self.gc, dry_run=False)
                logger.info(f"[{s_name}] Post-ingestion deduplication complete: purged {dedup_report.get('total_duplicates_found', 0)} duplicates across fleet.")
            except Exception as de:
                logger.error(f"[{s_name}] Error during post-ingestion cross-sheet deduplication: {de}")

            return total_new
        else:
            logger.info(f"[{s_name}] No new domains returned for active payloads.")
            return 0

    def start_discovery_engine_all_sheets(self, clear_first: bool = True, start_tc_jobs: bool = False) -> Dict[str, int]:
        """
        Executes sequential clearing of rows 3+ and fresh Tracxn domain ingestion across all 4 target sheets:
        1. Ingests domains for Sheet 1.
        2. Evaluates the 3 criteria (sameAs, status == PUBLISHED, feeds tagged) -> removes invalid.
        3. Evaluates duplicates -> removes duplicates.
        4. Repopulates until required number of domains matching all criteria are found.
        5. Passes verified clean domains into fleet_seen for Sheet 2.
        6. Optionally triggers background Tech Crawler Job ID creation.
        7. Repeats for Sheet 2 -> Sheet 3 -> Sheet 4.
        """
        summary = {}
        fleet_seen: Set[str] = set()

        for s_cfg in TARGET_SHEETS:
            s_name = s_cfg["name"]
            logger.info(f"\n==========================================")
            logger.info(f"[{s_name}] Ingesting fresh domains with sequential fleet handoff ({len(fleet_seen)} domains in fleet_seen)...")
            logger.info(f"==========================================")
            count = self.ingest_domains_for_sheet(s_cfg, clear_first=clear_first, fleet_seen=fleet_seen)
            summary[s_name] = count

            if start_tc_jobs:
                logger.info(f"[{s_name}] Initiating background Tech Crawler Job ID creation...")
                try:
                    from common.fleet_orchestrator import dispatch_job_generation_task
                    sheet_num = s_cfg.get("sheet_num", 1)
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(dispatch_job_generation_task(sheet_num))
                    except RuntimeError:
                        asyncio.run(dispatch_job_generation_task(sheet_num))
                except Exception as te:
                    logger.warning(f"[{s_name}] Could not trigger background TC job generation: {te}")

        logger.info("\n==========================================")
        logger.info("🎉 All 4 Sheets Ingestion Summary:")
        for name, cnt in summary.items():
            logger.info(f"  - {name}: {cnt} domains")
        logger.info("==========================================")
        return summary

if __name__ == "__main__":
    ingestor = TracxnDomainIngestor()
    ingestor.start_discovery_engine_all_sheets(clear_first=True)
