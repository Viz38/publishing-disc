import os
import sys
import json
import time
import logging
import datetime
from typing import List, Dict, Any, Optional
import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

load_dotenv(os.path.join(BASE_DIR, ".env"))

from common.key_manager import TracxnKeyManager

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
                elif resp.status_code in (401, 403):
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
                logger.error(f"Tracxn request exception: {e}")
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

    def ingest_domains_for_sheet(self, sheet_cfg: Dict[str, Any], clear_first: bool = True) -> int:
        s_name = sheet_cfg["name"]
        s_id = sheet_cfg["sheet_id"]
        logger.info(f"\n==========================================")
        logger.info(f"[{s_name}] Ingesting fresh domains (Sheet ID: {s_id})...")
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

        # 2. Clear Database if requested (Rows 3+)
        if clear_first:
            last_row = db_ws.row_count
            if last_row > 2:
                logger.info(f"[{s_name}] Clearing old data from Row 3 to Row {last_row}...")
                retry_call(lambda: db_ws.batch_clear([f"A3:BE{last_row}"]))
                logger.info(f"[{s_name}] Database cleared successfully.")

        # 3. Process each payload row
        new_rows_57 = []
        domain_seen = set()
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
            
            try:
                budget = int(p_row[5]) if len(p_row) > 5 and str(p_row[5]).strip() else 0
            except ValueError:
                budget = 0

            if budget <= 0:
                logger.info(f"[{s_name}] Skipping payload '{filter_name}' (budget is 0 or empty)")
                continue

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
                payload_str = json.dumps(query_payload)

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
                                if dom and dom not in domain_seen:
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
                        elif resp.status_code in (401, 403):
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
                        logger.error(f"Tracxn request exception: {e}")
                        time.sleep(2)
                        continue

                if f_id == "5dc559138787070a80be21ba":
                    cf_coverage = funnel_coverage
                else:
                    backlog_coverage = funnel_coverage

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

        # 4. Write new domains to CF_IF_Engine_Database starting at Row 3
        if new_rows_57:
            total_new = len(new_rows_57)
            logger.info(f"[{s_name}] Total unique domains collected: {total_new}. Writing to target sheet...")

            # Ensure row capacity
            target_needed_rows = total_new + 10
            if db_ws.row_count < target_needed_rows:
                retry_call(lambda: db_ws.resize(rows=target_needed_rows, cols=57))

            # Push in chunks of 500
            CHUNK_SIZE = 500
            for start_idx in range(0, total_new, CHUNK_SIZE):
                chunk = new_rows_57[start_idx : start_idx + CHUNK_SIZE]
                write_start_row = 3 + start_idx
                write_end_row = write_start_row + len(chunk) - 1
                target_range = f"A{write_start_row}:BE{write_end_row}"
                logger.info(f"[{s_name}] Writing chunk {target_range} ({len(chunk)} rows)...")
                retry_call(lambda: db_ws.update(range_name=target_range, values=chunk))
                time.sleep(1.5)

            logger.info(f"[{s_name}] ✅ Ingestion complete! Added {total_new} domains to {db_ws.title}.")
            return total_new
        else:
            logger.info(f"[{s_name}] No new domains returned for active payloads.")
            return 0

    def start_discovery_engine_all_sheets(self, clear_first: bool = True) -> Dict[str, int]:
        """
        Executes clearing of rows 3+ and fresh Tracxn domain ingestion across all 4 target sheets.
        Strictly does NOT trigger the Python pipeline (/discovery/start).
        """
        summary = {}
        for s_cfg in TARGET_SHEETS:
            count = self.ingest_domains_for_sheet(s_cfg, clear_first=clear_first)
            summary[s_cfg["name"]] = count

        logger.info("\n==========================================")
        logger.info("🎉 All 4 Sheets Ingestion Summary:")
        for name, cnt in summary.items():
            logger.info(f"  - {name}: {cnt} domains")
        logger.info("==========================================")
        return summary

if __name__ == "__main__":
    ingestor = TracxnDomainIngestor()
    ingestor.start_discovery_engine_all_sheets(clear_first=True)
