import os
import sys
import json
import csv
import time
import asyncio
import logging
import datetime
from typing import List, Dict, Any, Optional
import aiohttp
import requests
import gspread
from google.oauth2.service_account import Credentials
import subprocess
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

load_dotenv(os.path.join(BASE_DIR, ".env"))

from common.key_manager import TracxnKeyManager
from common.tech_crawler import TechCrawlerClient
from common.storage_cleaner import clean_old_storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AdvancePreCrawl")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

TARGET_SHEETS = [
    {"num": 1, "id": os.getenv("SHEET1_ID", "1IWXqnR52IzpFewE0QS0tlJFT65nJE_fmIoJs8ps9nZo"), "name": "Sheet 1 (CF-1)", "folder": "Sheet1"},
    {"num": 2, "id": os.getenv("SHEET2_ID", "1xH7HPdSYtj7J1AjycQ6pvwz5VJKFsNHngnWxtyKZNFQ"), "name": "Sheet 2 (CF-2)", "folder": "Sheet2"},
    {"num": 3, "id": os.getenv("SHEET3_ID", "1MHdkFCsoAY0V7ix_pJ5SpWFq3EFpJEug2ntJk1FkEl8"), "name": "Sheet 3 (CF-3)", "folder": "Sheet3"},
    {"num": 4, "id": os.getenv("SHEET4_ID", "1RYkvqgxMJpjMrD3x9D4VE8Zu4UJlf36gN-bfJ7T-bcA"), "name": "Sheet 4 (Filter-V2)", "folder": "Sheet4"},
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

def parse_company_to_57col(
    company: Dict[str, Any],
    filter_group_name: str,
    filter_group_id: str,
    filter_name: str,
    filter_id: str,
    payload_str: str,
    funnel_name: str,
    funnel_id: str,
    added_time_str: str
) -> List[str]:
    row = ["" for _ in range(57)]
    row[0] = company.get("domain", "")
    row[1] = funnel_name
    row[2] = funnel_id
    row[3] = filter_group_name
    row[4] = filter_group_id
    row[5] = filter_name
    row[6] = filter_id
    row[7] = payload_str
    row[8] = company.get("tracxnUrl", "") or f"https://tracxn.com/d/companies/{company.get('id', '')}"
    row[9] = str(company.get("domainStatus", ""))
    row[10] = str(company.get("domainProfileId", "") or company.get("id", ""))
    row[11] = str(company.get("globalPublishedStatus", ""))

    pub_date = company.get("companyPublishDate") or {}
    if pub_date and isinstance(pub_date, dict) and "year" in pub_date:
        try:
            d = datetime.date(pub_date["year"], pub_date.get("month", 1), pub_date.get("day", 1))
            row[12] = d.strftime("%d/%b/%Y")
        except Exception:
            row[12] = ""

    kws = company.get("keywords", {}) or {}
    hash_tags = kws.get("hashTags", [])
    row[13] = ", ".join(hash_tags) if isinstance(hash_tags, list) else str(hash_tags or "-")

    ps_list = company.get("publishabilityScoreList", []) or []
    if ps_list and isinstance(ps_list, list) and len(ps_list) > 0:
        vars_list = ps_list[0].get("variables", []) or []
        if vars_list and len(vars_list) > 0:
            row[14] = vars_list[0].get("source", {}).get("variableName", "")
            row[15] = str(vars_list[0].get("score", ""))

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
    row[17] = added_time_str
    row[20] = "DONE"
    return row

def fetch_domains_for_sheet(sheet_id: str, target_count: int = 15000, key_manager: Optional[TracxnKeyManager] = None) -> List[List[str]]:
    km = key_manager or TracxnKeyManager()
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    payload_ws = sh.worksheet("CF_IF Engine - Input Payload")
    payload_rows = payload_ws.get_all_values()

    new_rows_57 = []
    domain_seen = set()
    added_time_str = datetime.datetime.now().strftime("%d/%b/%Y  %H:%M:%S")

    for idx in range(2, len(payload_rows)):
        if len(new_rows_57) >= target_count:
            break

        p_row = payload_rows[idx]
        if not p_row or not p_row[0]:
            continue

        filter_group_name = p_row[0]
        filter_name = p_row[1] if len(p_row) > 1 else ""
        filter_id = p_row[2] if len(p_row) > 2 else ""
        filter_group_id = p_row[4] if len(p_row) > 4 else ""

        try:
            budget = int(p_row[5]) if len(p_row) > 5 and str(p_row[5]).strip() else 0
        except ValueError:
            budget = 0

        if budget <= 0:
            continue

        # Adjust budget to not exceed total target_count
        remaining_needed = target_count - len(new_rows_57)
        effective_budget = min(budget, remaining_needed)

        funnel_selection = p_row[7] if len(p_row) > 7 else "All"
        active_funnels = FUNNELS if "All" in funnel_selection or not funnel_selection else [
            f for f in FUNNELS if f["name"] == funnel_selection or f["id"] == funnel_selection
        ]
        if not active_funnels:
            active_funnels = FUNNELS

        domain_added_count = 0
        for funnel in active_funnels:
            if domain_added_count >= effective_budget:
                break

            f_name = funnel["name"]
            f_id = funnel["id"]

            query_payload = {
                "sort": [{"sortField": "funnelStatusUpdatedDate", "order": "ASC"}],
                "filter": {"funnelId": [f_id], "funnelFilterId": [filter_id]},
                "from": 0,
                "size": 100,
                "datatype": "companies",
                "dataset": "funnelassignment"
            }

            from_offset = 0
            payload_str = json.dumps(query_payload)

            while domain_added_count < effective_budget and len(new_rows_57) < target_count:
                page_size = 100
                page_payload = dict(query_payload)
                page_payload["from"] = from_offset
                page_payload["size"] = page_size

                token = km.get_next_token()
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
                        page_items = data.get("result", [])
                        total_cov = data.get("total_count", 0)
                        if not page_items:
                            break

                        for item in page_items:
                            dom = item.get("domain", "").strip().lower()
                            if dom and dom not in domain_seen:
                                domain_seen.add(dom)
                                row_57 = parse_company_to_57col(
                                    company=item,
                                    filter_group_name=filter_group_name,
                                    filter_group_id=filter_group_id,
                                    filter_name=filter_name,
                                    filter_id=filter_id,
                                    payload_str=payload_str,
                                    funnel_name=f_name,
                                    funnel_id=f_id,
                                    added_time_str=added_time_str
                                )
                                new_rows_57.append(row_57)
                                domain_added_count += 1
                                if len(new_rows_57) >= target_count:
                                    break
                        from_offset += page_size
                        if total_cov and from_offset >= total_cov:
                            break
                    elif resp.status_code in (401, 403):
                        km.handle_http_error(token, resp.status_code)
                        logger.warning(f"Tracxn token invalid/expired (status {resp.status_code}). Rotating token...")
                        continue
                    elif resp.status_code == 429:
                        km.handle_http_error(token, resp.status_code)
                        logger.warning("Tracxn rate limit 429 hit. Sleeping 2s and rotating...")
                        time.sleep(2)
                        continue
                    else:
                        km.handle_http_error(token, resp.status_code)
                        logger.warning(f"Tracxn error status {resp.status_code}. Rotating...")
                        break
                except Exception as e:
                    logger.warning(f"Error fetching Tracxn domains: {e}")
                    time.sleep(2)
                    continue

    logger.info(f"Fetched {len(new_rows_57)} unique domains for sheet {sheet_id}")
    return new_rows_57

async def generate_job_ids_for_rows(rows: List[List[str]], concurrency: int = 25) -> List[List[str]]:
    """
    Submits domains to Tracxn Tech Crawler API concurrently to create Job IDs.
    Populates Col 20 ('DONE'), Col 21 (Job ID), Col 22 (Timestamp), Col 23 ('PROCESSING').
    """
    token_manager = TracxnKeyManager()
    tc_client = TechCrawlerClient(token_rotator=token_manager)

    connector = aiohttp.TCPConnector(limit=concurrency + 10, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=35)

    created_count = 0
    failed_count = 0
    total = len(rows)

    queue: asyncio.Queue = asyncio.Queue()
    for item in enumerate(rows):
        queue.put_nowait(item)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async def _worker():
            nonlocal created_count, failed_count
            while True:
                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                idx, r = item
                domain = r[0].strip() if r and r[0] else ""
                if domain:
                    await asyncio.sleep(0.04)  # Small pacing
                    try:
                        job_id = await tc_client.generate_job_id(session, domain)
                        gen_time = datetime.datetime.now().strftime("%d/%b/%Y %H:%M:%S")
                        if job_id:
                            r[20] = "DONE"
                            r[21] = job_id
                            r[22] = gen_time
                            r[23] = "PROCESSING"
                            created_count += 1
                        else:
                            r[20] = "DONE"
                            r[21] = ""
                            r[22] = gen_time
                            r[23] = "FAILED"
                            failed_count += 1
                    except Exception as e:
                        logger.debug(f"Error generating job ID for {domain}: {e}")
                        failed_count += 1

                    done = created_count + failed_count
                    if done % 500 == 0 or done == total:
                        logger.info(f"Tech Crawler Job ID Generation Progress: {done}/{total} ({created_count} OK, {failed_count} FAIL)")

                queue.task_done()

        # Bounded worker pool: exactly `concurrency` workers running concurrently
        workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
        await queue.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    logger.info(f"✅ Tech Crawler Job ID Generation Complete: {created_count} Job IDs created, {failed_count} failed out of {total}")
    return rows

def spawn_advance_precrawl_subprocess(folder: str, sheet_num: int, budget: int = 15000) -> int:
    """
    Spawns advance_precrawl.py as a completely detached background subprocess.
    Redirects stdout/stderr to <folder>/Logs/precrawl_run.log and records PID in <folder>/.precrawl.pid.
    Ensures uvicorn event loop is NEVER blocked or starved by batch crawlers.
    Returns the spawned process PID.
    """
    sheet_dir = os.path.join(BASE_DIR, folder)
    logs_dir = os.path.join(sheet_dir, "Logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, "precrawl_run.log")
    pid_path = os.path.join(sheet_dir, ".precrawl.pid")

    script_path = os.path.join(BASE_DIR, "common", "advance_precrawl.py")
    cmd = [sys.executable, script_path, str(budget), str(sheet_num)]

    log_file = open(log_path, "a", encoding="utf-8")
    log_file.write(f"\n=== LAUNCHING ADVANCE PRE-CRAWL AT {datetime.datetime.now().isoformat()} ===\n")
    log_file.flush()

    process = subprocess.Popen(
        cmd,
        cwd=BASE_DIR,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        start_new_session=True  # Detached process group
    )

    try:
        with open(pid_path, "w", encoding="utf-8") as pf:
            pf.write(str(process.pid))
    except Exception as e:
        logger.warning(f"Could not write .precrawl.pid: {e}")

    logger.info(f"🚀 Spawned isolated advance pre-crawl subprocess (PID {process.pid}) for {folder} (Budget {budget})")
    return process.pid

def save_precrawl_to_csv(folder: str, rows: List[List[str]]) -> str:
    sheet_dir = os.path.join(BASE_DIR, folder)
    os.makedirs(sheet_dir, exist_ok=True)
    out_path = os.path.join(sheet_dir, "next_day_pre_crawl.csv")

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for r in rows:
            # Pad to 57 columns
            if len(r) < 57:
                r = r + [""] * (57 - len(r))
            writer.writerow(r[:57])

    logger.info(f"💾 Saved {len(rows)} pre-crawled domains to {out_path} ({os.path.getsize(out_path) / 1024:.1f} KB)")
    return out_path

async def run_precrawl_for_sheet(target: Dict[str, Any], budget: int = 15000):
    num = target["num"]
    sheet_id = target["id"]
    folder = target["folder"]
    name = target["name"]

    logger.info(f"\n=======================================================")
    logger.info(f"🚀 Advance Pre-Crawl: {name} (Budget: {budget})")
    logger.info(f"=======================================================")

    t0 = time.time()
    # 1. Fetch domains
    logger.info(f"1. Fetching {budget} domains from Tracxn API...")
    rows = await asyncio.to_thread(fetch_domains_for_sheet, sheet_id, budget)

    if not rows:
        logger.warning(f"No domains fetched for {name}!")
        return 0

    # 2. Concurrently generate Job IDs
    logger.info(f"2. Concurrently generating Tech Crawler Job IDs for {len(rows)} domains...")
    rows_with_jobs = await generate_job_ids_for_rows(rows, concurrency=25)

    # 3. Save to local CSV
    save_precrawl_to_csv(folder, rows_with_jobs)

    elapsed = time.time() - t0
    logger.info(f"🎉 Pre-Crawl for {name} finished in {elapsed/60:.2f} mins ({len(rows_with_jobs)} domains saved).\n")
    return len(rows_with_jobs)

async def run_full_advance_precrawl(budget_per_sheet: int = 15000):
    logger.info("==================================================================")
    logger.info(f"🌟 Starting Fleet Advance Pre-Crawl for Tomorrow ({budget_per_sheet * len(TARGET_SHEETS)} total domains)")
    logger.info("==================================================================")

    # First clean old storage (> 7 days)
    clean_old_storage(BASE_DIR, retention_days=7)

    total_generated = 0
    for target in TARGET_SHEETS:
        cnt = await run_precrawl_for_sheet(target, budget=budget_per_sheet)
        total_generated += cnt

    logger.info(f"\n🏆 ALL SHEETS ADVANCE PRE-CRAWL COMPLETE! Total: {total_generated} domains prepared with Job IDs.")
    return total_generated

def get_precrawl_csv_path(folder: str) -> str:
    return os.path.join(BASE_DIR, folder, "next_day_pre_crawl.csv")

def load_precrawl_to_sheet(sheet_id: str, folder: str, chunk_size: int = 500) -> int:
    csv_path = get_precrawl_csv_path(folder)
    if not os.path.exists(csv_path):
        logger.warning(f"No advance pre-crawl CSV found at {csv_path}")
        return 0

    logger.info(f"Loading advance pre-crawled domains from {csv_path} to Google Sheet {sheet_id}...")
    rows_to_load = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for r in reader:
            if r and r[0].strip():
                if len(r) < 57:
                    r = r + [""] * (57 - len(r))
                rows_to_load.append(r[:57])

    if not rows_to_load:
        logger.warning(f"Pre-crawl file {csv_path} is empty!")
        return 0

    total_rows = len(rows_to_load)
    gc = get_gspread_client()
    sh = retry_call(lambda: gc.open_by_key(sheet_id))
    try:
        ws = retry_call(lambda: sh.worksheet("CF_IF_Engine_Database"))
    except Exception:
        ws = retry_call(lambda: sh.get_worksheet(0))

    # 1. Clear old data from Row 3
    last_row = ws.row_count
    if last_row > 2:
        logger.info(f"Clearing old rows from Row 3 to Row {last_row} in CF_IF_Engine_Database...")
        retry_call(lambda: ws.batch_clear([f"A3:BE{last_row}"]))

    # 2. Ensure sheet has enough rows
    needed_total_rows = 2 + total_rows
    if ws.row_count < needed_total_rows:
        add_cnt = needed_total_rows - ws.row_count + 100
        logger.info(f"Expanding worksheet by {add_cnt} rows...")
        retry_call(lambda: ws.add_rows(add_cnt))

    # 3. Write in chunks
    logger.info(f"Writing {total_rows} rows in chunks of {chunk_size}...")
    for i in range(0, total_rows, chunk_size):
        chunk = rows_to_load[i : i + chunk_size]
        start_r = 3 + i
        end_r = 2 + i + len(chunk)
        range_name = f"A{start_r}:BE{end_r}"
        retry_call(lambda: ws.update(range_name=range_name, values=chunk, value_input_option="USER_ENTERED"))
        logger.info(f"  Written chunk {i + len(chunk)}/{total_rows} ({range_name})")
        time.sleep(1.0)

    # 4. Archive pre-crawl CSV
    cache_dir = os.path.join(BASE_DIR, folder, "Logs", "Cache")
    os.makedirs(cache_dir, exist_ok=True)
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    archive_path = os.path.join(cache_dir, f"pre_crawl_loaded_{date_str}.csv")
    try:
        os.rename(csv_path, archive_path)
        logger.info(f"Archived loaded pre-crawl CSV to {archive_path}")
    except Exception as e:
        logger.warning(f"Could not archive {csv_path}: {e}")

    logger.info(f"✅ Successfully loaded {total_rows} pre-crawled domains to sheet {sheet_id}!")
    return total_rows

if __name__ == "__main__":
    budget = 15000
    sheet_num = None
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        budget = int(sys.argv[1])
    if len(sys.argv) > 2 and sys.argv[2].isdigit():
        sheet_num = int(sys.argv[2])

    if sheet_num:
        target = [s for s in TARGET_SHEETS if s["num"] == sheet_num][0]
        clean_old_storage(BASE_DIR, retention_days=7)
        asyncio.run(run_precrawl_for_sheet(target, budget=budget))
    else:
        asyncio.run(run_full_advance_precrawl(budget_per_sheet=budget))

