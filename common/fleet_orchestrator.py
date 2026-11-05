#!/usr/bin/env python3
"""
Discovery Engine - Fleet Pre-Crawl Orchestrator & Peer Coordinator
Coordinates cross-sheet domain fetching in a rapid sequential relay (Sheets 1 -> 2 -> 3 -> 4),
decoupling fast domain reservation (~2 mins total) from parallel background Tech Crawler Job ID generation.
"""

import os
import sys
import json
import time
import socket
import logging
import asyncio
from typing import Dict, List, Set, Optional, Any
import httpx

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from common.advance_precrawl import (
    TARGET_SHEETS,
    initialize_fleet_exclusion_pool,
    fetch_domains_for_sheet,
    generate_job_ids_for_rows,
    save_precrawl_to_csv
)

logger = logging.getLogger("FleetOrchestrator")

FLEET_NODES = {
    1: {"name": "Sheet 1 (CF-1)", "port": 8769, "host": "5080-sheet1.rajath.world", "folder": "Sheet1", "id": "1IWXqnR52IzpFewE0QS0tlJFT65nJE_fmIoJs8ps9nZo"},
    2: {"name": "Sheet 2 (CF-2)", "port": 8770, "host": "5080-sheet2.rajath.world", "folder": "Sheet2", "id": "1xH7HPdSYtj7J1AjycQ6pvwz5VJKFsNHngnWxtyKZNFQ"},
    3: {"name": "Sheet 3 (CF-3)", "port": 8771, "host": "5321-sheet3.rajath.world", "folder": "Sheet3", "id": "1MHdkFCsoAY0V7ix_pJ5SpWFq3EFpJEug2ntJk1FkEl8"},
    4: {"name": "Sheet 4 (Filter-V2)", "port": 8772, "host": "5321-sheet4.rajath.world", "folder": "Sheet4", "id": "1RYkvqgxMJpjMrD3x9D4VE8Zu4UJlf36gN-bfJ7T-bcA"},
}

AUTH_HEADERS = {
    "Authorization": "Bearer REDACTED_SECRET",
    "Content-Type": "application/json"
}

def is_node_local(sheet_num: int) -> bool:
    hostname = socket.gethostname().lower()
    return ("508" in hostname and sheet_num in (1, 2)) or ("532" in hostname and sheet_num in (3, 4))

def get_node_url(sheet_num: int) -> str:
    node = FLEET_NODES.get(sheet_num)
    if not node:
        raise ValueError(f"Unknown sheet num: {sheet_num}")
    if is_node_local(sheet_num):
        return f"http://127.0.0.1:{node['port']}"
    return f"https://{node['host']}"

async def get_peer_staged_domains(peer_sheet_nums: Optional[List[int]] = None, check_local_disk: bool = True) -> Set[str]:
    """
    Queries peer sheet supervisor endpoints to get all domains currently claimed or staged.
    Also checks local filesystem files if check_local_disk is True.
    """
    targets = peer_sheet_nums or [1, 2, 3, 4]
    combined_domains = set()

    # 1. Local filesystem check (only for sheets local to this machine)
    if check_local_disk:
        for num in targets:
            if is_node_local(num):
                folder = FLEET_NODES.get(num, {}).get("folder")
                if folder:
                    staged_json = os.path.join(BASE_DIR, folder, "staged_domains.json")
                    if os.path.exists(staged_json):
                        try:
                            with open(staged_json, "r", encoding="utf-8") as f:
                                data = json.load(f)
                                if isinstance(data, list):
                                    for d in data:
                                        if d and isinstance(d, str):
                                            combined_domains.add(d.strip().lower())
                        except Exception as e:
                            logger.debug(f"Could not read local {staged_json}: {e}")

                    precrawl_csv = os.path.join(BASE_DIR, folder, "next_day_pre_crawl.csv")
                    if os.path.exists(precrawl_csv):
                        try:
                            import csv
                            with open(precrawl_csv, "r", encoding="utf-8") as cf:
                                for row in csv.reader(cf):
                                    if row and row[0].strip():
                                        combined_domains.add(row[0].strip().lower())
                        except Exception as e:
                            logger.debug(f"Could not read local {precrawl_csv}: {e}")

    # 2. REST API queries
    async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
        for num in targets:
            url = f"{get_node_url(num)}/fleet/staged-domains"
            try:
                resp = await client.get(url, headers=AUTH_HEADERS)
                if resp.status_code == 200:
                    payload = resp.json()
                    doms = payload.get("domains", [])
                    for d in doms:
                        if d and isinstance(d, str):
                            combined_domains.add(d.strip().lower())
                    logger.info(f"Loaded {len(doms)} staged domains from peer Sheet {num} ({url})")
            except Exception as e:
                logger.debug(f"Peer Sheet {num} staged-domains query returned: {e}")

    return combined_domains

def save_staged_domains_json(folder: str, domains: List[str]):
    folder_path = os.path.join(BASE_DIR, folder)
    os.makedirs(folder_path, exist_ok=True)
    out_path = os.path.join(folder_path, "staged_domains.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(domains, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save {out_path}: {e}")

async def fetch_and_stage_sheet(sheet_num: int, exclusions: Set[str]) -> List[List[str]]:
    """
    Fetches domains from Tracxn API for sheet_num, respecting exclusions.
    Saves domains locally and returns the raw 57-col rows.
    """
    node = FLEET_NODES[sheet_num]
    folder = node["folder"]
    sheet_id = node["id"]
    name = node["name"]

    logger.info(f"▶ [Fetch Relay] Starting domain fetch for {name} (Exclusions: {len(exclusions)})...")
    t0 = time.time()
    rows = await asyncio.to_thread(fetch_domains_for_sheet, sheet_id, None, None, exclusions)
    elapsed = time.time() - t0

    domains = [r[0].strip().lower() for r in rows if r and r[0]]
    save_staged_domains_json(folder, domains)
    # Also save raw pre-crawl CSV so domains are immediately persistent on disk
    save_precrawl_to_csv(folder, rows)

    logger.info(f"✅ [Fetch Relay] {name} claimed {len(rows)} domains in {elapsed:.1f}s.")
    return rows

async def dispatch_job_generation_task(sheet_num: int, rows: Optional[List[List[str]]] = None) -> bool:
    """
    Dispatches background Tech Crawler Job ID generation for the given sheet.
    Does NOT block subsequent sheet domain fetching.
    """
    node = FLEET_NODES[sheet_num]
    folder = node["folder"]
    name = node["name"]

    logger.info(f"🚀 [Job ID Generation] Launching background Tech Crawler Job generation for {name}...")

    async def _run_tc_job():
        try:
            target_rows = rows
            if not target_rows:
                # Load from next_day_pre_crawl.csv
                import csv
                csv_path = os.path.join(BASE_DIR, folder, "next_day_pre_crawl.csv")
                if os.path.exists(csv_path):
                    with open(csv_path, "r", encoding="utf-8") as cf:
                        target_rows = list(csv.reader(cf))
            if not target_rows:
                logger.warning(f"No rows found to generate Job IDs for {name}!")
                return

            logger.info(f"Starting Tech Crawler Job ID generation for {len(target_rows)} rows on {name}...")
            rows_with_jobs = await generate_job_ids_for_rows(target_rows, concurrency=25, save_folder=folder)
            save_precrawl_to_csv(folder, rows_with_jobs)
            logger.info(f"🏆 [Job ID Generation] Complete for {name}: {len(rows_with_jobs)} domains saved with Job IDs.")
        except Exception as e:
            logger.error(f"Error in background TC job generation for {name}: {e}")

    # Launch as completely detached background task
    asyncio.create_task(_run_tc_job())
    return True

async def orchestrate_fleet_precrawl_relay() -> Dict[str, Any]:
    """
    Master fleet orchestrator:
    1. Initializes fleet exclusion pool (daily processed registry + live Google Sheets).
    2. Sequentially executes domain fetch relay (Sheet 1 -> 2 -> 3 -> 4) across nodes via REST API,
       immediately passing claimed domains to subsequent sheets.
    3. As soon as any sheet completes domain fetch, immediately dispatches its TC Job ID generation
       in the background on that specific host without blocking the relay!
    4. Audits zero cross-sheet duplicate overlap across all 4 production sheets.
    """
    logger.info("==================================================================")
    logger.info("🌟 Starting Pipelined Fleet Advance Pre-Crawl Relay")
    logger.info("==================================================================")
    t_start = time.time()

    exclusions = initialize_fleet_exclusion_pool()
    initial_exclusions_count = len(exclusions)

    sheet_domains: Dict[int, List[str]] = {}
    report = {
        "status": "success",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "initial_exclusions": initial_exclusions_count,
        "sheets": {},
        "duplicates_across_sheets": 0
    }

    async with httpx.AsyncClient(timeout=900.0, verify=False) as client:
        for sheet_num in [1, 2, 3, 4]:
            node = FLEET_NODES[sheet_num]
            s_name = node["name"]
            node_url = get_node_url(sheet_num)

            # 1. Trigger domain fetch on that node
            logger.info(f"▶ [Relay Step] Triggering domain fetch on {s_name} ({node_url})...")
            fetch_url = f"{node_url}/fleet/fetch-and-stage"
            t0 = time.time()
            staged_count = 0
            api_success = False
            payload = {"exclusions": list(exclusions)}
            try:
                fetch_resp = await client.post(fetch_url, json=payload, headers=AUTH_HEADERS)
                if fetch_resp.status_code == 200:
                    fetch_data = fetch_resp.json()
                    staged_count = fetch_data.get("domains_staged", 0)
                    api_success = True
                    logger.info(f"✅ {s_name} successfully staged {staged_count} domains via API in {time.time() - t0:.1f}s.")
                else:
                    logger.warning(f"⚠️ {s_name} API returned status {fetch_resp.status_code}. Falling back to direct fetch.")
            except Exception as e:
                logger.warning(f"⚠️ Could not reach {s_name} via {fetch_url} ({e}). Falling back to direct fetch.")

            if not api_success:
                # Direct in-process fetch fallback (used in unit tests or offline recovery)
                rows = await fetch_and_stage_sheet(sheet_num, exclusions)
                staged_count = len(rows)

            # 2. Immediately dispatch background TC job generation on that node
            gen_url = f"{node_url}/fleet/generate-job-ids"
            gen_dispatched = False
            try:
                gen_resp = await client.post(gen_url, headers=AUTH_HEADERS)
                if gen_resp.status_code == 200:
                    gen_dispatched = True
                    logger.info(f"🚀 Dispatched background TC Job ID generation via API on {s_name}.")
            except Exception as e:
                logger.warning(f"Could not dispatch Job ID generation via API on {s_name} ({e})")

            if not gen_dispatched:
                await dispatch_job_generation_task(sheet_num, rows if not api_success else None)

            # 3. Retrieve staged domains to update cumulative exclusions and audit deduplication
            domains_url = f"{node_url}/fleet/staged-domains"
            doms = []
            if api_success:
                try:
                    dom_resp = await client.get(domains_url, headers=AUTH_HEADERS)
                    if dom_resp.status_code == 200:
                        doms = dom_resp.json().get("domains", [])
                except Exception as e:
                    logger.debug(f"Could not get staged domains from {s_name} via API: {e}")

            if not doms and not api_success and rows:
                doms = [r[0].strip().lower() for r in rows if r and r[0]]
            elif not doms:
                # Fallback to local disk check
                doms_set = await get_peer_staged_domains([sheet_num], check_local_disk=True)
                doms = list(doms_set)

            clean_doms = [d.strip().lower() for d in doms if d]
            sheet_domains[sheet_num] = clean_doms
            exclusions.update(clean_doms)

            report["sheets"][sheet_num] = {
                "name": s_name,
                "domains_claimed": len(clean_doms),
                "unique_domains": len(set(clean_doms))
            }

    # Verify cross-sheet duplicates
    total_dups = 0
    sheet_nums = list(sheet_domains.keys())
    for i in range(len(sheet_nums)):
        for j in range(i + 1, len(sheet_nums)):
            s1 = sheet_nums[i]
            s2 = sheet_nums[j]
            overlap = set(sheet_domains[s1]).intersection(set(sheet_domains[s2]))
            if overlap:
                logger.error(f"❌ Overlap between Sheet {s1} and Sheet {s2}: {len(overlap)} duplicate domains!")
                total_dups += len(overlap)

    report["duplicates_across_sheets"] = total_dups
    report["total_time_seconds"] = round(time.time() - t_start, 2)
    logger.info(f"🏆 Pipelined Domain Fetch Relay Finished in {report['total_time_seconds']}s. Duplicate Overlap: {total_dups}.")

    return report

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    asyncio.run(orchestrate_fleet_precrawl_relay())
