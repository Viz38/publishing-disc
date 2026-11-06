#!/usr/bin/env python3
"""
common/domain_filter_validator.py

Centralized validation and leak detection module for candidate domains.
Guarantees that no domain enters or remains in the discovery pipeline if it violates
any of the 3 fundamental funnel ingestion rules:
1. sameAs == yes (aliased or redirect domain)
2. status == "PUBLISHED" (already in Tracxn platform)
3. feedNames / businessModelList / sectorList has an assigned publishing feed
"""

import logging
from typing import Dict, Any, Tuple, List, Optional
import requests
import aiohttp
import asyncio

logger = logging.getLogger("DomainFilterValidator")

TRACXN_DOMAINS_API_URL = "https://platform.tracxn.com/api/2.2/domains"

def is_domain_leaked(item: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Evaluates a single Tracxn company/domain item dictionary for the 3 leaked criteria.
    Returns:
        (True, reason_str) if leaked/invalid.
        (False, "") if clean and eligible.
    """
    if not isinstance(item, dict):
        return False, ""

    # 1. Criteria 1: sameAs == yes (aliased or redirect domain)
    s_as = item.get("sameAs")
    if s_as is not None:
        if isinstance(s_as, (dict, list)):
            if len(s_as) > 0:
                return True, "sameAs"
        elif isinstance(s_as, bool):
            if s_as is True:
                return True, "sameAs"
        elif isinstance(s_as, str):
            clean_val = s_as.strip().lower()
            if clean_val and clean_val not in ("no", "false", "null", "none", "t_null", ""):
                return True, "sameAs"

    # 2. Criteria 2: status == "PUBLISHED"
    st = str(item.get("status") or "").strip().upper()
    if st == "PUBLISHED":
        return True, "status_published"

    gst = str(item.get("globalPublishedStatus") or "").strip().upper()
    if gst == "PUBLISHED":
        return True, "status_published"

    cst = str(item.get("companyPublishStatus") or "").strip().upper()
    if cst == "PUBLISHED":
        return True, "status_published"

    # 3. Criteria 3: feedNames is not empty or blank / feeds assigned
    # Check feedNames list
    fn = item.get("feedNames")
    if fn and isinstance(fn, list) and len(fn) > 0:
        if any(str(x).strip() for x in fn if x):
            return True, "feed_tagged"

    # Check businessModelList
    bm_list = item.get("businessModelList")
    if bm_list and isinstance(bm_list, list):
        for bm in bm_list:
            if isinstance(bm, dict):
                # Has a designated feed or published feed
                if bm.get("feedName") or bm.get("feedId"):
                    return True, "feed_tagged"

    # Check sectorList
    sector_list = item.get("sectorList")
    if sector_list and isinstance(sector_list, list):
        for s_item in sector_list:
            if isinstance(s_item, list):
                for sub in s_item:
                    if isinstance(sub, dict) and (sub.get("type") == "FEED" or sub.get("feedName")):
                        return True, "feed_tagged"
            elif isinstance(s_item, dict):
                if s_item.get("type") == "FEED" or s_item.get("feedName"):
                    return True, "feed_tagged"

    return False, ""


def batch_validate_domains(
    domains: List[str],
    token: Optional[str] = None,
    chunk_size: int = 100
) -> Dict[str, Tuple[bool, str]]:
    """
    Synchronously batch-validates a list of domain names against Tracxn API.
    Returns mapping: domain -> (is_leaked, reason).
    """
    if not domains:
        return {}

    clean_domains = [d.strip().lower() for d in domains if d and d.strip()]
    results: Dict[str, Tuple[bool, str]] = {d: (False, "") for d in clean_domains}

    if not token:
        try:
            from common.pipeline_engine import Tracxn_API_Token
            token = Tracxn_API_Token
        except Exception:
            token = ""

    headers = {
        "Content-Type": "application/json",
        "accessToken": token,
        "X-Request-Source": "Discovery Team",
        "cache-control": "no-cache"
    }

    for i in range(0, len(clean_domains), chunk_size):
        chunk = clean_domains[i:i + chunk_size]
        payload = {
            "dataset": "internal",
            "filter": {"domain": chunk},
            "size": len(chunk)
        }
        try:
            resp = requests.post(TRACXN_DOMAINS_API_URL, json=payload, headers=headers, timeout=25)
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("result", []):
                    dom = (item.get("domain") or item.get("name") or "").strip().lower()
                    if dom in results:
                        results[dom] = is_domain_leaked(item)
            else:
                logger.warning(f"Batch validation status {resp.status_code}: {resp.text[:100]}")
        except Exception as e:
            logger.error(f"Batch validation request exception: {e}")

    return results


async def batch_validate_domains_async(
    domains: List[str],
    token: Optional[str] = None,
    session: Optional[aiohttp.ClientSession] = None,
    chunk_size: int = 100
) -> Dict[str, Tuple[bool, str]]:
    """
    Asynchronously batch-validates a list of domain names against Tracxn API.
    Returns mapping: domain -> (is_leaked, reason).
    """
    if not domains:
        return {}

    clean_domains = [d.strip().lower() for d in domains if d and d.strip()]
    results: Dict[str, Tuple[bool, str]] = {d: (False, "") for d in clean_domains}

    if not token:
        try:
            from common.pipeline_engine import Tracxn_API_Token
            token = Tracxn_API_Token
        except Exception:
            token = ""

    headers = {
        "Content-Type": "application/json",
        "accessToken": token,
        "X-Request-Source": "Discovery Team",
        "cache-control": "no-cache"
    }

    async def _check_chunk(sess: aiohttp.ClientSession, chunk: List[str]):
        payload = {
            "dataset": "internal",
            "filter": {"domain": chunk},
            "size": len(chunk)
        }
        try:
            async with sess.post(TRACXN_DOMAINS_API_URL, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=25)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data.get("result", []):
                        dom = (item.get("domain") or item.get("name") or "").strip().lower()
                        if dom in results:
                            results[dom] = is_domain_leaked(item)
                else:
                    text = await resp.text()
                    logger.warning(f"Async batch validation status {resp.status}: {text[:100]}")
        except Exception as e:
            logger.error(f"Async batch validation request error: {e}")

    chunks = [clean_domains[i:i + chunk_size] for i in range(0, len(clean_domains), chunk_size)]
    if session is not None:
        tasks = [_check_chunk(session, c) for c in chunks]
        await asyncio.gather(*tasks)
    else:
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as new_session:
            tasks = [_check_chunk(new_session, c) for c in chunks]
            await asyncio.gather(*tasks)

    return results

