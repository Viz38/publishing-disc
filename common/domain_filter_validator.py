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
import time
from typing import Dict, Any, Tuple, List, Optional, Set
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


def is_domain_published_or_sameas(item: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Evaluates a single Tracxn company/domain item dictionary for publish and sameAs status.
    Returns:
        (True, "published") if status, companyPublishStatus, or globalPublishedStatus is PUBLISHED.
        (True, "sameAs") if sameAs is present and non-empty.
        (False, "") if clean/unpublished and not sameAs.
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
        return True, "published"

    gst = str(item.get("globalPublishedStatus") or "").strip().upper()
    if gst == "PUBLISHED":
        return True, "published"

    cst = str(item.get("companyPublishStatus") or "").strip().upper()
    if cst == "PUBLISHED":
        return True, "published"

    return False, ""


def _apply_validation_items(
    items: List[Dict[str, Any]],
    results: Dict[str, Tuple[bool, str]],
    returned_domains: Set[str]
) -> None:
    """Applies leak evaluation across returned profiles without clearing an already-flagged domain."""
    for item in items:
        dom = (item.get("domain") or item.get("name") or "").strip().lower()
        if dom not in results:
            continue
        returned_domains.add(dom)
        is_l, reason = is_domain_leaked(item)
        if is_l and not results[dom][0]:
            results[dom] = (True, reason)


def _get_key_manager_or_token(token: Optional[str]):
    if token:
        return None, token
    try:
        from common.key_manager import TracxnKeyManager
        return TracxnKeyManager(), None
    except Exception:
        try:
            from common.pipeline_engine import Tracxn_API_Token
            return None, Tracxn_API_Token
        except Exception:
            return None, ""


def _build_tracxn_headers(tok: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "accessToken": tok,
        "X-Request-Source": "Discovery Team",
        "cache-control": "no-cache"
    }


def batch_validate_domains(
    domains: List[str],
    token: Optional[str] = None,
    chunk_size: int = 25
) -> Dict[str, Tuple[bool, str]]:
    """
    Synchronously batch-validates a list of domain names against Tracxn API.
    Paginates multi-profile results, rotates API keys on throttling/errors, queries 'tracxn'
    first with 'internal' fallback for any missing domains, and preserves sticky leak flags.
    Returns mapping: domain -> (is_leaked, reason).
    """
    if not domains:
        return {}

    clean_domains = list(dict.fromkeys(d.strip().lower() for d in domains if d and d.strip()))
    results: Dict[str, Tuple[bool, str]] = {d: (False, "") for d in clean_domains}
    km, static_token = _get_key_manager_or_token(token)

    def _query_dataset(chunk: List[str], dataset: str) -> Set[str]:
        seen: Set[str] = set()
        offset = 0
        while True:
            payload = {"dataset": dataset, "filter": {"domain": chunk}, "from": offset, "size": 100}
            fetched_page = False
            for _ in range(5):
                tok = km.get_next_token() if km else static_token
                if not tok:
                    break
                try:
                    resp = requests.post(
                        TRACXN_DOMAINS_API_URL,
                        json=payload,
                        headers=_build_tracxn_headers(tok),
                        timeout=25
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        items = data.get("result", [])
                        total = data.get("total_count", len(items))
                        _apply_validation_items(items, results, seen)
                        fetched_page = True
                        if not items or offset + len(items) >= total:
                            return seen
                        offset += len(items)
                        break
                    if km:
                        km.handle_http_error(tok, resp.status_code)
                    logger.warning(f"Batch validation ({dataset}) status {resp.status_code}: {resp.text[:100]}")
                    time.sleep(1.5 if resp.status_code == 429 else 0.5)
                except Exception as e:
                    logger.error(f"Batch validation ({dataset}) request exception: {e}")
                    time.sleep(1.0)
            if not fetched_page:
                return seen

    for i in range(0, len(clean_domains), chunk_size):
        chunk = clean_domains[i:i + chunk_size]
        seen_tracxn = _query_dataset(chunk, "tracxn")
        missing = [d for d in chunk if d not in seen_tracxn]
        if missing:
            _query_dataset(missing, "internal")

    return results


async def batch_validate_domains_async(
    domains: List[str],
    token: Optional[str] = None,
    session: Optional[aiohttp.ClientSession] = None,
    chunk_size: int = 25
) -> Dict[str, Tuple[bool, str]]:
    """
    Asynchronously batch-validates a list of domain names against Tracxn API.
    Returns mapping: domain -> (is_leaked, reason).
    """
    if not domains:
        return {}

    clean_domains = list(dict.fromkeys(d.strip().lower() for d in domains if d and d.strip()))
    results: Dict[str, Tuple[bool, str]] = {d: (False, "") for d in clean_domains}
    km, static_token = _get_key_manager_or_token(token)

    async def _query_dataset_async(sess: aiohttp.ClientSession, chunk: List[str], dataset: str) -> Set[str]:
        seen: Set[str] = set()
        offset = 0
        while True:
            payload = {"dataset": dataset, "filter": {"domain": chunk}, "from": offset, "size": 100}
            fetched_page = False
            for _ in range(5):
                tok = km.get_next_token() if km else static_token
                if not tok:
                    break
                try:
                    async with sess.post(
                        TRACXN_DOMAINS_API_URL,
                        json=payload,
                        headers=_build_tracxn_headers(tok),
                        timeout=aiohttp.ClientTimeout(total=25)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            items = data.get("result", [])
                            total = data.get("total_count", len(items))
                            _apply_validation_items(items, results, seen)
                            fetched_page = True
                            if not items or offset + len(items) >= total:
                                return seen
                            offset += len(items)
                            break
                        if km:
                            km.handle_http_error(tok, resp.status)
                        text = await resp.text()
                        logger.warning(f"Async batch validation ({dataset}) status {resp.status}: {text[:100]}")
                        await asyncio.sleep(1.5 if resp.status == 429 else 0.5)
                except Exception as e:
                    logger.error(f"Async batch validation ({dataset}) request error: {e}")
                    await asyncio.sleep(1.0)
            if not fetched_page:
                return seen

    async def _check_chunk(sess: aiohttp.ClientSession, chunk: List[str]):
        seen_tracxn = await _query_dataset_async(sess, chunk, "tracxn")
        missing = [d for d in chunk if d not in seen_tracxn]
        if missing:
            await _query_dataset_async(sess, missing, "internal")

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


def filter_unpublished_non_sameas_domains(
    domains: List[str],
    token: Optional[str] = None,
    chunk_size: int = 100
) -> Tuple[Set[str], Dict[str, str]]:
    """
    Queries live Tracxn API (dataset 'tracxn', fallback 'internal') to verify
    exact publish and sameAs status for candidate domains.
    Returns:
        (accepted_domains_set, rejected_domains_dict {domain: reason})
    """
    if not domains:
        return set(), {}

    clean_domains = list(dict.fromkeys(d.strip().lower() for d in domains if d and d.strip()))
    rejected: Dict[str, str] = {}
    km, static_token = _get_key_manager_or_token(token)

    def _query_dataset(chunk: List[str], dataset: str) -> Set[str]:
        seen: Set[str] = set()
        offset = 0
        while True:
            payload = {"dataset": dataset, "filter": {"domain": chunk}, "from": offset, "size": 100}
            fetched_page = False
            for _ in range(5):
                tok = km.get_next_token() if km else static_token
                if not tok:
                    break
                try:
                    resp = requests.post(
                        TRACXN_DOMAINS_API_URL,
                        json=payload,
                        headers=_build_tracxn_headers(tok),
                        timeout=25
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        items = data.get("result", [])
                        total = data.get("total_count", len(items))
                        for it in items:
                            dom = (it.get("domain") or it.get("name") or "").strip().lower()
                            if not dom:
                                continue
                            seen.add(dom)
                            is_bad, reason = is_domain_published_or_sameas(it)
                            if is_bad and dom not in rejected:
                                rejected[dom] = reason
                        fetched_page = True
                        if not items or offset + len(items) >= total:
                            return seen
                        offset += len(items)
                        break
                    if km:
                        km.handle_http_error(tok, resp.status_code)
                    logger.warning(f"Filter live API ({dataset}) status {resp.status_code}: {resp.text[:100]}")
                    time.sleep(1.5 if resp.status_code == 429 else 0.5)
                except Exception as e:
                    logger.error(f"Filter live API ({dataset}) request exception: {e}")
                    time.sleep(1.0)
            if not fetched_page:
                return seen

    for i in range(0, len(clean_domains), chunk_size):
        chunk = clean_domains[i:i + chunk_size]
        seen_tracxn = _query_dataset(chunk, "tracxn")
        missing = [d for d in chunk if d not in seen_tracxn]
        if missing:
            _query_dataset(missing, "internal")

    accepted = set(clean_domains) - set(rejected.keys())
    return accepted, rejected


async def filter_unpublished_non_sameas_domains_async(
    domains: List[str],
    token: Optional[str] = None,
    session: Optional[aiohttp.ClientSession] = None,
    chunk_size: int = 100
) -> Tuple[Set[str], Dict[str, str]]:
    """
    Asynchronously queries live Tracxn API to verify exact publish and sameAs status.
    Returns:
        (accepted_domains_set, rejected_domains_dict {domain: reason})
    """
    if not domains:
        return set(), {}

    clean_domains = list(dict.fromkeys(d.strip().lower() for d in domains if d and d.strip()))
    rejected: Dict[str, str] = {}
    lock = asyncio.Lock()
    km, static_token = _get_key_manager_or_token(token)

    async def _query_dataset_async(sess: aiohttp.ClientSession, chunk: List[str], dataset: str) -> Set[str]:
        seen: Set[str] = set()
        offset = 0
        while True:
            payload = {"dataset": dataset, "filter": {"domain": chunk}, "from": offset, "size": 100}
            fetched_page = False
            for _ in range(5):
                tok = km.get_next_token() if km else static_token
                if not tok:
                    break
                try:
                    async with sess.post(
                        TRACXN_DOMAINS_API_URL,
                        json=payload,
                        headers=_build_tracxn_headers(tok),
                        timeout=aiohttp.ClientTimeout(total=25)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            items = data.get("result", [])
                            total = data.get("total_count", len(items))
                            async with lock:
                                for it in items:
                                    dom = (it.get("domain") or it.get("name") or "").strip().lower()
                                    if not dom:
                                        continue
                                    seen.add(dom)
                                    is_bad, reason = is_domain_published_or_sameas(it)
                                    if is_bad and dom not in rejected:
                                        rejected[dom] = reason
                            fetched_page = True
                            if not items or offset + len(items) >= total:
                                return seen
                            offset += len(items)
                            break
                        if km:
                            km.handle_http_error(tok, resp.status)
                        text = await resp.text()
                        logger.warning(f"Async live filter ({dataset}) status {resp.status}: {text[:100]}")
                        await asyncio.sleep(1.5 if resp.status == 429 else 0.5)
                except Exception as e:
                    logger.error(f"Async live filter ({dataset}) error: {e}")
                    await asyncio.sleep(1.0)
            if not fetched_page:
                return seen

    async def _check_chunk(sess: aiohttp.ClientSession, chunk: List[str]):
        seen_tracxn = await _query_dataset_async(sess, chunk, "tracxn")
        missing = [d for d in chunk if d not in seen_tracxn]
        if missing:
            await _query_dataset_async(sess, missing, "internal")

    chunks = [clean_domains[i:i + chunk_size] for i in range(0, len(clean_domains), chunk_size)]
    if session is not None:
        tasks = [_check_chunk(session, c) for c in chunks]
        await asyncio.gather(*tasks)
    else:
        connector = aiohttp.TCPConnector(limit=10)
        async with aiohttp.ClientSession(connector=connector) as new_session:
            tasks = [_check_chunk(new_session, c) for c in chunks]
            await asyncio.gather(*tasks)

    accepted = set(clean_domains) - set(rejected.keys())
    return accepted, rejected


