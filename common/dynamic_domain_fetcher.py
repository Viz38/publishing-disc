#!/usr/bin/env python3
"""
Dynamic Domain Fetcher for Tracxn Ingestion Pipelines.

Features:
1. Universal Payload Compatibility: Works with any payload (with or without funnelId,
   createdDate, sort, or custom filters).
2. Stagnation Detection: Detects high-duplicate saturation (>98% duplicate rate over a window)
   and automatically pivots to fresh temporal slices.
3. Elasticsearch 100k Limit Recovery: Recovers from HTTP 422 (from + size > 100000) by
   partitioning large queries into bounded date windows starting at offset 0.
4. Recursive Window Bounding: Windows exceeding max_window_coverage (default 85k) are
   recursively split into half-year / quarter / month sub-slices, prioritized newest first.
5. Token Rotation & Dataset Fallback: Transparently rotates Tracxn API keys and handles
   403 unauthorized dataset fallback to 'search'.
"""

import copy
import datetime
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import requests

logger = logging.getLogger("DynamicDomainFetcher")
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [DynamicDomainFetcher] %(message)s"))
    logger.addHandler(ch)
logger.setLevel(logging.INFO)

TRACXN_API_URL = "https://platform.tracxn.com/api/2.2/domains"


def split_temporal_window(
    start: datetime.datetime, end: datetime.datetime
) -> Tuple[Tuple[datetime.datetime, datetime.datetime], Tuple[datetime.datetime, datetime.datetime]]:
    """
    Splits a temporal window into two non-overlapping halves: (older, newer).
    Guarantees older[1] < newer[0], older[0] == start, newer[1] == end.
    """
    total_seconds = (end - start).total_seconds()
    mid_seconds = int(total_seconds / 2)
    mid_end = start + datetime.timedelta(seconds=mid_seconds)
    mid_start = mid_end + datetime.timedelta(seconds=1)
    if mid_start > end:
        mid_start = end
    older = (start, mid_end)
    newer = (mid_start, end)
    return older, newer


def _parse_iso_dt(val: Any) -> Optional[datetime.datetime]:
    if not val:
        return None
    if isinstance(val, datetime.datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=datetime.timezone.utc)
        return val
    s = str(val).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt
    except Exception:
        pass
    # Try common formats
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.datetime.strptime(s, fmt)
            return dt.replace(tzinfo=datetime.timezone.utc)
        except Exception:
            continue
    return None


def clamp_date_bounds(
    start: datetime.datetime,
    end: datetime.datetime,
    bounds: Optional[Dict[str, Any]],
) -> Tuple[datetime.datetime, datetime.datetime]:
    """Clamps a start/end date range to within the bounds dict {"min": ..., "max": ...} if present."""
    if not bounds or not isinstance(bounds, dict):
        return start, end
    b_min = _parse_iso_dt(bounds.get("min"))
    b_max = _parse_iso_dt(bounds.get("max"))
    if b_min and b_min > start:
        start = b_min
    if b_max and b_max < end:
        end = b_max
    return start, end


def build_initial_temporal_windows(
    now_dt: Optional[datetime.datetime] = None,
) -> List[Tuple[datetime.datetime, datetime.datetime]]:
    """
    Builds a list of non-overlapping historical date windows, ordered newest first.
    """
    if now_dt is None:
        now_dt = datetime.datetime.now(datetime.timezone.utc)
    elif now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=datetime.timezone.utc)

    curr_year = now_dt.year
    windows: List[Tuple[datetime.datetime, datetime.datetime]] = []

    # Window 0: Current year to now
    y_start = datetime.datetime(curr_year, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    windows.append((y_start, now_dt))

    # Past individual years down to 2021
    for y in range(curr_year - 1, 2020, -1):
        s = datetime.datetime(y, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        e = datetime.datetime(y, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
        windows.append((s, e))

    # 2018-2020
    windows.append((
        datetime.datetime(2018, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2020, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc),
    ))

    # 2014-2017
    windows.append((
        datetime.datetime(2014, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2017, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc),
    ))

    # Pre-2014
    windows.append((
        datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        datetime.datetime(2013, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc),
    ))

    return windows


class FetchResult:
    def __init__(
        self,
        accepted_count: int = 0,
        pivots_count: int = 0,
        total_coverage: int = 0,
        items_evaluated: int = 0,
    ):
        self.accepted_count = accepted_count
        self.pivots_count = pivots_count
        self.total_coverage = total_coverage
        self.items_evaluated = items_evaluated

    def __repr__(self) -> str:
        return (
            f"<FetchResult accepted={self.accepted_count} pivots={self.pivots_count} "
            f"coverage={self.total_coverage} evaluated={self.items_evaluated}>"
        )


def sanitize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitizes Tracxn payload by removing UI-specific null placeholders that break
    the /api/2.2/domains Elasticsearch index (e.g. status: ['t_null']).
    Defaults to sorting by createdDate DESC if sort is relevance or missing.
    """
    p = copy.deepcopy(payload)
    filt = p.get("filter")
    if isinstance(filt, dict):
        if filt.get("status") in (["t_null"], "t_null"):
            del filt["status"]
        if filt.get("sameAs") in (["t_null"], "t_null"):
            del filt["sameAs"]
        if filt.get("isInvestor") in (["t_null"], "t_null"):
            del filt["isInvestor"]
    
    sort_list = p.get("sort")
    if not sort_list or (
        isinstance(sort_list, list)
        and len(sort_list) > 0
        and sort_list[0].get("sortField") == "relevance"
    ):
        p["sort"] = [{"order": "DESC", "sortField": "createdDate"}]
    return p


class DynamicDomainFetcher:
    """
    Self-healing, adaptive Tracxn API domain fetcher.
    Pivots smoothly across date partitions when encountering duplicate stagnation
    or Elasticsearch 100k pagination limits.
    """

    def __init__(
        self,
        key_manager: Any,
        post_fn: Optional[Callable[..., Any]] = None,
        stagnation_item_threshold: int = 2500,
        max_window_coverage: int = 85000,
        base_url: str = TRACXN_API_URL,
    ):
        self.key_manager = key_manager
        self.post_fn = post_fn or requests.post
        self.stagnation_item_threshold = stagnation_item_threshold
        self.max_window_coverage = max_window_coverage
        self.base_url = base_url

    def _execute_request(
        self,
        payload: Dict[str, Any],
        max_retries: int = 3,
    ) -> Tuple[int, Dict[str, Any]]:
        """Executes a single API POST request with token rotation and error handling."""
        for attempt in range(1, max_retries + 1):
            token = self.key_manager.get_next_token()
            if not token:
                logger.error("No active Tracxn token available!")
                return 500, {}

            headers = {
                "Content-Type": "application/json",
                "accessToken": token,
                "X-Request-Source": "vishnu.bhagirathan@tracxn.com | Python | DynamicDomainFetcher | Discovery-Engine",
            }

            try:
                resp = self.post_fn(self.base_url, json=payload, headers=headers, timeout=30)
                code = getattr(resp, "status_code", 200)

                if code == 200:
                    try:
                        return 200, resp.json()
                    except Exception:
                        return 200, {}

                if code == 422:
                    # Elasticsearch offset limit (from + size > 100000)
                    try:
                        err_body = resp.json()
                    except Exception:
                        err_body = {"raw": getattr(resp, "text", "")}
                    return 422, err_body

                if code == 403:
                    is_unauth = False
                    try:
                        err_data = resp.json()
                        if err_data.get("errorCode") == 403000000 or "unauthorized" in str(err_data.get("message", "")).lower():
                            is_unauth = True
                    except Exception:
                        if "unauthorized" in getattr(resp, "text", "").lower():
                            is_unauth = True

                    if is_unauth and payload.get("dataset") != "search":
                        logger.info(f"Dataset '{payload.get('dataset')}' unauthorized (403000000). Falling back to 'search'.")
                        payload["dataset"] = "search"
                        continue

                    self.key_manager.handle_http_error(token, code)
                    logger.warning(f"Tracxn token 403 unauthorized. Rotating token... (attempt {attempt}/{max_retries})")
                    continue

                if code in (401, 429):
                    self.key_manager.handle_http_error(token, code)
                    if code == 429:
                        time.sleep(2)
                    continue

                self.key_manager.handle_http_error(token, code)
                logger.warning(f"Tracxn request status {code} (attempt {attempt}/{max_retries})")
            except Exception as e:
                logger.error(f"Tracxn request exception (attempt {attempt}/{max_retries}): {e}")
                time.sleep(1)

        return 500, {}

    def fetch_for_budget(
        self,
        base_payload: Dict[str, Any],
        budget: int,
        accept_item_fn: Callable[[Dict[str, Any]], bool],
        source_label: str = "",
        filter_name: str = "",
    ) -> FetchResult:
        """
        Fetches up to `budget` accepted items using `accept_item_fn` to test validity and uniqueness.
        If the unfiltered query hits stagnation (<1% acceptance over threshold) or HTTP 422,
        it automatically pivots into temporally windowed slices.
        """
        accepted_count = 0
        pivots_count = 0
        items_evaluated = 0
        total_root_coverage = 0

        # Phase 1: Try base query directly
        query_payload = sanitize_payload(base_payload)
        from_offset = 0
        page_size = 100
        root_stagnant = False

        while accepted_count < budget:
            page_payload = copy.deepcopy(query_payload)
            page_payload["from"] = from_offset
            page_payload["size"] = page_size

            code, data = self._execute_request(page_payload)
            if code == 422:
                logger.info(f"[{source_label}][{filter_name}] Hit Elasticsearch 100k offset limit (HTTP 422). Pivoting to temporal slicing.")
                pivots_count += 1
                root_stagnant = True
                break

            if code != 200:
                logger.warning(f"[{source_label}][{filter_name}] Base query returned status {code}. Pivoting to temporal slicing.")
                pivots_count += 1
                root_stagnant = True
                break

            total_root_coverage = data.get("total_count", 0)
            items = data.get("result", [])
            if not items:
                break

            page_accepted = 0
            for it in items:
                items_evaluated += 1
                if accept_item_fn(it):
                    accepted_count += 1
                    page_accepted += 1
                    if accepted_count >= budget:
                        break

            # Check stagnation on root query
            if (
                items_evaluated >= self.stagnation_item_threshold
                and (accepted_count / max(items_evaluated, 1)) < 0.02
                and total_root_coverage > self.max_window_coverage
            ):
                logger.info(
                    f"[{source_label}][{filter_name}] Root query is saturated with duplicates "
                    f"({accepted_count}/{items_evaluated} accepted, coverage: {total_root_coverage}). "
                    f"Pivoting to temporal windowing!"
                )
                pivots_count += 1
                root_stagnant = True
                break

            from_offset += len(items)
            if from_offset >= total_root_coverage:
                break

        if accepted_count >= budget or (not root_stagnant and from_offset >= total_root_coverage):
            return FetchResult(
                accepted_count=accepted_count,
                pivots_count=pivots_count,
                total_coverage=total_root_coverage,
                items_evaluated=items_evaluated,
            )

        # Phase 2: Dynamic Temporal Slicing & Window Stack
        existing_cdate = (
            query_payload.get("filter", {}).get("createdDate")
            if isinstance(query_payload.get("filter"), dict)
            else None
        )

        initial_windows = build_initial_temporal_windows()
        window_stack: List[Tuple[datetime.datetime, datetime.datetime]] = []
        for w_start, w_end in initial_windows:
            c_start, c_end = clamp_date_bounds(w_start, w_end, existing_cdate)
            if c_start < c_end:
                window_stack.append((c_start, c_end))

        while accepted_count < budget and window_stack:
            w_start, w_end = window_stack.pop(0)

            # Probe window size
            win_payload = copy.deepcopy(query_payload)
            if "filter" not in win_payload or not isinstance(win_payload["filter"], dict):
                win_payload["filter"] = {}

            win_payload["filter"]["createdDate"] = {
                "min": w_start.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                "max": w_end.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            }
            win_payload["sort"] = [{"order": "DESC", "sortField": "createdDate"}]
            win_payload["from"] = 0
            win_payload["size"] = page_size

            code, data = self._execute_request(win_payload)
            if code == 422:
                # If window hits 422 on probe, split and retry
                older, newer = split_temporal_window(w_start, w_end)
                window_stack.insert(0, older)
                window_stack.insert(0, newer)
                pivots_count += 1
                continue

            if code != 200:
                logger.warning(f"Window probe {w_start} - {w_end} failed with status {code}. Skipping window.")
                continue

            win_total = data.get("total_count", 0)
            if win_total == 0:
                continue

            # If window coverage exceeds max_window_coverage and span is > 1 day, recursively split
            if win_total > self.max_window_coverage and (w_end - w_start).total_seconds() > 86400:
                older, newer = split_temporal_window(w_start, w_end)
                window_stack.insert(0, older)
                window_stack.insert(0, newer)
                pivots_count += 1
                continue

            # Ingest from this window
            win_items = data.get("result", [])
            win_evaluated = 0
            win_accepted = 0

            for it in win_items:
                items_evaluated += 1
                win_evaluated += 1
                if accept_item_fn(it):
                    accepted_count += 1
                    win_accepted += 1
                    if accepted_count >= budget:
                        break

            win_offset = len(win_items)

            while accepted_count < budget and win_offset < win_total:
                # Check stagnation within this window
                if win_evaluated >= self.stagnation_item_threshold and (win_accepted / max(win_evaluated, 1)) < 0.02:
                    logger.info(
                        f"[{source_label}][{filter_name}] Window {w_start.date()} - {w_end.date()} stagnant "
                        f"({win_accepted}/{win_evaluated} accepted). Pivoting to next window."
                    )
                    pivots_count += 1
                    break

                page_win_payload = copy.deepcopy(win_payload)
                page_win_payload["from"] = win_offset
                page_win_payload["size"] = page_size

                code, data = self._execute_request(page_win_payload)
                if code == 422 or code != 200:
                    logger.info(f"Window page at offset {win_offset} returned {code}. Pivoting to next window.")
                    pivots_count += 1
                    break

                page_items = data.get("result", [])
                if not page_items:
                    break

                for it in page_items:
                    items_evaluated += 1
                    win_evaluated += 1
                    if accept_item_fn(it):
                        accepted_count += 1
                        win_accepted += 1
                        if accepted_count >= budget:
                            break

                win_offset += len(page_items)

        return FetchResult(
            accepted_count=accepted_count,
            pivots_count=pivots_count,
            total_coverage=total_root_coverage,
            items_evaluated=items_evaluated,
        )
