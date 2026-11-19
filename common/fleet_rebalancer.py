"""
Dynamic Fleet NIM Key Rebalancer & Concurrency Auto-Scaler
Autonomously detects idle/completed peer sheets across the fleet (Desktop-5080 & Desktop-5321)
and redistributes all 48 NIM keys deterministically among the active sheets,
dynamically scaling up worker concurrency without risking cross-sheet key collisions.
"""

import os
import time
import logging
import asyncio
from typing import List, Dict, Set, Optional, Callable, Any
import aiohttp

logger = logging.getLogger("FleetRebalancer")

FLEET_NODES: Dict[int, Dict[str, Any]] = {
    1: {"host": "Desktop-5080", "port": 8769, "public_url": "https://5080-sheet1.rajath.world"},
    2: {"host": "Desktop-5080", "port": 8770, "public_url": "https://5080-sheet2.rajath.world"},
    3: {"host": "Desktop-5321", "port": 8771, "public_url": "https://5321-sheet3.rajath.world"},
    4: {"host": "Desktop-5321", "port": 8772, "public_url": "https://5321-sheet4.rajath.world"},
}


def compute_dynamic_shard(
    current_sheet_num: int,
    active_sheets: List[int],
    all_keys: List[str]
) -> List[str]:
    """
    Deterministically partitions all_keys among active_sheets using interleaved round-robin.
    Ensures:
    1. Zero key collision across active sheets (disjoint subsets).
    2. 100% key utilization of available valid keys.
    3. Fair, even distribution.
    """
    if not all_keys:
        return []
    sorted_active = sorted(set(active_sheets))
    if current_sheet_num not in sorted_active:
        return []

    m = len(sorted_active)
    rank = sorted_active.index(current_sheet_num)
    return [k for idx, k in enumerate(all_keys) if idx % m == rank]


async def check_peer_sheet_active(
    sheet_num: int,
    current_sheet_num: int,
    session: aiohttp.ClientSession,
    auth_header: Dict[str, str],
    timeout_sec: float = 4.0,
    is_current_nim_done: bool = False
) -> bool:
    """
    Queries peer sheet supervisor endpoint to determine if it is currently running
    and actively processing Phase 2 (NIM) work.
    """
    if sheet_num == current_sheet_num:
        return not is_current_nim_done

    cfg = FLEET_NODES.get(sheet_num)
    if not cfg:
        return True

    # Sibling optimization: if on same machine (1&2 on 5080, 3&4 on 5321), prefer localhost
    is_sibling = (
        (sheet_num in (1, 2) and current_sheet_num in (1, 2)) or
        (sheet_num in (3, 4) and current_sheet_num in (3, 4))
    )
    urls_to_try = []
    if is_sibling:
        urls_to_try.append(f"http://127.0.0.1:{cfg['port']}/discovery/status")
    urls_to_try.append(f"{cfg['public_url']}/discovery/status")

    timeout = aiohttp.ClientTimeout(total=timeout_sec)

    for url in urls_to_try:
        try:
            async with session.get(url, headers=auth_header, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    status = str(data.get("status", "")).strip().lower()
                    active = bool(data.get("active", False))
                    nim_count = int(data.get("nim_count", 0) or 0)
                    tc_success = int(data.get("tc_success", 0) or data.get("progress_success", 0) or 0)
                    total = int(data.get("progress_total", 0) or data.get("total", 0) or 0)

                    # Sheet is idle or not running
                    if status not in ("running", "active") or not active:
                        return False

                    # Sheet has finished all its Phase 2 NIM summarizations
                    if tc_success > 0 and nim_count >= tc_success:
                        return False
                    if total > 0 and nim_count >= total:
                        return False

                    return True
                else:
                    logger.debug(f"Peer {sheet_num} returned status {resp.status} on {url}.")
        except Exception as e:
            logger.debug(f"Peer {sheet_num} query failed on {url} ({e}).")

    # If query failed across all URLs, assume active for safety
    logger.debug(f"Peer {sheet_num} unreachable, assuming active for collision safety.")
    return True


class FleetNIMRebalancer:
    """
    Background supervisor task that periodically checks peer sheet health
    and triggers safe key re-sharding and worker pool scaling.
    """

    def __init__(
        self,
        current_sheet_num: int,
        all_keys: List[str],
        token_rotator: Any,
        on_scale_callback: Optional[Callable[[int, List[str]], Any]] = None,
        poll_interval_sec: float = 30.0,
        is_nim_done_fn: Optional[Callable[[], bool]] = None
    ):
        self.current_sheet_num = current_sheet_num
        self.all_keys = [k.strip() for k in all_keys if k and k.strip()]
        self.token_rotator = token_rotator
        self.on_scale_callback = on_scale_callback
        self.poll_interval_sec = poll_interval_sec
        self.is_nim_done_fn = is_nim_done_fn
        self.current_active_sheets: List[int] = [1, 2, 3, 4]
        self.auth_token = os.getenv("SERVICE_AUTH_TOKEN", "Tracxn@SR")
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def poll_and_rebalance(self, session: aiohttp.ClientSession) -> bool:
        auth_header = {
            "Authorization": f"Bearer {self.auth_token}",
            "User-Agent": "DiscoveryFleetRebalancer/1.0",
        }
        is_cur_done = bool(self.is_nim_done_fn and self.is_nim_done_fn())

        active_list = []
        for s_num in [1, 2, 3, 4]:
            is_act = await check_peer_sheet_active(
                s_num,
                self.current_sheet_num,
                session,
                auth_header,
                is_current_nim_done=is_cur_done
            )
            if is_act:
                active_list.append(s_num)

        if not active_list or (not is_cur_done and self.current_sheet_num not in active_list):
            active_list = [self.current_sheet_num] if not is_cur_done else active_list or [self.current_sheet_num]

        if sorted(active_list) != sorted(self.current_active_sheets):
            old_sheets = list(self.current_active_sheets)
            self.current_active_sheets = sorted(active_list)
            new_keys = compute_dynamic_shard(
                self.current_sheet_num,
                self.current_active_sheets,
                self.all_keys
            )
            logger.info(
                f"🔄 [Fleet NIM Rebalancer] Active fleet sheets changed: {old_sheets} -> {self.current_active_sheets}. "
                f"Sheet {self.current_sheet_num} rebalanced to {len(new_keys)} keys."
            )
            if self.token_rotator and hasattr(self.token_rotator, "update_keys"):
                self.token_rotator.update_keys(new_keys)

            if self.on_scale_callback:
                try:
                    res = self.on_scale_callback(len(new_keys), new_keys)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception as ce:
                    logger.error(f"Error in on_scale_callback: {ce}")
            return True
        return False

    async def start(self):
        self._running = True
        logger.info(
            f"🚀 Started FleetNIMRebalancer for Sheet {self.current_sheet_num} "
            f"(Poll interval: {self.poll_interval_sec}s)."
        )
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    await self.poll_and_rebalance(session)
                except Exception as e:
                    logger.warning(f"Error in FleetNIMRebalancer poll loop: {e}")
                await asyncio.sleep(self.poll_interval_sec)

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
