"""
Fleet Cluster Manager for Cross-Desktop Load Balancing & Shared Intelligence.
Enables Desktop-5080 and Desktop-5321 to co-process NIM summarization batches
and share cached domain intelligence over Cloudflare Zero Trust Tunnels.
"""

import os
import logging
from typing import List, Dict, Any, Optional
import aiohttp
from pydantic import BaseModel

logger = logging.getLogger(__name__)

class FleetSummarizeRequest(BaseModel):
    items: List[Dict[str, Any]]

FLEET_TOPOGRAPHY: Dict[int, Dict[str, Any]] = {
    1: {"host": "Desktop-5080", "port": 8769, "public_url": "https://5080-sheet1.rajath.world"},
    2: {"host": "Desktop-5080", "port": 8770, "public_url": "https://5080-sheet2.rajath.world"},
    3: {"host": "Desktop-5321", "port": 8771, "public_url": "https://5321-sheet3.rajath.world"},
    4: {"host": "Desktop-5321", "port": 8772, "public_url": "https://5321-sheet4.rajath.world"},
}

DEFAULT_AUTH_HEADER = {"Authorization": "Bearer Tracxn@SR"}

class FleetClusterManager:
    """
    Manages inter-desktop remote work-stealing and distributed domain cache lookups.
    """

    def __init__(
        self,
        current_sheet_num: int,
        auth_token: str = "Bearer Tracxn@SR",
        offload_threshold: int = 300,
        max_batch_size: int = 25
    ):
        self.current_sheet_num = current_sheet_num
        self.auth_token = auth_token
        self.offload_threshold = offload_threshold
        self.max_batch_size = max_batch_size
        self._headers = {"Authorization": auth_token, "Content-Type": "application/json"}

    def get_peer_urls(self) -> List[str]:
        """Returns public URLs for peer sheets running on the other desktop host."""
        curr_cfg = FLEET_TOPOGRAPHY.get(self.current_sheet_num, {})
        curr_host = curr_cfg.get("host", "")

        peer_urls = []
        for s_num, cfg in FLEET_TOPOGRAPHY.items():
            if s_num != self.current_sheet_num and cfg.get("host") != curr_host:
                peer_urls.append(cfg["public_url"])
        return peer_urls

    def should_offload_work(self, local_queue_depth: int) -> bool:
        """Determines if local queue depth warrants offloading work to an idle peer."""
        return local_queue_depth >= self.offload_threshold

    async def query_peer_cache(
        self,
        domain: str,
        session: aiohttp.ClientSession,
        timeout_sec: float = 1.5
    ) -> Optional[Dict[str, Any]]:
        """
        Queries peer desktop for cached domain results.
        Returns cached record if found, enabling instant 0-second resolve.
        """
        if not domain:
            return None

        clean_d = str(domain).strip().lower()
        timeout = aiohttp.ClientTimeout(total=timeout_sec)

        for peer_base in self.get_peer_urls():
            target_url = f"{peer_base}/fleet/cache/{clean_d}"
            try:
                async with session.get(target_url, headers=self._headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("found") and data.get("record"):
                            logger.info(f"⚡ [Fleet Cache Hit] {clean_d} resolved via peer {peer_base}")
                            return data["record"]
            except Exception as e:
                logger.debug(f"Peer cache query to {peer_base} failed: {e}")

        return None

    async def offload_nim_batch(
        self,
        items: List[Dict[str, Any]],
        session: aiohttp.ClientSession,
        timeout_sec: float = 15.0
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Dispatches a micro-batch of raw domains to an idle peer for parallel NIM summarization.
        """
        if not items:
            return None

        batch = items[:self.max_batch_size]
        timeout = aiohttp.ClientTimeout(total=timeout_sec)

        for peer_base in self.get_peer_urls():
            target_url = f"{peer_base}/fleet/co-process/nim-summarize"
            try:
                payload = {"items": batch}
                async with session.post(target_url, headers=self._headers, json=payload, timeout=timeout) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        summaries = result.get("results")
                        if summaries and len(summaries) == len(batch):
                            logger.info(f"🚀 [Fleet Co-Process] Offloaded {len(batch)} domains to {peer_base} successfully")
                            return summaries
            except Exception as e:
                logger.debug(f"Peer offload to {peer_base} failed: {e}")

        return None
