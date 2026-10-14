import os
import logging
from typing import Any, Dict, Optional, Union
import aiohttp

logger = logging.getLogger(__name__)

class TracxnClient:
    """
    High-throughput asynchronous client for all Tracxn Write APIs.
    Consolidates funnel movements, snoozing, tagging, and assignment.
    Utilizes rotated tokens and connection-pooled aiohttp sessions with zero artificial sleep delays.
    """

    BASE_URL = "https://platform.tracxn.com"
    MOVE_URL = f"{BASE_URL}/data/funnel-action/move"
    SNOOZE_URL = f"{BASE_URL}/data/funnel-action/snooze"
    ESCALATE_URL = f"{BASE_URL}/data/funnel-action/escalate"
    MARK_LATER_URL = f"{BASE_URL}/data/funnel-action/mark-for-later"
    MARK_IRRELEVANT_URL = f"{BASE_URL}/data/funnel-action/mark-irrelevant"
    FORCE_ASSIGN_URL = f"{BASE_URL}/data/funnel-action/force-assign"
    CHANGE_STATUS_URL = f"{BASE_URL}/data/funnel-action/change-status"
    DOMAINS_API_URL = f"{BASE_URL}/api/2.2/domains"

    def __init__(self, token: Optional[str] = None, token_rotator: Optional[Any] = None):
        self.static_token = token
        self.token_rotator = token_rotator

    def get_token(self, override_token: Optional[str] = None) -> str:
        if override_token and str(override_token).strip():
            return str(override_token).strip()
        if self.token_rotator is not None:
            return self.token_rotator.get_next_token()
        return self.static_token or os.getenv("TRACXN_API_TOKEN", "")

    def _headers(self, token: str) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-Request-Source": "Write API",
            "accessToken": token,
        }

    def _build_source_details(self, filter_group_id: Any, filter_id: Any) -> Dict[str, Any]:
        source_details = {
            "source": "Write API",
            "sourceData": {}
        }
        if filter_group_id and str(filter_group_id).strip():
            source_details["sourceData"] = {
                "view": "Card",
                "tab": "Funnel Filter Group",
                "tabData": {
                    "id": str(filter_group_id).strip()
                }
            }
            if filter_id and str(filter_id).strip():
                source_details["sourceData"]["tabData"]["funnelFilterId"] = str(filter_id).strip()
        return source_details

    async def _send_put(self, session: aiohttp.ClientSession, url: str, payload: Dict[str, Any], token: Optional[str] = None) -> str:
        tok = self.get_token(token)
        try:
            async with session.put(
                url,
                json=payload,
                headers=self._headers(tok),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    return "YES"
                elif resp.status in (401, 403, 429) and self.token_rotator is not None:
                    self.token_rotator.mark_rate_limited(tok, penalty_sec=30)
                    logger.warning(f"Tracxn token ...{tok[-6:]} got status {resp.status} on PUT {url}")
                    return "NO"
                else:
                    text = await resp.text()
                    logger.warning(f"Tracxn PUT {url} failed ({resp.status}): {text}")
                    return "NO"
        except Exception as e:
            logger.error(f"Error executing Tracxn PUT {url}: {e}")
            return "NO"

    async def move_domain_funnel(
        self,
        session: aiohttp.ClientSession,
        domain_profile_id: str,
        current_funnel_id: str,
        filter_group_id: Any,
        filter_id: Any,
        dest_funnel: str,
        comments: str,
        token: Optional[str] = None,
    ) -> str:
        payload = {
            "funnelId": current_funnel_id,
            "domainProfileId": domain_profile_id,
            "movedTo": [dest_funnel],
            "comment": comments,
            "sourceDetails": self._build_source_details(filter_group_id, filter_id),
        }
        return await self._send_put(session, self.MOVE_URL, payload, token)

    async def snooze_domain(
        self,
        session: aiohttp.ClientSession,
        domain_profile_id: str,
        current_funnel_id: str,
        filter_group_id: Any,
        filter_id: Any,
        comments: str,
        snooze_days: str = "30",
        token: Optional[str] = None,
    ) -> str:
        payload = {
            "funnelId": current_funnel_id,
            "domainProfileId": domain_profile_id,
            "comment": comments,
            "sourceDetails": self._build_source_details(filter_group_id, filter_id),
            "snoozeMethod": {
                "type": "period",
                "value": str(snooze_days),
            },
        }
        return await self._send_put(session, self.SNOOZE_URL, payload, token)

    async def escalate_domain(
        self,
        session: aiohttp.ClientSession,
        domain_profile_id: str,
        current_funnel_id: str,
        filter_group_id: Any,
        filter_id: Any,
        comments: str,
        token: Optional[str] = None,
    ) -> str:
        payload = {
            "funnelId": current_funnel_id,
            "domainProfileId": domain_profile_id,
            "comment": comments,
            "sourceDetails": self._build_source_details(filter_group_id, filter_id),
        }
        return await self._send_put(session, self.ESCALATE_URL, payload, token)

    async def mark_domain_for_later(
        self,
        session: aiohttp.ClientSession,
        domain_profile_id: str,
        current_funnel_id: str,
        filter_group_id: Any,
        filter_id: Any,
        comments: str,
        token: Optional[str] = None,
    ) -> str:
        payload = {
            "funnelId": current_funnel_id,
            "domainProfileId": domain_profile_id,
            "comment": comments,
            "sourceDetails": self._build_source_details(filter_group_id, filter_id),
        }
        return await self._send_put(session, self.MARK_LATER_URL, payload, token)

    async def mark_domain_irrelevant(
        self,
        session: aiohttp.ClientSession,
        domain_profile_id: str,
        current_funnel_id: str,
        filter_group_id: Any,
        filter_id: Any,
        comments: str,
        token: Optional[str] = None,
    ) -> str:
        payload = {
            "funnelId": current_funnel_id,
            "domainProfileId": domain_profile_id,
            "comment": comments,
            "sourceDetails": self._build_source_details(filter_group_id, filter_id),
        }
        return await self._send_put(session, self.MARK_IRRELEVANT_URL, payload, token)

    async def force_assign_domain(
        self,
        session: aiohttp.ClientSession,
        domain_profile_id: str,
        funnel_id: str,
        filter_group_id: Any,
        filter_id: Any,
        comments: str,
        token: Optional[str] = None,
    ) -> str:
        payload = {
            "funnelId": funnel_id,
            "domainProfileId": domain_profile_id,
            "comment": comments,
            "sourceDetails": self._build_source_details(filter_group_id, filter_id),
        }
        return await self._send_put(session, self.FORCE_ASSIGN_URL, payload, token)

    async def change_domain_status(
        self,
        session: aiohttp.ClientSession,
        domain_profile_id: str,
        current_funnel_id: str,
        filter_group_id: Any,
        filter_id: Any,
        status: str,
        comments: str,
        token: Optional[str] = None,
    ) -> str:
        payload = {
            "funnelId": current_funnel_id,
            "domainProfileId": domain_profile_id,
            "status": status,
            "comment": comments,
            "sourceDetails": self._build_source_details(filter_group_id, filter_id),
        }
        return await self._send_put(session, self.CHANGE_STATUS_URL, payload, token)

    async def add_domain_hashtag(
        self,
        session: aiohttp.ClientSession,
        domain: str,
        tag: str = "NO",
        token: Optional[str] = None,
    ) -> str:
        tok = self.get_token(token)
        url = f"{self.DOMAINS_API_URL}/{domain}/hashtag"
        payload = {"domain": domain, "hashTag": tag}
        try:
            async with session.post(
                url,
                json=payload,
                headers=self._headers(tok),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    return "YES"
                return "NO"
        except Exception as e:
            logger.error(f"Error adding hashtag to domain {domain}: {e}")
            return "NO"

    async def remove_domain_hashtag(
        self,
        session: aiohttp.ClientSession,
        domain: str,
        tag: str = "NO",
        token: Optional[str] = None,
    ) -> str:
        tok = self.get_token(token)
        url = f"{self.DOMAINS_API_URL}/{domain}/hashtag"
        payload = {"domain": domain, "hashTag": tag}
        try:
            async with session.delete(
                url,
                json=payload,
                headers=self._headers(tok),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    return "YES"
                return "NO"
        except Exception as e:
            logger.error(f"Error removing hashtag from domain {domain}: {e}")
            return "NO"
