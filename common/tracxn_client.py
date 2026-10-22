import os
import logging
from typing import Any, Dict, Optional, Union, List, Set
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
    UNSNOOZE_URL = f"{BASE_URL}/data/funnel-action/unsnooze"
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

    def _handle_token_error(self, token: str, status_code: int) -> None:
        if self.token_rotator is not None:
            if hasattr(self.token_rotator, "handle_http_error"):
                self.token_rotator.handle_http_error(token, status_code)
            else:
                penalty = 60.0 if status_code == 429 else 30.0
                if hasattr(self.token_rotator, "mark_rate_limited"):
                    self.token_rotator.mark_rate_limited(token, penalty_sec=penalty)

    async def _acquire_token(self, override_token: Optional[str] = None) -> str:
        if override_token and str(override_token).strip():
            return str(override_token).strip()
        if self.token_rotator is not None and hasattr(self.token_rotator, "acquire"):
            return await self.token_rotator.acquire()
        return self.get_token(override_token)

    async def _send_put(self, session: aiohttp.ClientSession, url: str, payload: Dict[str, Any], token: Optional[str] = None) -> str:
        max_attempts = max(3, len(getattr(self.token_rotator, "tokens", []))) if self.token_rotator else 1
        for attempt in range(max_attempts):
            tok = await self._acquire_token(token if attempt == 0 else None)
            if not tok:
                logger.error(f"No valid Tracxn token available for PUT {url}")
                return "NO"
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
                        self._handle_token_error(tok, resp.status)
                        logger.warning(
                            f"Tracxn token ...{tok[-6:]} got status {resp.status} on PUT {url}. "
                            f"Retrying with next active token (attempt {attempt+1}/{max_attempts})..."
                        )
                        continue
                    elif resp.status == 422:
                        text = await resp.text()
                        if "unsnooze" in url:
                            logger.debug(f"Unsnooze 422 for domainProfileId (state not SNOOZED): {text[:80]}")
                            return "NOT_SNOOZED"
                        if "move" in url and "current state is 'MOVED_OUT'" in text:
                            logger.debug(f"Domain already moved out in funnel: {text[:80]}")
                            return "ALREADY_MOVED"
                        logger.debug(f"Tracxn PUT {url} failed with 422 (unassignable/assigned to other user): {text[:80]}. Fast-skipping.")
                        return "UNASSIGNABLE"
                    else:
                        text = await resp.text()
                        logger.warning(f"Tracxn PUT {url} failed ({resp.status}): {text}")
                        return "NO"
            except Exception as e:
                logger.error(f"Error executing Tracxn PUT {url} on attempt {attempt+1}: {e}")
                if attempt == max_attempts - 1:
                    return "NO"
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

    async def unsnooze_domain(
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
        return await self._send_put(session, self.UNSNOOZE_URL, payload, token)

    async def unsnooze_and_move_domain(
        self,
        session: aiohttp.ClientSession,
        domain_profile_id: str,
        current_funnel_id: str,
        filter_group_id: Any,
        filter_id: Any,
        dest_funnel: str,
        comments: str,
        token: Optional[str] = None,
        is_snoozed: bool = True,
    ) -> str:
        """
        Safely unsnoozes (if snoozed), force-assigns, and moves a domain into its target Industry Funnel.
        Handles state transitions: SNOOZED -> UNSNOOZED -> ASSIGNED -> MOVED_OUT.
        CRITICAL: All steps for a domain MUST use the exact same user token, because Tracxn
        strictly requires the moving user to match the assigned user.
        """
        tok = await self._acquire_token(token)
        if is_snoozed:
            unsnooze_res = await self.unsnooze_domain(
                session, domain_profile_id, current_funnel_id, filter_group_id, filter_id, comments, token=tok
            )
            logger.debug(f"Unsnooze result for {domain_profile_id}: {unsnooze_res}")

        assign_res = await self.force_assign_domain(
            session, domain_profile_id, current_funnel_id, filter_group_id, filter_id, comments, token=tok
        )
        if assign_res == "UNASSIGNABLE":
            return "UNASSIGNABLE"

        move_res = await self.move_domain_funnel(
            session, domain_profile_id, current_funnel_id, filter_group_id, filter_id, dest_funnel, comments, token=tok
        )
        if move_res in ("YES", "ALREADY_MOVED"):
            return "YES"
        return move_res

    async def sync_domain_hashtags(
        self,
        session: aiohttp.ClientSession,
        domain: str,
        add_tags: Optional[Union[List[str], Set[str], str]] = None,
        remove_tags: Optional[Union[List[str], Set[str], str]] = None,
        domain_profile_id: Optional[str] = None,
        token: Optional[str] = None,
    ) -> str:
        """
        Synchronizes hashtags for a domain in a single optimized pass.
        Fetches current hashtags once, modifies the set in memory,
        and executes at most ONE PUT request only if the hashtag list changed.
        Returns 'YES' if tags are up-to-date or successfully updated, else 'NO'.
        """
        if isinstance(add_tags, str):
            add_tags = [add_tags] if add_tags and add_tags != "NO" else []
        if isinstance(remove_tags, str):
            remove_tags = [remove_tags] if remove_tags and remove_tags != "NO" else []
        add_set = set(add_tags or [])
        remove_set = set(remove_tags or [])

        if not add_set and not remove_set:
            return "YES"

        tok = await self._acquire_token(token)
        cur_tags = []
        dp_id = domain_profile_id
        try:
            fetch_payload = {"filter": {"domain": [domain]}, "dataset": "internal"}
            async with session.post(
                self.DOMAINS_API_URL,
                json=fetch_payload,
                headers=self._headers(tok),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    res = data.get("result", [])
                    if res and res[0]:
                        cur_tags = res[0].get("keywords", {}).get("hashTags", []) or []
                        if not dp_id:
                            dp_id = res[0].get("domainProfileId", "")
                elif resp.status in (401, 403, 429) and self.token_rotator is not None:
                    self._handle_token_error(tok, resp.status)
        except Exception as e:
            logger.warning(f"Error fetching existing hashtags for {domain}: {e}")

        if not dp_id:
            return "NO"

        updated = set(cur_tags)
        updated.difference_update(remove_set)
        updated.update(add_set)

        if updated == set(cur_tags):
            return "YES"

        new_tags = list(updated)
        update_url = f"{self.BASE_URL}/data/entities/2.0/domain-profile?domain={domain}&tab=COMPANY_PROFILE"
        update_payload = {
            "id": dp_id,
            "name": domain,
            "domain": domain,
            "keywords": {"value": {"HASHTAGS": new_tags}},
            "dataset": "internal",
        }
        return await self._send_put(session, update_url, update_payload, tok)

    async def add_domain_hashtag(
        self,
        session: aiohttp.ClientSession,
        domain: str,
        tag: str = "NO",
        domain_profile_id: Optional[str] = None,
        token: Optional[str] = None,
    ) -> str:
        if not tag or tag == "NO":
            return "NO"
        tok = await self._acquire_token(token)
        cur_tags = []
        dp_id = domain_profile_id
        try:
            fetch_payload = {"filter": {"domain": [domain]}, "dataset": "internal"}
            async with session.post(
                self.DOMAINS_API_URL,
                json=fetch_payload,
                headers=self._headers(tok),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    res = data.get("result", [])
                    if res and res[0]:
                        cur_tags = res[0].get("keywords", {}).get("hashTags", []) or []
                        if not dp_id:
                            dp_id = res[0].get("domainProfileId", "")
                elif resp.status in (401, 403, 429) and self.token_rotator is not None:
                    self._handle_token_error(tok, resp.status)
        except Exception as e:
            logger.warning(f"Error fetching existing hashtags for {domain}: {e}")

        if not dp_id:
            return "NO"

        if tag in cur_tags:
            return "YES"

        new_tags = list(cur_tags) + [tag]
        update_url = f"{self.BASE_URL}/data/entities/2.0/domain-profile?domain={domain}&tab=COMPANY_PROFILE"
        update_payload = {
            "id": dp_id,
            "keywords": {"value": {"HASHTAGS": new_tags}},
            "dataset": "internal",
        }
        return await self._send_put(session, update_url, update_payload, tok)

    async def remove_domain_hashtag(
        self,
        session: aiohttp.ClientSession,
        domain: str,
        tag: str = "NO",
        domain_profile_id: Optional[str] = None,
        token: Optional[str] = None,
    ) -> str:
        if not tag or tag == "NO":
            return "NO"
        tok = await self._acquire_token(token)
        cur_tags = []
        dp_id = domain_profile_id
        try:
            fetch_payload = {"filter": {"domain": [domain]}, "dataset": "internal"}
            async with session.post(
                self.DOMAINS_API_URL,
                json=fetch_payload,
                headers=self._headers(tok),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    res = data.get("result", [])
                    if res and res[0]:
                        cur_tags = res[0].get("keywords", {}).get("hashTags", []) or []
                        if not dp_id:
                            dp_id = res[0].get("domainProfileId", "")
                elif resp.status in (401, 403, 429) and self.token_rotator is not None:
                    self._handle_token_error(tok, resp.status)
        except Exception as e:
            logger.warning(f"Error fetching existing hashtags for {domain}: {e}")

        if not dp_id:
            return "NO"

        if tag not in cur_tags:
            return "YES"

        new_tags = [t for t in cur_tags if t != tag]
        update_url = f"{self.BASE_URL}/data/entities/2.0/domain-profile?domain={domain}&tab=COMPANY_PROFILE"
        update_payload = {
            "id": dp_id,
            "keywords": {"value": {"HASHTAGS": new_tags}},
            "dataset": "internal",
        }
        return await self._send_put(session, update_url, update_payload, tok)

