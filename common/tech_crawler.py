import os
import time
import logging
import asyncio
from typing import Any, Dict, Optional, Union, Tuple
import aiohttp

logger = logging.getLogger(__name__)

def is_valid_s3_url(s3_url: Optional[str]) -> bool:
    """
    Checks if a URL has a valid S3 format before requesting signed URLs from Tracxn API.
    Prevents HTTP 400 'Invalid S3 URL format' errors on malformed links, timestamps, or domain URLs.
    """
    if not s3_url or not isinstance(s3_url, str):
        return False
    s = s3_url.strip().lower()
    return "s3.amazonaws.com" in s or s.startswith("s3://") or ".s3." in s or "s3-" in s


def extract_tc_error_details(track_data: Dict[str, Any]) -> Tuple[str, str]:
    """
    Extracts (error_code, error_reason) from a Tracxn Tech Crawler track response.
    Returns ("", "") if the crawl succeeded with non-empty markdown.
    If status is FAILED or SUCCESS with no markdown / HTTP error, extracts
    pageHttpStatus / error code into error_code, and error reason / signals into error_reason.
    """
    if not isinstance(track_data, dict):
        return ("", "")

    status = str(track_data.get("status") or "").strip().upper()
    output = track_data.get("output") or {}
    markdown = str(output.get("markdown") or "").strip()
    page_http_status = str(output.get("pageHttpStatus") or "").strip()

    # If SUCCESS and valid markdown is present and HTTP is not an error, not an error
    if status == "SUCCESS" and markdown and (not page_http_status or page_http_status in ("200", "201", "204", "0")):
        return ("", "")

    # If still PROCESSING, not yet a terminal error
    if status == "PROCESSING":
        return ("", "")

    error_obj = output.get("error") or {}
    error_code = str(error_obj.get("code") or output.get("pageHttpStatus") or "").strip()

    # Extract error reason from error object or signals
    reason = error_obj.get("reason")
    if not reason:
        signals = output.get("signals")
        if isinstance(signals, list):
            reason = ", ".join(str(s) for s in signals if s)
        elif signals:
            reason = str(signals)
        else:
            reason = track_data.get("error_reason") or ""

    error_reason = str(reason or "").strip()

    # Fallback error code if status is FAILED but no specific code
    if not error_code and status == "FAILED":
        error_code = "FAILED"

    return (error_code, error_reason)


class TechCrawlerClient:
    """
    Asynchronous client for Tracxn Tech Crawler / URL rendering backend.
    Includes token rotation and 15-minute polling cadence for up to 3 hours.
    """

    GENERATE_URL = "https://platform.tracxn.com/utils/web-scraper/fetch/url/bu-daily"
    TRACK_URL = "https://platform.tracxn.com/utils/web-scraper/fetch/url/track"
    S3_SIGN_URL = "https://platform.tracxn.com/utils/s3/signed-url"

    POLL_INTERVAL_SEC: int = 900          # 15 minutes
    MAX_POLL_DURATION_SEC: int = 10800    # 3 hours
    MAX_POLL_CHECKS: int = 12             # 3 hours / 15 minutes = 12 checks

    def __init__(self, token: Optional[str] = None, token_rotator: Optional[Any] = None):
        self.static_token = token
        self.token_rotator = token_rotator

    def get_token(self) -> str:
        if self.token_rotator is not None:
            return self.token_rotator.get_next_token()
        return self.static_token or os.getenv("TRACXN_API_TOKEN", "")

    def _handle_token_error(self, token: str, status_code: int) -> None:
        if self.token_rotator is not None:
            if hasattr(self.token_rotator, "handle_http_error"):
                self.token_rotator.handle_http_error(token, status_code)
            else:
                penalty = 60.0 if status_code == 429 else 30.0
                if hasattr(self.token_rotator, "mark_rate_limited"):
                    self.token_rotator.mark_rate_limited(token, penalty_sec=penalty)

    async def _acquire_token(self) -> str:
        if self.token_rotator is not None and hasattr(self.token_rotator, "acquire"):
            return await self.token_rotator.acquire()
        return self.get_token()

    def _headers(self, token: Optional[str] = None) -> Dict[str, str]:
        tok = token or self.get_token()
        return {
            "Content-Type": "application/json",
            "X-Request-Source": "Testing",
            "accessToken": tok,
        }

    async def generate_job_id(
        self,
        session: aiohttp.ClientSession,
        url: str,
        mode: str = "render",
        html_output: bool = True,
        markdown_output: bool = True,
    ) -> Optional[str]:
        if not url:
            return None

        clean_url = url.strip()
        if not clean_url.startswith("http://") and not clean_url.startswith("https://"):
            clean_url = f"https://{clean_url}"

        payload = {
            "url": clean_url,
            "configuration": {
                "mode": mode,
                "output": {
                    "html": html_output,
                    "markdown": markdown_output,
                },
            },
        }

        max_attempts = max(3, len(getattr(self.token_rotator, "tokens", []))) if self.token_rotator else 3
        for attempt in range(max_attempts):
            tok = await self._acquire_token()
            if not tok:
                logger.error(f"No valid Tracxn token available for generate_job_id on {clean_url}")
                return None
            try:
                async with session.post(
                    self.GENERATE_URL,
                    json=payload,
                    headers=self._headers(tok),
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status in (200, 201, 202):
                        data = await resp.json()
                        job_id = data.get("jobId")
                        logger.info(f"Generated Tech Crawler Job ID for {clean_url}: {job_id}")
                        return job_id
                    elif resp.status in (401, 403, 429) and self.token_rotator is not None:
                        self._handle_token_error(tok, resp.status)
                        logger.warning(
                            f"Tech Crawler generate token ...{tok[-6:]} returned status {resp.status}. "
                            f"Retrying with next active token (attempt {attempt+1}/{max_attempts})..."
                        )
                        continue
                    else:
                        text = await resp.text()
                        logger.warning(f"Failed to generate Tech Crawler Job ID ({resp.status}): {text}")
                        return None
            except Exception as e:
                logger.error(f"Error calling Tech Crawler generate API for {clean_url}: {e}")
                return None
        return None

    async def track_job_id(
        self,
        session: aiohttp.ClientSession,
        job_id: str,
    ) -> Dict[str, Any]:
        result = {
            "status": "",
            "final_url": "",
            "s3_html": "",
            "s3_markdown": "",
            "http_status": "",
            "signals": "",
            "error_code": "",
            "error_reason": "",
            "last_updated_at": "",
        }

        if not job_id:
            result["status"] = "FAILED"
            result["error_reason"] = "Empty job_id"
            return result

        payload = {"jobId": job_id}
        max_attempts = max(3, len(getattr(self.token_rotator, "tokens", []))) if self.token_rotator else 3

        for attempt in range(max_attempts):
            tok = await self._acquire_token()
            if not tok:
                result["status"] = "FAILED"
                result["error_reason"] = "No valid Tracxn token available"
                return result

            try:
                async with session.post(
                    self.TRACK_URL,
                    json=payload,
                    headers=self._headers(tok),
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        result["status"] = data.get("status", "")
                        result["last_updated_at"] = data.get("lastUpdatedAt", "")

                        output = data.get("output") or {}
                        result["final_url"] = output.get("finalUrl", "")
                        result["s3_html"] = output.get("html", "")
                        result["s3_markdown"] = output.get("markdown", "")
                        result["http_status"] = str(output.get("pageHttpStatus", "") or "")
                        result["signals"] = str(output.get("signals", "") or "")

                        err_c, err_r = extract_tc_error_details(data)
                        result["error_code"] = err_c
                        result["error_reason"] = err_r
                        return result
                    elif resp.status in (401, 403, 429) and self.token_rotator is not None:
                        self._handle_token_error(tok, resp.status)
                        logger.warning(
                            f"Track Job API token ...{tok[-6:]} returned status {resp.status}. "
                            f"Retrying with next active token (attempt {attempt+1}/{max_attempts})..."
                        )
                        continue
                    else:
                        text = await resp.text()
                        logger.warning(f"Track Job API returned status {resp.status} for {job_id}: {text}")
                        result["status"] = "FAILED"
                        result["error_code"] = str(resp.status)
                        result["error_reason"] = text
                        return result
            except Exception as e:
                logger.error(f"Error querying Tech Crawler track API for {job_id}: {e}")
                result["status"] = "FAILED"
                result["error_reason"] = str(e)
                return result
        result["status"] = "FAILED"
        result["error_reason"] = "Exhausted token retries for track_job_id"
        return result

    async def check_job_status_with_cadence(
        self,
        session: aiohttp.ClientSession,
        job_id: str,
        created_timestamp: float,
        current_time: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Evaluates job status respecting the 15-minute / 3-hour timeout cadence.
        If job is still PROCESSING after 3 hours (10,800s), transitions to FAILED to trigger Waterfall Scraper fallback.
        """
        now = current_time if current_time is not None else time.time()
        elapsed_sec = now - created_timestamp

        track_res = await self.track_job_id(session, job_id)
        status = track_res.get("status", "")

        if status == "PROCESSING" and elapsed_sec >= self.MAX_POLL_DURATION_SEC:
            logger.warning(
                f"Tech Crawler job {job_id} has been PROCESSING for >= 3 hours "
                f"({elapsed_sec/3600:.1f}h). Switching to Waterfall Scraper fallback."
            )
            track_res["status"] = "FAILED"
            track_res["error_code"] = "TIMEOUT_3HR"
            track_res["error_reason"] = (
                f"Job exceeded max duration of 3 hours ({elapsed_sec:.0f}s >= 10,800s). "
                f"Switching to Waterfall Scraper."
            )

        return track_res

    async def get_signed_s3_url(
        self,
        session: aiohttp.ClientSession,
        s3_url: str,
        max_retries: int = 3,
    ) -> Optional[str]:
        if not s3_url or not is_valid_s3_url(s3_url):
            if s3_url:
                logger.info(f"Fast-skipping malformed S3 URL format: {str(s3_url)[:60]}")
            return None

        payload = {"url": s3_url}
        retries = max(max_retries, len(getattr(self.token_rotator, "tokens", []))) if self.token_rotator else max_retries
        for attempt in range(retries):
            tok = await self._acquire_token()
            if not tok:
                return None
            try:
                async with session.post(
                    self.S3_SIGN_URL,
                    json=payload,
                    headers=self._headers(tok),
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("url")
                    elif resp.status in (401, 403, 429):
                        self._handle_token_error(tok, resp.status)
                        text = await resp.text()
                        logger.warning(f"S3 signed URL generation attempt {attempt+1}/{retries} failed ({resp.status}): {text}")
                        if attempt < retries - 1:
                            await asyncio.sleep(0.1)
                            continue
                        return None
                    else:
                        text = await resp.text()
                        logger.warning(f"S3 signed URL generation failed ({resp.status}): {text}")
                        return None
            except Exception as e:
                logger.error(f"Error getting signed S3 URL for {s3_url} (attempt {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)
                    continue
                return None
        return None

    async def fetch_raw_markdown(
        self,
        session: aiohttp.ClientSession,
        signed_url: str,
    ) -> Optional[str]:
        if not signed_url:
            return None

        for attempt in range(3):
            try:
                async with session.get(
                    signed_url,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    elif resp.status in (403, 404):
                        logger.warning(f"S3 returned status {resp.status} for signed URL")
                        return None
                    else:
                        logger.warning(f"Failed to fetch markdown from signed S3 URL ({resp.status}), attempt {attempt+1}")
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Error downloading raw markdown from {signed_url}: {e}")
                    return None
                await asyncio.sleep(0.5)
        return None
