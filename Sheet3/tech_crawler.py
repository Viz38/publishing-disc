import logging
from typing import Any, Dict, Optional
import aiohttp

logger = logging.getLogger(__name__)

class TechCrawlerClient:
    """
    Asynchronous client for Tracxn Tech Crawler / URL rendering backend.
    Handles job generation, status tracking, S3 pre-signed URL retrieval, and raw markdown downloading.
    """

    GENERATE_URL = "https://platform.tracxn.com/utils/web-scraper/fetch/url/bu-daily"
    TRACK_URL = "https://platform.tracxn.com/utils/web-scraper/fetch/url/track"
    S3_SIGN_URL = "https://platform.tracxn.com/utils/s3/signed-url"

    def __init__(self, token: str):
        self.token = token

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-Request-Source": "Testing",
            "accessToken": self.token,
        }

    async def generate_job_id(
        self,
        session: aiohttp.ClientSession,
        url: str,
        mode: str = "render",
        html_output: bool = True,
        markdown_output: bool = True,
    ) -> Optional[str]:
        """
        Submits a domain/URL to Tech Crawler to generate a rendering job ID.
        """
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

        try:
            async with session.post(
                self.GENERATE_URL,
                json=payload,
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status in (200, 201, 202):
                    data = await resp.json()
                    job_id = data.get("jobId")
                    logger.info(f"Generated Tech Crawler Job ID for {clean_url}: {job_id}")
                    return job_id
                else:
                    text = await resp.text()
                    logger.warning(f"Failed to generate Tech Crawler Job ID ({resp.status}): {text}")
                    return None
        except Exception as e:
            logger.error(f"Error calling Tech Crawler generate API for {clean_url}: {e}")
            return None

    async def track_job_id(
        self,
        session: aiohttp.ClientSession,
        job_id: str,
    ) -> Dict[str, Any]:
        """
        Queries the status of an ongoing Tech Crawler job.
        Returns a normalized dict containing status, S3 markdown URL, HTTP status, and signals.
        """
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

        try:
            async with session.post(
                self.TRACK_URL,
                json=payload,
                headers=self._headers(),
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

                    if result["status"] == "FAILED":
                        error_obj = output.get("error") or {}
                        result["error_code"] = str(
                            error_obj.get("code") or output.get("pageHttpStatus") or ""
                        )
                        result["error_reason"] = str(
                            error_obj.get("reason") or output.get("signals") or ""
                        )
                    return result
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

    async def get_signed_s3_url(
        self,
        session: aiohttp.ClientSession,
        s3_url: str,
    ) -> Optional[str]:
        """
        Exchanges a private Tracxn S3 URL for a temporary signed download URL.
        """
        if not s3_url:
            return None

        payload = {"url": s3_url}

        try:
            async with session.post(
                self.S3_SIGN_URL,
                json=payload,
                headers=self._headers(),
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("url")
                else:
                    text = await resp.text()
                    logger.warning(f"S3 signed URL generation failed ({resp.status}): {text}")
                    return None
        except Exception as e:
            logger.error(f"Error getting signed S3 URL for {s3_url}: {e}")
            return None

    async def fetch_raw_markdown(
        self,
        session: aiohttp.ClientSession,
        signed_url: str,
    ) -> Optional[str]:
        """
        Fetches the raw text content of the rendered markdown file via the signed S3 URL.
        """
        if not signed_url:
            return None

        try:
            async with session.get(
                signed_url,
                timeout=aiohttp.ClientTimeout(total=45),
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    logger.warning(f"Failed to fetch markdown from signed S3 URL ({resp.status})")
                    return None
        except Exception as e:
            logger.error(f"Error downloading raw markdown from {signed_url}: {e}")
            return None
