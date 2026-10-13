import asyncio
import logging
import re
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urldefrag
import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

try:
    from scrapling import Fetcher
    SCRAPLING_AVAILABLE = True
except ImportError:
    SCRAPLING_AVAILABLE = False

try:
    from camoufox.async_api import AsyncCamoufox
    CAMOUFOX_AVAILABLE = True
except ImportError:
    try:
        from camoufox.sync_api import Camoufox
        CAMOUFOX_AVAILABLE = True
    except ImportError:
        CAMOUFOX_AVAILABLE = False

# Global semaphore to limit concurrent browser instances per process
_camoufox_semaphore = asyncio.Semaphore(2)


def clean_html_to_text(html: str, base_url: str) -> Tuple[str, List[str]]:
    """
    Parse HTML, strip layout/noise tags, purge NUL bytes, and extract clean text and internal links.
    Standardized according to Scraper - Waterfall implementation.
    """
    if not html:
        return "", []

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return "", []

    # Strip script, style, navigation, headers, footers, forms, and decorative SVGs
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "svg", "canvas", "form"]):
        tag.decompose()

    # Purge any NUL characters and clean whitespace
    text = soup.get_text(separator=" ", strip=True).replace("\x00", "")
    text = re.sub(r"\s+", " ", text).strip()

    links: List[str] = []
    base_netloc = urlparse(base_url).netloc
    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "").strip()
        if not href or href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        try:
            full_url = urljoin(base_url, href)
            defragged, _ = urldefrag(full_url)
            parsed = urlparse(defragged)
            if parsed.netloc == base_netloc and defragged not in links and defragged != base_url:
                links.append(defragged)
        except Exception:
            continue

    return text, links


class WaterfallScraper:
    """
    Progressive Multi-Tier Scraper modeled after Tracxn Scraper - Waterfall:
    Tier 0: Fast async HTTP via aiohttp + BeautifulSoup
    Tier 1: Scrapling anti-bot fetcher fallback
    Tier 2: Camoufox basic headless browser
    Tier 3: Camoufox full stealth browser
    """

    def __init__(self, max_chars: int = 48000):
        self.max_chars = max_chars
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

    async def _try_http(
        self, session: Optional[aiohttp.ClientSession], url: str, timeout_sec: float = 10.0
    ) -> Tuple[Optional[str], str]:
        """Tier 0: Fast async HTTP GET."""
        if not session:
            return None, ""
        try:
            timeout = aiohttp.ClientTimeout(total=timeout_sec, connect=min(5.0, timeout_sec))
            async with session.get(
                url, headers=self.headers, timeout=timeout, allow_redirects=True, ssl=False
            ) as resp:
                if resp.status == 200:
                    text = await resp.text(errors="replace")
                    if text and text.strip():
                        return text, "HTTP"
        except Exception:
            pass
        return None, ""

    async def _try_scrapling(self, url: str, timeout_sec: float = 12.0) -> Tuple[Optional[str], str]:
        """Tier 1: Scrapling fetcher fallback."""
        if not SCRAPLING_AVAILABLE:
            return None, ""
        loop = asyncio.get_running_loop()
        try:
            def _sync_fetch():
                fetcher = Fetcher()
                resp = fetcher.get(url)
                return resp.text

            html = await asyncio.wait_for(
                loop.run_in_executor(None, _sync_fetch), timeout=timeout_sec
            )
            if html and html.strip():
                return html, "Scrapling"
        except Exception:
            pass
        return None, ""

    async def _try_camoufox(self, url: str, timeout_sec: float = 18.0) -> Tuple[Optional[str], str]:
        """Tier 2: Camoufox basic headless browser."""
        if not CAMOUFOX_AVAILABLE:
            return None, ""
        try:
            async with _camoufox_semaphore:
                try:
                    from camoufox.async_api import AsyncCamoufox
                    async with AsyncCamoufox(
                        headless=True, block_images=True, i_know_what_im_doing=True
                    ) as browser:
                        page = await browser.new_page()
                        await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_sec * 1000))
                        content = await page.content()
                        if content and content.strip():
                            return content, "Camoufox Basic"
                except Exception:
                    from camoufox.sync_api import Camoufox
                    loop = asyncio.get_running_loop()

                    def _sync_camoufox():
                        with Camoufox(headless=True, block_images=True, i_know_what_im_doing=True) as browser:
                            page = browser.new_page()
                            page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_sec * 1000))
                            return page.content()

                    content = await asyncio.wait_for(
                        loop.run_in_executor(None, _sync_camoufox), timeout=timeout_sec + 2.0
                    )
                    if content and content.strip():
                        return content, "Camoufox Basic"
        except Exception:
            pass
        return None, ""

    async def _try_camoufox_stealth(self, url: str, timeout_sec: float = 25.0) -> Tuple[Optional[str], str]:
        """Tier 3: Camoufox stealth browser with humanize=True."""
        if not CAMOUFOX_AVAILABLE:
            return None, ""
        try:
            async with _camoufox_semaphore:
                try:
                    from camoufox.async_api import AsyncCamoufox
                    async with AsyncCamoufox(
                        headless=True, humanize=True, block_images=True, i_know_what_im_doing=True
                    ) as browser:
                        page = await browser.new_page()
                        await page.goto(url, wait_until="domcontentloaded", timeout=int(timeout_sec * 1000))
                        content = await page.content()
                        if content and len(content.strip()) > 50:
                            return content, "Camoufox Full Stealth"
                except Exception:
                    pass
        except Exception:
            pass
        return None, ""

    async def fetch(
        self, session: Optional[aiohttp.ClientSession], url: str
    ) -> Tuple[Optional[str], str]:
        """Progressively try all tiers until one returns valid HTML."""
        # Tier 0: HTTP Requests
        html, method = await self._try_http(session, url)
        if html:
            return html, method

        # Tier 1: Scrapling
        html, method = await self._try_scrapling(url)
        if html:
            return html, method

        # Tier 2: Camoufox Basic
        html, method = await self._try_camoufox(url)
        if html:
            return html, method

        # Tier 3: Camoufox Full Stealth
        html, method = await self._try_camoufox_stealth(url)
        if html:
            return html, method

        return None, "FAILED"

    async def scrape_domain(
        self, session: Optional[aiohttp.ClientSession], domain: str
    ) -> Tuple[str, str]:
        """
        Scrapes a domain using the progressive waterfall model.
        Returns: (clean_text, winning_method)
        """
        domain_clean = domain.strip().lower()
        if not domain_clean.startswith("http://") and not domain_clean.startswith("https://"):
            start_url = f"https://{domain_clean}"
        else:
            start_url = domain_clean

        # Attempt primary HTTPS URL
        html, method = await self.fetch(session, start_url)

        # Fallback to HTTP if HTTPS failed completely
        if not html and start_url.startswith("https://"):
            http_fallback = "http://" + start_url[8:]
            html, method = await self.fetch(session, http_fallback)
            if html:
                start_url = http_fallback

        if not html:
            return "", "FAILED"

        main_text, inner_links = clean_html_to_text(html, start_url)
        accumulated_text = main_text

        # If homepage text is sparse (< 1000 chars), fetch up to 1 high-value subpage (about / company)
        if len(main_text) < 1000 and inner_links:
            sub_url = None
            for link in inner_links:
                path = urlparse(link).path.lower()
                if any(k in path for k in ("about", "company", "who-we-are", "overview")):
                    sub_url = link
                    break
            if sub_url:
                sub_html, _ = await self._try_http(session, sub_url, timeout_sec=6.0)
                if not sub_html:
                    sub_html, _ = await self._try_scrapling(sub_url, timeout_sec=8.0)
                if sub_html:
                    sub_text, _ = clean_html_to_text(sub_html, sub_url)
                    if sub_text:
                        accumulated_text = f"{accumulated_text}\n\n{sub_text}".strip()

        # Bounded cell text length safe for Google Sheets
        return accumulated_text[: self.max_chars], method
