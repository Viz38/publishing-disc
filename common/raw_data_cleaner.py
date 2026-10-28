"""
Discovery Engine — Dual-Heuristic Raw Data Cleaner
Specialized cleaning passes for Tech Crawler Markdown and Waterfall Scraper HTML.
Optimized for the Homepage Scrape Invariant.
"""
import re
from typing import Tuple, List, Optional
from bs4 import BeautifulSoup

# ==========================================
# 1. TECH CRAWLER (TC) MARKDOWN CLEANER
# ==========================================

RE_BASE64 = re.compile(r'data:image\/[a-zA-Z]+;base64,[A-Za-z0-9+/=]+')
RE_MD_IMAGE = re.compile(r'!\[.*?\]\(.*?\)')
RE_SVG = re.compile(r'<svg[\s\S]*?<\/svg>', re.IGNORECASE)
RE_SCRIPTS = re.compile(r'<(script|style|noscript)[\s\S]*?<\/\1>', re.IGNORECASE)
RE_HTML_COMMENTS = re.compile(r'<!--[\s\S]*?-->')

# Transform [Anchor Text](URL) -> Anchor Text
RE_MD_LINKS_HTTP = re.compile(r'\[([^\]]+)\]\(https?:\/\/[^\)]+\)')
RE_MD_LINKS_NULL = re.compile(r'\[([^\]]+)\]\(null\)')
RE_MD_LINKS_REL = re.compile(r'\[([^\]]+)\]\(\/[^\)]*\)')

# Country phone picker dumps: - [+1(USA)] - [+44(UK)]...
RE_PHONE_PICKERS = re.compile(r'(?:-\s*\+\d{1,4}\([^\)]+\)\s*){4,}')

# Review and testimonial carousels
RE_REVIEWS_STARS = re.compile(
    r'(?:\d\.?\d?\s*(?:\/|out of)\s*5\s*stars?|[★⭐]{3,5})[\s\S]{0,300}?(?:Highly recommend|great service|Verified Purchase|satisfaction guaranteed)',
    re.IGNORECASE
)
RE_REVIEWS_BLOCK = re.compile(
    r'(?:Customer Reviews?|What our clients say|Testimonials)[\s\S]{0,1500}?(?:View all reviews|Leave a review|Read more)',
    re.IGNORECASE
)

# Cookie / GDPR consent banners
RE_COOKIE_BANNER = re.compile(
    r'(?:We use cookies|This website uses cookies|By clicking [^\n]{0,30}Accept|Cookie Policy|Manage Cookies)[^\n\.]{0,250}\.?',
    re.IGNORECASE
)

# Footer legal text (matched in second half of text)
RE_LEGAL_COPYRIGHT = re.compile(
    r'(?:Copyright|©|\(c\))\s*\d{4}[^\n]{0,150}',
    re.IGNORECASE
)
RE_ALL_RIGHTS_RESERVED = re.compile(r'All [Rr]ights [Rr]eserved\.?', re.IGNORECASE)
RE_LEGAL_LINKS = re.compile(
    r'(?:Privacy Policy|Terms of (?:Service|Use)|Terms & Conditions|Cookie Settings)\s*(?:\|\s*)?',
    re.IGNORECASE
)
RE_SOCIAL_LINKS = re.compile(
    r'(?:Follow us on|Connect with us):?\s*(?:Facebook|Twitter|LinkedIn|Instagram|YouTube|TikTok)?',
    re.IGNORECASE
)

def strip_tc_header_nav(text: str) -> str:
    """
    Strips top homepage navigation menus that appear before the main hero/headline.
    Identifies pure link list bullets (* [About](...), * [Contact](...)) at the top of the body.
    """
    frontmatter = ""
    body = text
    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) >= 3:
            frontmatter = f"---{parts[1]}---\n\n"
            body = parts[2]

    lines = body.splitlines()
    body_lines = []
    in_header_nav = True

    for idx, line in enumerate(lines):
        line_s = line.strip()
        if not line_s:
            if not in_header_nav:
                body_lines.append(line)
            continue

        if in_header_nav and idx < 30:
            is_nav_bullet = bool(re.match(r'^[\*\-\+]\s*\[.*?\]', line_s) or re.match(r'^\[.*?\]\(.*?\)$', line_s))
            is_nav_keyword = any(k in line_s.lower() for k in [
                'skip to content', 'toggle navigation', 'menu', 'sign in', 'log in', 'cart', 'my account'
            ])
            if (is_nav_bullet and len(line_s) < 80) or is_nav_keyword:
                continue

            if line_s.startswith('# ') or len(line_s) > 60:
                in_header_nav = False

        body_lines.append(line)

    return frontmatter + "\n".join(body_lines)

def clean_tc_markdown(text: str) -> str:
    """
    Cleans Tech Crawler markdown text.
    Preserves all company descriptions, offerings, and product titles while stripping
    header navigation menus, URL paths, images, SVGs, review carousels, cookie notices, and footer sitemaps.
    """
    if not text or not isinstance(text, str):
        return ""

    t = text
    # 1. Base64 & Media
    t = RE_BASE64.sub('', t)
    t = RE_MD_IMAGE.sub('', t)
    t = RE_SVG.sub('', t)
    t = RE_SCRIPTS.sub('', t)
    t = RE_HTML_COMMENTS.sub('', t)

    # 2. Strip homepage header navigation menu
    t = strip_tc_header_nav(t)

    # 3. Markdown link targets -> anchor text
    t = RE_MD_LINKS_HTTP.sub(r'\1', t)
    t = RE_MD_LINKS_NULL.sub(r'\1', t)
    t = RE_MD_LINKS_REL.sub(r'\1', t)

    # 4. Country phone trees
    t = RE_PHONE_PICKERS.sub('', t)

    # 5. Review / Testimonial carousels
    t = RE_REVIEWS_STARS.sub('', t)
    t = RE_REVIEWS_BLOCK.sub('', t)

    # 6. Cookie consent
    t = RE_COOKIE_BANNER.sub('', t)

    # 7. Tail legal anchors and sitemaps (in second half of text)
    halfway = len(t) // 2
    head = t[:halfway]
    tail = t[halfway:]
    tail = RE_LEGAL_COPYRIGHT.sub('', tail)
    tail = RE_ALL_RIGHTS_RESERVED.sub('', tail)
    tail = RE_LEGAL_LINKS.sub('', tail)
    tail = RE_SOCIAL_LINKS.sub('', tail)
    t = head + tail

    # 8. Normalize whitespace
    t = re.sub(r'(\n\s*){3,}', '\n\n', t)
    t = re.sub(r'[ \t]{2,}', ' ', t)
    return t.strip()


# ===============================================
# 2. WATERFALL SCRAPER (WF) DOM & TEXT CLEANER
# ===============================================

WF_NOISE_CLASSES = re.compile(
    r'(?:header|footer|navbar|nav-menu|cookie|consent|banner|popup|modal|testimonial|reviews?|social-share|sidebar|widget|cart-drawer|newsletter)',
    re.IGNORECASE
)

RE_WF_SPINNERS = re.compile(r'(?:Cargando|Loading)\s*\d{1,3}%', re.IGNORECASE)
RE_WF_UI_ARTIFACTS = re.compile(
    r'(?:Skip to (?:main )?content|Toggle navigation|Click to enlarge|Search here\.{0,3}|Search our site)',
    re.IGNORECASE
)
RE_WF_CART = re.compile(
    r'(?:Shopping Cart|My Cart|Cart:\s*\d+\s*items?|Checkout|View Cart)',
    re.IGNORECASE
)

def clean_waterfall_html_dom(soup: BeautifulSoup) -> None:
    """
    In-place decomposition of boilerplate DOM elements in Waterfall Scraper HTML.
    Removes semantic noise tags and non-semantic noise containers (by class/id/role).
    Protects semantic content containers (<main>, <article>, <body>).
    """
    # 1. Semantic noise tags
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "svg", "canvas", "form", "iframe", "video", "audio"]):
        tag.decompose()

    # 2. Noise containers by class and id
    noise_elements = soup.find_all(attrs={"class": WF_NOISE_CLASSES}) + soup.find_all(attrs={"id": WF_NOISE_CLASSES})
    for el in noise_elements:
        if el and el.name not in ("main", "article", "body", "html"):
            el.decompose()

def clean_waterfall_text(text: str) -> str:
    """Post-extraction text filtering for Waterfall Scraper output."""
    if not text:
        return ""
    t = text
    t = RE_WF_SPINNERS.sub('', t)
    t = RE_WF_UI_ARTIFACTS.sub('', t)
    t = RE_WF_CART.sub('', t)

    halfway = len(t) // 2
    head = t[:halfway]
    tail = t[halfway:]
    tail = RE_LEGAL_COPYRIGHT.sub('', tail)
    tail = RE_ALL_RIGHTS_RESERVED.sub('', tail)
    tail = RE_LEGAL_LINKS.sub('', tail)
    t = head + tail
    return re.sub(r'\s{2,}', ' ', t).strip()
