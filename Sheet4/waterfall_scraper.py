import os, sys
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from common.waterfall_scraper import (
    WaterfallScraper,
    clean_html_to_text,
    is_permanent_network_error,
    is_parked_domain,
)

__all__ = ["WaterfallScraper", "clean_html_to_text", "is_permanent_network_error", "is_parked_domain"]

