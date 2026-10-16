"""
Common package for Discovery Engine.
Contains modular utilities, key managers, scrapers, API clients, and the streaming pipeline coordinator.
"""

from .key_manager import TracxnKeyManager, NIMKeyManager, get_sharded_nim_keys
from .tech_crawler import TechCrawlerClient
from .waterfall_scraper import (
    WaterfallScraper,
    clean_html_to_text,
    is_permanent_network_error,
    is_parked_domain,
)
from .local_cache import (
    LocalSheetCache,
    ExecutionTracker,
    is_5am_ist_cutoff,
    is_pre_cutoff_window,
    get_now_ist,
    get_ist_timestamp,
    safe_str,
)
from .backlog_manager import BacklogManager
from .tracxn_client import TracxnClient
from .pipeline_stream import PipelineCoordinator
from .system_health import trim_memory, SystemHealthMonitor

__all__ = [
    "TracxnKeyManager",
    "NIMKeyManager",
    "get_sharded_nim_keys",
    "TechCrawlerClient",
    "WaterfallScraper",
    "clean_html_to_text",
    "is_permanent_network_error",
    "is_parked_domain",
    "LocalSheetCache",
    "ExecutionTracker",
    "is_5am_ist_cutoff",
    "is_pre_cutoff_window",
    "get_now_ist",
    "get_ist_timestamp",
    "safe_str",
    "BacklogManager",
    "TracxnClient",
    "PipelineCoordinator",
    "trim_memory",
    "SystemHealthMonitor",
]
