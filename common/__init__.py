"""
Common package for Discovery Engine.
Contains modular utilities, key managers, scrapers, API clients, and the streaming pipeline coordinator.
"""

from .key_manager import TracxnKeyManager, NIMKeyManager, get_sharded_nim_keys
from .tech_crawler import TechCrawlerClient, is_valid_s3_url
from .waterfall_scraper import (
    WaterfallScraper,
    clean_html_to_text,
    is_permanent_network_error,
    is_parked_domain,
)
from .raw_data_cleaner import (
    clean_tc_markdown,
    strip_tc_header_nav,
    clean_waterfall_html_dom,
    clean_waterfall_text,
)
from .local_cache import (
    LocalSheetCache,
    ExecutionTracker,
    is_5am_ist_cutoff,
    is_pre_cutoff_window,
    is_before_4am_ist,
    is_nim_dead_domain,
    get_now_ist,
    get_ist_timestamp,
    safe_str,
    is_57_col_schema,
)
from .backlog_manager import BacklogManager
from .tracxn_client import TracxnClient
from .pipeline_stream import PipelineCoordinator
from .system_health import trim_memory, SystemHealthMonitor
from .gsheet_circuit_breaker import GSheetCircuitBreaker, is_doc_too_large_error, is_csv_only_mode, set_csv_only_mode
from .bulk_pusher import BulkSheetPusher, clear_sheet_data_rows, handle_5am_sheet_cleanup
from .tracxn_domain_ingestor import TracxnDomainIngestor
from .bulk_sync import bulk_sync_cache_to_sheet, group_bulk_sync_chunks
from .schema_constants import TOTAL_SCHEMA_COLUMNS, COLUMN_INDEX_MAP
from .fleet_cluster import FleetClusterManager, FleetSummarizeRequest
from .pipeline_engine import configure_engine, run_engine_for_sheet

__all__ = [
    "TracxnKeyManager",
    "NIMKeyManager",
    "get_sharded_nim_keys",
    "TechCrawlerClient",
    "is_valid_s3_url",
    "WaterfallScraper",
    "clean_html_to_text",
    "is_permanent_network_error",
    "is_parked_domain",
    "clean_tc_markdown",
    "strip_tc_header_nav",
    "clean_waterfall_html_dom",
    "clean_waterfall_text",
    "LocalSheetCache",
    "ExecutionTracker",
    "is_5am_ist_cutoff",
    "is_pre_cutoff_window",
    "is_before_4am_ist",
    "get_now_ist",
    "get_ist_timestamp",
    "safe_str",
    "is_57_col_schema",
    "BacklogManager",
    "TracxnClient",
    "PipelineCoordinator",
    "trim_memory",
    "SystemHealthMonitor",
    "GSheetCircuitBreaker",
    "is_doc_too_large_error",
    "is_csv_only_mode",
    "set_csv_only_mode",
    "BulkSheetPusher",
    "clear_sheet_data_rows",
    "handle_5am_sheet_cleanup",
    "TracxnDomainIngestor",
    "bulk_sync_cache_to_sheet",
    "group_bulk_sync_chunks",
    "FleetClusterManager",
    "FleetSummarizeRequest",
    "configure_engine",
    "run_engine_for_sheet",
]

