"""
GSheet Circuit Breaker for Discovery Engine.
Prevents infinite retry loops and log flooding when a Google Sheet reaches document size limits
(e.g., APIError: [400]: This document is too large to continue editing.).
Ensures data is safely persisted to local CSV cache, and skips further GSheet write actions for the session.
"""

import os
import csv
import logging
import threading
from datetime import datetime
from typing import Any, List, Optional

DOC_TOO_LARGE_KEYWORDS = [
    "this document is too large to continue editing",
    "document is too large",
    "document is too large to edit",
    "maximum document size exceeded"
]

def is_doc_too_large_error(ex: Exception) -> bool:
    """Checks whether an exception indicates Google Sheets document size/cell limit has been reached."""
    if not ex:
        return False
    msg = str(ex).lower()
    return any(kw in msg for kw in DOC_TOO_LARGE_KEYWORDS)

CSV_ONLY_MARKER_FILE = ".csv_only_mode"

def is_csv_only_mode(sheet_dir: str) -> bool:
    """
    Returns True if the engine is operating in CSV-only mode:
    1. If the .csv_only_mode marker file exists, or
    2. If pending_gsheet_sync.csv exists and contains unpushed rows.
    """
    if not sheet_dir or not os.path.exists(sheet_dir):
        return False

    marker = os.path.join(sheet_dir, CSV_ONLY_MARKER_FILE)
    if os.path.exists(marker):
        return True

    sync_file = os.path.join(sheet_dir, "pending_gsheet_sync.csv")
    if os.path.exists(sync_file) and os.path.getsize(sync_file) > 0:
        try:
            with open(sync_file, "r", encoding="utf-8", errors="ignore") as f:
                lines = [line.strip() for line in f if line.strip()]
                if len(lines) > 1:  # More than just the header line
                    return True
        except Exception:
            pass

    return False

def set_csv_only_mode(sheet_dir: str, enabled: bool = True) -> None:
    """Sets or clears the .csv_only_mode marker file in sheet_dir."""
    if not sheet_dir:
        return
    marker = os.path.join(sheet_dir, CSV_ONLY_MARKER_FILE)
    try:
        if enabled:
            os.makedirs(sheet_dir, exist_ok=True)
            with open(marker, "w", encoding="utf-8") as f:
                f.write(f"enabled_at={datetime.now().isoformat()}\n")
        else:
            if os.path.exists(marker):
                os.remove(marker)
    except Exception as e:
        logging.warning(f"Could not update {marker}: {e}")

class GSheetCircuitBreaker:
    """
    Thread-safe circuit breaker for Google Sheets write operations.
    When tripped, signals all sheet writers to bypass network updates to Google Sheets,
    avoiding recursive retries, 400 errors, and log file bloat.
    """
    def __init__(self, name: str = "DiscoverySheet", sheet_dir: Optional[str] = None):
        self.name = name
        self.sheet_dir = sheet_dir
        self._tripped = False
        self._trip_reason = ""
        self._tripped_at: Optional[datetime] = None
        self._lock = threading.Lock()

    def is_tripped(self) -> bool:
        with self._lock:
            return self._tripped

    def should_skip_gsheet(self) -> bool:
        return self.is_tripped()

    def trip(self, reason: str = "") -> None:
        with self._lock:
            if not self._tripped:
                self._tripped = True
                self._trip_reason = reason
                self._tripped_at = datetime.now()
                if self.sheet_dir:
                    set_csv_only_mode(self.sheet_dir, True)
                logging.critical(
                    f"🚨 [GSheetCircuitBreaker] Tripped for {self.name}! Reason: {reason}. "
                    "Skipping further Google Sheets updates for this session. "
                    "All data will be safely persisted to local CSV cache."
                )

    def reset(self) -> None:
        with self._lock:
            self._tripped = False
            self._trip_reason = ""
            self._tripped_at = None
            if self.sheet_dir:
                set_csv_only_mode(self.sheet_dir, False)
            logging.info(f"🔄 [GSheetCircuitBreaker] Reset for {self.name}.")

    def handle_error(self, ex: Exception) -> bool:
        """
        Evaluates an exception from a sheet write attempt.
        If it's a document size limit error, trips the circuit breaker and returns True.
        """
        if is_doc_too_large_error(ex):
            self.trip(str(ex))
            return True
        return False

    def record_pending_sync(
        self,
        sync_file_path: str,
        domain: str,
        row_number: int,
        start_col: int,
        values: List[Any]
    ) -> None:
        """
        Appends an unsynced row's metadata and serialized values to an append-only CSV file
        so it can be pushed in bulk once a new sheet is connected.
        """
        with self._lock:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(sync_file_path)), exist_ok=True)
                file_exists = os.path.exists(sync_file_path) and os.path.getsize(sync_file_path) > 0
                with open(sync_file_path, "a", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["timestamp", "domain", "row_number", "start_col", "values_json"])
                    import json
                    writer.writerow([
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        domain,
                        row_number,
                        start_col,
                        json.dumps(values, ensure_ascii=False)
                    ])
            except Exception as e:
                logging.warning(f"Failed to record pending sync for {domain}: {e}")
