"""
GSheet Circuit Breaker for Discovery Engine.
Prevents infinite retry loops and log flooding when a Google Sheet reaches document size limits
(e.g., APIError: [400]: This document is too large to continue editing.).
Ensures data is safely persisted to local CSV cache, and skips further GSheet write actions for the session.

Self-Healing / 1-Hour Recovery Loop:
When tripped into CSV-only mode, a background watcher checks every 1 hour (3600s) if the sheet
is responsive again. If responsive, it flushes accumulated pending backlog rows to Google Sheets,
archives the sync file, clears .csv_only_mode, and seamlessly restores normal GSheet mode.
"""

import os
import sys
import csv
csv.field_size_limit(sys.maxsize)
import time
import json
import logging
import threading
from datetime import datetime
from typing import Any, List, Optional, Callable

DOC_TOO_LARGE_KEYWORDS = [
    "this document is too large to continue editing",
    "document is too large",
    "document is too large to edit",
    "maximum document size exceeded"
]

def is_doc_too_large_error(ex: Optional[Exception]) -> bool:
    """Checks whether an exception indicates Google Sheets document size/cell limit has been reached."""
    if not ex:
        return False
    msg = str(ex).lower()
    return any(kw in msg for kw in DOC_TOO_LARGE_KEYWORDS)

CSV_ONLY_MARKER_FILE = ".csv_only_mode"

def is_csv_only_mode(sheet_dir: str) -> bool:
    """
    Returns True if the engine is operating in CSV-only mode.
    Per user rule: All sheets run in Gsheet mode by default.
    CSV mode is ONLY active when the .csv_only_mode marker file is explicitly set.
    """
    if not sheet_dir or not os.path.exists(sheet_dir):
        return False

    marker = os.path.join(sheet_dir, CSV_ONLY_MARKER_FILE)
    return os.path.exists(marker)

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

def check_sheet_responsiveness(sheet_id: str, sheet_name: str = "CF_IF_Engine_Database") -> bool:
    """
    Performs a lightweight read probe on the sheet to verify connectivity and responsiveness.
    """
    if not sheet_id:
        return False
    try:
        from common.pipeline_engine import get_service_account_credentials
        import googleapiclient.discovery
        creds = get_service_account_credentials(["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
        service = googleapiclient.discovery.build("sheets", "v4", credentials=creds, cache_discovery=False)
        res = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{sheet_name}!A1:B2"
        ).execute()
        return True
    except Exception as e:
        logging.warning(f"[GSheetCircuitBreaker] Probe failed for sheet {sheet_id} [{sheet_name}]: {e}")
        return False

def flush_pending_backlog_to_sheet(sheet_dir: str, sheet_id: str, sheet_name: str = "CF_IF_Engine_Database") -> int:
    """
    Flushes pending rows from pending_gsheet_sync.csv to the Google Sheet,
    safely masking Col AD, and rotating pending_gsheet_sync.csv to .bak.
    """
    if not sheet_dir or not sheet_id:
        return 0
    sync_file = os.path.join(sheet_dir, "pending_gsheet_sync.csv")
    if not os.path.exists(sync_file) or os.path.getsize(sync_file) == 0:
        return 0

    try:
        from common.pipeline_engine import get_service_account_credentials
        from common.dashboard_metrics import col_to_letter
        import googleapiclient.discovery
        creds = get_service_account_credentials(["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
        service = googleapiclient.discovery.build("sheets", "v4", credentials=creds, cache_discovery=False)

        batch_updates = []
        with open(sync_file, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for r in reader:
                if not r or len(r) < 5:
                    continue
                # Format: timestamp, domain, row_number, start_col, values_json
                try:
                    row_num = int(r[2])
                    start_col = int(r[3])
                    vals = json.loads(r[4])
                    # Mask Col AD (1-based index 30) if present in vals
                    ad_idx = 30 - start_col
                    if 0 <= ad_idx < len(vals):
                        if vals[ad_idx] and vals[ad_idx] != "Raw Data Appended - csv only":
                            vals[ad_idx] = "Raw Data Appended - csv only"

                    start_letter = col_to_letter(start_col)
                    end_letter = col_to_letter(start_col + len(vals) - 1)
                    rng = f"{sheet_name}!{start_letter}{row_num}:{end_letter}{row_num}"
                    batch_updates.append({"range": rng, "values": [vals]})
                except Exception:
                    continue

        if batch_updates:
            # Chunk into batches of 500
            for i in range(0, len(batch_updates), 500):
                sub_batch = batch_updates[i:i+500]
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=sheet_id,
                    body={"valueInputOption": "RAW", "data": sub_batch}
                ).execute()

        bak_file = f"{sync_file}.bak_{int(time.time())}"
        try:
            os.replace(sync_file, bak_file)
        except OSError:
            pass
        logging.info(f"✅ Successfully flushed {len(batch_updates)} pending updates to {sheet_id} and rotated backlog to {bak_file}")
        return len(batch_updates)
    except Exception as e:
        logging.error(f"Failed to flush pending backlog to sheet {sheet_id}: {e}")
        return 0

class GSheetCircuitBreaker:
    """
    Thread-safe circuit breaker for Google Sheets write operations.
    When tripped by document size/read-only limit, signals all sheet writers to bypass network updates,
    persisting writes to local CSV cache.

    Includes automatic 1-Hour Recovery Watcher:
    Periodically checks if the sheet is responsive again, flushes pending backlog data,
    clears .csv_only_mode, and seamlessly restores normal GSheet mode.
    """
    def __init__(
        self,
        name: str = "DiscoverySheet",
        sheet_dir: Optional[str] = None,
        failure_threshold: int = 3,
        sheet_id: Optional[str] = None,
        sheet_name: str = "CF_IF_Engine_Database",
        recovery_interval: int = 3600
    ):
        self.name = name
        self.sheet_dir = sheet_dir
        self.failure_threshold = max(1, failure_threshold)
        self.sheet_id = sheet_id
        self.sheet_name = sheet_name
        self.recovery_interval = max(10, recovery_interval)
        self._consecutive_write_failures = 0
        self._tripped = False
        self._trip_reason = ""
        self._tripped_at: Optional[datetime] = None
        self._recovery_thread: Optional[threading.Thread] = None
        self._stop_recovery = threading.Event()
        self._lock = threading.Lock()

        # If starting up and .csv_only_mode marker already exists, initialize recovery watcher
        if self.sheet_dir and is_csv_only_mode(self.sheet_dir):
            self._tripped = True
            self._trip_reason = "Initialized with existing .csv_only_mode marker"
            self.start_recovery_watcher()

    @property
    def consecutive_write_failures(self) -> int:
        with self._lock:
            return self._consecutive_write_failures

    def is_tripped(self) -> bool:
        with self._lock:
            return self._tripped

    def record_write_success(self) -> None:
        """Resets the consecutive write failure counter on any successful sheet write."""
        with self._lock:
            self._consecutive_write_failures = 0

    def record_write_failure(self, ex: Optional[Exception] = None) -> bool:
        """
        Records a write failure.
        Per user mandate: All sheets run in Gsheet mode. CSV mode is ONLY tripped
        when the Google Sheet becomes strictly read-only or exceeds document limits
        (e.g., APIError 400: This document is too large to continue editing).
        Transient write errors (429, timeouts, network glitches) will NOT trip to CSV mode.
        """
        with self._lock:
            self._consecutive_write_failures += 1

        if is_doc_too_large_error(ex):
            self.trip(f"Document size / read-only limit reached: {ex}")
            return True

        return False

    def should_skip_gsheet(self) -> bool:
        with self._lock:
            if self._tripped:
                return True
        if self.sheet_dir and is_csv_only_mode(self.sheet_dir):
            return True
        return False

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
                    "All data will be safely persisted to local CSV cache. "
                    f"Starting 1-hour periodic recovery probe (every {self.recovery_interval}s)..."
                )
        self.start_recovery_watcher()

    def reset(self) -> None:
        with self._lock:
            self._tripped = False
            self._trip_reason = ""
            self._tripped_at = None
            self._consecutive_write_failures = 0
            if self.sheet_dir:
                set_csv_only_mode(self.sheet_dir, False)
            logging.info(f"🔄 [GSheetCircuitBreaker] Reset to normal GSheet mode for {self.name}.")
        self.stop_recovery_watcher()

    def handle_error(self, ex: Exception) -> bool:
        """Evaluates an exception from a sheet write attempt."""
        return self.record_write_failure(ex)

    def probe_and_recover(
        self,
        probe_fn: Optional[Callable[[], bool]] = None,
        flush_fn: Optional[Callable[[], int]] = None
    ) -> bool:
        """
        Tests sheet responsiveness. If responsive, flushes pending backlog to Google Sheets,
        clears .csv_only_mode, resets the circuit breaker, and returns True.
        """
        # Determine probe function
        if probe_fn is None:
            if not self.sheet_id:
                return False
            probe_fn = lambda: check_sheet_responsiveness(self.sheet_id, self.sheet_name)

        logging.info(f"🔍 [GSheetCircuitBreaker] Probing responsiveness for {self.name}...")
        is_responsive = False
        try:
            is_responsive = probe_fn()
        except Exception as pe:
            logging.warning(f"Probe call failed: {pe}")
            is_responsive = False

        if not is_responsive:
            logging.info(f"⏳ [GSheetCircuitBreaker] Sheet {self.name} still unresponsive/locked. Retrying in {self.recovery_interval}s.")
            return False

        logging.info(f"🎉 [GSheetCircuitBreaker] Sheet {self.name} is responsive again! Flushing pending backlog...")
        if flush_fn is None:
            if self.sheet_dir and self.sheet_id:
                flush_fn = lambda: flush_pending_backlog_to_sheet(self.sheet_dir, self.sheet_id, self.sheet_name)
            else:
                flush_fn = lambda: 0

        try:
            flushed_count = flush_fn()
            logging.info(f"Flushed {flushed_count} pending rows for {self.name}.")
        except Exception as fe:
            logging.warning(f"Error during backlog flush for {self.name}: {fe}")

        self.reset()
        return True

    def _recovery_worker(self) -> None:
        logging.info(f"🚀 [Recovery Watcher] Started for {self.name} (probe interval: {self.recovery_interval}s).")
        while not self._stop_recovery.is_set():
            # Wait for recovery_interval in small slices so stop_recovery is prompt
            wait_slice = 5
            elapsed = 0
            while elapsed < self.recovery_interval and not self._stop_recovery.is_set():
                time.sleep(wait_slice)
                elapsed += wait_slice

            if self._stop_recovery.is_set():
                break

            try:
                recovered = self.probe_and_recover()
                if recovered:
                    logging.info(f"✅ [Recovery Watcher] Sheet {self.name} restored to normal GSheet mode!")
                    break
            except Exception as e:
                logging.warning(f"[Recovery Watcher] Probe exception for {self.name}: {e}")

    def start_recovery_watcher(self) -> None:
        with self._lock:
            if self._recovery_thread and self._recovery_thread.is_alive():
                return
            self._stop_recovery.clear()
            self._recovery_thread = threading.Thread(
                target=self._recovery_worker,
                name=f"RecoveryWatcher-{self.name}",
                daemon=True
            )
            self._recovery_thread.start()

    def stop_recovery_watcher(self) -> None:
        self._stop_recovery.set()

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
        so it can be pushed in bulk once normal GSheet mode is restored.
        """
        with self._lock:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(sync_file_path)), exist_ok=True)
                file_exists = os.path.exists(sync_file_path) and os.path.getsize(sync_file_path) > 0
                with open(sync_file_path, "a", encoding="utf-8", newline="") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["timestamp", "domain", "row_number", "start_col", "values_json"])
                    writer.writerow([
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        domain,
                        row_number,
                        start_col,
                        json.dumps(values, ensure_ascii=False)
                    ])
            except Exception as e:
                logging.warning(f"Failed to record pending sync for {domain}: {e}")
