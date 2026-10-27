import os
import csv
import json
import threading
import logging
import datetime
import shutil
from typing import Dict, Any, Optional, Tuple, List

IST_TZ = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

def get_now_ist() -> datetime.datetime:
    """Returns the current datetime in Indian Standard Time (IST, UTC+5:30)."""
    return datetime.datetime.now(IST_TZ)

def is_5am_ist_cutoff(current_dt: Optional[datetime.datetime] = None, now_dt: Optional[datetime.datetime] = None) -> bool:
    """
    Returns True if current time in IST (UTC+5:30) is within the daily cutoff window (23:30:00 - 23:34:59 IST).
    Apps Script / Ingestion refreshes domains at 11:30 PM IST, so any pipeline running in this window must be halted.
    """
    dt = current_dt or now_dt
    if dt is None:
        dt = get_now_ist()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST_TZ)
    else:
        dt = dt.astimezone(IST_TZ)
    return dt.hour == 23 and dt.minute >= 30 and dt.minute < 35

is_cutoff_window = is_5am_ist_cutoff

def is_pre_cutoff_window(current_dt: Optional[datetime.datetime] = None, now_dt: Optional[datetime.datetime] = None) -> bool:
    """
    Returns True if current time in IST (UTC+5:30) is within the Pre-Cutoff Flush window (23:00:00 - 23:29:59 IST).
    During this 11:00 PM - 11:30 PM window, no new URLs are accepted for scraping; all NIM-summarized domains are flushed
    through Gemini predictions and Review & Write APIs so they are marked DONE before 11:30 PM.
    """
    dt = current_dt or now_dt
    if dt is None:
        dt = get_now_ist()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST_TZ)
    else:
        dt = dt.astimezone(IST_TZ)
    return dt.hour == 23 and dt.minute < 30

def is_before_4am_ist(current_dt: Optional[datetime.datetime] = None, now_dt: Optional[datetime.datetime] = None) -> bool:
    """
    Returns True if current time in IST (UTC+5:30) has not reached the 11:00 PM IST cutoff.
    Between 23:00:00 and 23:35:00 IST, pipeline retries of previously failed phases are halted
    in preparation for the 11:30 PM daily refresh window.
    """
    dt = current_dt or now_dt
    if dt is None:
        dt = get_now_ist()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST_TZ)
    else:
        dt = dt.astimezone(IST_TZ)
    if dt.hour == 23:
        return False
    return True

is_before_cutoff_retry = is_before_4am_ist


def is_nim_dead_domain(raw_data: str) -> bool:
    """
    Returns True if scraped raw data represents a dead/unreachable domain or 404/DNS error.
    Such domains cannot produce a valid company summary and are marked non-retryable ('NO_CONTENT').
    """
    if not raw_data or not str(raw_data).strip():
        return True
    s = str(raw_data).strip().lower()
    dead_signals = [
        "website is not working",
        "dns_probe_finished_nxdomain",
        "404 not found",
        "err_name_not_resolved",
        "domain has expired",
        "domain expired",
        "domain is parked",
        "this domain is parked",
        "buy this domain",
        "buy it now",
        "this site can't be reached",
        "page not found",
        "server ip address could not be found"
    ]
    for d in dead_signals:
        if d in s and len(s) < 500:
            return True
    return False

def get_ist_timestamp() -> str:
    """Returns formatted IST timestamp for logging and sheet cells."""
    return get_now_ist().strftime("%d/%b/%Y %H:%M:%S")

def get_cycle_date_ist(current_dt: Optional[datetime.datetime] = None) -> str:
    """
    Returns the daily processing cycle date string (YYYY-MM-DD) in IST.
    Daily domains refresh and auto-start at 11:35 PM IST (23:35 IST).
    Therefore, the window from 23:35:00 to 23:59:59 IST belongs to the cycle
    operating primarily on the next calendar day (days=+1).
    From 00:00:00 to 23:34:59 IST, it belongs to the current calendar day.
    """
    if current_dt is None:
        current_dt = get_now_ist()
    elif current_dt.tzinfo is None:
        current_dt = current_dt.replace(tzinfo=IST_TZ)
    else:
        current_dt = current_dt.astimezone(IST_TZ)

    if current_dt.hour == 23 and current_dt.minute >= 35:
        current_dt = current_dt + datetime.timedelta(days=1)
    return current_dt.strftime("%Y-%m-%d")

def safe_str(val: Any) -> str:
    """Safely converts any value to string, stripping whitespace."""
    if val is None:
        return ""
    return str(val).strip()

LEGACY_SHEET_IDS = {
    "1Sil5fw0l0rc0aMoydtnn7ZHdDbY5hwl7D9rda7bETkc",
    "1xphjNDv0hHEr8ZXv4PEVDmnEeVrsw_XAUMGaDf8ySUY",
    "10pacDW-6EqAdWslv9DTJBrRZDsybMczwMSpmKJebWFc",
    "14kC-RUMrpJB1Ur4CGpXMh1Te-yv7qBjsINJgM7NqySY"
}

def is_57_col_schema(row: Optional[list] = None, sheet_id: Optional[str] = None) -> bool:
    """
    Returns True. All active production sheets strictly follow the 57-column master schema.
    Legacy 70/93-column schema is eliminated.
    """
    return True

class LocalSheetCache:
    """
    Local CSV cache for Discovery Engine pipelines.
    Provides redundancy, prevents re-doing already completed steps,
    and supports granular step-level retry.
    """
    CSV_FIELDS = [
        "domain", "enginePickedTime", "engineClearedTime", "engineClearedStatus", "engineTimeTaken",
        "tc_job_id", "tc_job_gen_time", "tc_status", "tc_final_url", "tc_s3_html",
        "tc_s3_markdown", "tc_http_status", "tc_signals", "tc_error_code", "tc_error_reason", "scrappingBeautySoup",
        "summary_status", "output_data_summery",
        "output_data_if", "output_data_if_name", "output_data_if_id",
        "output_data_if_validation", "output_data_feed",
        "final_feed_name", "final_feed_id", "final_pub_funnel_name", "final_pub_funnel_id",
        "cf_if_status", "cf_if_time", "if_pf_status", "if_pf_time",
        "tag_status_if", "tag_status_pf", "discovery_bq", "gsheet_synced", "updated_at"
    ]

    def __init__(self, csv_path: Optional[str] = None, cache_dir: Optional[str] = None, filename: str = ".local_cache.csv"):
        if csv_path:
            self.csv_path = csv_path
        elif cache_dir:
            self.csv_path = os.path.join(cache_dir, filename)
        else:
            self.csv_path = os.path.join(os.getcwd(), filename)

        self._cache: Dict[str, Dict[str, str]] = {}
        self._lock = threading.Lock()
        self._appended_count: int = 0
        self._compaction_threshold: int = int(os.getenv("CACHE_COMPACT_THRESHOLD", "1000"))
        self.load()

    def _normalize(self, domain: str) -> str:
        if not domain:
            return ""
        d = str(domain).strip().lower()
        if d.startswith("http://"):
            d = d[7:]
        elif d.startswith("https://"):
            d = d[8:]
        d = d.strip("/")
        if d.startswith("www."):
            d = d[4:]
        return d

    def load(self):
        with self._lock:
            if not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0:
                return
            try:
                with open(self.csv_path, mode="r", encoding="utf-8", newline="", errors="ignore") as f:
                    reader = csv.DictReader(f)
                    count = 0
                    for row in reader:
                        norm_d = self._normalize(row.get("domain", ""))
                        if norm_d:
                            self._cache[norm_d] = dict(row)
                            count += 1
                    logging.info(f"Loaded {count} cached domain records from local cache: {self.csv_path}")
            except Exception as e:
                logging.warning(f"Could not load local cache from {self.csv_path}: {e}")

    def get(self, domain: str) -> Optional[Dict[str, str]]:
        norm_d = self._normalize(domain)
        with self._lock:
            return self._cache.get(norm_d)

    def get_record(self, domain: str) -> Optional[Dict[str, str]]:
        """Alias for get(domain) for compatibility."""
        return self.get(domain)

    def update(self, domain: str, record: Dict[str, Any]):
        """Alias for save_record for compatibility."""
        return self.save_record(domain, record)

    def compact(self, force: bool = False) -> None:
        """
        Deduplicates the local cache CSV file on disk.
        Atomically rewrites the file containing only the latest unique record per domain.
        """
        with self._lock:
            if not force and self._appended_count < self._compaction_threshold:
                return

            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.csv_path)), exist_ok=True)
                tmp_path = f"{self.csv_path}.tmp.{os.getpid()}"
                with open(tmp_path, mode="w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS)
                    writer.writeheader()
                    for r in self._cache.values():
                        writer.writerow(r)
                os.replace(tmp_path, self.csv_path)
                self._appended_count = 0
                logging.debug(f"Compacted {len(self._cache)} unique domain records to {self.csv_path}")
            except Exception as e:
                logging.warning(f"Error compacting local cache to {self.csv_path}: {e}")

    def save_record(self, domain: str, record: Dict[str, Any]):
        """
        Appends an incremental domain update in O(1) time without rewriting the whole CSV.
        Maintains instant in-memory consistency and 100% crash durability.
        """
        norm_d = self._normalize(domain)
        if not norm_d:
            return

        with self._lock:
            existing = self._cache.get(norm_d, {})
            merged = dict(existing)
            for k, v in record.items():
                if v is not None and str(v).strip() != "":
                    # Cap string size to prevent CSV bloat while strictly preserving full 49k raw text
                    merged[k] = str(v)[:49500]

            merged["domain"] = norm_d
            self._cache[norm_d] = merged
            full_rec = {k: str(merged.get(k, "") or "") for k in self.CSV_FIELDS}

            # Fast incremental append in O(1) time
            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.csv_path)), exist_ok=True)
                file_exists = os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0
                with open(self.csv_path, mode="a", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(full_rec)
                self._appended_count += 1
            except Exception as e:
                logging.warning(f"Error appending record to local cache {self.csv_path}: {e}")

        # Debounced compaction check outside append lock if threshold met
        if self._appended_count >= self._compaction_threshold:
            self.compact(force=True)

    def rotate_daily_cache(self, archive_dir: str, current_dt: Optional[datetime.datetime] = None) -> str:
        """
        Rotates the current discovery_cache.csv into archive_dir with a date stamp
        (e.g. discovery_cache_YYYY-MM-DD.csv) and starts a fresh discovery_cache.csv.
        Clears the in-memory cache index.
        """
        if current_dt is None:
            current_dt = get_now_ist()
        elif current_dt.tzinfo is None:
            current_dt = current_dt.replace(tzinfo=IST_TZ)
        else:
            current_dt = current_dt.astimezone(IST_TZ)

        os.makedirs(archive_dir, exist_ok=True)
        date_str = current_dt.strftime("%Y-%m-%d")
        archive_filename = f"discovery_cache_{date_str}.csv"
        archive_filepath = os.path.join(archive_dir, archive_filename)

        with self._lock:
            # If cache file exists and has content, archive it
            if os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0:
                try:
                    shutil.copy2(self.csv_path, archive_filepath)
                    logging.info(f"📦 Archived daily cache to {archive_filepath}")
                except Exception as e:
                    logging.warning(f"Could not archive daily cache to {archive_filepath}: {e}")

            # Reset in-memory cache
            self._cache.clear()

            # Reinitialize fresh discovery_cache.csv with headers
            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.csv_path)), exist_ok=True)
                tmp_file = f"{self.csv_path}.tmp.{os.getpid()}"
                with open(tmp_file, mode="w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS)
                    writer.writeheader()
                os.replace(tmp_file, self.csv_path)
                logging.info(f"✨ Fresh daily discovery cache initialized at {self.csv_path}")
            except Exception as e:
                logging.error(f"Error initializing fresh daily cache: {e}")

        return archive_filepath

    def is_fully_completed(self, domain: str) -> bool:
        rec = self.get(domain)
        if not rec:
            return False
        has_summary = bool(rec.get("output_data_summery"))
        has_if = bool(rec.get("output_data_if"))
        valid_statuses = {
            "DONE", "YES", "TRUE", "MOVED", "MOVED_OUT",
            "SNOOZED", "MARKED_IRRELEVANT", "MARKED_FOR_LATER", "ESCALATED", "SKIPPED_UNASSIGNABLE", "SUCCESS", "SKIPPED"
        }
        cf_if_s = str(rec.get("cf_if_status", "")).strip().upper()
        if_pf_s = str(rec.get("if_pf_status", "")).strip().upper()
        cf_if_t = str(rec.get("cf_if_time", "")).strip()
        if_pf_t = str(rec.get("if_pf_time", "")).strip()

        has_tracxn_movement = (
            cf_if_s in valid_statuses or
            if_pf_s in valid_statuses or
            (len(cf_if_t) >= 8 and (":" in cf_if_t or "/" in cf_if_t)) or
            (len(if_pf_t) >= 8 and (":" in if_pf_t or "/" in if_pf_t))
        )
        return has_summary and has_if and has_tracxn_movement

    def hydrate_row_from_cache(self, domain: str, row: List[Any]) -> Tuple[List[Any], bool]:
        """
        Merges existing sheet row with cached CSV data strictly adhering to the 57-column master schema.
        Returns (hydrated_row, can_direct_write).
        can_direct_write is True if all pipeline steps are present in CSV
        and Google Sheets was never updated (missing engineClearedTime/summary).
        """
        rec = self.get(domain)
        if not rec:
            return row, False

        hydrated = list(row)
        min_len = 57
        if len(hydrated) < min_len:
            hydrated = hydrated + [""] * (min_len - len(hydrated))
        elif len(hydrated) > min_len:
            hydrated = hydrated[:min_len]

        # Section 2: Engine Stats (Cols 18-20 / R-T / idx 17-19)
        if not hydrated[17] and rec.get("databaseAddedTime"):
            hydrated[17] = rec["databaseAddedTime"]
        if not hydrated[18] and rec.get("enginePickedTime"):
            hydrated[18] = rec["enginePickedTime"]
        if not hydrated[19] and rec.get("engineClearedTime"):
            hydrated[19] = rec["engineClearedTime"]

        # Section 3: Tech Crawler (Cols 21-34 / U-AH / idx 20-33)
        if not hydrated[20] and rec.get("engineClearedStatus"):
            hydrated[20] = rec["engineClearedStatus"]
        if not hydrated[21] and rec.get("tc_job_id"):
            hydrated[21] = rec["tc_job_id"]
        if not hydrated[22] and rec.get("tc_job_gen_time"):
            hydrated[22] = rec["tc_job_gen_time"]
        if not hydrated[23] and rec.get("tc_status"):
            hydrated[23] = rec["tc_status"]
        if not hydrated[24] and rec.get("tc_final_url"):
            hydrated[24] = rec["tc_final_url"]
        if not hydrated[25] and rec.get("tc_s3_html"):
            hydrated[25] = rec["tc_s3_html"]
        if not hydrated[26] and rec.get("tc_s3_markdown"):
            hydrated[26] = rec["tc_s3_markdown"]
        if not hydrated[27] and rec.get("tc_http_status"):
            hydrated[27] = rec["tc_http_status"]
        if not hydrated[28] and rec.get("tc_signals"):
            hydrated[28] = rec["tc_signals"]
        if not hydrated[29] and rec.get("scrappingBeautySoup"):
            hydrated[29] = str(rec["scrappingBeautySoup"])[:49000]
        if not hydrated[30] and rec.get("tc_job_gen_time"):
            hydrated[30] = rec["tc_job_gen_time"]
        if not hydrated[31] and rec.get("tc_final_url"):
            hydrated[31] = rec["tc_final_url"]

        # Section 4: NIM Summary & Gemini Predictions (Cols 35-49 / AI-AW / idx 34-48)
        if not hydrated[34] and rec.get("summary_status"):
            hydrated[34] = rec["summary_status"]
        if not hydrated[35] and rec.get("output_data_summery"):
            hydrated[35] = rec["output_data_summery"]
        if not hydrated[36] and rec.get("output_data_summery"):
            hydrated[36] = rec["output_data_summery"]
        if not hydrated[37] and rec.get("output_data_if"):
            hydrated[37] = rec["output_data_if"]
        if not hydrated[38] and rec.get("output_data_if_name"):
            hydrated[38] = rec["output_data_if_name"]
        if not hydrated[39] and rec.get("output_data_if_id"):
            hydrated[39] = rec["output_data_if_id"]
        if not hydrated[40] and rec.get("output_data_if_name"):
            hydrated[40] = rec["output_data_if_name"]
        if not hydrated[41] and rec.get("output_data_if_validation"):
            hydrated[41] = rec["output_data_if_validation"]
        if not hydrated[42] and rec.get("output_data_feed"):
            hydrated[42] = rec["output_data_feed"]
        if not hydrated[43] and rec.get("output_data_feed"):
            hydrated[43] = rec["output_data_feed"]  # Col AR (idx 43): Feed Number
        if not hydrated[44] and rec.get("final_feed_name"):
            hydrated[44] = rec["final_feed_name"]  # Col AS (idx 44): Feed Name
        if not hydrated[45] and rec.get("final_feed_id"):
            hydrated[45] = rec["final_feed_id"]    # Col AT (idx 45): Feed ID
        if not hydrated[46] and rec.get("final_pub_funnel_name"):
            hydrated[46] = rec["final_pub_funnel_name"]  # Col AU (idx 46): PF Name
        if not hydrated[47] and rec.get("final_pub_funnel_id"):
            hydrated[47] = rec["final_pub_funnel_id"]    # Col AV (idx 47): PF ID

        # Section 5: Tracxn Write APIs & Hashtags (Cols 50-57 / AX-BE / idx 49-56)
        if not hydrated[49] and rec.get("cf_if_status"):
            hydrated[49] = rec["cf_if_status"]  # Col AX (idx 49): CF->IF Movement
        if not hydrated[50] and rec.get("cf_if_time"):
            hydrated[50] = rec["cf_if_time"]    # Col AY (idx 50): Movement Time
        if not hydrated[51] and rec.get("if_pf_status"):
            hydrated[51] = rec["if_pf_status"]  # Col AZ (idx 51): IF->PF Movement
        if not hydrated[52] and rec.get("if_pf_time"):
            hydrated[52] = rec["if_pf_time"]    # Col BA (idx 52): Movement Time
        if not hydrated[53] and rec.get("tag_status_if"):
            hydrated[53] = rec["tag_status_if"]  # Col BB (idx 53): IF Hashtag
        if not hydrated[54] and rec.get("tag_status_pf"):
            hydrated[54] = rec["tag_status_pf"]  # Col BC (idx 54): PF Hashtag
        if not hydrated[55] and rec.get("tag_status_review"):
            hydrated[55] = rec["tag_status_review"]  # Col BD (idx 55): Review Hashtag
        if not hydrated[56] and rec.get("discovery_bq"):
            hydrated[56] = rec["discovery_bq"]  # Col BE (idx 56): Disc Movement Hashtag

        # Can directly write if fully completed in CSV but missing in sheet
        can_direct_write = self.is_fully_completed(domain) and (not str(row[19] if len(row) > 19 else "").strip() or not str(row[35] if len(row) > 35 else "").strip())
        return hydrated, can_direct_write


    @staticmethod
    def build_full_write_row(
        domain: str,
        hydrated_row: List[Any],
        is_cf_sheet: bool = True,
        record: Optional[Dict[str, Any]] = None,
        cache_instance: Optional[Any] = None
    ) -> List[Any]:
        """
        Legacy compatibility method redirected strictly to build_57col_write_row.
        Returns the exact 57-column row.
        """
        return LocalSheetCache.build_57col_write_row(domain, hydrated_row, record=record, cache_instance=cache_instance)


    @staticmethod
    def build_57col_write_row(
        domain: str,
        hydrated_row: List[Any],
        record: Optional[Dict[str, Any]] = None,
        cache_instance: Optional[Any] = None
    ) -> List[Any]:
        """
        Builds the 57-column row adhering strictly to the official Google Sheet layout:
        - Section 1 (Cols 1-17 / A-Q / idx 0-16): INPUT metadata
        - Section 2 (Cols 18-20 / R-T / idx 17-19): ENGINE STATS (Database Added Time, Pick Time, Clear Time)
        - Section 3 (Cols 21-34 / U-AH / idx 20-33): TECH CRAWLER STATUS (Status, Job ID, S3 URLs, Raw Data capped at 49k)
        - Section 4 (Cols 35-49 / AI-AW / idx 34-48): LLM PREDICTIONS (NIM Status, NIM Summary, Gemini IF, Feed Number, Feed Name, PF)
        - Section 5 (Cols 50-57 / AX-BE / idx 49-56): TRACXN APIs (CF->IF, Movement Time, IF->PF, Movement Time, Hashtags)
        """
        rec = record or (cache_instance.get(domain) if cache_instance else {}) or {}

        in_row = list(hydrated_row)
        if len(in_row) < 57:
            in_row = in_row + [""] * (57 - len(in_row))
        elif len(in_row) > 57:
            in_row = in_row[:57]

        row_out = [""] * 57

        # 1. INPUT (Cols 1-17 / A-Q / idx 0-16)
        for i in range(17):
            row_out[i] = in_row[i]

        # 2. ENGINE STATS (Cols 18-20 / R-T / idx 17-19)
        row_out[17] = in_row[17] or rec.get("databaseAddedTime", "")
        row_out[18] = in_row[18] or rec.get("enginePickedTime", "")
        row_out[19] = in_row[19] or rec.get("engineClearedTime", "")

        # 3. TECH CRAWLER STATUS (Cols 21-34 / U-AH / idx 20-33)
        row_out[20] = in_row[20] or rec.get("engineClearedStatus", "") or "DONE"
        row_out[21] = in_row[21] or rec.get("tc_job_id", "")
        row_out[22] = in_row[22] or rec.get("tc_job_gen_time", "")
        row_out[23] = in_row[23] or rec.get("tc_status", "")
        row_out[24] = in_row[24] or rec.get("tc_final_url", "")
        row_out[25] = in_row[25] or rec.get("tc_s3_html", "")
        row_out[26] = in_row[26] or rec.get("tc_s3_markdown", "")
        row_out[27] = in_row[27] or rec.get("tc_http_status", "") or ""
        row_out[28] = in_row[28] or rec.get("tc_signals", "") or ""

        # Raw Data at Col 30 (idx 29 / Col AD) - CAP AT 49,000 CHARS
        raw_content = rec.get("scrappingBeautySoup") or in_row[29] or ""
        row_out[29] = str(raw_content)[:49000]

        row_out[30] = in_row[30] or rec.get("tc_job_gen_time", "")
        row_out[31] = in_row[31] or rec.get("tc_final_url", "")
        row_out[32] = in_row[32] or rec.get("tc_error_code", "") or ""  # Col AG (idx 32): TC Error Code
        row_out[33] = in_row[33] or rec.get("tc_error_reason", "") or ""  # Col AH (idx 33): TC Error Reason

        # 4. LLM PREDICTIONS (Cols 35-49 / AI-AW / idx 34-48)
        summary_val = rec.get("output_data_summery") or in_row[35] or ""
        row_out[34] = in_row[34] or rec.get("summary_status", "") or ""
        row_out[35] = summary_val
        row_out[36] = summary_val  # Domain Summery context
        row_out[37] = rec.get("output_data_if") or in_row[37] or ""
        row_out[38] = rec.get("output_data_if_name") or in_row[38] or ""
        row_out[39] = rec.get("output_data_if_id") or in_row[39] or ""
        row_out[40] = row_out[38]  # Context
        row_out[41] = rec.get("output_data_if_validation") or in_row[41] or ""
        row_out[42] = rec.get("output_data_feed") or in_row[42] or ""
        row_out[43] = rec.get("output_data_feed") or in_row[43] or "" # Col AR (idx 43): Feed Number
        row_out[44] = rec.get("final_feed_name") or in_row[44] or ""  # Col AS (idx 44): Feed Name
        row_out[45] = rec.get("final_feed_id") or in_row[45] or ""    # Col AT (idx 45): Feed ID
        row_out[46] = rec.get("final_pub_funnel_name") or in_row[46] or "" # Col AU (idx 46): PF Name
        row_out[47] = rec.get("final_pub_funnel_id") or in_row[47] or ""   # Col AV (idx 47): PF ID
        row_out[48] = ""  # Funnel Recheck

        # 5. TRACXN APIs & HASHTAGS (Cols 50-57 / AX-BE / idx 49-56)
        row_out[49] = rec.get("cf_if_status") or in_row[49] or ""  # Col AX: CF->IF Movement
        row_out[50] = rec.get("cf_if_time") or in_row[50] or ""    # Col AY: Movement Time
        row_out[51] = rec.get("if_pf_status") or in_row[51] or ""  # Col AZ: IF->PF Movement
        row_out[52] = rec.get("if_pf_time") or in_row[52] or ""    # Col BA: Movement Time
        row_out[53] = rec.get("tag_status_if") or in_row[53] or ""  # Col BB: IF Hashtag (bu_llm_industry_prediction)
        row_out[54] = rec.get("tag_status_pf") or in_row[54] or ""  # Col BC: PF Hashtag (bu_llm_feed_prediction)
        row_out[55] = rec.get("tag_status_review") or in_row[55] or ""  # Col BD: Review Hashtag (domainformanualreview)

        # Col BE: Disc Movement Hashtag (bu_llm_discovery_processing)
        # Added ONLY if PF Hashtag (bu_llm_feed_prediction) is YES/TRUE
        pf_tag = str(row_out[54]).strip().upper()
        if pf_tag in ("YES", "TRUE"):
            row_out[56] = rec.get("discovery_bq") or in_row[56] or "YES"
        elif row_out[49] or row_out[51]:
            row_out[56] = "NO"
        else:
            row_out[56] = rec.get("discovery_bq") or in_row[56] or ""

        return row_out


def is_pid_alive(pid: Optional[int]) -> bool:
    """Checks if a process ID is currently running in the operating system."""
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class ExecutionTracker:
    """
    Manages persistent execution state in .execution_state.json across runs.
    Tracks start_row, mode, sheet_id, PID, timestamps, and termination reasons.
    Enables autonomous smart resumption when a run was interrupted externally
    (desktop shutdowns, power cuts, OOM killer, kill -9) while respecting user cancellations
    and daily 11:30 PM IST cutoffs.
    """
    MAX_AUTO_RETRIES = 5

    def __init__(self, state_file_path: str):
        self.state_file_path = state_file_path
        self._lock = threading.RLock()

    def get_state(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not os.path.exists(self.state_file_path):
                return None
            try:
                with open(self.state_file_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return None

    def _save_state(self, state: Dict[str, Any]):
        tmp = f"{self.state_file_path}.tmp.{os.getpid()}"
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.state_file_path)), exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
            os.replace(tmp, self.state_file_path)
        except Exception as e:
            logging.warning(f"Error saving execution state to {self.state_file_path}: {e}")

    def record_start(
        self,
        sheet: str,
        sheet_id: str,
        start_row: int,
        mode: str,
        pid: int,
        run_id: Optional[str] = None,
        current_dt: Optional[datetime.datetime] = None
    ) -> Dict[str, Any]:
        with self._lock:
            if current_dt is None:
                current_dt = get_now_ist()
            elif current_dt.tzinfo is None:
                current_dt = current_dt.replace(tzinfo=IST_TZ)
            else:
                current_dt = current_dt.astimezone(IST_TZ)

            # Preserve or check existing retry count
            existing = self.get_state() or {}
            retry_count = existing.get("retry_count", 0)

            now_str = current_dt.strftime("%Y-%m-%d %H:%M:%S")
            date_str = get_cycle_date_ist(current_dt)
            if not run_id:
                run_id = f"{sheet}_{current_dt.strftime('%Y%m%d_%H%M%S')}"

            state = {
                "run_id": run_id,
                "sheet": sheet,
                "sheet_id": sheet_id,
                "start_row": start_row,
                "mode": mode,
                "active_pid": pid,
                "status": "running",
                "termination_reason": None,
                "start_time_ist": now_str,
                "last_heartbeat_ist": now_str,
                "date_ist": date_str,
                "retry_count": retry_count
            }
            self._save_state(state)
            return state

    def update_heartbeat(self):
        with self._lock:
            state = self.get_state()
            if state and state.get("status") == "running":
                state["last_heartbeat_ist"] = get_now_ist().strftime("%Y-%m-%d %H:%M:%S")
                self._save_state(state)

    def record_completion(self):
        with self._lock:
            state = self.get_state()
            if state:
                state["status"] = "completed"
                state["termination_reason"] = "completed"
                state["active_pid"] = None
                state["retry_count"] = 0
                state["last_heartbeat_ist"] = get_now_ist().strftime("%Y-%m-%d %H:%M:%S")
                self._save_state(state)

    def record_user_stop(self):
        with self._lock:
            state = self.get_state()
            if state:
                state["status"] = "stopped"
                state["termination_reason"] = "user_cancelled"
                state["active_pid"] = None
                state["retry_count"] = 0
                state["last_heartbeat_ist"] = get_now_ist().strftime("%Y-%m-%d %H:%M:%S")
                self._save_state(state)

    def record_5am_cutoff(self):
        with self._lock:
            state = self.get_state()
            if state:
                state["status"] = "stopped"
                state["termination_reason"] = "cutoff_5am"
                state["active_pid"] = None
                state["retry_count"] = 0
                state["last_heartbeat_ist"] = get_now_ist().strftime("%Y-%m-%d %H:%M:%S")
                self._save_state(state)

    def record_external_failure(self, exit_code: int):
        with self._lock:
            state = self.get_state()
            if state:
                state["status"] = "interrupted"
                state["termination_reason"] = "external_termination"
                state["exit_code"] = exit_code
                state["active_pid"] = None
                state["last_heartbeat_ist"] = get_now_ist().strftime("%Y-%m-%d %H:%M:%S")
                self._save_state(state)

    def increment_retry_count(self) -> int:
        with self._lock:
            state = self.get_state()
            if state:
                cnt = state.get("retry_count", 0) + 1
                state["retry_count"] = cnt
                self._save_state(state)
                return cnt
            return 0

    def reset_retry_count(self):
        with self._lock:
            state = self.get_state()
            if state:
                state["retry_count"] = 0
                self._save_state(state)

    def should_auto_resume(self, current_dt: Optional[datetime.datetime] = None) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Determines if the previous execution was terminated externally (desktop shutdown, OOM, crash)
        and should be autonomously smart-resumed.
        Returns (True, state_dict) if:
          1. Previous run was marked 'running' or 'interrupted'.
          2. Previous PID is no longer alive in the operating system.
          3. Termination reason is NOT user_cancelled, NOT cutoff_5am, and NOT completed.
          4. Run belongs to the current daily cycle (today in IST, after 5am refresh).
          5. Not currently within the 5:00 - 5:05 AM IST daily refresh window.
          6. Consecutive retry count < MAX_AUTO_RETRIES.
        """
        state = self.get_state()
        if not state:
            return False, None

        if current_dt is None:
            current_dt = get_now_ist()
        elif current_dt.tzinfo is None:
            current_dt = current_dt.replace(tzinfo=IST_TZ)
        else:
            current_dt = current_dt.astimezone(IST_TZ)

        # 1. User stopped, completed, or 5am cutoff -> NEVER resume
        term_reason = state.get("termination_reason")
        if term_reason in ("user_cancelled", "cutoff_5am", "completed"):
            return False, None

        status = state.get("status")
        if status == "completed":
            return False, None

        # 2. Daily cycle check: If run was from a previous date, sheet was wiped at 11:30 PM cutoff
        run_date_ist = state.get("date_ist")
        today_cycle_ist = get_cycle_date_ist(current_dt)
        if run_date_ist != today_cycle_ist:
            return False, None

        # 3. 11:30 PM IST blackout window: Do not resume while daily sheet refresh occurs
        if is_5am_ist_cutoff(current_dt):
            return False, None

        # 4. Max auto-retries cap to prevent tight crash loops
        retry_count = state.get("retry_count", 0)
        if retry_count >= self.MAX_AUTO_RETRIES:
            return False, None

        # 5. Check if process is still running
        pid = state.get("active_pid")
        if pid and is_pid_alive(pid):
            # Process is still actively running in OS!
            return False, None

        # Process was marked running/interrupted but PID is dead, and not stopped by user:
        # This was an external/infra termination!
        return True, state
