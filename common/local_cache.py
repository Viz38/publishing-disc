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
    Returns True if current time in IST (UTC+5:30) is within the 5:00 AM daily cutoff window (05:00:00 - 05:05:00 IST).
    Apps Script drops domains at 5:00 AM IST, so any pipeline running in this window must be halted.
    """
    dt = current_dt or now_dt
    if dt is None:
        dt = get_now_ist()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST_TZ)
    else:
        dt = dt.astimezone(IST_TZ)
    return dt.hour == 5 and dt.minute < 5

def is_pre_cutoff_window(current_dt: Optional[datetime.datetime] = None, now_dt: Optional[datetime.datetime] = None) -> bool:
    """
    Returns True if current time in IST (UTC+5:30) is within the Pre-Cutoff Flush window (04:30:00 - 04:59:59 IST).
    During this window, no new URLs are accepted for scraping; all NIM-summarized domains are flushed
    through Gemini predictions and Review & Write APIs so they are marked DONE before 05:00 AM.
    """
    dt = current_dt or now_dt
    if dt is None:
        dt = get_now_ist()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST_TZ)
    else:
        dt = dt.astimezone(IST_TZ)
    return dt.hour == 4 and dt.minute >= 30

def get_ist_timestamp() -> str:
    """Returns formatted IST timestamp for logging and sheet cells."""
    return get_now_ist().strftime("%d/%b/%Y %H:%M:%S")

def get_cycle_date_ist(current_dt: Optional[datetime.datetime] = None) -> str:
    """
    Returns the daily processing cycle date string (YYYY-MM-DD) in IST.
    Daily domains refresh from Apps Script at 05:00 AM IST.
    Therefore, the window from 00:00:00 to 04:59:59 IST belongs to the cycle
    that started at 05:00 AM on the previous calendar day.
    """
    if current_dt is None:
        current_dt = get_now_ist()
    elif current_dt.tzinfo is None:
        current_dt = current_dt.replace(tzinfo=IST_TZ)
    else:
        current_dt = current_dt.astimezone(IST_TZ)

    if current_dt.hour < 5:
        current_dt = current_dt - datetime.timedelta(days=1)
    return current_dt.strftime("%Y-%m-%d")

def safe_str(val: Any) -> str:
    """Safely converts any value to string, stripping whitespace."""
    if val is None:
        return ""
    return str(val).strip()

class LocalSheetCache:
    """
    Local CSV cache for Discovery Engine pipelines.
    Provides redundancy, prevents re-doing already completed steps,
    and supports granular step-level retry.
    """
    CSV_FIELDS = [
        "domain", "enginePickedTime", "engineClearedTime", "engineClearedStatus", "engineTimeTaken",
        "tc_job_id", "tc_job_gen_time", "tc_status", "tc_final_url", "tc_s3_html",
        "tc_s3_markdown", "tc_http_status", "tc_signals", "scrappingBeautySoup",
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

    def save_record(self, domain: str, record: Dict[str, Any]):
        norm_d = self._normalize(domain)
        if not norm_d:
            return

        with self._lock:
            existing = self._cache.get(norm_d, {})
            merged = dict(existing)
            for k, v in record.items():
                if v is not None and str(v).strip() != "":
                    # Cap string size to prevent CSV bloat while preserving full output
                    merged[k] = str(v)[:49500]

            merged["domain"] = norm_d
            self._cache[norm_d] = merged
            full_rec = {k: str(merged.get(k, "") or "") for k in self.CSV_FIELDS}

            # Write atomically using a temporary file
            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.csv_path)), exist_ok=True)
                tmp_path = self.csv_path + ".tmp"
                with open(tmp_path, mode="w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS)
                    writer.writeheader()
                    for r in self._cache.values():
                        writer.writerow(r)
                os.replace(tmp_path, self.csv_path)
            except Exception as e:
                logging.warning(f"Error persisting local cache to {self.csv_path}: {e}")

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
        Merges existing sheet row with cached CSV data.
        Returns (hydrated_row, can_direct_write).
        can_direct_write is True if all pipeline steps are present in CSV
        and Google Sheets was never updated (missing engineClearedTime/summary).
        """
        rec = self.get(domain)
        if not rec:
            return row, False

        hydrated = list(row)
        min_len = 70
        if len(hydrated) < min_len:
            hydrated = hydrated + [""] * (min_len - len(hydrated))

        # Tech Crawler: row[23]=tc_job_id, row[24]=gen_time, row[25]=status, row[26]=url, row[27]=s3_html, row[28]=s3_md
        if not hydrated[23] and rec.get("tc_job_id"):
            hydrated[23] = rec["tc_job_id"]
        if not hydrated[24] and rec.get("tc_job_gen_time"):
            hydrated[24] = rec["tc_job_gen_time"]
        if not hydrated[25] and rec.get("tc_status"):
            hydrated[25] = rec["tc_status"]
        if not hydrated[26] and rec.get("tc_final_url"):
            hydrated[26] = rec["tc_final_url"]
        if not hydrated[27] and rec.get("tc_s3_html"):
            hydrated[27] = rec["tc_s3_html"]
        if not hydrated[28] and rec.get("tc_s3_markdown"):
            hydrated[28] = rec["tc_s3_markdown"]
        if not hydrated[31] and rec.get("scrappingBeautySoup"):
            hydrated[31] = rec["scrappingBeautySoup"]

        # Summary: row[39]=status, row[40]=summary
        if not hydrated[39] and rec.get("summary_status"):
            hydrated[39] = rec["summary_status"]
        if not hydrated[40] and rec.get("output_data_summery"):
            hydrated[40] = rec["output_data_summery"]

        # Prediction: row[44]=output_data_if, row[45]=name, row[46]=id, row[50]=validation, row[54]=feed
        if not hydrated[44] and rec.get("output_data_if"):
            hydrated[44] = rec["output_data_if"]
        if not hydrated[45] and rec.get("output_data_if_name"):
            hydrated[45] = rec["output_data_if_name"]
        if not hydrated[46] and rec.get("output_data_if_id"):
            hydrated[46] = rec["output_data_if_id"]
        if not hydrated[50] and rec.get("output_data_if_validation"):
            hydrated[50] = rec["output_data_if_validation"]
        if not hydrated[54] and rec.get("output_data_feed"):
            hydrated[54] = rec["output_data_feed"]
        if not hydrated[55] and rec.get("final_feed_name"):
            hydrated[55] = rec["final_feed_name"]
        if not hydrated[56] and rec.get("final_feed_id"):
            hydrated[56] = rec["final_feed_id"]
        if not hydrated[57] and rec.get("final_pub_funnel_name"):
            hydrated[57] = rec["final_pub_funnel_name"]
        if not hydrated[58] and rec.get("final_pub_funnel_id"):
            hydrated[58] = rec["final_pub_funnel_id"]

        # Write API: row[62]=cf_if_status, row[63]=cf_if_time, row[64]=if_pf_status, row[65]=if_pf_time
        if not hydrated[62] and rec.get("cf_if_status"):
            hydrated[62] = rec["cf_if_status"]
        if not hydrated[63] and rec.get("cf_if_time"):
            hydrated[63] = rec["cf_if_time"]
        if not hydrated[64] and rec.get("if_pf_status"):
            hydrated[64] = rec["if_pf_status"]
        if not hydrated[65] and rec.get("if_pf_time"):
            hydrated[65] = rec["if_pf_time"]
        if not hydrated[66] and rec.get("tag_status_if"):
            hydrated[66] = rec["tag_status_if"]
        if not hydrated[67] and rec.get("tag_status_pf"):
            hydrated[67] = rec["tag_status_pf"]
        if not hydrated[69] and rec.get("discovery_bq"):
            hydrated[69] = rec["discovery_bq"]

        # Col U engineClearedTime and Col V engineClearedStatus
        if not hydrated[20] and rec.get("engineClearedTime"):
            hydrated[20] = rec["engineClearedTime"]
        if not hydrated[21] and rec.get("engineClearedStatus"):
            hydrated[21] = rec["engineClearedStatus"]

        # Can directly write if fully completed in CSV but missing in sheet
        can_direct_write = self.is_fully_completed(domain) and (not str(row[20]).strip() or not str(row[40]).strip())
        return hydrated, can_direct_write

    @staticmethod
    def build_full_write_row(domain: str, hydrated_row: List[Any], is_cf_sheet: bool = True, record: Optional[Dict[str, Any]] = None, cache_instance: Optional[Any] = None) -> List[Any]:
        """
        Builds the exact 50-element array (starting at Col T / Col 20)
        from hydrated data for writing directly to Google Sheets.
        """
        rec = record or (cache_instance.get(domain) if cache_instance else {}) or {}
        engine_picked = hydrated_row[19] or rec.get("enginePickedTime", "")
        engine_cleared = hydrated_row[20] or rec.get("engineClearedTime", "")
        engine_status = hydrated_row[21] or rec.get("engineClearedStatus", "DONE")
        engine_time = hydrated_row[22] or rec.get("engineTimeTaken", "")

        tc_job_id = hydrated_row[23] or rec.get("tc_job_id", "")
        tc_job_gen_time = hydrated_row[24] or rec.get("tc_job_gen_time", "")
        tc_status = hydrated_row[25] or rec.get("tc_status", "")
        tc_final_url = hydrated_row[26] or rec.get("tc_final_url", "")
        tc_s3_html = hydrated_row[27] or rec.get("tc_s3_html", "")
        tc_s3_markdown = hydrated_row[28] or rec.get("tc_s3_markdown", "")
        tc_http_status = hydrated_row[29] or rec.get("tc_http_status", "")
        tc_signals = hydrated_row[30] or rec.get("tc_signals", "")
        scrappingBeautySoup = str(hydrated_row[31] or rec.get("scrappingBeautySoup", ""))[:49000]

        summary_status = hydrated_row[39] or rec.get("summary_status", "SUCCESS")
        output_data_summery = hydrated_row[40] or rec.get("output_data_summery", "")

        output_data_if = hydrated_row[44] or rec.get("output_data_if", "")
        output_data_if_name = hydrated_row[45] or rec.get("output_data_if_name", "")
        output_data_if_id = hydrated_row[46] or rec.get("output_data_if_id", "")

        input_sumry_for_if_validation = hydrated_row[47] or rec.get("input_sumry_for_if_validation", "")
        input_data_for_if_validation = hydrated_row[49] or rec.get("input_data_for_if_validation", "")
        output_data_if_validation = hydrated_row[50] or rec.get("output_data_if_validation", "")

        input_sumry_for_feed = hydrated_row[51] or rec.get("input_sumry_for_feed", "")
        base_prompt_for_feed = hydrated_row[52] or rec.get("base_prompt_for_feed", "")
        input_data_for_feed = hydrated_row[53] or rec.get("input_data_for_feed", "")
        output_data_feed = hydrated_row[54] or rec.get("output_data_feed", "")

        final_feed_name = hydrated_row[55] or rec.get("final_feed_name", "")
        final_feed_id = hydrated_row[56] or rec.get("final_feed_id", "")
        final_pub_funnel_name = hydrated_row[57] or rec.get("final_pub_funnel_name", "")
        final_pub_funnel_id = hydrated_row[58] or rec.get("final_pub_funnel_id", "")

        cf_if_status = hydrated_row[62] or rec.get("cf_if_status", "")
        cf_if_time = hydrated_row[63] or rec.get("cf_if_time", "")
        if_pf_status = hydrated_row[64] or rec.get("if_pf_status", "")
        if_pf_time = hydrated_row[65] or rec.get("if_pf_time", "")
        tag_status_if = hydrated_row[66] or rec.get("tag_status_if", "")
        tag_status_pf = hydrated_row[67] or rec.get("tag_status_pf", "")
        discovery_bq = hydrated_row[69] or rec.get("discovery_bq", "")

        row_36 = hydrated_row[36] if len(hydrated_row) > 36 else ""

        if is_cf_sheet:
            return [
                engine_picked, engine_cleared, engine_status, engine_time,
                tc_job_id, tc_job_gen_time, tc_status, tc_final_url, tc_s3_html, tc_s3_markdown,
                tc_http_status, tc_signals, scrappingBeautySoup, "", "", "", "", row_36, "", "",
                summary_status, output_data_summery, "", "",
                output_data_if, output_data_if_name, output_data_if_id, input_sumry_for_if_validation,
                "", input_data_for_if_validation, output_data_if_validation,
                input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed, output_data_feed,
                final_feed_name, final_feed_id, final_pub_funnel_name, final_pub_funnel_id, "", "", "",
                cf_if_status, cf_if_time, if_pf_status, if_pf_time, tag_status_if, tag_status_pf, "", discovery_bq
            ]
        else:
            return [
                engine_picked, engine_cleared, engine_status, engine_time,
                tc_job_id, tc_job_gen_time, tc_status, tc_final_url, tc_s3_html, tc_s3_markdown,
                tc_http_status, tc_signals, scrappingBeautySoup, "", "", "", "", row_36, "", "",
                summary_status, output_data_summery, "",
                input_data_for_if_validation, output_data_if_validation, input_sumry_for_if_validation,
                "", "", output_data_if, output_data_if_name,
                output_data_if_id, input_sumry_for_feed, base_prompt_for_feed, input_data_for_feed,
                output_data_feed, final_feed_name, final_feed_id, final_pub_funnel_name,
                final_pub_funnel_id, "", "", "", cf_if_status, cf_if_time, if_pf_status,
                if_pf_time, tag_status_if, tag_status_pf, "", discovery_bq
            ]

    @staticmethod
    def build_57col_write_row(
        domain: str,
        hydrated_row: List[Any],
        record: Optional[Dict[str, Any]] = None,
        cache_instance: Optional[Any] = None
    ) -> List[Any]:
        """
        Builds the 57-column row adhering to the optimized Google Sheet layout:
        - Section 1 (Cols 1-17 / A-Q): INPUT metadata
        - Section 2 (Cols 18-20 / R-T): ENGINE STATS (Database Added, Pick Time, Clear Time)
        - Section 3 (Cols 21-34 / U-AH): TECH CRAWLER STATUS (Status, Job ID, S3 URLs, Raw Data capped at 49,000 chars)
        - Section 4 (Cols 35-49 / AI-AW): LLM PREDICTIONS (NIM Status, NIM Summary, Gemini IF, Validation, Feed, PF)
        - Section 5 (Cols 50-57 / AX-BE): TRACXN APIs (CF->IF Status & Time, IF->PF Status & Time, Tags, Hashtag yes/no)
        """
        rec = record or (cache_instance.get(domain) if cache_instance else {}) or {}

        in_row = list(hydrated_row)
        if len(in_row) < 18:
            in_row = in_row + [""] * (18 - len(in_row))

        row_out = [""] * 57

        is_legacy = (len(in_row) > 65)

        # 1. INPUT (Cols 1-17 / A-Q / idx 0-16)
        for i in range(17):
            row_out[i] = in_row[i] if i < len(in_row) else ""

        if is_legacy:
            # 2. ENGINE STATS (Cols 18-20 / R-T / idx 17-19)
            row_out[17] = rec.get("databaseAddedTime") or (in_row[18] if len(in_row) > 18 else "") or (in_row[17] if len(in_row) > 17 else "")
            row_out[18] = rec.get("enginePickedTime") or (in_row[19] if len(in_row) > 19 else "")
            row_out[19] = rec.get("engineClearedTime") or (in_row[20] if len(in_row) > 20 else "")

            # 3. TECH CRAWLER STATUS (Cols 21-34 / U-AH / idx 20-33)
            tc_status = rec.get("tc_status") or (in_row[25] if len(in_row) > 25 else "") or ""
            row_out[20] = rec.get("engineClearedStatus") or (in_row[21] if len(in_row) > 21 else "") or ""
            row_out[21] = rec.get("tc_job_id") or (in_row[23] if len(in_row) > 23 else "")
            row_out[22] = rec.get("tc_job_gen_time") or (in_row[24] if len(in_row) > 24 else "")
            row_out[23] = tc_status
            row_out[24] = rec.get("tc_final_url") or (in_row[26] if len(in_row) > 26 else "")
            row_out[25] = rec.get("tc_s3_html") or (in_row[27] if len(in_row) > 27 else "")
            row_out[26] = rec.get("tc_s3_markdown") or (in_row[28] if len(in_row) > 28 else "")
            row_out[27] = rec.get("tc_http_status") or (in_row[29] if len(in_row) > 29 else "") or ""
            row_out[28] = rec.get("tc_signals") or (in_row[30] if len(in_row) > 30 else "") or ""

            # Raw Data at Col 30 (idx 29 / Col AD) - CAP AT 49,000 CHARS
            raw_content = rec.get("scrappingBeautySoup") or (in_row[31] if len(in_row) > 31 and in_row[31] else "") or (in_row[37] if len(in_row) > 37 else "") or ""
            row_out[29] = str(raw_content)[:49000]

            row_out[30] = row_out[22]  # TimeStamp
            row_out[31] = row_out[24]  # URL
            row_out[32] = ""  # Error - Code
            row_out[33] = ""  # Error - Reason

            # 4. LLM PREDICTIONS (Cols 35-49 / AI-AW / idx 34-48)
            summary_val = rec.get("output_data_summery") or (in_row[40] if len(in_row) > 40 else "") or ""
            row_out[34] = rec.get("summary_status") or (in_row[39] if len(in_row) > 39 else "") or ""
            row_out[35] = summary_val
            row_out[36] = summary_val  # Domain Summery context
            row_out[37] = rec.get("output_data_if") or (in_row[43] if len(in_row) > 43 else "") or ""
            row_out[38] = rec.get("output_data_if_name") or (in_row[44] if len(in_row) > 44 else "") or ""
            row_out[39] = rec.get("output_data_if_id") or (in_row[45] if len(in_row) > 45 else "") or ""
            row_out[40] = row_out[38]  # Context
            row_out[41] = rec.get("output_data_if_validation") or (in_row[49] if len(in_row) > 49 and in_row[49] else (in_row[50] if len(in_row) > 50 else "")) or ""
            row_out[42] = rec.get("output_data_feed") or (in_row[53] if len(in_row) > 53 and in_row[53] else (in_row[54] if len(in_row) > 54 else "")) or ""
            row_out[43] = row_out[42]
            row_out[44] = rec.get("final_feed_name") or (in_row[54] if len(in_row) > 54 and in_row[54] else (in_row[55] if len(in_row) > 55 else "")) or ""
            row_out[45] = rec.get("final_feed_id") or (in_row[55] if len(in_row) > 55 and in_row[55] else (in_row[56] if len(in_row) > 56 else "")) or ""
            row_out[46] = rec.get("final_pub_funnel_name") or (in_row[56] if len(in_row) > 56 and in_row[56] else (in_row[57] if len(in_row) > 57 else "")) or ""
            row_out[47] = rec.get("final_pub_funnel_id") or (in_row[57] if len(in_row) > 57 and in_row[57] else (in_row[58] if len(in_row) > 58 else "")) or ""
            row_out[48] = ""  # Funnel Recheck

            # 5. TRACXN APIs (Cols 50-57 / AX-BE / idx 49-56)
            row_out[49] = rec.get("cf_if_status") or (in_row[61] if len(in_row) > 61 else "") or ""
            row_out[50] = rec.get("cf_if_time") or (in_row[62] if len(in_row) > 62 else "") or ""
            row_out[51] = rec.get("if_pf_status") or (in_row[63] if len(in_row) > 63 else "") or ""
            row_out[52] = rec.get("if_pf_time") or (in_row[64] if len(in_row) > 64 else "") or ""
            row_out[53] = rec.get("tag_status_if") or (in_row[65] if len(in_row) > 65 else "") or ""
            row_out[54] = rec.get("tag_status_pf") or (in_row[66] if len(in_row) > 66 else "") or ""
            row_out[55] = rec.get("tag_status_review") or (in_row[67] if len(in_row) > 67 else "") or ""
            pf_tag = str(row_out[54]).strip().upper()
            if pf_tag in ("YES", "TRUE"):
                row_out[56] = rec.get("discovery_bq") or (in_row[68] if len(in_row) > 68 else "") or "yes"
            else:
                row_out[56] = "no" if (row_out[49] or row_out[51]) else ""
        else:
            # 2. ENGINE STATS (Cols 18-20 / R-T / idx 17-19)
            row_out[17] = in_row[17] or rec.get("databaseAddedTime", "")
            row_out[18] = in_row[18] or rec.get("enginePickedTime", "")
            row_out[19] = in_row[19] or rec.get("engineClearedTime", "")

            # 3. TECH CRAWLER STATUS (Cols 21-34 / U-AH / idx 20-33)
            tc_status = rec.get("tc_status") or (in_row[23] if len(in_row) > 23 else "") or ""
            row_out[20] = in_row[20] or rec.get("engineClearedStatus", "") or ""
            row_out[21] = in_row[21] or rec.get("tc_job_id", "")
            row_out[22] = in_row[22] or rec.get("tc_job_gen_time", "")
            row_out[23] = in_row[23] or tc_status
            row_out[24] = in_row[24] or rec.get("tc_final_url", "")
            row_out[25] = in_row[25] or rec.get("tc_s3_html", "")
            row_out[26] = in_row[26] or rec.get("tc_s3_markdown", "")
            row_out[27] = in_row[27] or rec.get("tc_http_status", "") or ""
            row_out[28] = in_row[28] or rec.get("tc_signals", "") or ""

            # Raw Data at Col 30 (idx 29 / Col AD) - CAP AT 49,000 CHARS
            raw_content = rec.get("scrappingBeautySoup") or (in_row[29] if len(in_row) > 29 else "") or ""
            row_out[29] = str(raw_content)[:49000]

            row_out[30] = in_row[30] if len(in_row) > 30 else rec.get("tc_job_gen_time", "")
            row_out[31] = in_row[31] if len(in_row) > 31 else rec.get("tc_final_url", "")
            row_out[32] = ""  # Error - Code
            row_out[33] = ""  # Error - Reason

            # 4. LLM PREDICTIONS (Cols 35-49 / AI-AW / idx 34-48)
            summary_val = rec.get("output_data_summery") or (in_row[35] if len(in_row) > 35 else "") or ""
            row_out[34] = in_row[34] or rec.get("summary_status", "") or ""
            row_out[35] = summary_val
            row_out[36] = summary_val  # Domain Summery context
            row_out[37] = rec.get("output_data_if") or (in_row[37] if len(in_row) > 37 else "") or ""
            row_out[38] = rec.get("output_data_if_name") or (in_row[38] if len(in_row) > 38 else "") or ""
            row_out[39] = rec.get("output_data_if_id") or (in_row[39] if len(in_row) > 39 else "") or ""
            row_out[40] = row_out[38]  # Context
            row_out[41] = rec.get("output_data_if_validation") or (in_row[41] if len(in_row) > 41 else "") or ""
            row_out[42] = rec.get("output_data_feed") or ""
            row_out[43] = rec.get("output_data_feed") or (in_row[43] if len(in_row) > 43 else "") or ""
            row_out[44] = rec.get("final_feed_name") or (in_row[44] if len(in_row) > 44 else "") or ""
            row_out[45] = rec.get("final_feed_id") or (in_row[45] if len(in_row) > 45 else "") or ""
            row_out[46] = rec.get("final_pub_funnel_name") or (in_row[46] if len(in_row) > 46 else "") or ""
            row_out[47] = rec.get("final_pub_funnel_id") or (in_row[47] if len(in_row) > 47 else "") or ""
            row_out[48] = ""  # Funnel Recheck

            # 5. TRACXN APIs (Cols 50-57 / AX-BE / idx 49-56)
            row_out[49] = rec.get("cf_if_status") or (in_row[49] if len(in_row) > 49 else "") or ""
            row_out[50] = rec.get("cf_if_time") or (in_row[50] if len(in_row) > 50 else "") or ""
            row_out[51] = rec.get("if_pf_status") or (in_row[51] if len(in_row) > 51 else "") or ""
            row_out[52] = rec.get("if_pf_time") or (in_row[52] if len(in_row) > 52 else "") or ""
            row_out[53] = rec.get("tag_status_if") or (in_row[53] if len(in_row) > 53 else "") or ""
            row_out[54] = rec.get("tag_status_pf") or (in_row[54] if len(in_row) > 54 else "") or ""
            row_out[55] = rec.get("tag_status_review") or (in_row[55] if len(in_row) > 55 else "") or ""
            pf_tag = str(row_out[54]).strip().upper()
            if pf_tag in ("YES", "TRUE"):
                row_out[56] = rec.get("discovery_bq") or (in_row[56] if len(in_row) > 56 else "") or "yes"
            else:
                row_out[56] = "no" if (row_out[49] or row_out[51]) else ""

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
    and daily 5:00 AM IST cutoffs.
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

        # 2. Daily cycle check: If run was from a previous date, sheet was wiped by Apps Script at 5am
        run_date_ist = state.get("date_ist")
        today_cycle_ist = get_cycle_date_ist(current_dt)
        if run_date_ist != today_cycle_ist:
            return False, None

        # 3. 5:00 AM IST blackout window: Do not resume while Apps Script is refreshing daily domains
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
