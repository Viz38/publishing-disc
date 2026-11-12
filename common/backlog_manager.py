import csv
import os
import json
import logging
import tempfile
from typing import List, Dict, Any, Optional, Sequence

from common.dashboard_metrics import (
    IDX_DOMAIN,
    IDX_TC_JOB_ID,
    IDX_TC_STATUS,
    IDX_RAW_DATA,
    IDX_NIM_STATUS,
    IDX_CF_IF_STATUS,
    IDX_IF_PF_STATUS,
    _DONE_STATUSES,
    hydrate_row_from_cache,
    normalize_domain,
)
from common.gsheet_circuit_breaker import is_csv_only_mode

logger = logging.getLogger(__name__)

class BacklogManager:
    """
    Manages `.scraped_backlog.json` to persist uncompleted/scraped domains prior to the
    midnight cutoff and carry them forward (capped at 5,000 fleet-wide / 1,250 per sheet).
    """

    DEFAULT_BACKLOG_FILENAME = ".scraped_backlog.json"
    MAX_CUMULATIVE_BACKLOG = 5000
    MAX_PER_SHEET_BACKLOG = 1250

    def __init__(self, backlog_filepath: Optional[str] = None):
        if backlog_filepath:
            self.filepath = backlog_filepath
        else:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.filepath = os.path.join(base_dir, self.DEFAULT_BACKLOG_FILENAME)

    def load_backlog(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.filepath):
            return []
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except Exception as e:
            logger.error(f"Error loading backlog from {self.filepath}: {e}")
            return []

    def _atomic_write(self, data: List[Dict[str, Any]]) -> None:
        dir_name = os.path.dirname(os.path.abspath(self.filepath))
        os.makedirs(dir_name, exist_ok=True)
        temp_fd, temp_path = tempfile.mkstemp(dir=dir_name, prefix=".backlog_tmp_")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(temp_path, self.filepath)
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            logger.error(f"Atomic write failed for {self.filepath}: {e}")
            raise

    def save_scraped_backlog(self, new_items: List[Dict[str, Any]]) -> int:
        """
        Saves domains where scraping completed but NIM summary was pending.
        De-duplicates by domain.
        """
        if not new_items:
            return 0

        existing = self.load_backlog()
        existing_domains = {item["domain"] for item in existing if "domain" in item}

        added = 0
        for item in new_items:
            domain = item.get("domain")
            if domain and domain not in existing_domains:
                existing.append(item)
                existing_domains.add(domain)
                added += 1

        self._atomic_write(existing)
        logger.info(f"Saved {added} new scraped backlog items (total backlog: {len(existing)}) to {self.filepath}")
        return added

    def get_backlog_for_injection(self, max_cap: int = MAX_CUMULATIVE_BACKLOG) -> List[Dict[str, Any]]:
        """
        Returns up to max_cap backlog items for injection into the new daily list.
        Strictly enforces the 5,000 domain cumulative backlog ceiling.
        """
        backlog = self.load_backlog()
        cap = min(max_cap, self.MAX_CUMULATIVE_BACKLOG)
        return backlog[:cap]

    def clear_injected(self, injected_items: List[Dict[str, Any]]) -> None:
        """
        Removes successfully injected items from the backlog.
        """
        if not injected_items:
            return

        injected_domains = {item["domain"] for item in injected_items if "domain" in item}
        current_backlog = self.load_backlog()
        remaining = [item for item in current_backlog if item.get("domain") not in injected_domains]

        self._atomic_write(remaining)
        logger.info(f"Cleared {len(injected_domains)} items from backlog. Remaining: {len(remaining)}")

    def extract_uncompleted_items_from_rows(
        self,
        rows: Sequence[Sequence[Any]],
        cache_map: Optional[Dict[str, Dict[str, Any]]] = None,
        max_cap: int = MAX_PER_SHEET_BACKLOG,
    ) -> List[Dict[str, Any]]:
        """
        Extracts uncompleted domains from 57-col rows prior to daily cleanup/rotation,
        prioritizing scraped-but-uncompleted rows first, capped at max_cap.
        """
        scraped_pending: List[Dict[str, Any]] = []
        unscraped_pending: List[Dict[str, Any]] = []
        seen = set()

        for raw_row in rows:
            if not raw_row or not str(raw_row[0] or "").strip():
                continue
            row = hydrate_row_from_cache(raw_row, cache_map=cache_map)
            dom = row[IDX_DOMAIN].strip()
            norm_d = normalize_domain(dom)
            if not norm_d or norm_d in seen:
                continue

            nim_s = row[IDX_NIM_STATUS].strip().upper()
            ax = row[IDX_CF_IF_STATUS].strip().upper()
            az = row[IDX_IF_PF_STATUS].strip().upper()
            tc_s = row[IDX_TC_STATUS].strip().upper()

            # Skip terminal / finished rows
            if nim_s == "NO_CONTENT" or ax in _DONE_STATUSES or az in _DONE_STATUSES:
                continue

            rec = cache_map.get(norm_d, {}) if cache_map else {}
            raw_content = str(rec.get("scrappingBeautySoup", "") or "").strip()
            if not raw_content:
                cell_raw = row[IDX_RAW_DATA].strip()
                if cell_raw and cell_raw != "Raw Data Appended - csv only" and not cell_raw.startswith(("Error", "FAILED", "N/A")):
                    raw_content = cell_raw

            # Skip terminal TC failures with no usable content
            if tc_s == "FAILED" and not raw_content:
                continue

            seen.add(norm_d)
            clean_57 = list(row[:17]) + [""] * 40
            tc_job_id = row[IDX_TC_JOB_ID].strip()
            if tc_job_id:
                clean_57[IDX_TC_JOB_ID] = tc_job_id
            if tc_s == "SUCCESS" or raw_content:
                clean_57[IDX_TC_STATUS] = "SUCCESS"
            if raw_content:
                clean_57[IDX_RAW_DATA] = "Raw Data Appended - csv only"

            item = {
                "domain": dom,
                "raw_content": raw_content[:49500] if raw_content else "",
                "tc_job_id": tc_job_id,
                "row_57": clean_57,
            }
            if tc_s == "SUCCESS" or raw_content:
                scraped_pending.append(item)
            else:
                unscraped_pending.append(item)

        combined = scraped_pending + unscraped_pending
        cap = min(max_cap, self.MAX_CUMULATIVE_BACKLOG)
        return combined[:cap]

    def _build_57_row_from_item(self, item: Dict[str, Any], cache: Any = None) -> List[str]:
        base = item.get("row_57")
        if isinstance(base, list) and len(base) >= 17:
            row = [str(x) if x is not None else "" for x in base[:57]]
            if len(row) < 57:
                row.extend([""] * (57 - len(row)))
        else:
            row = [""] * 57

        domain = str(item.get("domain", "") or row[0]).strip()
        raw_data = str(item.get("raw_content", "") or "").strip()
        tc_job_id = str(item.get("tc_job_id", "") or row[IDX_TC_JOB_ID]).strip()
        row[0] = domain
        if tc_job_id:
            row[IDX_TC_JOB_ID] = tc_job_id
        if tc_job_id or raw_data:
            row[IDX_TC_STATUS] = "SUCCESS"
        if raw_data:
            row[IDX_RAW_DATA] = "Raw Data Appended - csv only"
            if cache is not None and domain:
                try:
                    cache.save_record(domain, {
                        "tc_job_id": tc_job_id,
                        "tc_status": "SUCCESS",
                        "scrappingBeautySoup": raw_data[:49500],
                    })
                except Exception as ce:
                    logger.warning(f"Could not hydrate carry-over raw data to cache for {domain}: {ce}")
        return row

    def append_backlog_to_sheet_or_csv(
        self,
        gspread_client: Any,
        sheet_id: str,
        sheet_name: str,
        sheet_dir: Optional[str] = None,
        cache: Any = None,
        max_cap: int = MAX_PER_SHEET_BACKLOG,
    ) -> int:
        """
        Appends up to max_cap carry-over backlog domains in exact 57-col format
        to either Google Sheet or local_sheet.csv (when CSV-only mode is active).
        """
        items_to_inject = self.get_backlog_for_injection(max_cap=max_cap)
        if not items_to_inject:
            logger.info("No scraped backlog domains available to append.")
            return 0

        use_csv_only = (bool(sheet_dir) and is_csv_only_mode(sheet_dir)) or (gspread_client is None)
        existing_domains = set()

        if use_csv_only and sheet_dir:
            local_csv = os.path.join(sheet_dir, "local_sheet.csv")
            if os.path.exists(local_csv):
                try:
                    with open(local_csv, "r", encoding="utf-8", newline="", errors="ignore") as f:
                        for r in csv.reader(f):
                            if r and r[0]:
                                existing_domains.add(normalize_domain(r[0]))
                except Exception:
                    pass
            filtered_items = [it for it in items_to_inject if normalize_domain(it.get("domain", "")) not in existing_domains]
            append_rows = [self._build_57_row_from_item(it, cache=cache) for it in filtered_items]
            if append_rows:
                with open(local_csv, "a", encoding="utf-8", newline="") as f:
                    csv.writer(f).writerows(append_rows)
                logger.info(f"📂 [CSV-Only] Appended {len(append_rows)} carry-over backlog domains to {local_csv}")
            self.clear_injected(items_to_inject)
            return len(append_rows)

        try:
            spreadsheet = gspread_client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            col_a = worksheet.col_values(1)
            for v in col_a[2:]:
                nd = normalize_domain(v)
                if nd:
                    existing_domains.add(nd)
            last_row = max(2, len(col_a))

            filtered_items = [it for it in items_to_inject if normalize_domain(it.get("domain", "")) not in existing_domains]
            append_rows = [self._build_57_row_from_item(it, cache=cache) for it in filtered_items]
            if append_rows:
                start_row = last_row + 1
                end_row = start_row + len(append_rows) - 1
                if getattr(worksheet, "row_count", 0) < end_row + 5:
                    worksheet.resize(rows=end_row + 10, cols=57)
                CHUNK = 500
                for idx in range(0, len(append_rows), CHUNK):
                    chunk = append_rows[idx : idx + CHUNK]
                    cs = start_row + idx
                    ce = cs + len(chunk) - 1
                    worksheet.update(range_name=f"A{cs}:BE{ce}", values=chunk)
                logger.info(f"Appended {len(append_rows)} 57-col carry-over backlog domains to {sheet_name} at row {start_row}")

            self.clear_injected(items_to_inject)
            return len(append_rows)
        except Exception as e:
            logger.error(f"Failed to append backlog domains to sheet {sheet_id}: {e}")
            return 0

    async def append_backlog_to_sheet(
        self,
        gspread_client: Any,
        sheet_id: str,
        sheet_name: str,
        max_cap: int = MAX_CUMULATIVE_BACKLOG,
        sheet_dir: Optional[str] = None,
        cache: Any = None,
    ) -> int:
        return self.append_backlog_to_sheet_or_csv(
            gspread_client=gspread_client,
            sheet_id=sheet_id,
            sheet_name=sheet_name,
            sheet_dir=sheet_dir,
            cache=cache,
            max_cap=max_cap,
        )
