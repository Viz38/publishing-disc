import os
import json
import logging
import tempfile
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class BacklogManager:
    """
    Manages `.scraped_backlog.json` to persist domains that completed scraping
    prior to the 5:00 AM cutoff but did not finish NIM summarization.
    Strictly caps cumulative appended backlog domains at 5,000 domains.
    """

    DEFAULT_BACKLOG_FILENAME = ".scraped_backlog.json"
    MAX_CUMULATIVE_BACKLOG = 5000

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

    async def append_backlog_to_sheet(
        self,
        gspread_client: Any,
        sheet_id: str,
        sheet_name: str,
        max_cap: int = MAX_CUMULATIVE_BACKLOG,
    ) -> int:
        """
        Appends up to max_cap backlog domains to the end of the new daily sheet.
        """
        items_to_inject = self.get_backlog_for_injection(max_cap=max_cap)
        if not items_to_inject:
            logger.info("No scraped backlog domains available to append.")
            return 0

        try:
            spreadsheet = gspread_client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            all_vals = worksheet.get_all_values()
            last_row = len(all_vals)

            # Build rows to append. Column 1: Domain, Col 24 (index 23): tc_job_id, Col 32 (index 31): raw_content
            append_rows = []
            for item in items_to_inject:
                domain = item.get("domain", "")
                raw_data = item.get("raw_content", "")
                tc_job_id = item.get("tc_job_id", "")
                row = [""] * 62
                row[0] = domain
                if tc_job_id:
                    row[23] = tc_job_id
                    row[25] = "SUCCESS"
                if raw_data:
                    row[31] = raw_data[:49500]
                append_rows.append(row)

            start_row = last_row + 1
            from common.local_cache import safe_str
            # Batch update worksheet
            worksheet.update(f"A{start_row}:BJ{start_row + len(append_rows) - 1}", append_rows)
            logger.info(f"Appended {len(append_rows)} backlog domains to {sheet_name} starting at row {start_row}")

            self.clear_injected(items_to_inject)
            return len(append_rows)
        except Exception as e:
            logger.error(f"Failed to append backlog domains to sheet {sheet_id}: {e}")
            return 0
