"""
Bulk Sheet Pusher for Discovery Engine.
Reads data from local CSV cache (e.g., discovery_cache.csv) and pushes all completed
domain predictions and movements in bulk to a fresh or ready Google Sheet in optimized batches.
"""

import os
import csv
import json
import logging
import asyncio
from typing import List, Dict, Any, Optional, Callable, Tuple
from common.local_cache import LocalSheetCache

class BulkSheetPusher:
    """
    Pushes cached domain data from CSV into Google Sheets in chunked batches.
    Caps cell sizes to prevent document bloat and respects API limits.
    """
    def __init__(
        self,
        csv_path: str,
        sheet_id: str,
        sheet_name: str = "CF_IF_Engine_Database",
        batch_size: int = 50,
        start_row: int = 3,
        max_cell_len: int = 49000,
        schema_cols: int = 57,
        start_col: Optional[int] = None
    ):
        self.csv_path = csv_path
        self.sheet_id = sheet_id
        self.sheet_name = sheet_name
        self.batch_size = max(1, batch_size)
        self.start_row = start_row
        self.max_cell_len = max_cell_len
        self.schema_cols = schema_cols
        self.start_col = start_col if start_col is not None else (1 if schema_cols == 57 else 4)

    def load_records(self) -> List[Dict[str, str]]:
        """Loads non-empty records from the CSV file."""
        if not os.path.exists(self.csv_path):
            logging.warning(f"Cache file not found: {self.csv_path}")
            return []

        records = []
        with open(self.csv_path, mode="r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("domain"):
                    records.append(row)
        return records

    def build_sheet_row(self, record: Dict[str, str], is_cf_sheet: bool = True) -> List[Any]:
        """Converts a CSV cache record into an optimized 57-col or 70-col Google Sheet row."""
        if self.schema_cols == 57:
            dummy_row = [""] * 57
            full_row = LocalSheetCache.build_57col_write_row(
                record.get("domain", ""),
                dummy_row,
                record=record
            )
        else:
            dummy_row = [""] * 70
            full_row = LocalSheetCache.build_full_write_row(
                record.get("domain", ""),
                dummy_row,
                is_cf_sheet=is_cf_sheet,
                record=record
            )
        # Cap string lengths to 49,000 characters to prevent Google Sheets 400 error
        capped_row = [
            (str(c)[:self.max_cell_len] if isinstance(c, str) and len(str(c)) > self.max_cell_len else c)
            for c in full_row
        ]
        return capped_row

    async def push_all(
        self,
        client: Optional[Any] = None,
        mock_writer: Optional[Callable] = None,
        is_cf_sheet: bool = True,
        rate_limit_delay: float = 1.0
    ) -> int:
        """
        Pushes all loaded records to Google Sheets in batches of batch_size.
        Uses mock_writer if provided (for testing), otherwise calls write_sheet_data.
        """
        records = self.load_records()
        if not records:
            logging.info("No records to push.")
            return 0

        total = len(records)
        logging.info(f"🚀 Starting bulk push of {total} records to sheet {self.sheet_id} [{self.sheet_name}]...")

        success_count = 0
        for i in range(0, total, self.batch_size):
            chunk_records = records[i : i + self.batch_size]
            chunk_rows = [self.build_sheet_row(r, is_cf_sheet=is_cf_sheet) for r in chunk_records]
            chunk_start_row = self.start_row + i

            if mock_writer:
                await mock_writer(client, self.sheet_id, self.sheet_name, chunk_rows, chunk_start_row, self.start_col)
            else:
                from Sheet1.main import write_sheet_data
                await write_sheet_data(client, self.sheet_id, self.sheet_name, chunk_rows, chunk_start_row, self.start_col)

            success_count += len(chunk_rows)
            logging.info(f"✅ Bulk push progress: {success_count}/{total} rows pushed to Sheet (start_row: {chunk_start_row})")
            if rate_limit_delay > 0:
                await asyncio.sleep(rate_limit_delay)

        return success_count

async def clear_sheet_data_rows(worksheet: Any, keep_rows: int = 2) -> bool:
    """
    Clears all cells from keep_rows + 1 downwards to the bottom of the worksheet,
    preserving header rows 1 and 2.
    """
    try:
        max_r = getattr(worksheet, "row_count", 10000) or 10000
        start_r = keep_rows + 1
        if max_r < start_r:
            return True
        clear_range = f"A{start_r}:ZZ{max_r}"
        await worksheet.batch_clear([clear_range])
        logging.info(f"🧹 Successfully cleared worksheet data range {clear_range} (kept rows 1..{keep_rows}).")
        return True
    except Exception as e:
        logging.error(f"Error clearing worksheet data rows: {e}")
        return False

async def handle_5am_sheet_cleanup(
    sheet_dir: str,
    client: Any,
    sheet_id: str,
    sheet_name: str,
    keep_rows: int = 2
) -> Tuple[bool, str]:
    """
    Executes 5:00 AM sheet data cleanup if NOT in CSV-only mode.
    If in CSV-only mode, preserves the sheet and skips clearing.
    """
    from common.gsheet_circuit_breaker import is_csv_only_mode
    if is_csv_only_mode(sheet_dir):
        logging.warning(
            f"⚠️ [5:00 AM Cutoff] Sheet directory {sheet_dir} is in CSV-only mode "
            "(circuit breaker tripped or unpushed data). Preserving existing sheet data."
        )
        return False, "csv_only_mode"

    try:
        sheet = await client.open_by_key(sheet_id)
        worksheet = await sheet.worksheet(sheet_name)
        cleared = await clear_sheet_data_rows(worksheet, keep_rows=keep_rows)
        if cleared:
            logging.info(f"🧹 [5:00 AM Cutoff] Successfully cleared sheet {sheet_id} rows {keep_rows+1}+.")
            return True, "cleared"
        return False, "clear_failed"
    except Exception as e:
        logging.error(f"Failed to execute 5:00 AM sheet cleanup for {sheet_id}: {e}")
        return False, f"error: {e}"

