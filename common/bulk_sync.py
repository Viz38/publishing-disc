import os
import asyncio
import logging
from typing import List, Tuple, Any, Optional

logger = logging.getLogger(__name__)

def group_bulk_sync_chunks(
    pending_updates: List[Tuple[int, List[Any]]],
    max_chunk_rows: int = 1000,
    max_chunk_bytes: int = 8 * 1024 * 1024
) -> List[List[Tuple[int, List[Any]]]]:
    """
    Groups (row_number, write_slice) tuples into contiguous blocks
    limited by max_chunk_rows and max_chunk_bytes.
    Guarantees maximum throughput without exceeding Google Sheets API payload limits.
    """
    if not pending_updates:
        return []
    
    pending_updates.sort(key=lambda x: x[0])
    chunks = []
    current_chunk = []
    current_bytes = 0
    
    for row_num, write_slice in pending_updates:
        row_bytes = sum(len(str(val)) for val in write_slice)
        
        if not current_chunk:
            current_chunk = [(row_num, write_slice)]
            current_bytes = row_bytes
            continue
            
        prev_row = current_chunk[-1][0]
        is_contiguous = (row_num == prev_row + 1)
        exceeds_count = (len(current_chunk) >= max_chunk_rows)
        exceeds_bytes = (current_bytes + row_bytes > max_chunk_bytes)
        
        if is_contiguous and not exceeds_count and not exceeds_bytes:
            current_chunk.append((row_num, write_slice))
            current_bytes += row_bytes
        else:
            chunks.append(current_chunk)
            current_chunk = [(row_num, write_slice)]
            current_bytes = row_bytes
            
    if current_chunk:
        chunks.append(current_chunk)
        
    return chunks

async def bulk_sync_cache_to_sheet(
    client,
    sheet_id: str,
    sheet_name: str,
    pf_data: list,
    local_cache: Any,
    write_sheet_fn: Any,
    start_row_num: int = 3,
    max_chunk_rows: int = 1000,
    max_chunk_bytes: int = 8 * 1024 * 1024
) -> int:
    """
    Scans pf_data against local_cache at startup/resume.
    Identifies all domains completed in local cache that have missing or partial data in the Google Sheet.
    Writes them in bulk contiguous chunks of up to 500 rows (or 4 MB) in single API calls.
    Updates pf_data in memory with the completed 57-column row so downstream checks see it immediately.
    Returns total rows synced.
    """
    if not pf_data or not local_cache:
        return 0

    pending_updates = []

    for idx, r in enumerate(pf_data):
        if not r or not r[0]:
            continue
        domain = str(r[0]).strip()
        if not domain:
            continue
        rec = local_cache.get(domain)
        if not rec:
            continue

        # Check if local cache has meaningful results
        has_summary = bool(rec.get("output_data_summery"))
        has_if = bool(rec.get("output_data_if"))
        has_feed = bool(rec.get("output_data_feed"))
        has_movement = bool(rec.get("cf_if_status") or rec.get("if_pf_status") or rec.get("cf_if_time"))

        if not (has_summary or has_if or has_feed or has_movement):
            continue

        # Check if sheet row is missing summary or movement
        sheet_tc_stat = str(r[23]).strip().upper() if len(r) > 23 else ""
        sheet_has_raw = bool(str(r[29]).strip() if len(r) > 29 else "")
        sheet_summary = str(r[35]).strip() if len(r) > 35 else ""
        sheet_if = str(r[37]).strip() if len(r) > 37 else ""
        sheet_movement = str(r[49]).strip() if len(r) > 49 else ""

        sheet_needs_sync = (
            (has_summary and not sheet_summary) or
            (has_if and not sheet_if) or
            (has_movement and not sheet_movement) or
            (rec.get("tc_status") == "SUCCESS" and sheet_tc_stat != "SUCCESS" and not sheet_has_raw)
        )

        if sheet_needs_sync:
            hydrated, _ = local_cache.hydrate_row_from_cache(domain, r)
            full_57 = local_cache.build_57col_write_row(domain, hydrated)
            # Update pf_data in memory immediately so downstream checks see it!
            pf_data[idx] = full_57
            row_num = start_row_num + idx
            write_slice = full_57[17:]  # Cols 18-57 (40 columns)
            pending_updates.append((row_num, write_slice))

    if not pending_updates:
        logger.info("✨ [Bulk Cache Sync] All cached domains are already fully in sync with Google Sheet. 0 rows needed flush.")
        return 0

    chunks = group_bulk_sync_chunks(pending_updates, max_chunk_rows=max_chunk_rows, max_chunk_bytes=max_chunk_bytes)
    logger.info(
        f"🚀 [Bulk Cache Sync] Found {len(pending_updates)} cached domains needing sheet writeback! "
        f"Flushing in {len(chunks)} bulk chunks (up to {max_chunk_rows} rows each) to Google Sheet..."
    )

    synced_count = 0
    for ci, chunk in enumerate(chunks, 1):
        start_r = chunk[0][0]
        end_r = chunk[-1][0]
        chunk_rows = [x[1] for x in chunk]
        try:
            await write_sheet_fn(client, sheet_id, sheet_name, chunk_rows, start_r, start_col=18)
            synced_count += len(chunk_rows)
            logger.info(f"✨ [Bulk Cache Sync] Flushed chunk {ci}/{len(chunks)} ({len(chunk_rows)} rows: R{start_r}:BE{end_r}) in 1 API call!")
            await asyncio.sleep(0.5)
        except Exception as ex:
            logger.error(f"Error flushing bulk cache chunk {ci} (rows {start_r}-{end_r}): {ex}")

    logger.info(f"✅ [Bulk Cache Sync] Successfully synced {synced_count}/{len(pending_updates)} cached rows to Google Sheet!")
    return synced_count
