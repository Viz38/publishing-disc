#!/usr/bin/env python3
"""
Bulk Push Cache to Google Sheet CLI Tool.
Pushes all cached domain records from local discovery_cache.csv to Google Sheets in chunked batches.
Use this when a new Google Sheet is created or an existing sheet is cleaned to receive data.
"""

import os
import sys
import argparse
import asyncio
import logging

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from common.bulk_pusher import BulkSheetPusher

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def async_main():
    parser = argparse.ArgumentParser(description="Bulk push cached Discovery Engine CSV data to Google Sheets.")
    parser.add_argument("--sheet-id", required=True, help="Target Google Sheet ID (new or cleaned sheet).")
    parser.add_argument("--sheet-name", default="CF_IF_Engine_Database", help="Target worksheet name.")
    parser.add_argument("--csv", required=True, help="Path to discovery_cache.csv.")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of rows per batch update (default: 50).")
    parser.add_argument("--start-row", type=int, default=3, help="Row index to start writing to (default: 3).")
    parser.add_argument("--max-cell-len", type=int, default=5000, help="Max length per cell to avoid size bloat (default: 5000).")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay in seconds between batch writes (default: 1.0).")
    parser.add_argument("--non-cf", action="store_true", help="Set flag if sheet is NOT a CF sheet.")

    args = parser.parse_args()

    if not os.path.exists(args.csv):
        logging.error(f"CSV file not found: {args.csv}")
        sys.exit(1)

    pusher = BulkSheetPusher(
        csv_path=args.csv,
        sheet_id=args.sheet_id,
        sheet_name=args.sheet_name,
        batch_size=args.batch_size,
        start_row=args.start_row,
        max_cell_len=args.max_cell_len
    )

    records = pusher.load_records()
    logging.info(f"Loaded {len(records)} domain records from {args.csv}.")
    if not records:
        logging.warning("No domain records found to push.")
        return

    # Use primary gspread client
    from Sheet1.main import get_gspread_client
    primary_mgr = await get_gspread_client()
    gc = await primary_mgr.authorize()

    count = await pusher.push_all(
        client=gc,
        is_cf_sheet=not args.non_cf,
        rate_limit_delay=args.delay
    )
    logging.info(f"🎉 Successfully pushed {count} records to Google Sheet {args.sheet_id} [{args.sheet_name}]!")

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
