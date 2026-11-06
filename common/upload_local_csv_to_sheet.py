#!/usr/bin/env python3
"""
Upload local_sheet.csv directly into Google Sheets CF_IF_Engine_Database.
Caps all cell strings at 49,000 characters to comply with Google Sheets limitations.
Uploads in chunks of 500 rows starting at Row 3 (A3:BE...).
Removes .csv_only_mode and .stop_requested upon successful upload.
"""
import os
import sys
import csv
import time
import argparse
import logging

csv.field_size_limit(sys.maxsize)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from common.pipeline_engine import get_gspread_client_General
from common.tracxn_domain_ingestor import retry_call

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("UploadLocalCSV")

def upload_csv_to_sheet(sheet_id: str, csv_path: str, sheet_num: int, chunk_size: int = 500):
    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)

    logger.info(f"Reading CSV from {csv_path}...")
    rows_to_upload = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for r in reader:
            # 57 columns, cap each cell string at 49,000 characters
            clean_row = [str(col)[:49000] for col in r[:57]]
            if len(clean_row) < 57:
                clean_row.extend([""] * (57 - len(clean_row)))
            rows_to_upload.append(clean_row)

    total_rows = len(rows_to_upload)
    logger.info(f"Read {total_rows} rows from {csv_path}")
    if total_rows == 0:
        logger.warning("No rows found in CSV to upload.")
        return

    logger.info(f"Connecting to Google Sheets (Sheet ID: {sheet_id})...")
    gc = get_gspread_client_General()
    sh = retry_call(lambda: gc.open_by_key(sheet_id))
    ws = retry_call(lambda: sh.worksheet("CF_IF_Engine_Database"))
    logger.info(f"Connected to sheet: '{sh.title}', tab: '{ws.title}', current rows: {ws.row_count}")

    needed_rows = total_rows + 20
    if ws.row_count < needed_rows:
        logger.info(f"Resizing worksheet from {ws.row_count} to {needed_rows} rows...")
        retry_call(lambda: ws.resize(rows=needed_rows, cols=57), max_retries=5, delay=3)
        logger.info("Resized worksheet successfully.")

    start_time = time.time()
    for i in range(0, total_rows, chunk_size):
        chunk = rows_to_upload[i:i + chunk_size]
        start_row = 3 + i
        end_row = start_row + len(chunk) - 1
        range_str = f"A{start_row}:BE{end_row}"

        retry_call(lambda: ws.update(chunk, range_name=range_str), max_retries=8, delay=4)
        elapsed = time.time() - start_time
        rate = (i + len(chunk)) / elapsed if elapsed > 0 else 0
        logger.info(f"Wrote rows {start_row}-{end_row} ({len(chunk)} rows) [{end_row - 2}/{total_rows}] ({rate:.1f} rows/sec)")
        time.sleep(1.5)

    logger.info(f"✅ Successfully uploaded all {total_rows} rows into {sheet_id}!")

    # Remove .csv_only_mode
    folder = f"Sheet{sheet_num}"
    csv_mode_file = os.path.join(BASE_DIR, folder, ".csv_only_mode")
    if os.path.exists(csv_mode_file):
        try:
            os.remove(csv_mode_file)
            logger.info(f"Removed {csv_mode_file}")
        except Exception as e:
            logger.warning(f"Could not remove {csv_mode_file}: {e}")

    stop_file = os.path.join(BASE_DIR, folder, ".stop_requested")
    if os.path.exists(stop_file):
        try:
            os.remove(stop_file)
            logger.info(f"Removed {stop_file}")
        except Exception as e:
            logger.warning(f"Could not remove {stop_file}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Upload local_sheet.csv to Google Sheet")
    parser.add_argument("--sheet_id", required=True, help="Target Google Sheet ID")
    parser.add_argument("--csv_path", required=True, help="Path to local_sheet.csv")
    parser.add_argument("--sheet_num", required=True, type=int, help="Sheet number (2 or 4)")
    parser.add_argument("--chunk_size", default=500, type=int, help="Batch chunk size (default 500)")
    args = parser.parse_args()

    upload_csv_to_sheet(args.sheet_id, args.csv_path, args.sheet_num, args.chunk_size)

if __name__ == "__main__":
    main()
