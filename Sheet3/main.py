#!/usr/bin/env python3
"""
Discovery Engine - Sheet 3 Main Pipeline Runner.
Parameterized entry facade delegating to common.pipeline_engine.
"""

import sys
import os
import asyncio

_SHEET_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SHEET_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
if _SHEET_DIR not in sys.path:
    sys.path.insert(0, _SHEET_DIR)

from common.pipeline_engine import (
    configure_engine,
    run_engine_for_sheet,
    get_gspread_client,
    get_gspread_client_General,
    write_sheet_data,
    read_sheet_data,
    Nvidia_API_ASYNC,
    Gemini_API_ASYNC,
    is_row_fully_completed,
    update_progress_file,
    TOKEN_ROTATOR,
    NIM_KEY_MANAGER,
)

# Initialize engine context for Sheet 3
configure_engine(sheet_num=3, sheet_dir=_SHEET_DIR)

async def main():
    await run_engine_for_sheet(sheet_num=3, sheet_dir=_SHEET_DIR)

if __name__ == "__main__":
    asyncio.run(main())
