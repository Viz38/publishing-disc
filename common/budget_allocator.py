"""
Production Budget Allocation & 4-Way Fleet Partitioning Engine
==============================================================
Manages central fleet budget allocation for Discovery Engine Sheets 1-4.
Provides mathematical 4-way budget splitting, master payload loading from the
Operations Dashboard ('Production Budget Allocation' tab), and fallback mechanisms.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

MASTER_BUDGET_SPREADSHEET_ID = "1eVKXWnGNsHp8WmoqXo8Pz-mB6f7V7xGleMfd7CcahSA"
MASTER_BUDGET_WORKSHEET_NAME = "Production Budget Allocation"

# Main Fleet Sheets mapped to their sheet numbers (1-4)
MAIN_FLEET_SHEET_MAP = {
    "1IWXqnR52IzpFewE0QS0tlJFT65nJE_fmIoJs8ps9nZo": 1,
    "1QGraMZOJjc-EIZlt_CieOVpr1jxw6tLa_QnWgWHou5I": 2,
    "1MHdkFCsoAY0V7ix_pJ5SpWFq3EFpJEug2ntJk1FkEl8": 3,
    "1z3wH--c2qHGlNqW0e-6Mc8IVqh_kN5_fbWjILM4FxQM": 4,
}

def calculate_4way_budget(overall_budget: int) -> Tuple[int, int, int, int]:
    """
    Splits an overall fleet budget evenly across 4 production sheets with fair remainder distribution.
    For any integer B >= 0:
      s1 + s2 + s3 + s4 == B
      s1 >= s2 >= s3 >= s4
      max(s) - min(s) <= 1
    """
    if overall_budget <= 0:
        return (0, 0, 0, 0)
    base = overall_budget // 4
    rem = overall_budget % 4
    s1 = base + (1 if rem >= 1 else 0)
    s2 = base + (1 if rem >= 2 else 0)
    s3 = base + (1 if rem >= 3 else 0)
    s4 = base
    return (s1, s2, s3, s4)

def get_sheet_budget_slice(overall_budget: int, sheet_num: int) -> int:
    """
    Returns the specific budget slice for a given sheet number (1, 2, 3, or 4).
    If sheet_num is not in {1, 2, 3, 4} or budget <= 0, returns 0.
    """
    if sheet_num not in (1, 2, 3, 4) or overall_budget <= 0:
        return 0
    base = overall_budget // 4
    rem = overall_budget % 4
    return base + (1 if rem >= sheet_num else 0)

def resolve_sheet_number(sheet_id: str, default: Optional[int] = None) -> Optional[int]:
    """
    Resolves sheet number (1..4) from sheet ID.
    """
    return MAIN_FLEET_SHEET_MAP.get(sheet_id, default)

def load_payload_rows_for_sheet(
    gc,
    sheet_id: str,
    sheet_name: str = "Production Engine",
    sheet_num: Optional[int] = None
) -> Tuple[List[List[str]], str]:
    """
    Loads payload rows for the specified sheet.
    If the sheet belongs to the Main Fleet (Sheets 1-4), reads from the common master
    'Production Budget Allocation' tab in the Operations Dashboard.
    If reading from master fails or sheet is not in Main Fleet (e.g. NIM Fleet),
    falls back to reading the sheet's local 'CF_IF Engine - Input Payload' tab.
    
    Returns:
        (rows, source_description)
    """
    resolved_num = sheet_num or resolve_sheet_number(sheet_id)

    # 1. Main Fleet (Sheets 1-4): Attempt to read from master "Production Budget Allocation"
    if resolved_num in (1, 2, 3, 4):
        try:
            logger.info(f"[{sheet_name}] Loading master payloads from Operations Dashboard ('{MASTER_BUDGET_WORKSHEET_NAME}')...")
            master_sh = gc.open_by_key(MASTER_BUDGET_SPREADSHEET_ID)
            master_ws = master_sh.worksheet(MASTER_BUDGET_WORKSHEET_NAME)
            raw_rows = master_ws.get_all_values()
            
            if len(raw_rows) >= 3:
                # Transform rows into standard payload format for this sheet
                # Header row 1 is title, header row 2 is columns
                # Data starts at row 3 (index 2)
                transformed_rows = [raw_rows[0], raw_rows[1]]
                # Budget column for this sheet: Sheet 1: Col I (idx 8), Sheet 2: Col J (idx 9),
                # Sheet 3: Col K (idx 10), Sheet 4: Col L (idx 11)
                sheet_col_idx = 7 + resolved_num

                for idx in range(2, len(raw_rows)):
                    r = raw_rows[idx]
                    if not r or not any(c.strip() for c in r[:4]):
                        continue
                    
                    # Extract overall budget
                    b_str = r[5].replace(",", "").strip() if len(r) > 5 else "0"
                    try:
                        overall_b = int(b_str)
                    except ValueError:
                        overall_b = 0

                    # Extract sheet specific budget
                    sheet_b_val = None
                    if len(r) > sheet_col_idx:
                        s_str = r[sheet_col_idx].replace(",", "").strip()
                        if s_str.isdigit():
                            sheet_b_val = int(s_str)
                    
                    # If sheet specific budget cell was not pre-evaluated as int (e.g. raw formula),
                    # calculate it directly via 4-way budget splitting
                    if sheet_b_val is None:
                        sheet_b_val = get_sheet_budget_slice(overall_b, resolved_num)

                    # Build standard row compatible with existing ingestor/precrawl:
                    # Col 0: Filter Group Name
                    # Col 1: Sub Filter Name
                    # Col 2: Filter ID
                    # Col 3: Sub Filter Payload
                    # Col 4: FunnelGroupID
                    # Col 5: num_domains (budget for this sheet!)
                    # Col 6: funnelId
                    # Col 7: FilterGroup Wise Funnel Selection >>
                    # Col 8..9: empty
                    # Col 10: New Updated Payload
                    # Col 11: Platform Payload
                    # Col 12: Platform Link
                    new_r = [
                        r[0] if len(r) > 0 else "",
                        r[1] if len(r) > 1 else "",
                        r[2] if len(r) > 2 else "",
                        r[3] if len(r) > 3 else "",
                        r[4] if len(r) > 4 else "",
                        str(sheet_b_val),
                        r[6] if len(r) > 6 else "",
                        r[7] if len(r) > 7 else "All",
                        "",
                        "",
                        r[12] if len(r) > 12 else "",
                        r[13] if len(r) > 13 else "",
                        r[14] if len(r) > 14 else "",
                    ]
                    transformed_rows.append(new_r)

                logger.info(f"[{sheet_name}] Successfully loaded {len(transformed_rows)-2} master payloads from Operations Dashboard for Sheet {resolved_num}.")
                return transformed_rows, f"Operations Dashboard -> '{MASTER_BUDGET_WORKSHEET_NAME}' (Sheet {resolved_num} slice)"
            else:
                logger.warning(f"[{sheet_name}] Master sheet has fewer than 3 rows. Falling back to local tab.")
        except Exception as e:
            logger.warning(f"[{sheet_name}] Could not load master budget allocation sheet ({e}). Falling back to local tab.")

    # 2. Fallback or Non-Main-Fleet: Load from local spreadsheet's "CF_IF Engine - Input Payload" tab
    logger.info(f"[{sheet_name}] Loading local payload tab 'CF_IF Engine - Input Payload' from sheet {sheet_id}...")
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("CF_IF Engine - Input Payload")
    rows = ws.get_all_values()
    return rows, f"Local Sheet ({sheet_id}) -> 'CF_IF Engine - Input Payload'"
