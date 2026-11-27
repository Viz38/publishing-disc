"""
Production & Test Fleet Budget Allocation & Multi-Fleet Partitioning Engine
==========================================================================
Manages central fleet budget allocation for Discovery Engine Sheets:
  - Main Fleet (Sheets 1-4): reads from 'Production Budget Allocation' tab.
  - Testing / NIM Fleet (Sheet 1): reads from 'Test Fleet Budget Allocation' tab.
Both live in the Operations Dashboard ('1eVKXWnGNsHp8WmoqXo8Pz-mB6f7V7xGleMfd7CcahSA').
Provides mathematical 4-way budget splitting, payload transformations, and resilient fallbacks.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

MASTER_BUDGET_SPREADSHEET_ID = "1eVKXWnGNsHp8WmoqXo8Pz-mB6f7V7xGleMfd7CcahSA"
PROD_BUDGET_WORKSHEET_NAME = "Production Budget Allocation"
TEST_BUDGET_WORKSHEET_NAME = "Test Fleet Budget Allocation"

# Backward compatibility alias
MASTER_BUDGET_WORKSHEET_NAME = PROD_BUDGET_WORKSHEET_NAME

# Main Fleet Sheets mapped to their sheet numbers (1-4)
MAIN_FLEET_SHEET_MAP: Dict[str, int] = {
    "1IWXqnR52IzpFewE0QS0tlJFT65nJE_fmIoJs8ps9nZo": 1,
    "1QGraMZOJjc-EIZlt_CieOVpr1jxw6tLa_QnWgWHou5I": 2,
    "1MHdkFCsoAY0V7ix_pJ5SpWFq3EFpJEug2ntJk1FkEl8": 3,
    "1z3wH--c2qHGlNqW0e-6Mc8IVqh_kN5_fbWjILM4FxQM": 4,
}

# Test / NIM Fleet Sheets mapped to their sheet numbers (1-4)
TEST_FLEET_SHEET_MAP: Dict[str, int] = {
    "1XT_GlvBf2BGO4cEiskpUsK2Fzc5PzB-tcRHFMubDXbk": 1,
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
    Checks Main Fleet first, then Test Fleet.
    """
    if sheet_id in MAIN_FLEET_SHEET_MAP:
        return MAIN_FLEET_SHEET_MAP[sheet_id]
    if sheet_id in TEST_FLEET_SHEET_MAP:
        return TEST_FLEET_SHEET_MAP[sheet_id]
    return default

def resolve_fleet_and_sheet(
    sheet_id: str,
    sheet_num: Optional[int] = None,
    sheet_name: str = ""
) -> Tuple[str, str, int]:
    """
    Determines fleet category ('main' or 'test'), target master worksheet name,
    and the 1-based sheet slice number (1..4).
    
    Returns:
        (fleet_category, worksheet_name, resolved_sheet_num)
    """
    s_name_lower = (sheet_name or "").lower()
    
    # 1. Explicit Test / NIM Fleet matching
    if (
        sheet_id in TEST_FLEET_SHEET_MAP
        or sheet_num == 5
        or "nim" in s_name_lower
        or "test" in s_name_lower
    ):
        target_ws = TEST_BUDGET_WORKSHEET_NAME
        resolved_num = TEST_FLEET_SHEET_MAP.get(sheet_id)
        if resolved_num is None:
            if sheet_num and sheet_num > 4:
                resolved_num = sheet_num - 4
            else:
                resolved_num = sheet_num or 1
        return "test", target_ws, min(max(resolved_num, 1), 4)

    # 2. Main Fleet matching
    target_ws = PROD_BUDGET_WORKSHEET_NAME
    resolved_num = MAIN_FLEET_SHEET_MAP.get(sheet_id, sheet_num or 1)
    return "main", target_ws, min(max(resolved_num, 1), 4)

def load_payload_rows_for_sheet(
    gc,
    sheet_id: str,
    sheet_name: str = "Production Engine",
    sheet_num: Optional[int] = None
) -> Tuple[List[List[str]], str]:
    """
    Loads payload rows for the specified sheet.
    - If Main Fleet (Sheets 1-4): reads from 'Production Budget Allocation' in Operations Dashboard.
    - If Testing / NIM Fleet (Sheet 1): reads from 'Test Fleet Budget Allocation' in Operations Dashboard.
    - Fallback: reads the sheet's local 'CF_IF Engine - Input Payload' tab if master is unavailable.
    
    Returns:
        (rows, source_description)
    """
    fleet_cat, target_ws, resolved_num = resolve_fleet_and_sheet(sheet_id, sheet_num, sheet_name)
    fleet_label = "Testing / NIM Fleet" if fleet_cat == "test" else "Main Fleet"

    # 1. Attempt to read from the appropriate master worksheet in Operations Dashboard
    try:
        logger.info(f"[{sheet_name}] Loading {fleet_label} payloads from Operations Dashboard ('{target_ws}') for Sheet {resolved_num}...")
        master_sh = gc.open_by_key(MASTER_BUDGET_SPREADSHEET_ID)
        master_ws = master_sh.worksheet(target_ws)
        raw_rows = master_ws.get_all_values()
        
        if len(raw_rows) >= 3:
            # Transform rows into standard payload format for this sheet
            # Header row 1 is title, header row 2 is columns
            # Data starts at row 3 (index 2)
            transformed_rows = [raw_rows[0], raw_rows[1]]
            
            # Detect schema: 12-column single sheet (Test Fleet) vs 15-column 4-sheet (Production Fleet)
            header_col_count = len(raw_rows[1])
            is_single_sheet_schema = (target_ws == TEST_BUDGET_WORKSHEET_NAME) or (header_col_count <= 12)
            
            if is_single_sheet_schema:
                # Test Fleet: strictly 1 sheet (Sheet 1 Budget in Col I / idx 8)
                sheet_col_idx = 8
                payload_col_idx = 9            # Col J (New Updated Payload)
                platform_payload_col_idx = 10  # Col K (Platform Payload)
                platform_link_col_idx = 11     # Col L (Platform Link)
            else:
                # Production Fleet: 4 sheets (Sheet 1-4 Budget in Cols I-L / idx 8-11)
                sheet_col_idx = 7 + resolved_num
                payload_col_idx = 12           # Col M (New Updated Payload)
                platform_payload_col_idx = 13  # Col N (Platform Payload)
                platform_link_col_idx = 14     # Col O (Platform Link)

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
                
                # If sheet specific budget cell was not pre-evaluated as int:
                if sheet_b_val is None:
                    if is_single_sheet_schema:
                        sheet_b_val = overall_b
                    else:
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
                    r[payload_col_idx] if len(r) > payload_col_idx else "",
                    r[platform_payload_col_idx] if len(r) > platform_payload_col_idx else "",
                    r[platform_link_col_idx] if len(r) > platform_link_col_idx else "",
                ]
                transformed_rows.append(new_r)

            logger.info(f"[{sheet_name}] Successfully loaded {len(transformed_rows)-2} master payloads from Operations Dashboard for {fleet_label} Sheet {resolved_num}.")
            return transformed_rows, f"Operations Dashboard -> '{target_ws}' ({fleet_label} Sheet {resolved_num} slice)"
        else:
            logger.warning(f"[{sheet_name}] Master sheet '{target_ws}' has fewer than 3 rows. Falling back to local tab.")
    except Exception as e:
        logger.warning(f"[{sheet_name}] Could not load master budget allocation sheet from '{target_ws}' ({e}). Falling back to local tab.")

    # 2. Fallback: Load from local spreadsheet's "CF_IF Engine - Input Payload" tab if present
    try:
        logger.info(f"[{sheet_name}] Loading local payload tab 'CF_IF Engine - Input Payload' from sheet {sheet_id}...")
        sh = gc.open_by_key(sheet_id)
        ws = sh.worksheet("CF_IF Engine - Input Payload")
        rows = ws.get_all_values()
        return rows, f"Local Sheet ({sheet_id}) -> 'CF_IF Engine - Input Payload'"
    except Exception as e:
        logger.error(f"[{sheet_name}] Failed to load local payload tab ({e}). Returning empty payload list.")
        return [], f"Failed to load payloads ({e})"
