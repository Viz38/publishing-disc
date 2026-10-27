"""
Official 57-Column Master Schema Constants (Columns A through BE / 0-indexed as 0 to 56).
Adheres strictly to GEMINI.md Section 4: Complete Column-by-Column Reference.
Eliminates magic numbers across all discovery pipeline sheets.
"""

from typing import Dict, Final

TOTAL_SCHEMA_COLUMNS: Final[int] = 57

# =============================================================================
# Section 1: Input Metadata (Cols 1-17 / A-Q / idx 0-16)
# =============================================================================
COL_DOMAIN: Final[int] = 0                  # Col A: Domain input (Total row count from Row 3 downwards)
COL_FUNNEL_NAME: Final[int] = 1             # Col B: Funnel Name
COL_FUNNEL_ID: Final[int] = 2               # Col C: Funnel ID
COL_FILTER_GROUP_NAME: Final[int] = 3       # Col D: Filter Group Name
COL_FILTER_GROUP_ID: Final[int] = 4         # Col E: Filter Group ID
COL_FILTER_NAME: Final[int] = 5             # Col F: Filter Name
COL_FILTER_ID: Final[int] = 6               # Col G: Filter ID
COL_PAYLOAD: Final[int] = 7                 # Col H: Payload metadata JSON
COL_PLATFORM_LINK: Final[int] = 8           # Col I: Tracxn platform URL
COL_DOMAIN_STATUS: Final[int] = 9           # Col J: HTTP status code
COL_DOMAIN_PROFILE_ID: Final[int] = 10      # Col K: Tracxn profile ID
COL_RESERVED_L: Final[int] = 11             # Col L: Reserved
COL_PUBLISHED_DATE: Final[int] = 12         # Col M: Domain publish timestamp
COL_INPUT_HASHTAG: Final[int] = 13          # Col N: Pre-existing input hashtags
COL_PS2_VARIABLE: Final[int] = 14           # Col O: Platform variable data
COL_PS2_SCORE: Final[int] = 15              # Col P: Platform relevance score
COL_TRACXN_DESCRIPTIONS: Final[int] = 16    # Col Q: Existing platform overview

# =============================================================================
# Section 2: Engine Stats (Cols 18-20 / R-T / idx 17-19)
# =============================================================================
COL_DB_ADDED_TIME: Final[int] = 17          # Col R: Database Added Time
COL_ENGINE_PICKED_TIME: Final[int] = 18     # Col S: Engine Pick Time
COL_ENGINE_CLEARED_TIME: Final[int] = 19    # Col T: Engine Clear Time

# =============================================================================
# Section 3: Tech Crawler (Cols 21-34 / U-AH / idx 20-33)
# =============================================================================
COL_CRAWLING_STATUS: Final[int] = 20        # Col U: DONE / To be checked
COL_TC_JOB_ID: Final[int] = 21              # Col V: Tech Crawler Job ID
COL_TC_JOB_GEN_TIME: Final[int] = 22        # Col W: Job Generation Timestamp
COL_TC_STATUS: Final[int] = 23              # Col X: Crawler Status (SUCCESS / FAILED)
COL_FINAL_URL: Final[int] = 24              # Col Y: Final resolved redirected URL
COL_S3_HTML: Final[int] = 25                # Col Z: S3 HTML link
COL_S3_MARKDOWN: Final[int] = 26            # Col AA: S3 Markdown link
COL_HTTP_STATUS: Final[int] = 27            # Col AB: Response code (200, 404, etc.)
COL_SIGNALS: Final[int] = 28                # Col AC: Scraper diagnostic signals
COL_RAW_DATA: Final[int] = 29               # Col AD: Scraped Markdown text (strictly capped at 49,000 chars)
COL_TC_COMPLETED_TIME: Final[int] = 30      # Col AE: Crawler completion timestamp
COL_VALIDATED_URL: Final[int] = 31          # Col AF: Crawler validated URL
COL_RESERVED_AG: Final[int] = 32            # Col AG: Reserved
COL_RESERVED_AH: Final[int] = 33            # Col AH: Reserved

# =============================================================================
# Section 4: AI Predictions (Cols 35-49 / AI-AW / idx 34-48)
# =============================================================================
COL_NIM_STATUS: Final[int] = 34             # Col AI: NIM Status (SUCCESS / NO_CONTENT / FAILED)
COL_NIM_SUMMARY: Final[int] = 35            # Col AJ: NIM Summary text (Llama 3 70B output)
COL_GEMINI_IF_INPUT: Final[int] = 36        # Col AK: Prompt input for Industry Funnel
COL_GEMINI_IF_STATUS: Final[int] = 37       # Col AL: Industry Funnel Status
COL_GEMINI_IF_NAME: Final[int] = 38         # Col AM: Target Industry Funnel Name
COL_GEMINI_IF_ID: Final[int] = 39           # Col AN: Target Industry Funnel ID
COL_VALIDATION_INPUT: Final[int] = 40       # Col AO: Prompt input for validation check
COL_VALIDATION_FLAG: Final[int] = 41        # Col AP: Validation flag (Yes / Different IF Found)
COL_FEED_INPUT: Final[int] = 42             # Col AQ: Prompt input for Feed classification
COL_FEED_NUMBER: Final[int] = 43            # Col AR: Gemini Output Feed Number (e.g. 27)
COL_FEED_NAME: Final[int] = 44              # Col AS: Feed Name (e.g. Marketing Services)
COL_RESERVED_AT: Final[int] = 45            # Col AT: Reserved
COL_PUBLISH_FUNNEL_NAME: Final[int] = 46    # Col AU: Publish Funnel Name
COL_PUBLISH_FUNNEL_ID: Final[int] = 47      # Col AV: Publish Funnel ID
COL_RESERVED_AW: Final[int] = 48            # Col AW: Reserved

# =============================================================================
# Section 5: Tracxn Movement & Hashtags (Cols 50-57 / AX-BE / idx 49-56)
# =============================================================================
COL_WRITE_API_STATUS: Final[int] = 49       # Col AX: CF->IF Write API Status (TRUE / FALSE)
COL_MOVEMENT_TIME: Final[int] = 50          # Col AY: CF->IF Movement Timestamp
COL_WRITE_API_PF_STATUS: Final[int] = 51    # Col AZ: IF->PF Write API Status
COL_MOVEMENT_PF_TIME: Final[int] = 52       # Col BA: IF->PF Movement Timestamp
COL_TAG_STATUS_IF: Final[int] = 53          # Col BB: Industry Funnel Tag flag
COL_TAG_STATUS_PF: Final[int] = 54          # Col BC: Publish Funnel Tag flag
COL_TAGS_ADDED_PROFILE: Final[int] = 55     # Col BD: Tags Added to Profile
COL_HASHTAG_DISCOVERY: Final[int] = 56      # Col BE: bu_llm_discovery_processing (yes/no)

# Mapping of index to official column name
COLUMN_INDEX_MAP: Final[Dict[int, str]] = {
    0: "Domain",
    1: "Funnel Name",
    2: "Funnel ID",
    3: "Filter Group Name",
    4: "Filter Group ID",
    5: "Filter Name",
    6: "Filter ID",
    7: "Payload",
    8: "Platform Link",
    9: "Domain Status",
    10: "Domain Profile ID",
    11: "Reserved_L",
    12: "Published Date",
    13: "HashTag",
    14: "PS2 Variable",
    15: "PS2 Score",
    16: "Tracxn Platform (Descriptions)",
    17: "Database Added Time",
    18: "Engine Pick Time",
    19: "Engine Clear Time",
    20: "Crawling Status",
    21: "Job ID",
    22: "TimeStamp_Gen",
    23: "Status",
    24: "Final URL",
    25: "S3 - HTML",
    26: "S3 - Markdown",
    27: "HTTP Status",
    28: "Signals",
    29: "Raw Data",
    30: "TimeStamp_Comp",
    31: "Validated URL",
    32: "Reserved_AG",
    33: "Reserved_AH",
    34: "NIM Status",
    35: "NIM Summary",
    36: "Gemini Input (IF)",
    37: "Gemini Output (IF Status)",
    38: "Gemini Output (IF Name)",
    39: "Gemini Output (IF ID)",
    40: "Gemini Input (Validation)",
    41: "Gemini Output (IF Validation)",
    42: "Gemini Input (Feed)",
    43: "Gemini Output (Feed Number)",
    44: "Feed Name",
    45: "Reserved_AT",
    46: "Publish Funnel Name",
    47: "Publish Funnel ID",
    48: "Reserved_AW",
    49: "Write API Status",
    50: "Movement Time",
    51: "Write API Status (PF)",
    52: "Movement Time (PF)",
    53: "bu_llm_industry_prediction",
    54: "bu_llm_feed_prediction",
    55: "Tags Added to Profile",
    56: "Hashtag (bu_llm_discovery_processing)",
}
