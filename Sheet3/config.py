import os
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
# Priority: root .env, then local folder .env
load_dotenv(os.path.join(ROOT_DIR, ".env"))
load_dotenv(os.path.join(BASE_DIR, ".env"))

@dataclass
class Settings:
    SERVICE_NAME: str = "CF-3"
    FULL_NAME: str = "Discovery AI - CF >> IF Automation (NVDEIA) - Nandan Domains DB - CF-3-Python Automation"
    SERVICE_AUTH_TOKEN: str = os.getenv("SERVICE_AUTH_TOKEN", "")
    DISCOVERY_PORT: int = int(os.getenv("SHEET3_PORT", os.getenv("DISCOVERY_PORT", "8771")))

    # Google Sheets Configuration
    CF_SHEET_ID: str = os.getenv("SHEET3_ID", os.getenv("CF_SHEET_ID", ""))
    CF_EXTRACTING_SHEET_NAME: str = os.getenv("CF_EXTRACTING_SHEET_NAME", "CF_IF_Engine_Database")
    IF_SHEET_ID: str = os.getenv("IF_SHEET_ID", "")
    IF_EXTRACTING_SHEET_NAME: str = os.getenv("IF_EXTRACTING_SHEET_NAME", "IF_Engine_Database")
    Config_Sheet_ID: str = os.getenv("CONFIG_SHEET_ID", "")

    # Credentials File
    CREDENTIALS_FILE: str = os.getenv("CREDENTIALS_FILE", "")
    GOOGLE_SERVICE_ACCOUNT_JSON: str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON: str = os.getenv("GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON", "")

    # Tokens & API Configurations
    GEMINI_TOKEN: str = os.getenv("GEMINI_API_KEY") or os.getenv("GEMINI_TOKEN", "")
    Tracxn_API_Token: str = os.getenv("TRACXN_API_TOKEN", "")

    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS_SHEET3", os.getenv("MAX_WORKERS_5080", os.getenv("MAX_WORKERS", "14"))))
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))

    # Nvidia NIM Configuration
    Nvidia_Base_Model: str = os.getenv("NVIDIA_BASE_MODEL", "meta/muse-glimmer-30b")
    Nvidia_Endpoint: str = os.getenv("NVIDIA_ENDPOINT", "https://integrate.api.nvidia.com/v1/chat/completions")
    NIM_API_KEYS: List[str] = field(default_factory=lambda: [
        k.strip() for k in os.getenv("NIM_API_KEYS", "").split(",") if k.strip()
    ])

    # Review tags
    To_Review_TAG_NAME: str = "bu_llm_domainformanualreview"
    Wrong_Validated_IF_Tag_Name: str = "bu_llm_domainwrong_if_validation"
    Wrong_Validated_Feed_Tag_Name: str = "bu_llm_domainwrong_feed_validation"
    Discovery_Processing_TAG_NAME: str = "bu_llm_discovery_processing"
    COMMENT: str = "This is a Write API Movement by Gemini"
    Wrong_Validation_COMMENT: str = "This is a Write API Movement by Gemini(Marked for Manager review)"

settings = Settings()
