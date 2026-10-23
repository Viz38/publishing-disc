import os
import re
import time
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("StorageCleaner")

def clean_old_storage(base_dir: str, retention_days: int = 7) -> dict:
    """
    Cleans up discovery cache CSVs, log archives, and temp files older than retention_days.
    Checks both modification timestamp and YYYY-MM-DD patterns in file names.
    Returns summary of deleted files and freed space.
    """
    cutoff_time = time.time() - (retention_days * 86400)
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")

    deleted_files = []
    freed_bytes = 0

    target_patterns = [
        re.compile(r"discovery_cache_(\d{4}-\d{2}-\d{2})"),
        re.compile(r"Cache_discovery_cache_(\d{4}-\d{2}-\d{2})"),
    ]

    # Directories to scan for cached data and logs
    scan_dirs = [
        os.path.join(base_dir, "Logs"),
        os.path.join(base_dir, "Temp_Test", "Archived_CSVs_15_21_Oct"),
        os.path.join(base_dir, "Temp_Test", "archived_logs"),
    ]
    for s in ["Sheet1", "Sheet2", "Sheet3", "Sheet4"]:
        scan_dirs.append(os.path.join(base_dir, s, "Logs"))
        scan_dirs.append(os.path.join(base_dir, s, "Logs", "Cache"))

    for d in scan_dirs:
        if not os.path.exists(d):
            continue
        for root, _, files in os.walk(d):
            for fname in files:
                fpath = os.path.join(root, fname)
                is_expired = False

                # 1. Check filename for date pattern
                for pat in target_patterns:
                    m = pat.search(fname)
                    if m:
                        fdate_str = m.group(1)
                        if fdate_str < cutoff_date_str:
                            is_expired = True
                        break

                # 2. Check mtime if not matched by name but in a cache dir
                if not is_expired and ("Cache" in root or fname.endswith(".tmp") or fname.endswith(".csv")):
                    if not fname.startswith("next_day_pre_crawl") and not fname == "discovery_cache.csv":
                        try:
                            if os.path.getmtime(fpath) < cutoff_time:
                                is_expired = True
                        except OSError:
                            pass

                if is_expired:
                    try:
                        sz = os.path.getsize(fpath)
                        os.remove(fpath)
                        deleted_files.append(fpath)
                        freed_bytes += sz
                    except Exception as e:
                        logger.warning(f"Failed to remove {fpath}: {e}")

    freed_mb = freed_bytes / (1024 * 1024)
    logger.info(f"Storage cleanup complete: Removed {len(deleted_files)} files, freed {freed_mb:.2f} MB (retention: {retention_days} days).")
    return {
        "deleted_count": len(deleted_files),
        "freed_bytes": freed_bytes,
        "freed_mb": freed_mb,
        "deleted_files": deleted_files[:10]  # sample
    }

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    res = clean_old_storage(root_dir, retention_days=7)
    print(f"Cleaned {res['deleted_count']} files, freed {res['freed_mb']:.2f} MB")
