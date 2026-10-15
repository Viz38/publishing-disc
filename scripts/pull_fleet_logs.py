#!/usr/bin/env python3
"""
Fleet Logs Puller & Diagnostic Tool for Discovery Engine.
Fetches, aggregates, saves, and summarizes logs and progress from all remote desktop workers
(5321, 5080, Vishnu, Local) across Sheet1..Sheet4.
"""

import os
import sys
import json
import ssl
import argparse
import urllib.request
import urllib.error
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

# Terminal color codes
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load token from .env if available
def load_default_token() -> str:
    env_token = os.getenv("SERVICE_AUTH_TOKEN")
    if env_token:
        return env_token
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("SERVICE_AUTH_TOKEN="):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
        except Exception:
            pass
    return ""

DESKTOP_DOMAINS = {
    "5321": {
        "Sheet1": "https://5321-sheet1.rajath.world",
        "Sheet2": "https://5321-sheet2.rajath.world",
        "Sheet3": "https://5321-sheet3.rajath.world",
        "Sheet4": "https://5321-sheet4.rajath.world",
    },
    "5080": {
        "Sheet1": "https://5080-sheet1.rajath.world",
        "Sheet2": "https://5080-sheet2.rajath.world",
        "Sheet3": "https://5080-sheet3.rajath.world",
        "Sheet4": "https://5080-sheet4.rajath.world",
    },
    "vishnu": {
        "Sheet1": "https://vishnu-sheet1.rajath.world",
        "Sheet2": "https://vishnu-sheet2.rajath.world",
        "Sheet3": "https://vishnu-sheet3.rajath.world",
        "Sheet4": "https://vishnu-sheet4.rajath.world",
    },
    "local": {
        "Sheet1": "http://localhost:8769",
        "Sheet2": "http://localhost:8770",
        "Sheet3": "http://localhost:8771",
        "Sheet4": "http://localhost:8772",
    }
}

SHEET_LABELS = {
    "Sheet1": "CF-1",
    "Sheet2": "CF-2",
    "Sheet3": "CF-3",
    "Sheet4": "Filter-V2",
}

def create_ssl_context():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

def fetch_json(url: str, token: str, timeout: int = 10) -> Tuple[Optional[Dict[str, Any]], str]:
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "DiscoveryFleetLogsPuller/1.0"
    }
    req = urllib.request.Request(url, headers=headers)
    ctx = create_ssl_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            if resp.status == 200:
                body = resp.read().decode("utf-8", errors="replace")
                return json.loads(body), "OK"
            return None, f"HTTP_{resp.status}"
    except urllib.error.HTTPError as e:
        return None, f"HTTP_{e.code}"
    except urllib.error.URLError as e:
        return None, f"ConnectionError: {e.reason}"
    except Exception as e:
        return None, f"Error: {e}"

def scan_log_for_issues(log_content: str, max_issues: int = 5) -> List[str]:
    issues = []
    if not log_content:
        return issues
    
    critical_keywords = ["Traceback", "Exception", "ERROR", "CRITICAL", "FATAL", "Quota exceeded", "Rate limit", "timed out"]
    lines = log_content.splitlines()
    for line in lines:
        if any(kw in line for kw in critical_keywords):
            issues.append(line.strip())
            if len(issues) >= max_issues:
                break
    return issues

def pull_logs_for_target(
    desktop: str,
    sheet: str,
    base_url: str,
    token: str,
    lines: int,
    log_type: str,
    output_dir: str,
    timeout: int = 15
) -> Dict[str, Any]:
    sheet_num = sheet.replace("Sheet", "")
    endpoints_to_try = [
        f"{base_url}/discovery/logs?lines={lines}&log_type={log_type}",
        f"{base_url}/sheet{sheet_num}/logs?lines={lines}&log_type={log_type}"
    ]

    log_data = None
    last_err = ""
    for ep in endpoints_to_try:
        data, err = fetch_json(ep, token, timeout=timeout)
        if data:
            log_data = data
            break
        last_err = err

    status_data, status_err = fetch_json(f"{base_url}/discovery/status", token, timeout=5)

    is_reachable = (log_data is not None) or (status_data is not None)

    result = {
        "desktop": desktop,
        "sheet": sheet,
        "service": SHEET_LABELS.get(sheet, sheet),
        "url": base_url,
        "reachable": is_reachable,
        "logs_available": log_data is not None,
        "error": last_err if not is_reachable else (None if log_data else last_err),
        "status_data": status_data,
        "log_data": log_data,
        "saved_files": [],
        "issues": []
    }

    if is_reachable and output_dir:
        target_dir = os.path.join(output_dir, desktop, sheet)
        os.makedirs(target_dir, exist_ok=True)

        # Save individual logs
        logs = log_data.get("logs", {})
        for name, content in logs.items():
            if isinstance(content, str):
                file_path = os.path.join(target_dir, f"{name}.log")
                try:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    result["saved_files"].append(file_path)
                    issues = scan_log_for_issues(content)
                    if issues:
                        result["issues"].extend(issues[:3])
                except Exception as e:
                    pass

        # Save progress if included
        if "progress" in log_data and log_data["progress"]:
            prog_path = os.path.join(target_dir, "progress.json")
            try:
                with open(prog_path, "w", encoding="utf-8") as f:
                    json.dump(log_data["progress"], f, indent=2)
                result["saved_files"].append(prog_path)
            except Exception:
                pass

        # Save status snapshot
        if status_data:
            stat_path = os.path.join(target_dir, "status.json")
            try:
                with open(stat_path, "w", encoding="utf-8") as f:
                    json.dump(status_data, f, indent=2)
                result["saved_files"].append(stat_path)
            except Exception:
                pass

    return result

def main():
    parser = argparse.ArgumentParser(
        description="Discovery Engine Fleet Log Puller & Diagnostics",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-d", "--desktop",
        default="all",
        help="Desktop target: 'all', '5321', '5080', 'vishnu', 'local', or comma-separated (default: all)"
    )
    parser.add_argument(
        "-s", "--sheet",
        default="all",
        help="Sheet target: 'all', 'Sheet1'..'Sheet4', or '1'..'4' (default: all)"
    )
    parser.add_argument(
        "-n", "--lines",
        type=int,
        default=200,
        help="Number of lines to tail per log file (default: 200)"
    )
    parser.add_argument(
        "-t", "--log-type",
        default="all",
        choices=["all", "discovery", "stdout", "stderr", "api"],
        help="Log type to pull: all, discovery, stdout, stderr, api (default: all)"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=os.path.join(BASE_DIR, "Logs", "Fleet"),
        help="Directory where logs will be saved locally (default: Logs/Fleet)"
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Authorization token override (defaults to SERVICE_AUTH_TOKEN from .env)"
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Only display status summary table without error excerpts"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=12,
        help="Network request timeout in seconds (default: 12)"
    )

    args = parser.parse_args()

    token = args.token or load_default_token()
    if not token:
        print(f"{RED}Error: SERVICE_AUTH_TOKEN is missing.{RESET}")
        sys.exit(1)

    # Determine desktops
    if args.desktop.lower() == "all":
        desktops = ["5321", "5080"]
    else:
        desktops = [d.strip() for d in args.desktop.split(",") if d.strip() in DESKTOP_DOMAINS]

    # Determine sheets
    if args.sheet.lower() == "all":
        sheets = ["Sheet1", "Sheet2", "Sheet3", "Sheet4"]
    else:
        raw_sheets = [s.strip() for s in args.sheet.split(",")]
        sheets = []
        for s in raw_sheets:
            if s.isdigit():
                sheets.append(f"Sheet{s}")
            elif s.capitalize() in SHEET_LABELS:
                sheets.append(s.capitalize())
            elif s in SHEET_LABELS:
                sheets.append(s)

    print(f"\n{BOLD}{CYAN}================================================================================{RESET}")
    print(f"{BOLD}{CYAN}🚀 DISCOVERY ENGINE - FLEET LOG PULLER & DIAGNOSTIC REPORT{RESET}")
    print(f"{CYAN}================================================================================{RESET}")
    print(f"Target Desktops : {', '.join(desktops)}")
    print(f"Target Sheets   : {', '.join(sheets)}")
    print(f"Tailing Lines   : {args.lines}")
    print(f"Log Type        : {args.log_type}")
    print(f"Save Directory  : {args.output_dir}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}\n")

    results = []
    total_files_saved = 0

    for d in desktops:
        domain_map = DESKTOP_DOMAINS.get(d, {})
        for s in sheets:
            base_url = domain_map.get(s)
            if not base_url:
                continue

            print(f"Fetching {BOLD}[Desktop {d} | {s} ({SHEET_LABELS.get(s)})]{RESET} from {base_url} ...", end=" ", flush=True)
            res = pull_logs_for_target(
                desktop=d,
                sheet=s,
                base_url=base_url,
                token=token,
                lines=args.lines,
                log_type=args.log_type,
                output_dir=args.output_dir,
                timeout=args.timeout
            )
            results.append(res)

            if res["reachable"]:
                files_cnt = len(res["saved_files"])
                total_files_saved += files_cnt
                stat = res.get("status_data") or {}
                st = stat.get("status", "ONLINE").upper()
                if res["logs_available"]:
                    print(f"{GREEN}SUCCESS{RESET} ({files_cnt} files saved, status: {st})")
                else:
                    print(f"{YELLOW}ONLINE{RESET} (Status: {st}, Logs API needs desktop update)")
            else:
                print(f"{RED}FAILED{RESET} ({res['error']})")

    # Display Diagnostic Summary Table
    print(f"\n{BOLD}{CYAN}================================================================================{RESET}")
    print(f"{BOLD}📊 FLEET SUMMARY OVERVIEW{RESET}")
    print(f"{CYAN}================================================================================{RESET}")
    header = f"{'DESKTOP':<9} {'SHEET':<8} {'SERVICE':<10} {'STATUS':<12} {'PROGRESS':<16} {'PHASE':<10} {'ISSUES':<8}"
    print(header)
    print("-" * 80)

    for r in results:
        desktop = r["desktop"]
        sheet = r["sheet"]
        service = r["service"]
        stat = r.get("status_data") or {}
        
        if r["reachable"]:
            active = stat.get("active", False)
            status_text = stat.get("status", "online").upper()
            status_display = f"{GREEN}● {status_text}{RESET}" if active else f"{YELLOW}○ {status_text}{RESET}"
            
            cur = stat.get("progress_current", 0)
            tot = stat.get("progress_total", 0)
            prog_text = f"{cur}/{tot}"
            phase = str(stat.get("phase", "N/A"))
            
            issues_cnt = len(r["issues"])
            if not r["logs_available"]:
                issues_display = f"{YELLOW}API 404{RESET}"
            elif issues_cnt > 0:
                issues_display = f"{RED}{issues_cnt} ERR{RESET}"
            else:
                issues_display = f"{GREEN}0{RESET}"
        else:
            status_display = f"{RED}✖ UNREACHABLE{RESET}"
            prog_text = "-"
            phase = "-"
            issues_display = f"{RED}OFFLINE{RESET}"

        print(f"{desktop:<9} {sheet:<8} {service:<10} {status_display:<21} {prog_text:<16} {phase:<10} {issues_display}")

    # Display Error Excerpts if found and not summary-only
    if not args.summary_only:
        has_issues = False
        for r in results:
            if r["issues"]:
                if not has_issues:
                    print(f"\n{BOLD}{YELLOW}⚠️  DETECTED RECENT ISSUES / WARNINGS ACROSS FLEET LOGS:{RESET}")
                    has_issues = True
                print(f"\n{BOLD}[Desktop {r['desktop']} - {r['sheet']} ({r['service']})]:{RESET}")
                for iss in r["issues"][:3]:
                    print(f"  {RED}↳ {iss}{RESET}")

    print(f"\n{BOLD}{GREEN}✅ Done. Pulled logs saved to: {args.output_dir}{RESET}\n")

if __name__ == "__main__":
    main()
