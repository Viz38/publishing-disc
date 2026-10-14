#!/bin/bash

# ==========================================================
# 🚀 DISCOVERY ENGINE - PRODUCTION v0.1
# ==========================================================
# Supports: macOS (Intel/Silicon) & Ubuntu Linux
# ==========================================================

# Add Homebrew and User binaries to PATH
export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

BASE_DIR=$(cd "$(dirname "$0")" && pwd)
OS=$(uname -s)
REAL_USER=${SUDO_USER:-$USER}
REAL_GROUP=$(id -gn "$REAL_USER")
LOCK_FILE="/tmp/discovery_control_${REAL_USER:-$USER}.lock"

# --- Smart Sudo Detection ---
if [[ "$OS" == "Darwin" ]]; then
    MAYBE_SUDO=""
else
    MAYBE_SUDO="sudo"
fi

# --- Lock File Mechanism ---
if [ -f "$LOCK_FILE" ]; then
    OLD_PID=$(cat "$LOCK_FILE" 2>/dev/null)
    if [ -n "$OLD_PID" ] && ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo -e "\033[0;31m⚠️  Another instance of control.sh is already running (PID: $OLD_PID).\033[0m"
        if [ -t 0 ]; then
            read -p "Would you like to kill it and start fresh? (y/n): " kill_confirm
            if [[ "$kill_confirm" == "y" || "$kill_confirm" == "Y" ]]; then
                kill -9 "$OLD_PID" 2>/dev/null
                rm -f "$LOCK_FILE" 2>/dev/null
            else
                echo "Exiting."
                exit 1
            fi
        else
            echo "Non-interactive session detected. Killing old instance..."
            kill -9 "$OLD_PID" 2>/dev/null
            rm -f "$LOCK_FILE" 2>/dev/null
        fi
    else
        rm -f "$LOCK_FILE" 2>/dev/null
    fi
fi
if ( : > "$LOCK_FILE" ) 2>/dev/null; then
    echo $$ > "$LOCK_FILE" 2>/dev/null || true
    trap "rm -f '$LOCK_FILE' 2>/dev/null; exit" INT TERM EXIT
fi

# Centralized logging for control.sh
LOG_FILE="$BASE_DIR/control.logs"
SETUP_LOG="$BASE_DIR/setup.logs"

log() {
    local level=$1
    local msg=$2
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo -e "[$timestamp] $level: $msg" >> "$LOG_FILE"
}

echo -e "\n--- Session Started: $(date) ---" >> "$LOG_FILE"

ensure_system_dependencies() {
    local ARCH=$(uname -m 2>/dev/null || echo "x86_64")
    log "INFO" "Ensuring system dependencies (OS: $OS, Arch: $ARCH)"

    if [[ "$OS" == "Linux" ]]; then
        local CF_DEB_ARCH="amd64"
        local CF_RPM_ARCH="x86_64"
        case "$ARCH" in
            x86_64|amd64)
                CF_DEB_ARCH="amd64"
                CF_RPM_ARCH="x86_64"
                ;;
            aarch64|arm64)
                CF_DEB_ARCH="arm64"
                CF_RPM_ARCH="aarch64"
                ;;
            armv7l|armhf)
                CF_DEB_ARCH="arm"
                CF_RPM_ARCH="armhf"
                ;;
            *)
                CF_DEB_ARCH="amd64"
                CF_RPM_ARCH="x86_64"
                ;;
        esac

        # 1. Debian / Ubuntu family (apt-get)
        if command -v apt-get &>/dev/null; then
            local missing_cmds=()
            for cmd in git screen lsof curl; do
                if ! command -v $cmd &>/dev/null; then
                    missing_cmds+=("$cmd")
                fi
            done
            if ! dpkg -s ca-certificates &>/dev/null; then
                missing_cmds+=("ca-certificates")
            fi
            if [ ${#missing_cmds[@]} -gt 0 ]; then
                echo -e "${YELLOW}⚠️  Installing missing packages (${missing_cmds[*]})...${NC}"
                log "INFO" "Installing packages: ${missing_cmds[*]}"
                sudo apt-get update >> "$SETUP_LOG" 2>&1
                sudo apt-get install -y "${missing_cmds[@]}" >> "$SETUP_LOG" 2>&1
            fi

            local dev_pkgs=()
            if ! dpkg -s build-essential &>/dev/null; then dev_pkgs+=("build-essential"); fi
            if ! dpkg -s python3-dev &>/dev/null; then dev_pkgs+=("python3-dev"); fi
            if ! dpkg -s python3-venv &>/dev/null; then dev_pkgs+=("python3-venv"); fi
            if [ ${#dev_pkgs[@]} -gt 0 ]; then
                echo -e "${YELLOW}⚠️  Installing build & Python tools (${dev_pkgs[*]})...${NC}"
                log "INFO" "Installing dev tools: ${dev_pkgs[*]}"
                sudo apt-get update >> "$SETUP_LOG" 2>&1
                sudo apt-get install -y "${dev_pkgs[@]}" >> "$SETUP_LOG" 2>&1
            fi

            if ! command -v cloudflared &>/dev/null; then
                echo -e "${YELLOW}⚠️  Installing cloudflared ($CF_DEB_ARCH)...${NC}"
                log "INFO" "Installing cloudflared ($CF_DEB_ARCH)"
                curl -L --output /tmp/cloudflared.deb "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${CF_DEB_ARCH}.deb" >> "$SETUP_LOG" 2>&1
                sudo dpkg -i /tmp/cloudflared.deb >> "$SETUP_LOG" 2>&1 || sudo apt-get install -f -y >> "$SETUP_LOG" 2>&1
                rm -f /tmp/cloudflared.deb
            fi

        # 2. Red Hat / CentOS / Fedora / Rocky / AlmaLinux family (dnf / yum)
        elif command -v dnf &>/dev/null || command -v yum &>/dev/null; then
            local PKG_MGR="dnf"
            command -v dnf &>/dev/null || PKG_MGR="yum"
            local missing_cmds=()
            for cmd in git screen lsof curl; do
                if ! command -v $cmd &>/dev/null; then
                    missing_cmds+=("$cmd")
                fi
            done
            if ! command -v gcc &>/dev/null; then missing_cmds+=("gcc" "gcc-c++" "make"); fi
            if ! rpm -q python3-devel &>/dev/null; then missing_cmds+=("python3-devel"); fi
            if ! rpm -q ca-certificates &>/dev/null; then missing_cmds+=("ca-certificates"); fi

            if [ ${#missing_cmds[@]} -gt 0 ]; then
                echo -e "${YELLOW}⚠️  Installing missing packages (${missing_cmds[*]})...${NC}"
                log "INFO" "Installing packages: ${missing_cmds[*]}"
                sudo $PKG_MGR install -y "${missing_cmds[@]}" >> "$SETUP_LOG" 2>&1
            fi

            if ! command -v cloudflared &>/dev/null; then
                echo -e "${YELLOW}⚠️  Installing cloudflared ($CF_RPM_ARCH)...${NC}"
                log "INFO" "Installing cloudflared ($CF_RPM_ARCH)"
                curl -L --output /tmp/cloudflared.rpm "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${CF_RPM_ARCH}.rpm" >> "$SETUP_LOG" 2>&1
                sudo rpm -i /tmp/cloudflared.rpm >> "$SETUP_LOG" 2>&1 || sudo $PKG_MGR install -y /tmp/cloudflared.rpm >> "$SETUP_LOG" 2>&1
                rm -f /tmp/cloudflared.rpm
            fi

        # 3. Arch Linux family (pacman)
        elif command -v pacman &>/dev/null; then
            local missing_cmds=()
            for cmd in git screen lsof curl; do
                if ! command -v $cmd &>/dev/null; then
                    missing_cmds+=("$cmd")
                fi
            done
            if ! command -v gcc &>/dev/null; then missing_cmds+=("base-devel"); fi
            if ! command -v python3 &>/dev/null; then missing_cmds+=("python" "python-pip"); fi
            if [ ${#missing_cmds[@]} -gt 0 ]; then
                echo -e "${YELLOW}⚠️  Installing missing packages (${missing_cmds[*]})...${NC}"
                log "INFO" "Installing packages: ${missing_cmds[*]}"
                sudo pacman -Sy --noconfirm "${missing_cmds[@]}" >> "$SETUP_LOG" 2>&1
            fi

            if ! command -v cloudflared &>/dev/null; then
                echo -e "${YELLOW}⚠️  Installing cloudflared binary ($CF_DEB_ARCH)...${NC}"
                log "INFO" "Installing cloudflared binary ($CF_DEB_ARCH)"
                sudo curl -L --output /usr/local/bin/cloudflared "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${CF_DEB_ARCH}" >> "$SETUP_LOG" 2>&1
                sudo chmod +x /usr/local/bin/cloudflared 2>/dev/null
            fi
        fi

    elif [[ "$OS" == "Darwin" ]]; then
        if ! command -v git &>/dev/null; then
            echo -e "${YELLOW}⚠️  git is missing on macOS. Prompting installation...${NC}"
            if command -v brew &>/dev/null; then
                brew install git >> "$SETUP_LOG" 2>&1
            else
                xcode-select --install 2>/dev/null || true
            fi
        fi

        if ! command -v cloudflared &>/dev/null; then
            echo -e "${YELLOW}⚠️  cloudflared is missing on macOS.${NC}"
            if command -v brew &>/dev/null; then
                echo -e "   ▶ Installing cloudflared via Homebrew..."
                brew install cloudflared >> "$SETUP_LOG" 2>&1
            else
                local MAC_ARCH="arm64"
                [[ "$ARCH" == "x86_64" ]] && MAC_ARCH="amd64"
                echo -e "   ▶ Downloading cloudflared for macOS ($MAC_ARCH)..."
                curl -L --output /tmp/cloudflared.tgz "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-${MAC_ARCH}.tgz" >> "$SETUP_LOG" 2>&1
                tar -xzf /tmp/cloudflared.tgz -C /tmp/ >> "$SETUP_LOG" 2>&1
                sudo mv /tmp/cloudflared /usr/local/bin/cloudflared 2>/dev/null || mv /tmp/cloudflared "$HOME/.local/bin/cloudflared" 2>/dev/null
                chmod +x /usr/local/bin/cloudflared 2>/dev/null || chmod +x "$HOME/.local/bin/cloudflared" 2>/dev/null
                rm -f /tmp/cloudflared.tgz
            fi
        fi
    fi
}

ensure_system_dependencies


FOLDERS=("Sheet1" "Sheet2" "Sheet3" "Sheet4")
PORTS=(8769 8770 8771 8772)
NAMES=("discovery-cf1" "discovery-cf2" "discovery-cf3" "discovery-filterv2")
TITLES=("CF-1" "CF-2" "CF-3" "Filter-V2")
SHEET_IDS=(
    "1Sil5fw0l0rc0aMoydtnn7ZHdDbY5hwl7D9rda7bETkc"
    "1xphjNDv0hHEr8ZXv4PEVDmnEeVrsw_XAUMGaDf8ySUY"
    "10pacDW-6EqAdWslv9DTJBrRZDsybMczwMSpmKJebWFc"
    "14kC-RUMrpJB1Ur4CGpXMh1Te-yv7qBjsINJgM7NqySY"
)

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

get_python() {
    for v in 13 12 11 10; do
        if command -v "python3.$v" &> /dev/null; then echo "python3.$v"; return; fi
    done
    if command -v python3 &> /dev/null; then echo "python3"; return; fi
    echo ""
}

# Load environment variables from .env
if [ -f "$BASE_DIR/.env" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ "$line" =~ ^[[:space:]]*$ ]] && continue
        # Strip comments
        cleaned_line=$(echo "$line" | sed 's/[[:space:]]*#.*$//')
        if [[ "$cleaned_line" =~ = ]]; then
            export "$cleaned_line" 2>/dev/null
        fi
    done < "$BASE_DIR/.env"
    if [ -n "$GOOGLE_SERVICE_ACCOUNT_JSON" ]; then
        GOOGLE_SERVICE_ACCOUNT_JSON="${GOOGLE_SERVICE_ACCOUNT_JSON#\'}"
        GOOGLE_SERVICE_ACCOUNT_JSON="${GOOGLE_SERVICE_ACCOUNT_JSON%\'}"
        GOOGLE_SERVICE_ACCOUNT_JSON="${GOOGLE_SERVICE_ACCOUNT_JSON#\"}"
        GOOGLE_SERVICE_ACCOUNT_JSON="${GOOGLE_SERVICE_ACCOUNT_JSON%\"}"
        export GOOGLE_SERVICE_ACCOUNT_JSON
    fi
    if [ -n "$GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON" ]; then
        GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON="${GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON#\'}"
        GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON="${GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON%\'}"
        GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON="${GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON#\"}"
        GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON="${GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON%\"}"
        export GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON
    fi
fi

PYTHON_CMD=$(get_python)
CURRENT_USER=$(whoami)
HOSTNAME_LOWER=$(hostname 2>/dev/null | tr '[:upper:]' '[:lower:]')
USER_LOWER=$(echo "$CURRENT_USER" | tr '[:upper:]' '[:lower:]')

if [[ "$HOSTNAME_LOWER" == *"5321"* || "$USER_LOWER" == *"5321"* ]]; then
    IDENTITY="Desktop-5321"
    [ -z "$TUNNEL_TOKEN" ] && TUNNEL_TOKEN="$TUNNEL_TOKEN_5321"
elif [[ "$HOSTNAME_LOWER" == *"5080"* || "$USER_LOWER" == *"5080"* ]]; then
    IDENTITY="Desktop-5080"
    [ -z "$TUNNEL_TOKEN" ] && TUNNEL_TOKEN="$TUNNEL_TOKEN_5080"
elif [[ "$USER_LOWER" == *"vishnu"* ]]; then
    IDENTITY="Device-Vishnu"
    [ -z "$TUNNEL_TOKEN" ] && TUNNEL_TOKEN="$TUNNEL_TOKEN_VISHNU"
else
    IDENTITY="${WORKER_IDENTITY:-Desktop-Local}"
fi
export WORKER_IDENTITY="$IDENTITY"
log "INFO" "Session started for $IDENTITY (User: $CURRENT_USER)"

check_port() {
    local port=$1
    if command -v lsof &>/dev/null; then
        lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1
        return $?
    elif command -v ss &>/dev/null; then
        ss -ltn 2>/dev/null | grep -E ":$port |:$port$" >/dev/null
        return $?
    else
        netstat -ltn 2>/dev/null | grep -E ":$port |:$port$" >/dev/null
        return $?
    fi
}

check_status() {
    echo -e "${BLUE}📡 System Status (${IDENTITY}):${NC}"
    for i in "${!FOLDERS[@]}"; do
        local f_name="${FOLDERS[$i]}"
        local f_port="${PORTS[$i]}"
        local f_title="${TITLES[$i]}"
        if check_port $f_port; then
            echo -e "   ▶ ${f_name} [${f_title}]: ${GREEN}ONLINE${NC} (Port ${f_port})"
        else
            echo -e "   ▶ ${f_name} [${f_title}]: ${RED}OFFLINE${NC} (Port ${f_port})"
        fi
    done

    if [[ "$OS" == "Darwin" ]]; then
        echo -ne "   ▶ LaunchAgents: "
        launchctl list 2>/dev/null | grep -qE "discovery-cf|discovery-sheet|com.tracxn.discovery" && echo -e "${GREEN}LOADED${NC}" || echo -e "${YELLOW}NONE${NC}"
    else
        echo -ne "   ▶ Systemd: "
        systemctl list-units --type=service 2>/dev/null | grep -qE "discovery-cf|discovery-sheet" && echo -e "${GREEN}LOADED${NC}" || echo -e "${YELLOW}NONE${NC}"
    fi

    if pgrep -x "cloudflared" >/dev/null 2>&1; then
        echo -e "   ▶ Cloudflare Tunnel: ${GREEN}CONNECTED${NC}"
    else
        echo -e "   ▶ Cloudflare Tunnel: ${RED}DISCONNECTED${NC}"
    fi
}

check_for_updates() {
    if [ ! -d "$BASE_DIR/.git" ]; then
        echo -e "${RED}❌ Error: This folder was not cloned via Git.${NC}"
        echo -e "${YELLOW}Updates are only supported if the repository was cloned using 'git clone'.${NC}"
        return
    fi
    echo -e "${BLUE}🔍 Checking for updates across all branches...${NC}"
    cd "$BASE_DIR"
    git fetch --all --prune &>/dev/null

    local CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "main")
    local HAS_UPDATES=0
    local BRANCHES_WITH_UPDATES=()

    for b in $(git for-each-ref --format='%(refname:short)' refs/heads/); do
        local up=$(git rev-parse --abbrev-ref "${b}@{u}" 2>/dev/null || echo "origin/$b")
        if git rev-parse --verify "$up" &>/dev/null; then
            local INCOMING=$(git rev-list --count "$b..$up" 2>/dev/null || echo "0")
            if [ "$INCOMING" -gt 0 ]; then
                HAS_UPDATES=1
                BRANCHES_WITH_UPDATES+=("$b ($INCOMING incoming commit(s) from $up)")
            fi
        fi
    done

    if [ $HAS_UPDATES -eq 1 ]; then
        echo -e "${YELLOW}✨ New updates available for branches:${NC}"
        for b_info in "${BRANCHES_WITH_UPDATES[@]}"; do
            echo -e "   • ${YELLOW}$b_info${NC}"
        done
        echo -e "${RED}⚠️  Warning: Updating will stop running processes and discard local code changes on updated branches.${NC}"
        echo -e "${BLUE}ℹ️  .env, virtual environments, and logs will remain preserved.${NC}"
        read -p "Do you want to update now? (y/n): " confirm
        if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
            stop_all
            echo -e "${BLUE}🗑️  Removing uv.lock...${NC}"
            rm -f "$BASE_DIR/uv.lock"

            local CURRENT_UP=$(git rev-parse --abbrev-ref "${CURRENT_BRANCH}@{u}" 2>/dev/null || echo "origin/$CURRENT_BRANCH")
            local ACTIVE_INCOMING=0
            if git rev-parse --verify "$CURRENT_UP" &>/dev/null; then
                ACTIVE_INCOMING=$(git rev-list --count "$CURRENT_BRANCH..$CURRENT_UP" 2>/dev/null || echo "0")
            fi

            if [ "$ACTIVE_INCOMING" -gt 0 ]; then
                echo -e "${BLUE}🔄 Discarding local tracked code changes on active branch ($CURRENT_BRANCH)...${NC}"
                git reset --hard HEAD &>/dev/null
            fi

            echo -e "${BLUE}📥 Updating branches with incoming changes...${NC}"
            for b in $(git for-each-ref --format='%(refname:short)' refs/heads/); do
                local up=$(git rev-parse --abbrev-ref "${b}@{u}" 2>/dev/null || echo "origin/$b")
                if git rev-parse --verify "$up" &>/dev/null; then
                    local INCOMING=$(git rev-list --count "$b..$up" 2>/dev/null || echo "0")
                    if [ "$INCOMING" -gt 0 ]; then
                        if [ "$b" == "$CURRENT_BRANCH" ]; then
                            echo -e "   ▶ Updating active branch ($INCOMING new commits): ${GREEN}$b${NC} -> ${GREEN}$up${NC}"
                            git reset --hard "$up" &>/dev/null || git pull --force origin "$b" &>/dev/null
                        else
                            echo -e "   ▶ Updating background branch ($INCOMING new commits): ${GREEN}$b${NC} -> ${GREEN}$up${NC}"
                            git branch -f "$b" "$up" &>/dev/null
                        fi
                    else
                        echo -e "   ⏭️  Preserving local branch ${BLUE}$b${NC} (no incoming changes from $up)"
                    fi
                fi
            done

            echo -e "${GREEN}✅ All updates applied successfully.${NC}"
            read -p "Hit ENTER to exit and restart the script manually..."
            exit 0
        fi
    else
        echo -e "${GREEN}✅ You are on the latest version across all branches.${NC}"
    fi
}

clear_logs() {
    echo -e "${YELLOW}🧹 Clear Logs Menu:${NC}"
    echo "1) Clear All Logs (Including control.logs)"
    echo "2) Clear Sheet 1 Logs"
    echo "3) Clear Sheet 2 Logs"
    echo "4) Clear Sheet 3 Logs"
    echo "5) Clear Sheet 4 Logs"
    echo "6) Clear control.logs & setup.logs Only"
    echo "7) Back"
    read -p "Option [1-7]: " log_opt
    case $log_opt in
        1)
            for f in "$BASE_DIR"/Sheet*/Logs/*.log "$BASE_DIR"/Sheet*/Logs/*.logs; do
                [ -f "$f" ] && > "$f"
            done
            for f in "${FOLDERS[@]}"; do
                rm -f "$BASE_DIR/$f/.stop_requested" "$BASE_DIR/$f/.progress.json" "$BASE_DIR/$f/.progress.json.tmp."* 2>/dev/null
            done
            rm -f "$BASE_DIR/.stop_requested" "$BASE_DIR/.progress.json" 2>/dev/null
            > "$LOG_FILE"
            > "$SETUP_LOG"
            echo -e "${GREEN}✅ All logs cleared.${NC};;
        2)
            for f in "$BASE_DIR/Sheet1"/Logs/*.log "$BASE_DIR/Sheet1"/Logs/*.logs; do [ -f "$f" ] && > "$f"; done
            rm -f "$BASE_DIR/Sheet1/.stop_requested" "$BASE_DIR/Sheet1/.progress.json" 2>/dev/null
            echo -e "${GREEN}✅ Sheet 1 logs cleared.${NC};;
        3)
            for f in "$BASE_DIR/Sheet2"/Logs/*.log "$BASE_DIR/Sheet2"/Logs/*.logs; do [ -f "$f" ] && > "$f"; done
            rm -f "$BASE_DIR/Sheet2/.stop_requested" "$BASE_DIR/Sheet2/.progress.json" 2>/dev/null
            echo -e "${GREEN}✅ Sheet 2 logs cleared.${NC};;
        4)
            for f in "$BASE_DIR/Sheet3"/Logs/*.log "$BASE_DIR/Sheet3"/Logs/*.logs; do [ -f "$f" ] && > "$f"; done
            rm -f "$BASE_DIR/Sheet3/.stop_requested" "$BASE_DIR/Sheet3/.progress.json" 2>/dev/null
            echo -e "${GREEN}✅ Sheet 3 logs cleared.${NC};;
        5)
            for f in "$BASE_DIR/Sheet4"/Logs/*.log "$BASE_DIR/Sheet4"/Logs/*.logs; do [ -f "$f" ] && > "$f"; done
            rm -f "$BASE_DIR/Sheet4/.stop_requested" "$BASE_DIR/Sheet4/.progress.json" 2>/dev/null
            echo -e "${GREEN}✅ Sheet 4 logs cleared.${NC};;
        6)
            > "$LOG_FILE"
            > "$SETUP_LOG"
            echo -e "${GREEN}✅ control.logs and setup.logs cleared.${NC};;
        *) return;;
    esac
}

stop_all() {
    echo -e "${RED}🛑 Stopping all services...${NC}"

    if [[ "$OS" == "Darwin" ]]; then
        local all_labels=("discovery-cf1" "discovery-cf2" "discovery-cf3" "discovery-filterv2" "discovery-sheet1" "discovery-sheet2" "discovery-sheet3" "discovery-sheet4")
        for n in "${all_labels[@]}"; do
            launchctl bootout "gui/$(id -u)/$n" 2>/dev/null
            launchctl unload "$HOME/Library/LaunchAgents/$n.plist" 2>/dev/null
            rm -f "$HOME/Library/LaunchAgents/$n.plist" 2>/dev/null
        done
        for n in "${NAMES[@]}"; do screen -X -S "${n}_Engine" quit 2>/dev/null; done
    else
        for n in "${NAMES[@]}"; do
            sudo systemctl disable --now $n 2>/dev/null
            sudo rm -f "/etc/systemd/system/$n.service" 2>/dev/null
        done
        sudo systemctl daemon-reload 2>/dev/null
        for n in "${NAMES[@]}"; do screen -X -S "${n}_Engine" quit 2>/dev/null; done
    fi

    # Unified Cloudflare Cleanup
    if [[ "$OS" == "Linux" ]]; then
        sudo systemctl disable --now cloudflared 2>/dev/null
    else
        if [ -f "/Library/LaunchDaemons/com.cloudflare.cloudflared.plist" ]; then
            sudo launchctl unload -w /Library/LaunchDaemons/com.cloudflare.cloudflared.plist 2>/dev/null
        fi
    fi

    # Nuclear Cleanup for orphaned processes
    pkill -f "uvicorn.*8769" 2>/dev/null
    pkill -f "uvicorn.*8770" 2>/dev/null
    pkill -f "uvicorn.*8771" 2>/dev/null
    pkill -f "uvicorn.*8772" 2>/dev/null
    pkill -f "cloudflared" 2>/dev/null

    for p in "${PORTS[@]}"; do lsof -ti :$p 2>/dev/null | xargs kill -9 2>/dev/null; done
    echo -e "${GREEN}Cleanup complete.${NC}"
}

verify_port() {
    local port=$1
    local name=$2
    local f_path=$3
    echo -ne "   ⏳ Waiting for $name (Port $port)..."
    sleep 2
    for i in {1..30}; do
        if check_port $port; then
            echo -e " ${GREEN}ONLINE${NC}"
            log "INFO" "Service $name started on port $port"
            return 0
        fi
        sleep 1
    done
    echo -e " ${RED}FAILED${NC}"
    log "ERROR" "Service $name failed to start on port $port"
    echo -e "${YELLOW}   Last logs from $name:${NC}"
    if [[ "$OS" == "Linux" ]] && systemctl is-active --quiet "$name" 2>/dev/null; then
        sudo journalctl -u "$name" -n 5 --no-pager | sed 's/^/      /'
    else
        tail -n 5 "$f_path/Logs/api.logs" 2>/dev/null | sed 's/^/      /'
    fi
    return 1
}

restore_credentials() {
    local f_name=$1
    local f_path=$2
    local f_creds="$f_path/creds.json"

    # In Discovery Engine, credentials can be read directly from GOOGLE_SERVICE_ACCOUNT_JSON in .env
    # If a creds.json file is required, extract it from .env
    if [ ! -f "$f_creds" ] && [ -n "$GOOGLE_SERVICE_ACCOUNT_JSON" ]; then
        echo -e "   ▶ Generating $f_name creds.json from .env..."
        local clean_json="$GOOGLE_SERVICE_ACCOUNT_JSON"
        clean_json="${clean_json#\'}"
        clean_json="${clean_json%\'}"
        clean_json="${clean_json#\"}"
        clean_json="${clean_json%\"}"
        echo "$clean_json" > "$f_creds"
        chmod 600 "$f_creds"
    fi
}

auto_create_credentials_explicit() {
    local force_overwrite=$1
    echo -e "${BLUE}🔑 Verifying credentials in .env...${NC}"

    if [ -f "$BASE_DIR/.env" ]; then
        source "$BASE_DIR/.env" 2>/dev/null
    else
        echo -e "${RED}❌ .env file not found in $BASE_DIR!${NC}"
        return 1
    fi

    if [ -n "$GOOGLE_SERVICE_ACCOUNT_JSON" ]; then
        echo -e "${GREEN}✅ GOOGLE_SERVICE_ACCOUNT_JSON is present in .env!${NC}"
        local clean_json="$GOOGLE_SERVICE_ACCOUNT_JSON"
        clean_json="${clean_json#\'}"
        clean_json="${clean_json%\'}"
        clean_json="${clean_json#\"}"
        clean_json="${clean_json%\"}"
        for f in "${FOLDERS[@]}"; do
            local f_creds="$BASE_DIR/$f/creds.json"
            if [ -f "$f_creds" ] && [ "$force_overwrite" != "true" ]; then
                echo -e "   ▶ $f/creds.json already exists."
            else
                echo "$clean_json" > "$f_creds"
                chmod 600 "$f_creds"
                echo -e "   ${GREEN}▶ Created $f/creds.json${NC}"
            fi
        done
    else
        echo -e "${RED}❌ GOOGLE_SERVICE_ACCOUNT_JSON is missing or empty in .env!${NC}"
    fi
}

configure_credentials_menu() {
    while true; do
        clear
        clear
        echo -e "${BLUE}==========================================================${NC}"
        echo -e "🔑  DISCOVERY ENGINE - CREDENTIALS CONFIGURATION  🔑"
        echo -e "${BLUE}==========================================================${NC}"

        if [ -f "$BASE_DIR/.env" ]; then
            source "$BASE_DIR/.env" 2>/dev/null
        fi

        if [ -n "$GOOGLE_SERVICE_ACCOUNT_JSON" ]; then
            echo -e "   ▶ Primary GOOGLE_SERVICE_ACCOUNT_JSON (.env): ${GREEN}CONFIGURED${NC} (${#GOOGLE_SERVICE_ACCOUNT_JSON} chars)"
        else
            echo -e "   ▶ Primary GOOGLE_SERVICE_ACCOUNT_JSON (.env): ${RED}MISSING${NC}"
        fi
        if [ -n "$GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON" ]; then
            echo -e "   ▶ Backup GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON (.env): ${GREEN}CONFIGURED${NC} (${#GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON} chars)"
        else
            echo -e "   ▶ Backup GOOGLE_BACKUP_SERVICE_ACCOUNT_JSON (.env): ${YELLOW}NOT CONFIGURED (Optional)${NC}"
        fi

        for f in "${FOLDERS[@]}"; do
            local f_creds="$BASE_DIR/$f/creds.json"
            if [ -f "$f_creds" ]; then
                local size=$(wc -c < "$f_creds" | tr -d ' ')
                echo -e "   ▶ $f/creds.json: ${GREEN}PRESENT${NC} ($size bytes)"
            else
                echo -e "   ▶ $f/creds.json: ${YELLOW}OPTIONAL (loads from .env)${NC}"
            fi
        done
        echo ""
        echo "----------------------------------------------------------"
        echo "1) Verify / Re-export creds.json files from .env"
        echo "2) Manually paste service account JSON to .env"
        echo "3) Back to Main Menu"
        echo -e "${BLUE}==========================================================${NC}"
        read -p "Option [1-3]: " cred_opt

        case $cred_opt in
            1)
                auto_create_credentials_explicit true
                read -p "Press Enter to continue..."
                ;;
            2)
                echo -e "${BLUE}Paste raw service account JSON content (press ENTER, then Ctrl+D when finished):${NC}"
                RAW_JSON=$(cat)
                if [ -n "$RAW_JSON" ]; then
                    # Escaping single quotes safely or storing one-liner
                    ONE_LINE=$(echo "$RAW_JSON" | tr '\n' ' ' | sed 's/  */ /g')
                    if grep -q "GOOGLE_SERVICE_ACCOUNT_JSON=" "$BASE_DIR/.env" 2>/dev/null; then
                        # Replace in .env
                        python3 -c "
with open('$BASE_DIR/.env') as f:
    lines = f.readlines()
with open('$BASE_DIR/.env', 'w') as f:
    for line in lines:
        if line.startswith('GOOGLE_SERVICE_ACCOUNT_JSON='):
            f.write('GOOGLE_SERVICE_ACCOUNT_JSON=' + repr('$ONE_LINE') + '\n')
        else:
            f.write(line)
"
                    else
                        echo "GOOGLE_SERVICE_ACCOUNT_JSON='$ONE_LINE'" >> "$BASE_DIR/.env"
                    fi
                    echo -e "${GREEN}✅ Successfully saved credentials to .env!${NC}"
                else
                    echo -e "${RED}❌ Input was empty. No changes made.${NC}"
                fi
                read -p "Press Enter to continue..."
                ;;
            3|*)
                return
                ;;
        esac
    done
}

create_runner() {
    local f_path=$1
    local f_port=$2
    local f_label=$3
    local runner="$f_path/runner.sh"

    $MAYBE_SUDO mkdir -p "$f_path/Logs"
    if [[ "$OS" == "Linux" ]]; then
        sudo chown -R $REAL_USER:$REAL_GROUP "$f_path"
    fi
    $MAYBE_SUDO chmod -R 775 "$f_path/Logs"
    touch "$f_path/Logs/api.logs"

    local uv_bin_path=$(command -v uv)
    local uvicorn_cmd
    if [ -f "$BASE_DIR/.venv/bin/uvicorn" ]; then
        uvicorn_cmd="\"$BASE_DIR/.venv/bin/uvicorn\""
    elif [ -n "$uv_bin_path" ]; then
        uvicorn_cmd="\"$uv_bin_path\" run uvicorn"
    else
        uvicorn_cmd="python3 -m uvicorn"
    fi

    cat <<EOF > "$runner"
#!/bin/bash
cd "$f_path"
export PYTHONPATH="$BASE_DIR:$f_path:\$PYTHONPATH"
export PYTHONUNBUFFERED=1
$uvicorn_cmd api:app --host 0.0.0.0 --port $f_port --workers 1 --log-level info >> "$f_path/Logs/api.logs" 2>&1
EOF
    chmod +x "$runner"
    xattr -d com.apple.quarantine "$runner" 2>/dev/null
}

start_standard() {
    stop_all
    echo -e "${BLUE}🚀 Starting Engines in Temp Persistent Background (via Screen)...${NC}"
    log "INFO" "Starting all engines via Screen"

    export PYTHONPATH="$BASE_DIR:$PYTHONPATH"

    # Cloudflare Tunnel setup
    if [[ "$OS" == "Darwin" ]]; then
        if [ ! -f "/Library/LaunchDaemons/com.cloudflare.cloudflared.plist" ] && [ -n "$TUNNEL_TOKEN" ]; then
            echo -e "${YELLOW}   ▶ Installing Cloudflare Tunnel Service (requires sudo)...${NC}"
            sudo cloudflared service install "$TUNNEL_TOKEN" 2>/dev/null
        fi
        sudo launchctl load -w /Library/LaunchDaemons/com.cloudflare.cloudflared.plist 2>/dev/null
    else
        if [ ! -f "/etc/systemd/system/cloudflared.service" ] && [ -n "$TUNNEL_TOKEN" ]; then
            echo -e "${YELLOW}   ▶ Installing Cloudflare Tunnel Service (requires sudo)...${NC}"
            sudo cloudflared service install "$TUNNEL_TOKEN" 2>/dev/null
            sudo systemctl daemon-reload 2>/dev/null
        fi
        sudo systemctl enable --now cloudflared 2>/dev/null
    fi

    # Check for unified venv
    if [ ! -d "$BASE_DIR/.venv" ]; then
        echo -e "${RED}❌ Unified Virtual Environment missing!${NC}"
        echo -e "${YELLOW}   Please run Option 1 (Init Workspace) first.${NC}"
        return
    fi

    for i in "${!FOLDERS[@]}"; do
        local f_name="${FOLDERS[$i]}"
        local f_port="${PORTS[$i]}"
        local f_path="$BASE_DIR/$f_name"
        local f_label="${NAMES[$i]}"
        local f_title="${TITLES[$i]}"

        restore_credentials "$f_name" "$f_path"

        echo -e "   ▶ Launching ${f_name} [${f_title}] on port ${f_port}..."
        mkdir -p "$f_path/Logs"
        create_runner "$f_path" "$f_port" "$f_label"
        screen -d -m -S "${f_label}_Engine" "$f_path/runner.sh"
        verify_port $f_port "$f_name" "$f_path"
    done
    echo -e "${GREEN}Engines and Tunnel active.${NC}"
}

start_service_mode() {
    stop_all
    echo -e "${BLUE}🚀 Installing OS-Native Services (Production Mode)...${NC}"
    log "INFO" "Starting all engines in Production Service Mode"

    # Tunnel
    if [[ "$OS" == "Darwin" ]]; then
        if [ ! -f "/Library/LaunchDaemons/com.cloudflare.cloudflared.plist" ] && [ -n "$TUNNEL_TOKEN" ]; then
            sudo cloudflared service install "$TUNNEL_TOKEN" 2>/dev/null
        fi
        sudo launchctl load -w /Library/LaunchDaemons/com.cloudflare.cloudflared.plist 2>/dev/null
    else
        if [ ! -f "/etc/systemd/system/cloudflared.service" ] && [ -n "$TUNNEL_TOKEN" ]; then
            sudo cloudflared service install "$TUNNEL_TOKEN" 2>/dev/null
            sudo systemctl daemon-reload 2>/dev/null
        fi
        sudo systemctl enable --now cloudflared 2>/dev/null
    fi

    for i in "${!FOLDERS[@]}"; do
        local f_name="${FOLDERS[$i]}"
        local f_port="${PORTS[$i]}"
        local f_path="$BASE_DIR/$f_name"
        local f_label="${NAMES[$i]}"
        local f_title="${TITLES[$i]}"
        local f_runner="$f_path/runner.sh"

        restore_credentials "$f_name" "$f_path"
        create_runner "$f_path" "$f_port" "$f_label"

        if [[ "$OS" == "Darwin" ]]; then
            local plist="$HOME/Library/LaunchAgents/$f_label.plist"
            mkdir -p "$HOME/Library/LaunchAgents"
            cat <<EOF > "$plist"
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
    <key>Label</key><string>$f_label</string>
    <key>ProgramArguments</key><array><string>$f_runner</string></array>
    <key>WorkingDirectory</key><string>$f_path</string>
    <key>RunAtLoad</key><true/><key>KeepAlive</key><true/>
    <key>StandardOutPath</key><string>$f_path/Logs/api.logs</string>
    <key>StandardErrorPath</key><string>$f_path/Logs/api.logs</string>
</dict></plist>
EOF
            chmod 644 "$plist"
            launchctl load -w "$plist" 2>/dev/null
        else
            sudo bash -c "cat <<EOF > /etc/systemd/system/$f_label.service
[Unit]
Description=Discovery $f_title ($f_label)
After=network.target
[Service]
Type=simple
User=$REAL_USER
WorkingDirectory=$f_path
ExecStart=$f_runner
Restart=always
RestartSec=5
StandardOutput=append:$f_path/Logs/api.logs
StandardError=append:$f_path/Logs/api.logs
[Install]
WantedBy=multi-user.target
EOF"
            sudo systemctl daemon-reload
            sudo systemctl enable --now $f_label
        fi
        verify_port $f_port "$f_name" "$f_path"
    done
    echo -e "${GREEN}Services registered and persistent across reboots.${NC}"
}

# --- Non-Interactive CLI Command Dispatch ---
if [ -n "$1" ]; then
    case "$1" in
        pull-logs|pull_logs|fetch-logs)
            shift
            if [ -f "$BASE_DIR/.venv/bin/python" ]; then
                "$BASE_DIR/.venv/bin/python" "$BASE_DIR/scripts/pull_fleet_logs.py" "$@"
            else
                python3 "$BASE_DIR/scripts/pull_fleet_logs.py" "$@"
            fi
            exit $?
            ;;
        status)
            check_status
            exit 0
            ;;
        stop)
            stop_all
            exit 0
            ;;
        start)
            start_standard
            exit 0
            ;;
        clear-logs)
            clear_logs
            exit 0
            ;;
    esac
fi

# --- Main Interactive Menu ---
while true; do
    clear
    clear
    echo -e "${BLUE}==========================================================${NC}"
    echo -e "🚀  DISCOVERY ENGINE - PRODUCTION v0.1  🚀"
    echo -e "👤  Identity: $IDENTITY | Python: ${PYTHON_CMD:-'NOT FOUND'} | OS: $OS"
    echo -e "${BLUE}==========================================================${NC}"
    check_status
    echo -e "----------------------------------------------------------"
    echo "1) Init Workspace (Venv + Dependencies via uv + Credentials)"
    echo "2) Start Engines (Temp Persistent Background via Screen)"
    if [[ "$OS" == "Linux" ]]; then
        echo "3) Start Engines (Production Service Mode - Ubuntu Systemd)"
    else
        echo "3) Start Engines (Production Service Mode - macOS LaunchAgents)"
    fi
    echo "4) Stop & Clean All"
    echo "5) View Live Logs"
    echo "6) Check for Updates"
    echo "7) Clear Logs"
    echo "8) Configure Credentials"
    echo "9) Exit"
    echo "10) Deep Clean Workspace (Venvs + Tunnel + Logs)"
    echo "11) Pull Remote Fleet Logs (5321 & 5080)"
    echo -e "${BLUE}==========================================================${NC}"
    read -p "Option [1-11]: " opt
    case $opt in
        1)
            ensure_system_dependencies
            if [ -z "$PYTHON_CMD" ]; then echo -e "${RED}Error: No Python 3.10+ found!${NC}"; sleep 3; continue; fi

            export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
            if ! command -v uv &> /dev/null; then
                echo -e "${YELLOW}⚠️  'uv' (ultra-fast package installer) is missing!${NC}"
                echo -e "${BLUE}▶ Installing 'uv' globally...${NC}"
                curl -LsSf https://astral.sh/uv/install.sh | sh
                export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
                if command -v uv &> /dev/null; then
                    echo -e "${GREEN}✅ 'uv' installed successfully!${NC}"
                else
                    echo -e "${RED}❌ 'uv' installation failed. Exiting setup.${NC}"
                    sleep 3
                    continue
                fi
            else
                echo -e "${GREEN}⚡ 'uv' detected! Using ultra-fast dependency resolution.${NC}"
            fi

            echo -e "${BLUE}Initializing Unified Workspace Environment...${NC}"
            mkdir -p "$BASE_DIR/Logs"

            [ ! -f "$BASE_DIR/.venv/bin/python" ] && uv venv "$BASE_DIR/.venv" --python "$PYTHON_CMD" --clear > /dev/null 2>&1
            echo -e "   ▶ Installing dependencies via uv..."
            uv sync --python "$BASE_DIR/.venv/bin/python" >> "$SETUP_LOG" 2>&1

            if [ -f "$BASE_DIR/.venv/bin/camoufox" ]; then
                echo -e "   ▶ Pre-fetching Camoufox stealth browser binaries..."
                "$BASE_DIR/.venv/bin/camoufox" fetch >> "$SETUP_LOG" 2>&1 || true
            fi

            for f in "${FOLDERS[@]}"; do
                mkdir -p "$BASE_DIR/$f/Logs"
            done

            echo -e "${YELLOW}🔑 Auto-verifying Credentials from .env...${NC}"
            auto_create_credentials_explicit true

            echo -e "${GREEN}✅ Initialization Complete.${NC}"
            read -p "Init Done. Enter...";;
        2) start_standard; read -p "Enter..." ;;
        3) start_service_mode; read -p "Enter..." ;;
        4) stop_all; read -p "Enter..." ;;
        5) trap 'echo "Returning...";' INT; tail -f "$BASE_DIR"/Sheet*/Logs/*.logs "$BASE_DIR"/Sheet*/Logs/*.log 2>/dev/null; trap - INT ;;
        6) check_for_updates; read -p "Enter..." ;;
        7) clear_logs; read -p "Enter..." ;;
        8) configure_credentials_menu ;;
        9) echo "Exiting. Engines remain active in background."; exit 0 ;;
        10)
            echo -e "${RED}⚠️  WARNING: This will delete the virtual environment and stop all engines.${NC}"
            read -p "Are you sure? (y/n): " confirm
            if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
                stop_all
                echo -e "${BLUE}🧹 Removing virtual environment...${NC}"
                rm -rf "$BASE_DIR/.venv" "$BASE_DIR"/Sheet*/.venv
                echo -e "${BLUE}🧹 Uninstalling Cloudflare Tunnel...${NC}"
                if [[ "$OS" == "Linux" ]]; then
                    sudo cloudflared service uninstall 2>/dev/null
                else
                    if [ -f "/Library/LaunchDaemons/com.cloudflare.cloudflared.plist" ]; then
                        sudo cloudflared service uninstall 2>/dev/null
                    fi
                fi
                echo -e "${BLUE}🧹 Removing logs...${NC}"
                rm -rf "$BASE_DIR"/Sheet*/Logs
                echo -e "${GREEN}✅ Deep clean complete. Use Option 1 to reinstall.${NC}"
            fi
            read -p "Enter..." ;;
        11)
            if [ -f "$BASE_DIR/.venv/bin/python" ]; then
                "$BASE_DIR/.venv/bin/python" "$BASE_DIR/scripts/pull_fleet_logs.py"
            else
                python3 "$BASE_DIR/scripts/pull_fleet_logs.py"
            fi
            read -p "Press Enter to continue..." ;;
        *)
            echo -e "${RED}Invalid option.${NC}"; sleep 1 ;;
    esac
done
