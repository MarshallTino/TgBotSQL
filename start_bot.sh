#!/bin/bash
# filepath: /home/marshall/TgBot/start_bot.sh

# Color definitions for better output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Store PIDs for cleanup
BOT_PID=""
ENV="dev"
FORWARDING_STATUS_FILE="config/forwarding_status.txt" # Path to the flag file
CURRENT_FORWARDING_STATUS="unknown"

# Define all available commands for tab completion
commands="start stop restart status update-prices update-all update-token classify token-stats show-failures reset-failures diagnose-token recover-token analyze-failures fix-deadlocks auto-updates reload-celery rebuild-celery celery-debug logs quick-logs extract-groups help clear quit exit enable-forwarding disable-forwarding forwarding-status"

# =============================================
# CRITICAL FIX: Direct bash completion setup without relying on external files
# =============================================
# Define the completion function
_tgbot_completion() {
    local cur prev opts
    
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    
    # All commands available
    opts="start stop restart status update-prices update-all update-token classify token-stats show-failures reset-failures diagnose-token recover-token analyze-failures fix-deadlocks auto-updates reload-celery rebuild-celery celery-debug logs quick-logs extract-groups help clear quit exit"

    # Handle command-specific arguments
    case "${prev}" in
        update-token|diagnose-token)
            return 0
            ;;
        recover-token)
            COMPREPLY=( $(compgen -W "all ethereum eth bsc polygon solana arbitrum avalanche base" -- "${cur}") )
            return 0
            ;;
        auto-updates)
            COMPREPLY=( $(compgen -W "on off status" -- "${cur}") )
            return 0
            ;;
        reset-failures)
            COMPREPLY=( $(compgen -W "all" -- "${cur}") )
            return 0
            ;;
        logs|quick-logs)
            local containers=$(docker ps --format "{{.Names}}" 2>/dev/null)
            COMPREPLY=( $(compgen -W "${containers}" -- "${cur}") )
            return 0
            ;;
        show-failures)
            COMPREPLY=( $(compgen -W "1 3 5 10" -- "${cur}") )
            return 0
            ;;
        *)
            # This is the key part - properly match partial commands
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
    esac
}

# Make the completion function available and register it
complete -F _tgbot_completion -o bashdefault -o default ./start_bot.sh
complete -F _tgbot_completion -o bashdefault -o default "$(basename "$0" 2>/dev/null || echo "start_bot.sh")"

# Configure readline for better tab completion
if [[ $- == *i* ]]; then
    # Only run these in interactive mode
    bind 'set completion-ignore-case on' 2>/dev/null
    bind 'set show-all-if-ambiguous on' 2>/dev/null
    bind 'set mark-symlinked-directories on' 2>/dev/null
    bind 'set colored-stats on' 2>/dev/null
    bind 'set visible-stats on' 2>/dev/null
    bind 'set menu-complete-display-prefix on' 2>/dev/null
    bind '"\e[A": history-search-backward' 2>/dev/null
    bind '"\e[B": history-search-forward' 2>/dev/null
fi

# Enable better command line editing with readline
if [ -f /etc/inputrc ]; then
    export INPUTRC="/etc/inputrc"
fi

# Enable command history
export HISTSIZE=1000
export HISTFILESIZE=2000
export HISTCONTROL=ignoreboth:erasedups
HISTFILE="$PWD/.tgbot_history"

# Create history file if it doesn't exist
if [ ! -f "$HISTFILE" ]; then
    touch "$HISTFILE"
    chmod 600 "$HISTFILE"  # Secure permissions
fi

# Load history
history -c  # Clear the history in memory
history -r  # Read the history file

# CRITICAL FIX: Set the prompt properly using ANSI codes directly
# This avoids the issue with the raw escape sequences being displayed
PS1='\033[0;36mTgBot>\033[0m '

# Enable bash features that enhance CLI experience
shopt -s histappend     # Append to history file
shopt -s cmdhist        # Save multi-line commands as one entry
shopt -s lithist        # Preserve newlines in history
shopt -s direxpand      # Expand directory names

# Print header banner
print_banner() {
    clear
    echo -e "${BLUE}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${GREEN}             TgBot Price Tracker               ${BLUE}║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════╝${NC}"
    echo -e "${YELLOW}Environment:${NC} ${ENV}"
    echo -e "${YELLOW}System:${NC} $(uname -sr)"
    echo -e "${YELLOW}Date:${NC} $(date)"
    # Show Nova forwarding status in the banner
    get_forwarding_status quiet
    echo -e "${YELLOW}Nova Forwarding:${NC} ${CURRENT_FORWARDING_STATUS}"
    echo
}

# Function to stop only the Telegram bot
stop_bot() {
    if [ -n "$BOT_PID" ]; then
        echo -e "${YELLOW}Stopping Telegram bot (PID: $BOT_PID)...${NC}"
        kill -TERM $BOT_PID 2>/dev/null || true
        wait $BOT_PID 2>/dev/null || true
        BOT_PID=""
        echo -e "${GREEN}✅ Telegram bot stopped. Celery workers are still running.${NC}"
    else
        echo -e "${CYAN}ℹ️ Telegram bot is not running.${NC}"
    fi
}

# Function to start the bot
start_bot() {
    if [ -n "$BOT_PID" ]; then
        echo -e "${YELLOW}⚠️ Telegram bot is already running with PID: $BOT_PID${NC}"
        return
    fi
    
    echo -e "${BLUE}🚀 Starting Telegram bot...${NC}"
    python scripts/telegram_monitor.py & 
    BOT_PID=$!
    echo -e "${GREEN}✅ Telegram bot started with PID: $BOT_PID${NC}"
}

# Function to stop everything
stop_all() {
    echo -e "${YELLOW}Stopping all processes...${NC}"
    
    # Stop the Telegram bot if running
    if [ -n "$BOT_PID" ]; then
        echo -e "${YELLOW}Stopping Telegram bot (PID: $BOT_PID)...${NC}"
        kill -TERM $BOT_PID 2>/dev/null || true
        wait $BOT_PID 2>/dev/null || true
    fi
    
    # Stop Celery workers via docker-compose
    echo -e "${YELLOW}Stopping Celery workers...${NC}"
    docker compose stop celery_worker celery_beat
    
    echo -e "${GREEN}✅ All processes stopped.${NC}"
    exit 0
}

# Function to ensure environment is ready (WSL + Docker)
ensure_environment() {
    echo -e "${BLUE}🔄 Running environment checks...${NC}"
    
    # Run the Python script that checks/starts Docker and WSL
    python scripts/startup_checks.py
    CHECK_RESULT=$?
    
    if [ $CHECK_RESULT -ne 0 ]; then
        echo -e "${RED}❌ Environment checks failed. Please fix the issues above.${NC}"
        echo -e "${YELLOW}1. Make sure Docker Desktop is running on Windows${NC}"
        echo -e "${YELLOW}2. Make sure WSL integration is enabled in Docker Desktop settings${NC}"
        echo -e "${YELLOW}3. Try running this script again${NC}"
        echo -e "Exiting."
        exit 1
    fi
    
    echo -e "${GREEN}✅ Environment ready! Docker is running and available.${NC}"
    return 0
}


# Main setup function - run at start
main_setup() {
    # Initialize forwarding status before printing banner
    initialize_forwarding_status
    
    print_banner
    
    # Ensure WSL and Docker are running before anything else
    ensure_environment
    
    # Initialize services
    initialize_services
    
    # Activate virtual environment and set environment variables
    activate_venv && set_environment_vars

    # Initialize database
    echo -e "${BLUE}Setting up database...${NC}"
    python scripts/setup_database.py

    # Start the Telegram bot
    start_bot
}

# Function to ensure WSL and Docker are running
ensure_wsl_and_docker() {
    echo -e "${BLUE}🔄 Ensuring WSL and Docker are running...${NC}"

    # Check if WSL is running (Windows only)
    if [[ "$(uname -r)" == *"microsoft"* ]]; then
        echo -e "${CYAN}Checking WSL status...${NC}"
        if ! powershell.exe -Command "wsl.exe -l -q" | grep -q "Running"; then
            echo -e "${YELLOW}Starting WSL...${NC}"
            powershell.exe -Command "wsl.exe --start"
            sleep 5  # Give WSL time to start
        else
            echo -e "${GREEN}WSL is already running.${NC}"
        fi
    fi

    # Check if Docker is running
    if ! docker info &>/dev/null; then
        echo -e "${YELLOW}Starting Docker...${NC}"
        if [[ "$(uname -r)" == *"microsoft"* ]]; then
            powershell.exe -Command "Start-Process 'C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe'"
            sleep 10  # Give Docker time to start
        else
            sudo systemctl start docker
        fi
    else
        echo -e "${GREEN}Docker is already running.${NC}"
    fi
}

# Function to shut down environment components with options
shutdown_all() {
    # Save current terminal settings
    local saved_settings=$(stty -g)
    
    # Ensure terminal is in a proper state for input
    stty sane
    
    echo -e "${RED}🚨 Shutting down all services...${NC}"

    # 1. Stop the Telegram bot process
    stop_bot

    # 2. Stop and remove all docker-compose services
    echo -e "${YELLOW}Stopping Docker containers (postgres, mongo, redis, celery)...${NC}"
    docker compose down
    echo -e "${GREEN}✅ Docker containers stopped and removed.${NC}"

    # Ask if Docker should also be stopped
    docker_response=""
    while [ -z "$docker_response" ]; do
        echo -en "${YELLOW}Do you want to stop Docker Desktop as well? (y/n): ${NC}"
        read -r answer
        case $answer in
            [yY])
                docker_response="yes"
                echo -e "${YELLOW}Stopping Docker Desktop...${NC}"
                if [[ "$(uname -r)" == *"microsoft"* ]]; then
                    powershell.exe -Command "Stop-Process -Name 'Docker Desktop' -ErrorAction SilentlyContinue -Force"
                else
                    sudo systemctl stop docker
                fi
                echo -e "${GREEN}✅ Docker stopped.${NC}"
                ;;
            [nN])
                docker_response="no"
                echo -e "${CYAN}Keeping Docker running.${NC}"
                ;;
            *)
                echo -e "${RED}Please answer with y or n${NC}"
                ;;
        esac
    done
        
    # Only ask about WSL if we're in WSL and Docker was stopped
    if [[ "$(uname -r)" == *"microsoft"* && "$docker_response" == "yes" ]]; then
        wsl_response=""
        while [ -z "$wsl_response" ]; do
            echo -en "${YELLOW}Do you want to shut down WSL as well? (WARNING: This will close your terminal) (y/n): ${NC}"
            read -r stop_wsl
            case $stop_wsl in
                [yY])
                    wsl_response="yes"
                    echo -e "${RED}⚠️ Shutting down WSL...${NC}"
                    echo -e "${RED}⚠️ Your terminal will close after this operation.${NC}"
                    echo -e "${RED}⚠️ Press Ctrl+C within 5 seconds to cancel.${NC}"
                    sleep 5
                    powershell.exe -Command "wsl.exe --shutdown"
                    ;;
                [nN])
                    wsl_response="no"
                    echo -e "${CYAN}Keeping WSL running.${NC}"
                    ;;
                *)
                    echo -e "${RED}Please answer with y or n${NC}"
                    ;;
            esac
        done
    fi

    # Restore terminal settings before exit
    stty "$saved_settings" 2>/dev/null || true
    
    echo -e "${RED}✅ System shutdown complete.${NC}"
    exit 0
}

# Function to just stop running services but keep Docker/WSL running
stop_services() {
    echo -e "${YELLOW}Stopping services but keeping Docker and WSL running...${NC}"

    # Stop the Telegram bot
    stop_bot

    # Stop Docker containers but leave them intact for quick restart
    echo -e "${YELLOW}Stopping Docker containers...${NC}"
    docker compose stop
    echo -e "${GREEN}✅ Services stopped. Docker and WSL are still running.${NC}"
    
    echo -e "${CYAN}You can restart the bot with the 'start' command.${NC}"
}

# Function to shut down everything gracefully
shutdown_all_docker_and_wsl() {
    echo -e "${RED}🚨 Full Shutdown: Bot + Docker + WSL${NC}"

    # Stop the Telegram bot
    stop_bot

    # Stop and remove all docker-compose services
    echo -e "${YELLOW}Stopping Docker containers...${NC}"
    docker compose down
    echo -e "${GREEN}✅ Docker containers stopped.${NC}"

    # Stop Docker Desktop or Docker daemon based on the environment
    if [[ "$(uname -r)" == *"microsoft"* ]]; then
        echo -e "${YELLOW}Stopping Docker Desktop...${NC}"
        powershell.exe -Command "Stop-Process -Name 'Docker Desktop' -Force"
        echo -e "${GREEN}✅ Docker Desktop stopped.${NC}"

        # Warning and sleep for WSL shutdown
        echo -e "${RED}⚠️ Shutting down WSL in 5 seconds...${NC}"
        sleep 5
        powershell.exe -Command "wsl.exe --shutdown"
    else
        echo -e "${YELLOW}Stopping Docker daemon...${NC}"
        sudo systemctl stop docker
        echo -e "${GREEN}✅ Docker stopped.${NC}"
    fi

    echo -e "${GREEN}✅ Full shutdown complete.${NC}"
    exit 0
}

# =============================================
# ENHANCED INTERRUPT HANDLER: Offers options to the user
# =============================================
handle_interrupt() {
    local old_tty_settings=$(stty -g)
    stty sane

    echo
    echo -e "${YELLOW}🛑 Interrupted - What would you like to do?${NC}"
    echo -e "${CYAN}    1) Stop just the Telegram bot${NC}"
    echo -e "${CYAN}    2) Stop everything (bot + Celery containers only)${NC}"
    echo -e "${CYAN}    3) Continue running${NC}"
    echo -e "${CYAN}    4) Full shutdown (bot + Docker + WSL)${NC}"

    read -p "Enter choice [1-4]: " choice
    stty "$old_tty_settings"

    case $choice in
        1)
            stop_bot
            ;;
        2)
            stop_services
            ;;
        3|"")
            echo -e "${GREEN}Continuing...${NC}"
            ;;
        4)
            shutdown_all_docker_and_wsl
            ;;
        *)
            echo -e "${YELLOW}Invalid choice. Continuing...${NC}"
            ;;
    esac
}

# Register the interrupt handler
trap handle_interrupt INT

function extract-groups {
    echo "Extracting Telegram groups..."
    # Activate the virtual environment if needed
    source env/bin/activate || source tg_env/bin/activate || true
    python scripts/utils/helpers/extract_groups.py
}

# NEW FUNCTIONS FOR TOKEN MANAGEMENT

# Function to trigger a manual token price update
update_token_prices() {
    echo -e "${BLUE}🔄 Manually triggering token price update...${NC}"
    
    # Check if Celery worker is running
    if ! docker ps | grep -q celery_worker; then
        echo -e "${RED}❌ Celery worker is not running. Cannot update token prices.${NC}"
        return 1
    fi
    
    # Run the task via Celery
    echo -e "${CYAN}Sending update task to Celery...${NC}"
    
    # Two options: update all tokens or update by frequency
    if [[ "$1" == "all" ]]; then
        docker exec $(docker ps -qf "name=celery_worker") celery -A scripts.price_tracker.celery_app call scripts.price_tracker.tasks.update_all_token_prices
        echo -e "${GREEN}✅ Token price update triggered for ALL tokens${NC}"
    else
        docker exec $(docker ps -qf "name=celery_worker") celery -A scripts.price_tracker.celery_app call scripts.price_tracker.tasks.update_token_prices_by_frequency
        echo -e "${GREEN}✅ Token price update triggered for due tokens${NC}"
    fi
}



# Function to toggle auto-updates
toggle_auto_updates() {
    current_state=$(docker exec $(docker ps -qf "name=celery_beat") ps aux | grep -q "[c]elery beat" && echo "on" || echo "off")
    
    if [[ "$1" == "on" && "$current_state" == "off" ]]; then
        echo -e "${BLUE}🔄 Enabling automatic price updates...${NC}"
        docker compose start celery_beat
        echo -e "${GREEN}✅ Automatic updates enabled${NC}"
    elif [[ "$1" == "off" && "$current_state" == "on" ]]; then
        echo -e "${YELLOW}⏸️ Disabling automatic price updates...${NC}"
        docker compose stop celery_beat
        echo -e "${GREEN}✅ Automatic updates disabled${NC}"
    elif [[ "$1" == "status" || "$1" == "" ]]; then
        if [[ "$current_state" == "on" ]]; then
            echo -e "${GREEN}✅ Automatic updates are currently ENABLED${NC}"
        else
            echo -e "${YELLOW}⚠️ Automatic updates are currently DISABLED${NC}"
        fi
    else
        echo -e "${YELLOW}⚠️ Auto-updates are already ${current_state}${NC}"
    fi
}

# Function to show token statistics
token_stats() {
    echo -e "${BLUE}📊 Retrieving token statistics...${NC}"
    
    # Check if PostgreSQL is accessible
    if ! docker ps | grep -q postgres; then
        echo -e "${RED}❌ PostgreSQL is not running!${NC}"
        return 1
    fi
    
    # Execute query and display results
    echo -e "${CYAN}Querying database...${NC}"
    
    # Run PostgreSQL query
    docker exec -it $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -c "
    SELECT 
        COUNT(*) AS total_tokens,
        COUNT(CASE WHEN is_active = TRUE THEN 1 END) AS active_tokens,
        COUNT(CASE WHEN is_active = FALSE THEN 1 END) AS inactive_tokens,
        COUNT(CASE WHEN update_interval <= 30 THEN 1 END) AS high_priority,
        COUNT(CASE WHEN update_interval > 30 AND update_interval <= 300 THEN 1 END) AS medium_priority,
        COUNT(CASE WHEN update_interval > 300 AND update_interval <= 900 THEN 1 END) AS low_priority,
        COUNT(CASE WHEN update_interval > 900 THEN 1 END) AS lowest_priority
    FROM tokens;
    
    SELECT 
        blockchain,
        COUNT(*) AS token_count,
        COUNT(CASE WHEN is_active = TRUE THEN 1 END) AS active
    FROM tokens
    GROUP BY blockchain
    ORDER BY token_count DESC;
    
    SELECT 
        date_trunc('day', last_updated_at) AS update_day,
        COUNT(*) AS tokens_updated
    FROM tokens
    WHERE last_updated_at IS NOT NULL
    GROUP BY date_trunc('day', last_updated_at)
    ORDER BY update_day DESC
    LIMIT 7;
    
    -- Recent token update activity
    SELECT 
        token_id, name, ticker, blockchain, 
        update_interval, 
        last_updated_at,
        failed_updates_count,
        is_active
    FROM tokens
    ORDER BY last_updated_at DESC NULLS LAST
    LIMIT 10;
    "
}

# Function to update a specific token
update_specific_token() {
    if [[ -z "$1" ]]; then
        echo -e "${YELLOW}⚠️ Please specify a token ID or contract address${NC}"
        echo -e "Usage: update-token [token_id|contract_address]"
        return 1
    fi
    
    token_id="$1"
    
    echo -e "${BLUE}🔍 Looking up token: ${token_id}...${NC}"
    
    # Check if PostgreSQL is accessible
    if ! docker ps | grep -q postgres; then
        echo -e "${RED}❌ PostgreSQL is not running!${NC}"
        return 1
    fi
    
    # Try to find the token by ID or contract address
    result=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
    SELECT token_id, name, ticker, blockchain, is_active 
    FROM tokens 
    WHERE token_id = '$token_id' OR contract_address ILIKE '%$token_id%'
    LIMIT 1;")
    
    if [[ -z "$result" ]]; then
        echo -e "${RED}❌ Token not found with ID or address: ${token_id}${NC}"
        return 1
    fi
    
    # Parse token information
    read -r db_token_id name ticker blockchain is_active <<< "$result"
    
    echo -e "${GREEN}Found token: ${name} (${ticker}) on ${blockchain} [ID: ${db_token_id}]${NC}"
    
    # Check if Celery worker is running
    if ! docker ps | grep -q celery_worker; then
        echo -e "${RED}❌ Celery worker is not running. Cannot update token.${NC}"
        return 1
    fi
    
    # Run the task via Celery
    echo -e "${CYAN}Triggering price update for token...${NC}"
    docker exec $(docker ps -qf "name=celery_worker") celery -A scripts.price_tracker.celery_app call scripts.price_tracker.tasks.process_token_batch --args="[[${db_token_id}]]"
    
    echo -e "${GREEN}✅ Token update triggered${NC}"
}

# Function to classify tokens
classify_tokens() {
    echo -e "${BLUE}🏷️ Running token classification...${NC}"
    
    # Check if Celery worker is running
    if ! docker ps | grep -q celery_worker; then
        echo -e "${RED}❌ Celery worker is not running. Cannot classify tokens.${NC}"
        return 1
    fi
    
    # Run the task via Celery
    echo -e "${CYAN}Classifying tokens based on activity and relevance...${NC}"
    docker exec $(docker ps -qf "name=celery_worker") celery -A scripts.price_tracker.celery_app call scripts.price_tracker.tasks.classify_all_tokens
    
    echo -e "${GREEN}✅ Token classification initiated${NC}"
}

# Function to show token update failures
show_failures() {
    echo -e "${BLUE}⚠️ Checking token update failures...${NC}"
    
    # Check if PostgreSQL is accessible
    if ! docker ps | grep -q postgres; then
        echo -e "${RED}❌ PostgreSQL is not running!${NC}"
        return 1
    fi
    
    # Accept a threshold argument with default value 1
    threshold=${1:-1}
    
    # Run PostgreSQL query with more details
    docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -c "
    SELECT 
        token_id, 
        name, 
        ticker, 
        blockchain, 
        contract_address,
        best_pair_address,
        failed_updates_count,
        last_updated_at,
        update_interval,
        is_active
    FROM tokens
    WHERE failed_updates_count >= $threshold
    ORDER BY failed_updates_count DESC, last_updated_at ASC
    LIMIT 30;
    "
}

# Function to reset token failure counts
reset_failures() {
    if [[ -z "$1" ]]; then
        echo -e "${YELLOW}⚠️ Please specify a token ID or 'all'${NC}"
        echo -e "Usage: reset-failures [token_id|all] [force]"
        return 1
    fi
    
    token_id="$1"
    force="$2"
    
    echo -e "${BLUE}🔄 Resetting failure count for token(s)...${NC}"
    
    # Check if PostgreSQL is accessible
    if ! docker ps | grep -q postgres; then
        echo -e "${RED}❌ PostgreSQL is not running!${NC}"
        return 1
    fi
    
    if [[ "$token_id" == "all" ]]; then
        if [[ "$force" != "force" ]]; then
            read -p "Are you sure you want to reset ALL token failures? (y/n): " confirm
            if [[ $confirm != "y" && $confirm != "Y" ]]; then
                echo -e "${YELLOW}Operation cancelled${NC}"
                return 0
            fi
        fi
        
        # Reset all tokens
        docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -c "
        UPDATE tokens SET failed_updates_count = 0 WHERE failed_updates_count > 0;
        "
        echo -e "${GREEN}✅ Reset failure count for all tokens${NC}"
    else
        # Try to find the token by ID or contract address
        result=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
        SELECT token_id, name, ticker 
        FROM tokens 
        WHERE token_id = '$token_id' OR contract_address ILIKE '%$token_id%'
        LIMIT 1;")
        
        if [[ -z "$result" ]]; then
            echo -e "${RED}❌ Token not found with ID or address: ${token_id}${NC}"
            return 1
        fi
        
        # Parse token information
        read -r db_token_id name ticker <<< "$result"
        
        # Reset specific token
        docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -c "
        UPDATE tokens SET failed_updates_count = 0 WHERE token_id = $db_token_id;
        "
        echo -e "${GREEN}✅ Reset failure count for token: ${name} (${ticker}) [ID: ${db_token_id}]${NC}"
    fi
}

# Function to diagnose token issues
diagnose_token() {
    if [[ -z "$1" ]]; then
        echo -e "${YELLOW}⚠️ Please specify a token ID or contract address${NC}"
        echo -e "Usage: diagnose-token [token_id|contract_address]"
        return 1
    fi
    
    token_id="$1"
    
    echo -e "${BLUE}🔍 Diagnosing token: ${token_id}...${NC}"
    
    # Check if PostgreSQL is accessible
    if ! docker ps | grep -q postgres; then
        echo -e "${RED}❌ PostgreSQL is not running!${NC}"
        return 1
    fi
    
    # Try to find the token by ID or contract address
    result=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
    SELECT token_id, name, ticker, blockchain
    FROM tokens 
    WHERE token_id = '$token_id' OR contract_address ILIKE '%$token_id%'
    LIMIT 1;")
    
    if [[ -z "$result" ]]; then
        echo -e "${RED}❌ Token not found with ID or address: ${token_id}${NC}"
        return 1
    fi
    
    # Parse token information
    read -r db_token_id name ticker blockchain <<< "$result"
    
    echo -e "${GREEN}Found token: ${name} (${ticker}) on ${blockchain} [ID: ${db_token_id}]${NC}"
    
    # Check if token is active
    is_active=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
    SELECT is_active FROM tokens WHERE token_id = $db_token_id;")
    
    if [[ $is_active =~ t ]]; then
        echo -e "${GREEN}✅ Token is ACTIVE${NC}"
    else
        echo -e "${YELLOW}⚠️ Token is INACTIVE${NC}"
    fi
    
    # Get failure info
    failures=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
    SELECT failed_updates_count FROM tokens WHERE token_id = $db_token_id;")
    failures=$(echo $failures | xargs)
    
    if [[ $failures -gt 0 ]]; then
        echo -e "${YELLOW}⚠️ Token has $failures failed update attempts${NC}"
    else
        echo -e "${GREEN}✅ Token has no failed updates${NC}"
    fi
    
    # Get last update time
    last_updated=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
    SELECT last_updated_at FROM tokens WHERE token_id = $db_token_id;")
    last_updated=$(echo $last_updated | xargs)
    
    if [[ -n "$last_updated" && "$last_updated" != "null" ]]; then
        echo -e "${BLUE}🕒 Last updated: $last_updated${NC}"
        
        # Calculate time since last update
        now=$(date +%s)
        last_updated_ts=$(date -d "$last_updated" +%s 2>/dev/null || echo $now)
        diff_secs=$((now - last_updated_ts))
        diff_hours=$((diff_secs / 3600))
        
        if [[ $diff_hours -gt 24 ]]; then
            echo -e "${YELLOW}⚠️ Last update was $diff_hours hours ago (>24 hours)${NC}"
        else
            echo -e "${GREEN}✅ Last update was $diff_hours hours ago${NC}"
        fi
    else
        echo -e "${YELLOW}⚠️ Token has never been updated${NC}"
    fi
    
    # Get more detailed diagnostics using our Python script
    echo -e "${CYAN}Running deep diagnostics...${NC}"
    
    # Check if virtual env is activated, if not try to activate
    if [[ -z "$VIRTUAL_ENV" ]]; then
        source tg_env/bin/activate || source env/bin/activate || true
    fi
    
    python -m scripts.price_tracker.token_recovery diagnose $db_token_id
}

# Function to recover failing tokens
recover_token() {
    if [[ -z "$1" ]]; then
        echo -e "${YELLOW}⚠️ Please specify a token ID, blockchain or 'all'${NC}"
        echo -e "Usage: recover-token [token_id|blockchain|all] [min_failures]"
        echo -e "Examples:"
        echo -e "  recover-token 123           # Recover specific token"
        echo -e "  recover-token solana 10     # Recover Solana tokens with 10+ failures"
        echo -e "  recover-token all 15        # Recover all tokens with 15+ failures"
        return 1
    fi
    
    token_id="$1"
    min_failures="${2:-10}"  # Default to 10 if not specified
    
    if [[ "$token_id" == "all" ]]; then
        echo -e "${BLUE}🔄 Attempting to recover all tokens with $min_failures+ failures...${NC}"
        
        # Check if virtual env is activated, if not try to activate
        if [[ -z "$VIRTUAL_ENV" ]]; then
            source tg_env/bin/activate || source env/bin/activate || true
        fi
        
        python -m scripts.price_tracker.token_recovery bulk-recover $min_failures
        return
    fi
    
    # Check if it's a blockchain name
    if [[ "$token_id" =~ ^(ethereum|eth|bsc|polygon|solana|arbitrum|avalanche|base)$ ]]; then
        blockchain=$token_id
        
        # Normalize blockchain names
        if [[ "$blockchain" == "eth" ]]; then
            blockchain="ethereum"
        fi
        
        echo -e "${BLUE}🔄 Attempting to recover $blockchain tokens with $min_failures+ failures...${NC}"
        
        # Check if virtual env is activated, if not try to activate
        if [[ -z "$VIRTUAL_ENV" ]]; then
            source tg_env/bin/activate || source env/bin/activate || true
        fi
        
        python -m scripts.price_tracker.token_recovery bulk-recover $min_failures $blockchain
        return
    fi
    
    # Otherwise assume it's a token ID
    echo -e "${BLUE}🔄 Attempting to recover token: ${token_id}...${NC}"
    
    # Check if PostgreSQL is accessible
    if ! docker ps | grep -q postgres; then
        echo -e "${RED}❌ PostgreSQL is not running!${NC}"
        return 1
    fi
    
    # Try to find the token by ID or contract address
    result=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
    SELECT token_id, name, ticker, blockchain 
    FROM tokens 
    WHERE token_id = '$token_id' OR contract_address ILIKE '%$token_id%'
    LIMIT 1;")
    
    if [[ -z "$result" ]]; then
        echo -e "${RED}❌ Token not found with ID or address: ${token_id}${NC}"
        return 1
    fi
    
    # Parse token information
    read -r db_token_id name ticker blockchain <<< "$result"
    
    echo -e "${GREEN}Found token: ${name} (${ticker}) on ${blockchain} [ID: ${db_token_id}]${NC}"
    
    # Check if virtual env is activated, if not try to activate
    if [[ -z "$VIRTUAL_ENV" ]]; then
        source tg_env/bin/activate || source env/bin/activate || true
    fi
    
    # Try to recover the token
    python -m scripts.price_tracker.token_recovery recover $db_token_id
}

# Function to analyze token failures by blockchain
analyze_failures() {
    echo -e "${BLUE}🔍 Analyzing token failures by blockchain...${NC}"
    
    # Check if virtual env is activated, if not try to activate
    if [[ -z "$VIRTUAL_ENV" ]]; then
        source tg_env/bin/activate || source env/bin/activate || true
    fi
    
    python -m scripts.price_tracker.token_recovery analyze
}

# Function to fix deadlocks on tokens
fix_deadlocks() {
    echo -e "${BLUE}🔧 Checking for and fixing database deadlocks...${NC}"
    
    # Check if PostgreSQL is accessible
    if ! docker ps | grep -q postgres; then
        echo -e "${RED}❌ PostgreSQL is not running!${NC}"
        return 1
    fi
    
    # Check for active deadlocks
    echo -e "${CYAN}Checking for active deadlocks...${NC}"
    deadlocks=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
    SELECT count(*) FROM pg_locks l 
    JOIN pg_stat_activity a ON l.pid = a.pid 
    WHERE l.granted = false AND a.wait_event_type = 'Lock'
    ")
    deadlocks=$(echo $deadlocks | xargs)
    
    if [[ $deadlocks -gt 0 ]]; then
        echo -e "${YELLOW}⚠️ Found $deadlocks deadlocked processes${NC}"
        
        # Show detailed information about locks
        echo -e "${CYAN}Deadlock details:${NC}"
        docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -c "
        SELECT a.pid, 
               a.usename, 
               a.query_start,
               now() - a.query_start AS duration,
               a.query
        FROM pg_locks l 
        JOIN pg_stat_activity a ON l.pid = a.pid 
        WHERE l.granted = false AND a.wait_event_type = 'Lock'
        "
        
        # Ask before canceling queries
        read -p "Do you want to cancel deadlocked queries? (y/n): " cancel_deadlocks
        
        if [[ $cancel_deadlocks == "y" || $cancel_deadlocks == "Y" ]]; then
            echo -e "${YELLOW}Canceling deadlocked queries...${NC}"
            
            # Get list of deadlocked PIDs
            pids=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
            SELECT a.pid FROM pg_locks l 
            JOIN pg_stat_activity a ON l.pid = a.pid 
            WHERE l.granted = false AND a.wait_event_type = 'Lock'
            ")
            
            # Cancel each query
            for pid in $pids; do
                echo -e "Canceling query with PID $pid"
                docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -c "
                SELECT pg_cancel_backend($pid)
                "
            done
            
            echo -e "${GREEN}✅ Deadlocked queries canceled${NC}"
        fi
    else
        echo -e "${GREEN}✅ No deadlocks detected${NC}"
    fi
    
    # Also check for long-running transactions
    echo -e "${CYAN}Checking for long-running transactions...${NC}"
    long_txns=$(docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -t -c "
    SELECT count(*) FROM pg_stat_activity 
    WHERE state = 'active' 
    AND now() - query_start > interval '30 seconds'
    AND query NOT LIKE '%pg_stat_activity%'
    ")
    long_txns=$(echo $long_txns | xargs)
    
    if [[ $long_txns -gt 0 ]]; then
        echo -e "${YELLOW}⚠️ Found $long_txns long-running transactions${NC}"
        
        # Show details
        echo -e "${CYAN}Long transaction details:${NC}"
        docker exec $(docker ps -qf "name=postgres") psql -U bot -d crypto_db -c "
        SELECT pid, 
               usename, 
               query_start,
               now() - query_start AS duration,
               query
        FROM pg_stat_activity 
        WHERE state = 'active' 
        AND now() - query_start > interval '30 seconds'
        AND query NOT LIKE '%pg_stat_activity%'
        ORDER BY now() - query_start DESC
        "
    else
        echo -e "${GREEN}✅ No long-running transactions detected${NC}"
    fi
}

# NEW FUNCTION: Fast reload for Celery code without rebuilding
fast_reload_celery() {
    echo -e "${BLUE}🔄 Fast-reloading Celery code (no rebuild)...${NC}"
    
    # Check if containers are running
    if ! docker ps | grep -q celery_worker; then
        echo -e "${RED}❌ Celery worker is not running. Cannot do fast reload.${NC}"
        echo "Try using 'rebuild-celery' instead."
        return 1
    fi
    
    # Copy the updated Python files into the container
    echo -e "${CYAN}Copying updated price_tracker code to container...${NC}"
    docker cp ./scripts/price_tracker/. $(docker ps -qf "name=celery_worker"):/app/scripts/price_tracker/
    
    # Reload Celery workers with HUP signal
    echo -e "${CYAN}Sending HUP signal to reload workers...${NC}"
    docker exec $(docker ps -qf "name=celery_worker") pkill -HUP -f 'celery'
    
    echo -e "${GREEN}✅ Celery code updated and workers reloaded!${NC}"
    echo -e "${YELLOW}Note: Only Python files were updated. For dependency changes, use 'rebuild-celery'.${NC}"
    return 0
}

# Enhance rebuild_celery for better feedback
rebuild_celery() {
    echo -e "${YELLOW}🔄 Rebuilding and restarting Celery services...${NC}"
    
    echo -e "${CYAN}Stopping services...${NC}"
    docker compose stop celery_worker celery_beat
    
    echo -e "${CYAN}Removing containers...${NC}"
    docker compose rm -f celery_worker celery_beat
    
    echo -e "${CYAN}Rebuilding images (this may take a minute)...${NC}"
    docker compose build celery_worker celery_beat
    
    echo -e "${CYAN}Starting fresh containers...${NC}"
    docker compose up -d celery_worker celery_beat
    
    echo -e "${GREEN}✅ Celery services rebuilt and restarted${NC}"
}

# Enhanced Celery diagnostics
debug_celery() {
    echo -e "${BLUE}🔍 Running Celery diagnostics...${NC}"
    
    # Check Docker status
    echo -e "${CYAN}Checking Docker status:${NC}"
    if ! docker info &>/dev/null; then
        echo -e "${RED}❌ Docker is not running!${NC}"
        return 1
    else
        echo -e "${GREEN}✓ Docker is running${NC}"
    fi
    
    # Check if Redis is accessible
    echo -e "${CYAN}Testing Redis connection:${NC}"
    if ! docker ps -q --filter "name=redis" | grep -q .; then
        echo -e "${RED}❌ Redis container is not running!${NC}"
    else
        redis_ping=$(docker exec -it $(docker ps -qf "name=redis") redis-cli ping 2>/dev/null)
        if [ "$redis_ping" == "PONG" ]; then
            echo -e "${GREEN}✓ Redis connection OK${NC}"
        else
            echo -e "${RED}❌ Redis not responding correctly${NC}"
        fi
    fi
    
    # Check Celery worker status
    echo -e "${CYAN}Checking Celery workers:${NC}"
    if ! docker ps -q --filter "name=celery_worker" | grep -q .; then
        echo -e "${RED}❌ Celery worker container is not running!${NC}"
    else
        echo -e "${GREEN}✓ Celery worker container is running${NC}"
        
        # Check for running Celery processes inside container
        celery_processes=$(docker exec $(docker ps -qf "name=celery_worker") ps aux | grep -c "[c]elery" || echo "0")
        echo -e "${CYAN}Found ${celery_processes} Celery processes${NC}"
        
        # Check Celery logs for errors
        recent_errors=$(docker logs --tail 50 $(docker ps -qf "name=celery_worker") 2>&1 | grep -i "error" | wc -l)
        if [ "$recent_errors" -gt "0" ]; then
            echo -e "${YELLOW}⚠️ Found $recent_errors recent errors in logs${NC}"
        fi
    fi
    
    # Check task queues
    echo -e "${CYAN}Checking task queues in Redis:${NC}"
    docker exec $(docker ps -qf "name=redis") redis-cli -n 0 info | grep connected
    
    # Watch logs if requested
    read -p "View Celery worker logs? (y/n): " view_logs
    if [[ $view_logs == "y" || $view_logs == "Y" ]]; then
        echo -e "${YELLOW}Showing Celery worker logs (Ctrl+C to stop viewing):${NC}"
        docker compose logs -f celery_worker
    fi
}

# Improved function for viewing container logs with dynamic container ID lookup
view_logs() {
    local service=${1:-"celery_worker"}
    local lines=${2:-50}
    local follow=${3:-"no"}
    
    # Find the container ID for the service
    local container_id=$(docker ps -qf "name=$service" | head -n1)
    
    if [[ -z "$container_id" ]]; then
        echo -e "${RED}❌ No container found for service: $service${NC}"
        echo -e "Available containers:"
        docker ps --format "   ${YELLOW}• ${GREEN}{{.Names}}${NC} ({{.ID}})"
        return 1
    fi
    
    # Get container name for better display
    local container_name=$(docker ps --format "{{.Names}}" -f "id=$container_id")
    
    echo -e "${BLUE}📋 Viewing logs for ${GREEN}$container_name${BLUE} (${YELLOW}$container_id${BLUE}):${NC}"
    
    # Check if the container exists and is running
    if ! docker ps -q --filter "id=$container_id" | grep -q .; then
        echo -e "${RED}❌ Container $container_id is not running${NC}"
        return 1
    fi
    
    # Display the logs with or without following
    if [[ "$follow" == "yes" || "$follow" == "y" ]]; then
        echo -e "${YELLOW}Showing last ${lines} lines and following new logs (Ctrl+C to stop viewing)${NC}"
        docker logs --tail $lines -f $container_id
    else
        echo -e "${YELLOW}Showing last ${lines} lines${NC}"
        docker logs --tail $lines $container_id
    fi
}

# Initialize services if needed
initialize_services() {
    echo -e "${BLUE}Checking required services...${NC}"
    missing_services=()
    
    # Check PostgreSQL
    if ! docker ps | grep -q postgres; then
        missing_services+=("postgres")
    fi
    
    # Check MongoDB
    if ! docker ps | grep -q mongo; then
        missing_services+=("mongo")
    fi
    
    # Check Redis
    if ! docker ps | grep -q redis; then
        missing_services+=("redis")
    fi
    
    # Start missing services if any
    if [ ${#missing_services[@]} -gt 0 ]; then
        echo -e "${YELLOW}Starting missing services: ${missing_services[*]}${NC}"
        docker compose up -d ${missing_services[@]} pgadmin
        
        # Give services time to initialize
        echo -e "${CYAN}Waiting for services to initialize...${NC}"
        sleep 5
    else
        echo -e "${GREEN}✓ All required services are running${NC}"
    fi
    
    # Check Celery workers
    if ! docker ps | grep -q celery_worker; then
        echo -e "${YELLOW}Starting Celery services...${NC}"
        docker compose up -d celery_worker celery_beat
    fi
}

# Activate virtual environment
activate_venv() {
    echo -e "${BLUE}Activating the virtual environment tg_env...${NC}"
    if [ ! -d "tg_env" ]; then
        echo -e "${YELLOW}⚠️ Virtual environment not found. Creating one...${NC}"
        python -m venv tg_env
    fi
    source tg_env/bin/activate
    
    # Check if we're in the venv
    if [[ "$VIRTUAL_ENV" != "" ]]; then
        echo -e "${GREEN}✓ Virtual environment activated${NC}"
    else
        echo -e "${RED}❌ Failed to activate virtual environment${NC}"
        return 1
    fi
}

# Set environment variables
set_environment_vars() {
    # Set environment variables for local development
    # IMPORTANT: These must match docker-compose.yml settings
    export PG_HOST=localhost
    export PG_PORT=5432
    export PG_DATABASE=crypto_db
    export PG_USER=bot
    export PG_PASSWORD=bot1234

    export MONGO_HOST=localhost
    export MONGO_PORT=27017
    export MONGO_USER=bot
    export MONGO_PASSWORD=bot1234
    export MONGO_AUTH_SOURCE=admin
    export MONGO_DB=tgbot_db

    export REDIS_HOST=localhost
    export REDIS_PORT=6379
}

# --- Add Startup Checks ---
echo "Running startup checks..."
# Activate virtual environment if needed for the check script
source tg_env/bin/activate
source ./nova_forwarding_functions.sh
# Run the startup check script
python scripts/startup_checks.py
CHECK_RESULT=$? # Capture the exit code

if [ $CHECK_RESULT -ne 0 ]; then
    echo "❌ Startup checks failed. Exiting."
    # Deactivate if you activated it
    deactivate
    exit 1
fi

echo "✅ Startup checks passed."
# Ensure env is active for the rest of the script
# If the rest of your script assumes the env is active, keep it active.
# If not, you might add `deactivate` here and reactivate later if needed.
# --- End of Startup Checks ---

# --- Original Bot Startup Commands ---

# Main setup
main_setup

# Interactive help
show_help() {
    echo -e "${CYAN}📋 Available commands:${NC}"
    echo -e "   ${GREEN}start${NC}         - Start the Telegram bot"
    echo -e "   ${GREEN}stop${NC}          - Stop just the Telegram bot"
    echo -e "   ${GREEN}restart${NC}       - Restart the Telegram bot"
    echo -e "   ${GREEN}status${NC}        - Show process status"
    echo -e ""
    echo -e "   ${CYAN}NOVA BOT FORWARDING:${NC}"
    echo -e "   ${GREEN}enable-forwarding${NC}  - Enable forwarding CAs from configured groups to Nova Bot"
    echo -e "   ${GREEN}disable-forwarding${NC} - Disable forwarding CAs to Nova Bot"
    echo -e "   ${GREEN}forwarding-status${NC}  - Check if CA forwarding to Nova Bot is currently enabled"
    echo -e ""
    echo -e "   ${CYAN}TOKEN MANAGEMENT:${NC}"
    echo -e "   ${GREEN}update-prices${NC}     - Manually trigger token price updates"
    echo -e "   ${GREEN}update-all${NC}        - Update ALL token prices (regardless of schedule)"
    echo -e "   ${GREEN}update-token${NC} ID   - Update a specific token by ID or contract address"
    echo -e "   ${GREEN}classify${NC}          - Run token classification"
    echo -e "   ${GREEN}token-stats${NC}       - Show token statistics"
    echo -e "   ${GREEN}show-failures${NC} [threshold]    - List tokens with update failures (default threshold: 1)"
    echo -e "   ${GREEN}reset-failures${NC} ID|all - Reset failure count for tokens"
    echo -e "   ${GREEN}diagnose-token${NC} ID - Diagnose issues with a specific token"
    echo -e "   ${GREEN}recover-token${NC} ID|blockchain|all [min_failures] - Attempt to recover failing tokens"
    echo -e "   ${GREEN}analyze-failures${NC}  - Analyze token failures by blockchain"
    echo -e "   ${GREEN}fix-deadlocks${NC}     - Detect and fix database deadlocks"
    echo -e "   ${GREEN}auto-updates${NC} on/off - Enable/disable automatic price updates"
    echo -e ""
    echo -e "   ${CYAN}CELERY MANAGEMENT:${NC}"
    echo -e "   ${GREEN}reload-celery${NC}     - Quick reload Celery code without rebuilding"
    echo -e "   ${GREEN}rebuild-celery${NC}    - Rebuild and restart Celery services"
    echo -e "   ${GREEN}celery-debug${NC}      - Run Celery diagnostics"
    echo -e "   ${GREEN}logs${NC} [container]  - View logs with following (default: celery_worker)"
    echo -e "   ${GREEN}quick-logs${NC} [container] [lines] - View specific lines of logs"
    echo -e ""
    echo -e "   ${CYAN}OTHER COMMANDS:${NC}"
    echo -e "   ${GREEN}extract-groups${NC}    - Extract Telegram groups"
    echo -e "   ${GREEN}help${NC}              - Show this help message"
    echo -e "   ${GREEN}clear${NC}             - Clear the screen"
    echo -e "   ${RED}shutdown${NC}           - Stop bot, Docker containers, and optionally WSL"
    echo -e "   ${RED}quit${NC} / ${RED}exit${NC}       - Alias for shutdown"
    echo
}

# Show initial help
show_help

# Main command processing loop with improved handling
while true; do
    # Check if bot is still running
    if [ -n "$BOT_PID" ] && ! ps -p $BOT_PID > /dev/null; then
        echo -e "${YELLOW}⚠️ Telegram bot process ($BOT_PID) has stopped unexpectedly${NC}"
        BOT_PID=""
    fi
    
    # Save history after each command
    history -a
    
    # Use read with proper prompt and command editing capabilities
    read -e -p "$PS1" cmd_full
    
    # Add command to history if not empty
    if [ -n "$cmd_full" ]; then
        history -s "$cmd_full"
    fi
    
    # Extract command and arguments
    cmd=$(echo "$cmd_full" | awk '{print $1}')
    args=$(echo "$cmd_full" | cut -d' ' -f2-)
    
    # Process commands
    case "$cmd" in
        "stop")
            stop_bot
            ;;
        "start")
            start_bot
            ;;
        "restart")
            stop_bot
            sleep 1
            start_bot
            ;;
        "status")
            echo -e "${BLUE}🔍 Status:${NC}"
            if [ -n "$BOT_PID" ]; then
                echo -e "   ${GREEN}✅ Telegram bot running (PID: $BOT_PID)${NC}"
            else
                echo -e "   ${RED}❌ Telegram bot not running${NC}"
            fi
            
            # Check Docker services
            services=("celery_worker" "celery_beat" "redis" "postgres" "mongo")
            for service in "${services[@]}"; do
                if docker ps | grep -q $service; then
                    echo -e "   ${GREEN}✅ $service running${NC}"
                else
                    echo -e "   ${RED}❌ $service not running${NC}"
                fi
            done
            ;;
        "update-prices")
            update_token_prices
            ;;
        "update-all")
            update_token_prices "all"
            ;;
        "update-token")
            update_specific_token "$args"
            ;;
        "classify")
            classify_tokens
            ;;
        "token-stats")
            token_stats
            ;;
        "show-failures")
            show_failures "$args"
            ;;
        "reset-failures")
            reset_failures "$args"
            ;;
        "auto-updates")
            toggle_auto_updates "$args"
            ;;
        "celery-debug")
            debug_celery
            ;;
        "reload-celery"|"fast-reload")
            fast_reload_celery
            ;;
        "rebuild-celery")
            rebuild_celery
            ;;
        "logs")
            service=$(echo "$args" | awk '{print $1}')
            lines=$(echo "$args" | awk '{print $2}')
            service=${service:-"celery_worker"}
            lines=${lines:-50}
            
            # Check if lines is a valid number, if not, assume it's another service
            if ! [[ "$lines" =~ ^[0-9]+$ ]]; then
                view_logs "$service" 50 "yes"
            else
                view_logs "$service" "$lines" "yes"
            fi
            ;;
        "quick-logs")
            service=$(echo "$args" | awk '{print $1}')
            lines=$(echo "$args" | awk '{print $2}')
            service=${service:-"celery_worker"}
            lines=${lines:-50}
            
            # Check if lines is a valid number, if not, default to 50
            if ! [[ "$lines" =~ ^[0-9]+$ ]]; then
                lines=50
            fi
            
            view_logs "$service" "$lines" "no"
            ;;
        "extract-groups")
            extract-groups
            ;;
        "diagnose-token")
            diagnose_token "$args"
            ;;
        "recover-token")
            arg1=$(echo "$args" | awk '{print $1}')
            arg2=$(echo "$args" | awk '{print $2}')
            recover_token "$arg1" "$arg2"
            ;;
        "analyze-failures")
            analyze_failures
            ;;
        "fix-deadlocks")
            fix_deadlocks
            ;;
        "enable-forwarding")
            enable_forwarding
            ;;
        "disable-forwarding")
            disable_forwarding
            ;;
        "forwarding-status")
            get_forwarding_status
            ;;
        "shutdown")
            shutdown_all
            ;;
        "help")
            show_help
            ;;
        "clear")
            print_banner
            ;;
        "quit"|"exit")
            # Make quit/exit also perform the full shutdown
            shutdown_all
            break
            ;;
        "")
            # Empty command, just ignore
            ;;
        *)
            if [ -n "$cmd" ]; then  # Only show error for non-empty commands
                echo -e "${YELLOW}❓ Unknown command: $cmd${NC}"
                echo -e "   Type ${GREEN}help${NC} for available commands"
            fi
            ;;
    esac
done
