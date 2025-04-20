#!/bin/bash

# Functions for Nova Bot CA forwarding

# Function to ensure the forwarding status file exists and is initialized
initialize_forwarding_status() {
    if [ ! -f "$FORWARDING_STATUS_FILE" ]; then
        echo "disabled" > "$FORWARDING_STATUS_FILE"
        echo -e "${YELLOW}🔧 Initialized Nova forwarding status file to 'disabled'.${NC}"
    fi
}

# Function to get current forwarding status
get_forwarding_status() {
    local mode=$1 # Optional: pass 'quiet' to suppress output
    
    if [ -f "$FORWARDING_STATUS_FILE" ]; then
        CURRENT_FORWARDING_STATUS=$(cat "$FORWARDING_STATUS_FILE")
    else
        CURRENT_FORWARDING_STATUS="disabled (file not found)"
    fi

    if [ "$mode" != "quiet" ]; then
        if [[ "$CURRENT_FORWARDING_STATUS" == "enabled" ]]; then
            echo -e "${GREEN}✅ Nova CA Forwarding is currently ENABLED.${NC}"
        else
            echo -e "${YELLOW}🟡 Nova CA Forwarding is currently DISABLED.${NC}"
        fi
    fi
}

# Function to enable forwarding
enable_forwarding() {
    echo "enabled" > "$FORWARDING_STATUS_FILE"
    echo -e "${GREEN}✅ Nova CA Forwarding ENABLED.${NC} The monitor script will pick this up shortly."
    CURRENT_FORWARDING_STATUS="enabled" # Update status for prompt if needed
}

# Function to disable forwarding
disable_forwarding() {
    echo "disabled" > "$FORWARDING_STATUS_FILE"
    echo -e "${YELLOW}🟡 Nova CA Forwarding DISABLED.${NC} The monitor script will pick this up shortly."
    CURRENT_FORWARDING_STATUS="disabled" # Update status for prompt if needed
}
