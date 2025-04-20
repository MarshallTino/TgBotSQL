# Function to initialize required services
initialize_services() {
    echo -e "${BLUE}🔄 Initializing services...${NC}"
    
    # Check if required directories exist
    if [ ! -d "logs" ]; then
        echo -e "${YELLOW}Creating logs directory...${NC}"
        mkdir -p logs
    fi
    
    # Check if the config directory exists
    if [ ! -d "config" ]; then
        echo -e "${YELLOW}Creating config directory...${NC}"
        mkdir -p config
    fi
    
    echo -e "${GREEN}✅ Services initialized.${NC}"
}

# Function to activate the virtual environment
activate_venv() {
    echo -e "${BLUE}🔄 Activating virtual environment...${NC}"
    if [ -f "tg_env/bin/activate" ]; then
        source tg_env/bin/activate
        echo -e "${GREEN}✅ Virtual environment activated.${NC}"
        return 0
    else
        echo -e "${YELLOW}⚠️ Virtual environment not found, creating it now...${NC}"
        python -m venv tg_env
        source tg_env/bin/activate
        pip install -r requirements.txt
        echo -e "${GREEN}✅ Virtual environment created and activated.${NC}"
        return 0
    fi
    return 1
}

# Function to set environment variables
set_environment_vars() {
    echo -e "${BLUE}🔄 Setting environment variables...${NC}"
    export PYTHONPATH=$PYTHONPATH:$(pwd)
    export ENV=${ENV:-"dev"}
    echo -e "${GREEN}✅ Environment variables set.${NC}"
    return 0
}
