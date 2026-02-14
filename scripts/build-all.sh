#!/bin/bash

set -e

echo "════════════════════════════════════════════════"
echo "  Building Distributed Job Orchestrator"
echo "════════════════════════════════════════════════"
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

build_worker() {
    echo -e "${BLUE}[1/4] Building Worker${NC}"
    cd worker
    if command -v cargo &> /dev/null; then
        cargo build --release
        echo -e "${GREEN}✓ Worker built${NC}"
    else
        echo -e "${YELLOW}⚠ Build tool not found, skipping worker build${NC}"
    fi
    cd ..
    echo ""
}

build_coordinator() {
    echo -e "${BLUE}[2/4] Building Coordinator${NC}"
    cd coordinator
    if command -v rebar3 &> /dev/null; then
        rebar3 compile
        echo -e "${GREEN}✓ Coordinator built${NC}"
    else
        echo -e "${YELLOW}⚠ Build tool not found, skipping coordinator build${NC}"
    fi
    cd ..
    echo ""
}

build_gateway() {
    echo -e "${BLUE}[3/4] Building Gateway${NC}"
    cd gateway
    if command -v crystal &> /dev/null; then
        mkdir -p bin
        crystal build src/server.cr -o bin/gateway --release
        crystal build src/cli.cr -o bin/job-cli --release
        echo -e "${GREEN}✓ Gateway built${NC}"
    else
        echo -e "${YELLOW}⚠ Build tool not found, skipping gateway build${NC}"
    fi
    cd ..
    echo ""
}

build_scheduler() {
    echo -e "${BLUE}[4/4] Setting up Scheduler${NC}"
    cd scheduler
    if command -v python3 &> /dev/null; then
        # Create virtual environment if it doesn't exist
        if [ ! -d "venv" ]; then
            python3 -m venv venv
        fi
        
        # Activate venv and install requirements
        source venv/bin/activate
        pip install --upgrade pip
        pip install -r requirements.txt
        chmod +x scheduler.py
        deactivate
        
        echo -e "${GREEN}✓ Scheduler ready${NC}"
    else
        echo -e "${YELLOW}⚠ Python not found, skipping scheduler setup${NC}"
    fi
    cd ..
    echo ""
}

# Main build
build_worker
build_coordinator
build_gateway
build_scheduler

echo "════════════════════════════════════════════════"
echo -e "${GREEN}✓ Build complete!${NC}"
echo "════════════════════════════════════════════════"
echo ""
echo "Next steps:"
echo "  1. Start coordinator: cd coordinator && rebar3 shell"
echo "  2. Start worker: cd worker && cargo run -- --id worker-1"
echo "  3. Start gateway: cd gateway && ./bin/gateway 8080"
echo "  4. Start scheduler: cd scheduler && source venv/bin/activate && python scheduler.py"
echo "  5. Submit job: cd gateway && ./bin/job-cli submit echo hello"
echo ""