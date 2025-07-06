#!/bin/bash

# Ethereum Extraction Pipeline Setup
# ==================================

set -e

echo "Setting up Ethereum Extraction Pipeline"

# Install dependencies
echo "Installing Python dependencies..."
pip3 install -r requirements.txt

# Create .env from template if needed
if [ ! -f ".env" ]; then
    if [ -f ".env.template" ]; then
        cp .env.template .env
        echo "Created .env from template"
    else
        cat > .env << 'EOF'
GCP_PROJECT_ID=lgu-bot-462519
GITHUB_REPO=https://github.com/andrewearlosborne/eth-transaction-scraping/tree/main.git
START_DATE=2021-01-01-00:00
END_DATE=2025-05-01-23:59
NUM_VMS=5
ETHEREUM_PROVIDER_URLS=https://eth.drpc.org

GCP_ZONE=us-central1-a
GCP_MACHINE_TYPE=e2-standard-2
GCP_BOOT_DISK_SIZE=10GB
INTERVAL_SPAN_TYPE=day
INTERVAL_SPAN_LENGTH=1.0
OBSERVATIONS_PER_INTERVAL=100
PROVIDER_FETCH_DELAY_SECONDS=0.05
MONITOR_CHECK_INTERVAL=1200
LOCAL_DATA_DIR=data
EOF
        echo "Created basic .env file"
    fi
    echo "Edit .env file with your configuration"
fi

# Check gcloud auth
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" --quiet | head -n1 > /dev/null 2>&1; then
    echo "Error: Not authenticated with gcloud"
    echo "Run: gcloud auth login"
    exit 1
fi

# Check current project
current_project=$(gcloud config get-value project 2>/dev/null || echo "")
if [ -z "$current_project" ]; then
    echo "Warning: No default project set"
    echo "Run: gcloud config set project YOUR_PROJECT_ID"
else
    echo "Current project: $current_project"
fi

mkdir -p collected_data

echo "Setup complete"
echo ""
echo "Next steps:"
echo "1. Edit .env with your configuration"
echo "2. Run: python3 main.py"