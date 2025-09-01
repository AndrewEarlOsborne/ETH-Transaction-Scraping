#!/bin/bash
set -e

echo "=== Ethereum Extraction VM Startup ==="
echo "Started at: $(date)"

# Update system packages
echo "Updating system packages..."
apt-get update -qq
apt-get install -y git python3.11 python3-pip python3.11-venv screen curl htop

# Create Python virtual environment
echo "Setting up Python environment..."
python3.11 -m venv py3-venv

# Install Python dependencies
echo "Installing Python dependencies..."
source py3-venv/bin/activate && \
pip install --upgrade pip && \
pip install -r requirements.txt

# Verify .env file was created by orchestrator
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found!"
    exit 1
fi

echo "Configuration loaded:"
cat .env

# Start extraction in screen session
echo "Starting extraction process in screen session..."
screen -dmS extraction bash -c "
    cd /home/extractor/ETH-Transaction-Scraping
    source py3-venv/bin/activate
    echo 'Starting extraction at $(date)' >> extraction.log
    python3.11 extractor.py 2>&1 | tee -a extraction.log
    echo 'Extraction finished at $(date)' >> extraction.log"

echo "=== Startup Complete ==="
echo "Extraction is running in screen session 'extraction'"
echo "Use 'screen -r extraction' to attach to the session"
echo "Check logs with: tail -f /home/ethereum/extraction/extraction.log"
