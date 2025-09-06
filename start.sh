#!/bin/bash
set -e

echo "=== Ethereum Extraction VM Startup ==="
echo "Started at: $(date)"

# Update system packages
echo "Updating system packages..."
sudo apt-get update -qq
sudo apt-get install -y python3.11 python3-pip python3.11-venv screen curl htop

# Create Python virtual environment
echo "Setting up Python environment..."
python3.11 -m venv extractor/py3-venv

# Install Python dependencies
echo "Installing Python dependencies..."
source extractor/py3-venv/bin/activate && \
pip install --upgrade pip && \
pip install -r extractor/requirements.txt

echo "Configuration loaded:"
cat ../.env

# Start extraction process in screen session
echo "Starting extraction process in screen session..."
screen -dmS extraction bash -c 'source extractor/py3-venv/bin/activate && python3 extractor/extractor.py > extraction.log 2>&1; echo "COMPLETED" > status.txt'
echo screen -ls

echo "=== Startup Complete ==="
echo "Extraction is running in screen session 'extraction'"
echo "Use 'screen -r extraction' to attach to the session"
echo "Check logs with: tail -f /home/$(whoami)/extraction.log"
