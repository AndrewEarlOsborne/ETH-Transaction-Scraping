#!/bin/bash
"""
Ethereum Extraction VM Startup Script
=====================================


"""

set -e

echo "=== Ethereum Extraction VM Startup ==="
echo "Started at: $(date)"

# Update system packages
echo "Updating system packages..."
apt-get update -qq
apt-get install -y git python3-pip python3-venv screen curl htop

# Create ethereum user if not exists
if ! id "ethereum" &>/dev/null; then
    echo "Creating ethereum user..."
    useradd -m -s /bin/bash ethereum
fi

# Switch to ethereum user directory
cd /home/ethereum

# Clone the extraction repository
echo "Cloning extraction repository..."
if [ ! -d "extraction" ]; then
    sudo -u ethereum git clone $EXTRACTION_REPO extraction
else
    echo "Extraction directory already exists"
fi

cd extraction

# Create Python virtual environment
echo "Setting up Python environment..."
if [ ! -d "venv" ]; then
    sudo -u ethereum python3 -m venv venv
fi

# Install Python dependencies
echo "Installing Python dependencies..."
sudo -u ethereum bash -c "source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt"

# Verify .env file exists (should be created by orchestrator)
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found!"
    exit 1
fi

echo "Configuration loaded:"
cat .env

# Start extraction in screen session
echo "Starting extraction process in screen session..."
sudo -u ethereum screen -dmS extraction bash -c "
    cd /home/ethereum/extraction
    source venv/bin/activate
    echo 'Starting extraction at $(date)' >> extraction.log
    python3 extractor.py 2>&1 | tee -a extraction.log
    echo 'Extraction finished at $(date)' >> extraction.log
"

# Create completion marker
touch /tmp/startup-complete
echo "VM startup completed at: $(date)" >> /tmp/startup-complete

echo "=== Startup Complete ==="
echo "Extraction is running in screen session 'extraction'"
echo "Use 'screen -r extraction' to attach to the session"
echo "Check logs with: tail -f /home/ethereum/extraction/extraction.log"