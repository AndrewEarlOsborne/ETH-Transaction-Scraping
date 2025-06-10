#!/usr/bin/env python3
"""
Google Cloud Platform VM Orchestrator for Ethereum Feature Extraction
=====================================================================

Core orchestration logic for managing GCP VMs running Ethereum extraction tasks.
"""

import os
import time
import logging
import subprocess
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Optional
from dotenv import load_dotenv


@dataclass
class VMConfig:
    """Configuration for a single VM instance."""
    name: str
    zone: str
    start_date: str
    end_date: str
    provider_url: str


class EthereumVMOrchestrator:
    """Orchestrates multiple GCP VMs for Ethereum extraction tasks."""
    
    def __init__(self, config_file: str = '.env'):
        """Initialize orchestrator with configuration from .env file."""
        self._setup_logging()
        self._load_config(config_file)
        self.running_vms: Dict[str, VMConfig] = {}
        
    def _setup_logging(self):
        """Configure minimal logging."""
        logging.basicConfig(
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler("pipeline_errors.log")]
        )
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self, config_file: str):
        """Load and validate configuration."""
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file {config_file} not found")
        
        load_dotenv(config_file)
        
        # Required parameters
        required = {
            'GCP_PROJECT_ID': os.getenv('GCP_PROJECT_ID'),
            'GITHUB_REPO': os.getenv('GITHUB_REPO'),
            'START_DATE': os.getenv('START_DATE'),
            'END_DATE': os.getenv('END_DATE'),
            'NUM_VMS': os.getenv('NUM_VMS'),
            'ETHEREUM_PROVIDER_URLS': os.getenv('ETHEREUM_PROVIDER_URLS')
        }
        
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")
        
        self.project_id = required['GCP_PROJECT_ID']
        self.github_repo = required['GITHUB_REPO']
        self.start_date = required['START_DATE']
        self.end_date = required['END_DATE']
        self.num_vms = int(required['NUM_VMS'])
        self.provider_urls = [url.strip() for url in required['ETHEREUM_PROVIDER_URLS'].split(',')]
        
        # Optional parameters
        self.zone = os.getenv('GCP_ZONE', 'us-central1-a')
        self.machine_type = os.getenv('GCP_MACHINE_TYPE', 'e2-standard-2')
        self.disk_size = os.getenv('GCP_BOOT_DISK_SIZE', '20GB')
        self.data_dir = os.getenv('LOCAL_DATA_DIR', 'collected_data')
        self.check_interval = int(os.getenv('MONITOR_CHECK_INTERVAL', '300'))
        
        # Extraction parameters for VMs
        self.vm_config = {
            'interval_type': os.getenv('INTERVAL_SPAN_TYPE', 'day'),
            'interval_length': os.getenv('INTERVAL_SPAN_LENGTH', '1.0'),
            'observations': os.getenv('OBSERVATIONS_PER_INTERVAL', '100'),
            'delay': os.getenv('DELAY_SECONDS', '0.05')
        }
        
    def _generate_vm_configs(self) -> List[VMConfig]:
        """Generate VM configurations by splitting date range."""
        start_dt = datetime.strptime(self.start_date, '%Y-%m-%d-%H:%M')
        end_dt = datetime.strptime(self.end_date, '%Y-%m-%d-%H:%M')
        duration_per_vm = (end_dt - start_dt) / self.num_vms
        
        configs = []
        for i in range(self.num_vms):
            vm_start = start_dt + (duration_per_vm * i)
            vm_end = start_dt + (duration_per_vm * (i + 1))
            if i == self.num_vms - 1:
                vm_end = end_dt
                
            configs.append(VMConfig(
                name=f"eth-extractor-{i+1:03d}",
                zone=self.zone,
                start_date=vm_start.strftime('%Y-%m-%d-%H:%M'),
                end_date=vm_end.strftime('%Y-%m-%d-%H:%M'),
                provider_url=self.provider_urls[i % len(self.provider_urls)]
            ))
        return configs
        
    def _create_startup_script(self, config: VMConfig) -> str:
        """Generate startup script for VM."""
        env_vars = f"""ETHEREUM_PROVIDER_URL={config.provider_url}
START_DATE={config.start_date}
END_DATE={config.end_date}
OBSERVATIONS_PER_INTERVAL={self.vm_config['observations']}
DELAY_SECONDS={self.vm_config['delay']}
INTERVAL_SPAN_TYPE={self.vm_config['interval_type']}
INTERVAL_SPAN_LENGTH={self.vm_config['interval_length']}
DATA_DIRECTORY=data"""

        return f'''#!/bin/bash
set -e
exec > /var/log/startup-script.log 2>&1

apt-get update -qq
apt-get install -y git python3-pip python3-full screen

cd /home/ethereum
mkdir -p extraction_pipeline && cd extraction_pipeline
git clone {self.github_repo} .

python3 -m venv venv
source venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt

cat > .env << EOF
{env_vars}
EOF

cat > /home/ethereum/check_status.sh << 'EOF'
if pgrep -f "python.*main.py" > /dev/null; then
    echo "RUNNING"
elif [ -f "data/"*".csv" ] 2>/dev/null; then
    echo "COMPLETED"
else
    echo "FAILED"
fi
EOF
chmod +x /home/ethereum/check_status.sh

screen -dmS extraction bash -c "cd /home/ethereum/extraction_pipeline && source venv/bin/activate && python main.py"
touch /tmp/startup-complete
'''

    def _create_vm(self, config: VMConfig) -> bool:
        """Create single VM instance."""
        try:
            script_file = f"/tmp/startup-{config.name}.sh"
            with open(script_file, 'w') as f:
                f.write(self._create_startup_script(config))
            
            cmd = [
                "gcloud", "compute", "instances", "create", config.name,
                "--project", self.project_id, "--zone", config.zone,
                "--machine-type", self.machine_type,
                "--image-family", "ubuntu-2204-lts",
                "--image-project", "ubuntu-os-cloud",
                "--boot-disk-size", self.disk_size,
                "--metadata-from-file", f"startup-script={script_file}",
                "--tags", "ethereum-extractor", "--quiet"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            success = result.returncode == 0
            
            if success:
                self.running_vms[config.name] = config
            else:
                self.logger.error(f"VM creation failed {config.name}: {result.stderr}")
                
            os.remove(script_file)
            return success
            
        except Exception as e:
            self.logger.error(f"Exception creating VM {config.name}: {e}")
            return False
            
    def _check_vm_status(self, vm_name: str) -> str:
        """Check VM and extraction status."""
        try:
            # Check VM is running
            cmd = ["gcloud", "compute", "instances", "describe", vm_name,
                   "--project", self.project_id, "--zone", self.zone,
                   "--format", "value(status)", "--quiet"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0 or result.stdout.strip() != "RUNNING":
                return "STOPPED"
            
            # Check extraction status
            ssh_cmd = ["gcloud", "compute", "ssh", vm_name,
                      "--project", self.project_id, "--zone", self.zone,
                      "--command", "/home/ethereum/check_status.sh", "--quiet"]
            ssh_result = subprocess.run(ssh_cmd, capture_output=True, text=True)
            
            return ssh_result.stdout.strip() if ssh_result.returncode == 0 else "ERROR"
            
        except Exception as e:
            self.logger.error(f"Status check failed {vm_name}: {e}")
            return "ERROR"
            
    def _download_data(self, vm_name: str) -> bool:
        """Download data from completed VM."""
        try:
            vm_dir = os.path.join(self.data_dir, vm_name)
            os.makedirs(vm_dir, exist_ok=True)
            
            cmd = ["gcloud", "compute", "scp", "--recurse",
                   "--project", self.project_id, "--zone", self.zone,
                   f"{vm_name}:/home/ethereum/extraction_pipeline/data/*",
                   vm_dir, "--quiet"]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
            
        except Exception as e:
            self.logger.error(f"Download failed {vm_name}: {e}")
            return False
            
    def _delete_vm(self, vm_name: str):
        """Delete VM instance."""
        try:
            cmd = ["gcloud", "compute", "instances", "delete", vm_name,
                   "--project", self.project_id, "--zone", self.zone, "--quiet"]
            subprocess.run(cmd, capture_output=True)
            self.running_vms.pop(vm_name, None)
        except Exception as e:
            self.logger.error(f"Delete failed {vm_name}: {e}")
            
    def _aggregate_data(self):
        """Aggregate all collected CSV files."""
        try:
            # Find all CSV files
            all_files = []
            for vm_dir in os.listdir(self.data_dir):
                vm_path = os.path.join(self.data_dir, vm_dir)
                if os.path.isdir(vm_path):
                    for file in os.listdir(vm_path):
                        if file.endswith('.csv'):
                            all_files.append(os.path.join(vm_path, file))
            
            if not all_files:
                return
                
            # Separate file types
            validator_files = [f for f in all_files if '_validator_transactions.csv' in f]
            transaction_files = [f for f in all_files if '_transactions.csv' in f and '_validator_transactions.csv' not in f]
            
            # Clean and aggregate
            for file in all_files:
                self._clean_csv(file)
                
            output_dir = os.path.join(self.data_dir, "aggregated")
            os.makedirs(output_dir, exist_ok=True)
            
            if validator_files:
                combined = pd.concat([pd.read_csv(f) for f in validator_files], ignore_index=True)
                combined.to_csv(os.path.join(output_dir, "validators.csv"), index=False)
                
            if transaction_files:
                combined = pd.concat([pd.read_csv(f) for f in transaction_files], ignore_index=True)
                combined.to_csv(os.path.join(output_dir, "transactions.csv"), index=False)
                
        except Exception as e:
            self.logger.error(f"Aggregation failed: {e}")
            
    def _clean_csv(self, file_path: str):
        """Clean CSV file formatting."""
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
            if not lines:
                return
                
            headers = lines[0].strip().split(',')
            num_headers = len(headers)
            
            cleaned = [lines[0]]
            for line in lines[1:]:
                cols = line.strip().split(',')
                if len(cols) > num_headers:
                    cols = cols[:num_headers]
                cleaned.append(','.join(cols) + '\n')
                
            with open(file_path, 'w') as f:
                f.writelines(cleaned)
                
        except Exception as e:
            self.logger.error(f"CSV cleaning failed {file_path}: {e}")
            
    def run_pipeline(self) -> Dict[str, str]:
        """Execute the complete extraction pipeline."""
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Generate and create VMs
        configs = self._generate_vm_configs()
        
        print(f"Creating {len(configs)} VMs...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self._create_vm, config): config.name for config in configs}
            created = sum(1 for future in as_completed(futures) if future.result())
        
        print(f"Created {created}/{len(configs)} VMs successfully")
        
        if not self.running_vms:
            raise RuntimeError("No VMs created successfully")
        
        # Monitor and collect
        print("Monitoring extraction progress...")
        results = {}
        
        while self.running_vms:
            for vm_name in list(self.running_vms.keys()):
                status = self._check_vm_status(vm_name)
                
                if status == "COMPLETED":
                    if self._download_data(vm_name):
                        results[vm_name] = "SUCCESS"
                        print(f"✓ {vm_name}")
                    else:
                        results[vm_name] = "DOWNLOAD_FAILED"
                        print(f"✗ {vm_name} (download failed)")
                    self._delete_vm(vm_name)
                    
                elif status in ["FAILED", "STOPPED", "ERROR"]:
                    results[vm_name] = status
                    print(f"✗ {vm_name} ({status.lower()})")
                    self._delete_vm(vm_name)
            
            if self.running_vms:
                time.sleep(self.check_interval)
        
        # Aggregate results
        print("Aggregating data...")
        self._aggregate_data()
        
        return results