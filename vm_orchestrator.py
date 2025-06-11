#!/usr/bin/env python3
"""
Google Cloud Platform VM Orchestrator for Ethereum Feature Extraction
=====================================================================

Simple two-stage orchestration: deploy VMs, then later collect results.
VM configuration reconstructed from .env file each time.
"""

import os
import logging
import subprocess
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from dotenv import load_dotenv


class EthereumVMOrchestrator:
    """Simple two-stage orchestrator: deploy then collect."""
    
    def __init__(self, config_file: str = '.env'):
        """Initialize orchestrator with configuration."""
        self._setup_logging()
        self._load_config(config_file)
        
    def _setup_logging(self):
        """Configure error-only logging."""
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
        required = {
            'GCP_PROJECT_ID': os.getenv('GCP_PROJECT_ID'),
            'GITHUB_REPO': os.getenv('GITHUB_REPO'),
            'START_DATE': os.getenv('START_DATE'),
            'END_DATE': os.getenv('END_DATE'),
            'NUM_VMS': os.getenv('NUM_VMS'),
            'ETHEREUM_PROVIDER_URLS': os.getenv('ETHEREUM_PROVIDER_URLS')
        }
        
        if [k for k, v in required.items() if not v]:
            raise ValueError(f"Missing required configs")
        
        self.project_id = required['GCP_PROJECT_ID']
        self.github_repo = required['GITHUB_REPO']
        self.start_date = required['START_DATE']
        self.end_date = required['END_DATE']
        self.num_vms = int(required['NUM_VMS'])
        self.provider_urls = [url.strip() for url in required['ETHEREUM_PROVIDER_URLS'].split(',')]
        
        # Optional parameters
        self.zone = os.getenv('GCP_ZONE', 'us-central1-a')
        self.machine_type = os.getenv('GCP_MACHINE_TYPE', 'e2-standard-2')
        self.disk_size = os.getenv('GCP_BOOT_DISK_SIZE', '10GB')
        self.data_dir = os.getenv('LOCAL_DATA_DIR', 'data')
        
        # Extraction parameters for VMs
        self.vm_config = {
            'interval_type': os.getenv('INTERVAL_SPAN_TYPE', 'day'),
            'interval_length': os.getenv('INTERVAL_SPAN_LENGTH', '1.0'),
            'observations': os.getenv('OBSERVATIONS_PER_INTERVAL', '100'),
            'delay': os.getenv('DELAY_SECONDS', '0.05')
        }
        
    def _get_vm_names(self) -> List[str]:
        """Generate VM names based on config."""
        return [f"eth-extractor-{i+1:03d}" for i in range(self.num_vms)]
        
    def _get_vm_time_range(self, vm_index: int) -> tuple:
        """Get time range for specific VM."""
        start_dt = datetime.strptime(self.start_date, '%Y-%m-%d-%H:%M')
        end_dt = datetime.strptime(self.end_date, '%Y-%m-%d-%H:%M')
        duration_per_vm = (end_dt - start_dt) / self.num_vms
        
        vm_start = start_dt + (duration_per_vm * vm_index)
        vm_end = start_dt + (duration_per_vm * (vm_index + 1))
        
        if vm_index == self.num_vms - 1:
            vm_end = end_dt
            
        return (vm_start.strftime('%Y-%m-%d-%H:%M'), vm_end.strftime('%Y-%m-%d-%H:%M'))
        
    def _create_startup_script(self, vm_index: int) -> str:
        """Generate startup script for VM."""
        vm_start, vm_end = self._get_vm_time_range(vm_index)
        provider_url = self.provider_urls[vm_index % len(self.provider_urls)]
        
        dot_env_vars = f"""ETHEREUM_PROVIDER_URL={provider_url}
    START_DATE={vm_start}
    END_DATE={vm_end}
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
        {dot_env_vars}
        EOF

        screen -dmS extraction bash -c "cd /home/ethereum/extraction_pipeline && source venv/bin/activate && python main.py"
        touch /tmp/startup-complete
        '''

    def _create_vm(self, vm_name: str, vm_index: int) -> bool:
        """Create single VM instance."""
        try:
            script_file = f"/tmp/startup-{vm_name}.sh"
            with open(script_file, 'w') as f:
                f.write(self._create_startup_script(vm_index))
            
            cmd = [
                "gcloud", "compute", "instances", "create", vm_name,
                "--project", self.project_id, "--zone", self.zone,
                "--machine-type", self.machine_type,
                "--image-family", "ubuntu-2204-lts",
                "--image-project", "ubuntu-os-cloud",
                "--boot-disk-size", self.disk_size,
                "--metadata-from-file", f"startup-script={script_file}",
                "--tags", "ethereum-extractor", "--quiet"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            success = result.returncode == 0
            
            if not success:
                self.logger.error(f"VM creation failed {vm_name}: {result.stderr}")
                
            os.remove(script_file)
            return success
            
        except Exception as e:
            self.logger.error(f"Exception creating VM {vm_name}: {e}")
            return False
            
    def _list_existing_vms(self) -> List[str]:
        """List existing VMs with our tag."""
        try:
            cmd = ["gcloud", "compute", "instances", "list",
                   "--project", self.project_id,
                   "--filter", "tags.items=ethereum-extractor",
                   "--format", "value(name,zone)",
                   "--quiet"]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return []
                
            vms = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    name, zone = line.split('\t')
                    if name.startswith('eth-extractor-'):
                        vms.append(name)
            return vms
            
        except Exception as e:
            self.logger.error(f"Failed to list VMs: {e}")
            return []
            
    def _check_vm_status(self, vm_name: str) -> str:
        """Check VM status."""
        try:
            cmd = ["gcloud", "compute", "instances", "describe", vm_name,
                   "--project", self.project_id, "--zone", self.zone,
                   "--format", "value(status)", "--quiet"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                return "NOT_FOUND"
            
            vm_status = result.stdout.strip()
            if vm_status != "RUNNING":
                return f"VM_{vm_status}"
            
            # Check if data files exist (simple completion check)
            ssh_cmd = ["gcloud", "compute", "ssh", vm_name,
                      "--project", self.project_id, "--zone", self.zone,
                      "--command", "ls /home/ethereum/extraction_pipeline/data/*.csv 2>/dev/null | wc -l",
                      "--quiet"]
            ssh_result = subprocess.run(ssh_cmd, capture_output=True, text=True)
            
            if ssh_result.returncode == 0:
                file_count = int(ssh_result.stdout.strip() or "0")
                return "COMPLETED" if file_count > 0 else "RUNNING"
            else:
                return "RUNNING"
                
        except Exception as e:
            self.logger.error(f"Status check failed {vm_name}: {e}")
            return "ERROR"
            
    def _download_data(self, vm_name: str) -> bool:
        """Download data from VM."""
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
            
    def _delete_vm(self, vm_name: str) -> bool:
        """Delete VM instance."""
        try:
            cmd = ["gcloud", "compute", "instances", "delete", vm_name,
                   "--project", self.project_id, "--zone", self.zone, "--quiet"]
            result = subprocess.run(cmd, capture_output=True)
            return result.returncode == 0
        except Exception as e:
            self.logger.error(f"Delete failed {vm_name}: {e}")
            return False
            
    def _aggregate_data(self):
        """Aggregate all collected CSV files."""
        try:
            all_files = []
            for vm_dir in os.listdir(self.data_dir):
                vm_path = os.path.join(self.data_dir, vm_dir)
                if os.path.isdir(vm_path) and vm_dir != "aggregated":
                    for file in os.listdir(vm_path):
                        if file.endswith('.csv'):
                            all_files.append(os.path.join(vm_path, file))
            
            if not all_files:
                return
                
            # Separate file types
            validator_files = [f for f in all_files if '_validator_transactions.csv' in f]
            transaction_files = [f for f in all_files if '_transactions.csv' in f and '_validator_transactions.csv' not in f]
            
            # Clean files
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
            
    def setup(self) -> Dict[str, str]:
        """Deploy all VMs and start extraction."""
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Check for existing VMs
        existing_vms = self._list_existing_vms()
        if existing_vms:
            print(f"Found {len(existing_vms)} existing VMs with ethereum-extractor tag")
            print("Use 'cleanup' command to collect results and remove them")
            return {"status": "existing_vms_found", "vms": existing_vms}
        
        # Create VMs
        vm_names = self._get_vm_names()
        print(f"Creating {len(vm_names)} VMs...")
        
        results = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self._create_vm, vm_name, i): vm_name 
                      for i, vm_name in enumerate(vm_names)}
            
            for future in as_completed(futures):
                vm_name = futures[future]
                success = future.result()
                results[vm_name] = "CREATED" if success else "FAILED"
                
                if success:
                    print(f"✓ {vm_name}")
                else:
                    print(f"✗ {vm_name}")
        
        created = sum(1 for status in results.values() if status == "CREATED")
        print(f"\nCreated {created}/{len(vm_names)} VMs successfully")
        
        if created == 0:
            raise RuntimeError("No VMs created successfully")
            
        return results
        
    def cleanup(self) -> Dict[str, str]:
        """Collect results and delete all VMs."""
        # Find existing VMs
        existing_vms = self._list_existing_vms()
        if not existing_vms:
            print("No VMs found with ethereum-extractor tag")
            return {}
        
        print(f"Found {len(existing_vms)} VMs")
        
        # Check status of all VMs
        results = {}
        completed_vms = []
        
        for vm_name in existing_vms:
            status = self._check_vm_status(vm_name)
            results[vm_name] = status
            
            if status == "COMPLETED":
                completed_vms.append(vm_name)
                print(f"✓ {vm_name} (completed)")
            elif status == "RUNNING":
                print(f"⏳ {vm_name} (still running)")
            else:
                print(f"✗ {vm_name} ({status.lower()})")
        
        # Download data from completed VMs
        if completed_vms:
            print(f"\nDownloading data from {len(completed_vms)} completed VMs...")
            for vm_name in completed_vms:
                if self._download_data(vm_name):
                    print(f"✓ {vm_name} data downloaded")
                    results[f"{vm_name}_download"] = "SUCCESS"
                else:
                    print(f"✗ {vm_name} download failed")
                    results[f"{vm_name}_download"] = "FAILED"
        
        # Delete all VMs
        print(f"\nDeleting {len(existing_vms)} VMs...")
        for vm_name in existing_vms:
            if self._delete_vm(vm_name):
                print(f"✓ {vm_name} deleted")
                results[f"{vm_name}_delete"] = "SUCCESS"
            else:
                print(f"✗ {vm_name} delete failed")
                results[f"{vm_name}_delete"] = "FAILED"
        
        # Aggregate data if any downloads succeeded
        successful_downloads = [vm for vm in completed_vms 
                              if results.get(f"{vm}_download") == "SUCCESS"]
        
        if successful_downloads:
            print("\nAggregating collected data...")
            self._aggregate_data()
            print(f"Data aggregated in: {self.data_dir}/aggregated/")
        
        return results


def validate_environment() -> bool:
    """Validate required tools and authentication."""
    try:
        # Check gcloud
        result = subprocess.run(['gcloud', 'version'], capture_output=True)
        if result.returncode != 0:
            print("Error: gcloud CLI not working")
            return False
            
        # Check authentication
        result = subprocess.run(['gcloud', 'auth', 'list', '--filter=status:ACTIVE', '--format=value(account)'], 
                              capture_output=True, text=True)
        if not result.stdout.strip():
            print("Error: Not authenticated with gcloud")
            return False
            
        return True
        
    except FileNotFoundError:
        print("Error: gcloud CLI not found")
        return False