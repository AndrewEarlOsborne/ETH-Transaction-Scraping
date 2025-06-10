#!/usr/bin/env python3
"""
Ethereum Extraction Pipeline - Main Runner
==========================================

Simple execution script for the GCP VM orchestrated Ethereum extraction pipeline.
"""

import sys
import os
from datetime import datetime

try:
    from vm_orchestrator import EthereumVMOrchestrator, validate_environment
except ImportError:
    print("Error: gcp_eth_orchestrator.py not found")
    sys.exit(1)


def main():
    """Execute the extraction pipeline."""
    
    try:
        # Initialize and run
        orchestrator = EthereumVMOrchestrator()
        
        print(f"Pipeline Configuration:")
        print(f"  Project: {orchestrator.project_id}")
        print(f"  VMs: {orchestrator.num_vms}")
        print(f"  Period: {orchestrator.start_date} → {orchestrator.end_date}")
        print(f"  Machine: {orchestrator.machine_type} in {orchestrator.zone}")
            
        
        print(f"\nStarting pipeline at {datetime.now().strftime('%H:%M:%S')}")
        results = orchestrator.run_pipeline()
        
        successful = sum(1 for status in results.values() if status == "SUCCESS")
        total = len(results)
        
        print(f"\nPipeline Complete:")
        print(f"  Successful: {successful}/{total}")
        
        if successful > 0:
            print(f"  Data: {orchestrator.data_dir}/aggregated/")
            
        failures = {vm: status for vm, status in results.items() if status != "SUCCESS"}
        if failures:
            print(f"  Failures: {len(failures)}")
            for vm, status in failures.items():
                print(f"    {vm}: {status}")
                
        print(f"  Completed: {datetime.now().strftime('%H:%M:%S')}")
        
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        print("Check GCP console for any remaining VMs")
        sys.exit(1)
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        print("Check pipeline_errors.log for details")
        sys.exit(1)


if __name__ == "__main__":
    main()