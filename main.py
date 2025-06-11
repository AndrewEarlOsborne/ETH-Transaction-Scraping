#!/usr/bin/env python3
import sys
import os
from datetime import datetime
from vm_orchestrator import EthereumVMOrchestrator

def show_usage():
    """Show usage instructions."""
    print("Ethereum Extraction Pipeline")
    print("- - - - - - - - - - - - - - - - - - ")
    print("Commands:")
    print("  deploy   - Deploy VMs and start extraction (Stage 1)")
    print("  collect  - Collect results from completed VMs (Stage 2)")
    print("  status   - Check status of deployed VMs")
    print("- - - - - - - - - - - - - - - - - - ")

def deploy_instances():
    """Use Scheduler to deploy VMs."""
    print("=== Stage 1: Deploying VMs ===")
    
    try:
        orchestrator = EthereumVMOrchestrator()
        
        # Show config
        print(f"Configuration:")
        print(f"  Project: {orchestrator.project_id}")
        print(f"  VMs: {orchestrator.num_vms}")
        print(f"  Period: {orchestrator.start_date} → {orchestrator.end_date}")
        print(f"  Machine: {orchestrator.machine_type}")
        
        # Confirm
        response = input("\nDeploy VMs? [y/N]: ").strip().lower()
        if response not in ['y', 'yes']:
            print("Cancelled")
            return
            
        # Deploy
        results = orchestrator.deploy_vms()
        
        if results.get("status") == "already_deployed":
            return
            
        successful = sum(1 for status in results.values() if status == "DEPLOYED")
        print(f"\n=== Deployment Complete ===")
        print(f"VMs deployed: {successful}/{len(results)}")
        
        if successful > 0:
            print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("")
            print("VMs are now running extraction")
        else:
            print("ERROR: VMs not deployed successfully")
            sys.exit(1)
            
    except Exception as e:
        print(f"Deployment failed: {e}")
        sys.exit(1)


def collect_stage():
    """Stage 2: Collect results."""
    print("=== Stage 2: Collecting Results ===")
    
    try:
        orchestrator = EthereumVMOrchestrator()
        results = orchestrator.collect_results()
        
        # Count results
        completed = sum(1 for status in results.values() if status == "COMPLETED")
        successful = sum(1 for status in results.values() if status == "SUCCESS")
        running = sum(1 for status in results.values() if status == "RUNNING")
        
        print(f"\n=== Collection Complete ===")
        print(f"Completed VMs: {completed}")
        print(f"Successfully downloaded: {successful}")
        print(f"Still running: {running}")
        
        if successful > 0:
            print(f"📁 Data available: {orchestrator.data_dir}/aggregated/")
        
        if running > 0:
            print("⏳ Some VMs still running - run collect again later")
            
    except Exception as e:
        print(f"Collection failed: {e}")
        sys.exit(1)


def status_check():
    """Check deployment status."""
    print("=== Deployment Status ===")
    
    try:
        orchestrator = EthereumVMOrchestrator()
        status = orchestrator.status_check()
        
        if status["status"] == "no_deployment":
            print("No active deployment found")
            print("Run: python3 main.py deploy")
            return
            
        print(f"Deployed: {status['deployment_time']}")
        print(f"Total VMs: {status['total_vms']}")
        print("")
        
        vm_statuses = status['vm_statuses']
        running = sum(1 for s in vm_statuses.values() if s == "RUNNING")
        completed = sum(1 for s in vm_statuses.values() if s == "COMPLETED")
        failed = len(vm_statuses) - running - completed
        
        print(f"Running: {running}")
        print(f"Completed: {completed}")
        print(f"Failed/Other: {failed}")
        
            
    except Exception as e:
        print(f"Status check failed: {e}")


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        return
        
    command = sys.argv[1].lower()
    
    try:
        if command == "deploy":
            deploy_instances()
        elif command == "collect":
            collect_stage()
        elif command == "status":
            status_check()
            show_usage()
        else:
            print(f"Unknown command: {command}")
            print("Accepted commands are: ['deploy', 'collect', 'status']")
            
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()