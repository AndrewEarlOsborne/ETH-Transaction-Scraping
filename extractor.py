#!/usr/bin/env python3
"""
Ethereum Feature Extractor - VM Component
=========================================

Minimal extraction script that runs independently on VMs.
Designed to run until completion in a screen session.
"""

import os
import sys
import json
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv


class EthereumExtractor:
    """Minimal Ethereum data extractor for VM deployment."""
    
    def __init__(self, config_file: str = '.env'):
        """Initialize extractor with configuration."""
        self._setup_logging()
        self._load_config(config_file)
        self._setup_data_directory()
        self._create_status_file()
        
    def _setup_logging(self):
        """Configure logging for VM environment."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("extraction.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self, config_file: str):
        """Load configuration from environment file."""
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file {config_file} not found")
            
        load_dotenv(config_file)
        
        # Required configurations
        required = {
            'ETHEREUM_PROVIDER_URL': os.getenv('ETHEREUM_PROVIDER_URL'),
            'START_DATE': os.getenv('START_DATE'),
            'END_DATE': os.getenv('END_DATE'),
        }
        
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing required config: {missing}")
            
        self.provider_url = required['ETHEREUM_PROVIDER_URL']
        self.start_date = required['START_DATE']
        self.end_date = required['END_DATE']
        
        # Optional configurations with defaults
        self.observations_per_interval = int(os.getenv('OBSERVATIONS_PER_INTERVAL', '100'))
        self.fetch_delay = float(os.getenv('PROVIDER_FETCH_DELAY_SECONDS', '0.05'))
        self.interval_type = os.getenv('INTERVAL_SPAN_TYPE', 'day')
        self.interval_length = float(os.getenv('INTERVAL_SPAN_LENGTH', '1.0'))
        self.data_directory = os.getenv('DATA_DIRECTORY', 'data')
        
        # Parse dates
        self.start_dt = datetime.strptime(self.start_date, '%Y-%m-%d-%H:%M')
        self.end_dt = datetime.strptime(self.end_date, '%Y-%m-%d-%H:%M')
        
        self.logger.info(f"Configured for {self.start_date} to {self.end_date}")
        self.logger.info(f"Provider: {self.provider_url}")
        
    def _setup_data_directory(self):
        """Create data directory structure."""
        os.makedirs(self.data_directory, exist_ok=True)
        self.logger.info(f"Data directory: {os.path.abspath(self.data_directory)}")
        
    def _create_status_file(self):
        """Create status file for monitoring."""
        self.status_file = "status.txt"
        self._update_status("STARTED")
        
    def _update_status(self, status: str, details: str = ""):
        """Update status file."""
        timestamp = datetime.now().isoformat()
        status_info = f"{timestamp} - {status}"
        if details:
            status_info += f" - {details}"
            
        with open(self.status_file, 'w') as f:
            f.write(status_info + "\n")
            
        self.logger.info(f"Status: {status} {details}")
        
    def _make_eth_request(self, method: str, params: List = None) -> Optional[Dict]:
        """Make JSON-RPC request to Ethereum provider."""
        if params is None:
            params = []
            
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }
        
        try:
            response = requests.post(
                self.provider_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'result' in result:
                    return result['result']
                else:
                    self.logger.error(f"RPC error: {result.get('error', 'Unknown error')}")
            else:
                self.logger.error(f"HTTP error {response.status_code}: {response.text}")
                
        except Exception as e:
            self.logger.error(f"Request failed: {e}")
            
        return None
        
    def _get_block_by_number(self, block_number: int, full_transactions: bool = True) -> Optional[Dict]:
        """Get block data by number."""
        hex_number = hex(block_number)
        return self._make_eth_request("eth_getBlockByNumber", [hex_number, full_transactions])
        
    def _get_latest_block_number(self) -> Optional[int]:
        """Get the latest block number."""
        result = self._make_eth_request("eth_blockNumber")
        if result:
            return int(result, 16)
        return None
        
    def _get_block_number_by_timestamp(self, target_timestamp: int) -> Optional[int]:
        """Estimate block number for a given timestamp using binary search."""
        latest_block_num = self._get_latest_block_number()
        if not latest_block_num:
            return None
            
        # Binary search for block with closest timestamp
        low, high = 1, latest_block_num
        closest_block = None
        
        while low <= high:
            mid = (low + high) // 2
            block = self._get_block_by_number(mid, False)
            
            if not block or 'timestamp' not in block:
                high = mid - 1
                continue
                
            block_timestamp = int(block['timestamp'], 16)
            
            if block_timestamp <= target_timestamp:
                closest_block = mid
                low = mid + 1
            else:
                high = mid - 1
                
            time.sleep(self.fetch_delay)  # Rate limiting
            
        return closest_block
        
    def _extract_transaction_features(self, transaction: Dict) -> Dict:
        """Extract features from a transaction."""
        return {
            'hash': transaction.get('hash', ''),
            'block_number': int(transaction.get('blockNumber', '0x0'), 16),
            'transaction_index': int(transaction.get('transactionIndex', '0x0'), 16),
            'from_address': transaction.get('from', ''),
            'to_address': transaction.get('to', ''),
            'value': int(transaction.get('value', '0x0'), 16),
            'gas': int(transaction.get('gas', '0x0'), 16),
            'gas_price': int(transaction.get('gasPrice', '0x0'), 16),
            'nonce': int(transaction.get('nonce', '0x0'), 16),
            'input_data_size': len(transaction.get('input', '')) // 2 - 1,  # Subtract '0x'
            'is_contract_creation': transaction.get('to') is None
        }
        
    def _extract_block_features(self, block: Dict) -> Dict:
        """Extract features from a block."""
        return {
            'number': int(block.get('number', '0x0'), 16),
            'timestamp': int(block.get('timestamp', '0x0'), 16),
            'transaction_count': len(block.get('transactions', [])),
            'gas_limit': int(block.get('gasLimit', '0x0'), 16),
            'gas_used': int(block.get('gasUsed', '0x0'), 16),
            'difficulty': int(block.get('difficulty', '0x0'), 16),
            'total_difficulty': int(block.get('totalDifficulty', '0x0'), 16),
            'size': int(block.get('size', '0x0'), 16),
            'miner': block.get('miner', ''),
            'extra_data_size': len(block.get('extraData', '')) // 2 - 1
        }
        
    def _generate_time_intervals(self) -> List[tuple]:
        """Generate time intervals for extraction."""
        intervals = []
        current = self.start_dt
        
        if self.interval_type == 'day':
            delta = timedelta(days=self.interval_length)
        elif self.interval_type == 'hour':
            delta = timedelta(hours=self.interval_length)
        elif self.interval_type == 'minute':
            delta = timedelta(minutes=self.interval_length)
        else:
            raise ValueError(f"Unsupported interval type: {self.interval_type}")
            
        while current < self.end_dt:
            interval_end = min(current + delta, self.end_dt)
            intervals.append((current, interval_end))
            current = interval_end
            
        return intervals
        
    def _extract_interval_data(self, start_time: datetime, end_time: datetime) -> tuple:
        """Extract data for a specific time interval."""
        self.logger.info(f"Processing interval: {start_time} to {end_time}")
        
        # Find block range for this time interval
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())
        
        start_block = self._get_block_number_by_timestamp(start_timestamp)
        end_block = self._get_block_number_by_timestamp(end_timestamp)
        
        if not start_block or not end_block:
            self.logger.error(f"Could not find blocks for interval {start_time} to {end_time}")
            return [], []
            
        self.logger.info(f"Block range: {start_block} to {end_block}")
        
        # Sample blocks within the interval
        total_blocks = end_block - start_block + 1
        if total_blocks <= self.observations_per_interval:
            # Use all blocks if we have fewer than requested observations
            block_numbers = list(range(start_block, end_block + 1))
        else:
            # Sample evenly distributed blocks
            step = total_blocks / self.observations_per_interval
            block_numbers = [start_block + int(i * step) for i in range(self.observations_per_interval)]
            
        # Extract data from sampled blocks
        transactions = []
        validators = []
        
        for i, block_num in enumerate(block_numbers):
            try:
                block = self._get_block_by_number(block_num, True)
                if not block:
                    self.logger.warning(f"Could not fetch block {block_num}")
                    continue
                    
                # Extract block/validator features
                block_features = self._extract_block_features(block)
                validators.append(block_features)
                
                # Extract transaction features
                for tx in block.get('transactions', []):
                    if isinstance(tx, dict):  # Full transaction object
                        tx_features = self._extract_transaction_features(tx)
                        transactions.append(tx_features)
                        
                # Progress update
                if (i + 1) % 10 == 0:
                    progress = (i + 1) / len(block_numbers) * 100
                    self._update_status("PROCESSING", f"{progress:.1f}% complete")
                    
                time.sleep(self.fetch_delay)  # Rate limiting
                
            except Exception as e:
                self.logger.error(f"Error processing block {block_num}: {e}")
                continue
                
        return transactions, validators
        
    def _save_data(self, interval_start: datetime, transactions: List[Dict], validators: List[Dict]):
        """Save extracted data to CSV files."""
        timestamp_str = interval_start.strftime('%Y%m%d_%H%M%S')
        
        # Save transactions
        if transactions:
            tx_df = pd.DataFrame(transactions)
            tx_file = os.path.join(self.data_directory, f"{timestamp_str}_transactions.csv")
            tx_df.to_csv(tx_file, index=False)
            self.logger.info(f"Saved {len(transactions)} transactions to {tx_file}")
            
        # Save validators/blocks
        if validators:
            val_df = pd.DataFrame(validators)
            val_file = os.path.join(self.data_directory, f"{timestamp_str}_validator_transactions.csv")
            val_df.to_csv(val_file, index=False)
            self.logger.info(f"Saved {len(validators)} validator records to {val_file}")
            
    def run(self):
        """Main extraction process."""
        try:
            self.logger.info("Starting Ethereum feature extraction")
            self._update_status("RUNNING", "Generating time intervals")
            
            # Generate time intervals
            intervals = self._generate_time_intervals()
            self.logger.info(f"Generated {len(intervals)} time intervals")
            
            # Process each interval
            total_transactions = 0
            total_validators = 0
            
            for i, (start_time, end_time) in enumerate(intervals):
                try:
                    self._update_status("PROCESSING", f"Interval {i+1}/{len(intervals)}")
                    
                    # Extract data for this interval
                    transactions, validators = self._extract_interval_data(start_time, end_time)
                    
                    # Save data
                    self._save_data(start_time, transactions, validators)
                    
                    total_transactions += len(transactions)
                    total_validators += len(validators)
                    
                    self.logger.info(f"Completed interval {i+1}/{len(intervals)}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing interval {i+1}: {e}")
                    continue
                    
            # Final status update
            self._update_status("COMPLETED", f"Extracted {total_transactions} transactions, {total_validators} validator records")
            self.logger.info("Extraction completed successfully")
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            self._update_status("FAILED", str(e))
            raise


def main():
    """Main entry point for VM extraction."""
    try:
        print("=" * 60)
        print("Ethereum Feature Extractor - VM Component")
        print("=" * 60)
        
        extractor = EthereumExtractor()
        extractor.run()
        
        print("=" * 60)
        print("Extraction completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Extraction failed: {e}")
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()