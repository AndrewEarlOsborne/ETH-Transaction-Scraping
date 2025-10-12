#!/usr/bin/env python3.11
"""
Ethereum Feature Extractor
=========================================

Minimal extraction script that runs independently on VMs.
Designed to run until completion in a screen session.
"""

import os
import sys
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv
from tqdm import tqdm


class EthereumExtractor:
    """Minimal Ethereum data extractor for VM deployment."""
    
    def __init__(self):
        """Initialize extractor with configuration."""
        self._setup_logging()
        self._load_config('.env')
        self._setup_data_directory()
        self._create_status_file()
        
    def _setup_logging(self):
        """Configure logging for VM environment."""
        # Configure file logging with timestamps
        file_handler = logging.FileHandler("extraction.log")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # Configure console logging without timestamps for clean CLI output
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(message)s'))
        
        # Set up logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Prevent duplicate logs
        self.logger.propagate = False
        
    def _load_config(self, config_file: str):
        """Load configuration from environment file."""
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file {config_file} not found")
            
        load_dotenv(config_file)
        
        # Required configurations
        required = {
            'ETHEREUM_PROVIDER_URL': os.getenv('ETHEREUM_PROVIDER_URL'),
            'INTERVAL_START': os.getenv('INTERVAL_START'),
            'INTERVAL_END': os.getenv('INTERVAL_END'),
        }
        
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing required config: {missing}")
            
        self.provider_url = required['ETHEREUM_PROVIDER_URL']
        self.interval_start = required['INTERVAL_START']
        self.interval_end = required['INTERVAL_END']
        
        # Optional configurations with defaults
        self.observations_per_interval = int(os.getenv('OBSERVATIONS_PER_INTERVAL', '100'))
        self.fetch_delay = float(os.getenv('PROVIDER_FETCH_DELAY_SECONDS', '0.07'))
        self.interval_type = os.getenv('INTERVAL_SPAN_TYPE', 'day')
        self.interval_length = float(os.getenv('INTERVAL_SPAN_LENGTH', '1.0'))
        self.data_directory = os.getenv('DATA_DIRECTORY', 'data')
        
        # ETH 2.0 deposit contract address (mainnet)
        self.eth2_deposit_contract = '0x00000000219ab540356cBB839Cbe05303d7705Fa'
        
        # Parse dates
        self.start_dt = datetime.strptime(self.interval_start, '%Y-%m-%d-%H:%M')
        self.end_dt = datetime.strptime(self.interval_end, '%Y-%m-%d-%H:%M')
        
        # These will be shown in the main() header instead
        
    def _setup_data_directory(self):
        """Create data directory structure."""
        os.makedirs(self.data_directory, exist_ok=True)
        # Data directory info will be shown in main() header
        
    def _create_status_file(self):
        """Create status file for monitoring."""
        self.status_file = "status.txt"
        self._update_status("STARTED")
        
    def _update_status(self, status: str, details: str = ""):
        """Update status file."""
        try:
            timestamp = datetime.now().isoformat()
            status_info = f"{timestamp} - {status}"
            if details:
                status_info += f" - {details}"

            with open(self.status_file, 'w') as f:
                f.write(status_info + "\n")
                f.flush()  # Ensure immediate write
        except Exception as e:
            self.logger.error(f"Failed to update status: {e}")

        # Only log status changes to file, not console
        pass
        
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
        }

    def _is_validator_transaction(self, transaction: Dict) -> bool:
        """Check if transaction is sent to ETH 2.0 deposit contract with 32+ ETH."""
        to_address = transaction.get('to')
        if not to_address or to_address.lower() != self.eth2_deposit_contract.lower():
            return False

        # Check if transaction value is at least 32 ETH (32 * 10^18 wei)
        value = transaction.get('value', '0x0')
        if isinstance(value, str):
            value_wei = int(value, 16)
        else:
            value_wei = int(value) if value else 0

        min_validator_amount = 32 * 10**18  # 32 ETH in wei
        return value_wei >= min_validator_amount
        
        
    def _normalize_to_interval_boundary(self, dt: datetime) -> datetime:
        """Normalize datetime to the interval boundary based on interval_type."""
        if self.interval_type == 'day':
            # Round down to start of day
            normalized = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif self.interval_type == 'hour':
            # Round down to start of hour
            normalized = dt.replace(minute=0, second=0, microsecond=0)
        elif self.interval_type == 'minute':
            # Round down to start of minute
            normalized = dt.replace(second=0, microsecond=0)
        else:
            # No normalization for unsupported types
            normalized = dt

        return normalized

    def _generate_time_intervals(self) -> List[tuple]:
        """Generate time intervals for extraction."""
        intervals = []

        # Normalize start and end times to interval boundaries
        current = self._normalize_to_interval_boundary(self.start_dt)
        end = self._normalize_to_interval_boundary(self.end_dt)

        # Log if adjustments were made
        if current != self.start_dt:
            self.logger.info(f"Adjusted start time from {self.start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {current.strftime('%Y-%m-%d %H:%M:%S')} (interval boundary)")
        if end != self.end_dt:
            self.logger.info(f"Adjusted end time from {self.end_dt.strftime('%Y-%m-%d %H:%M:%S')} to {end.strftime('%Y-%m-%d %H:%M:%S')} (interval boundary)")

        if self.interval_type == 'day':
            delta = timedelta(days=self.interval_length)
        elif self.interval_type == 'hour':
            delta = timedelta(hours=self.interval_length)
        elif self.interval_type == 'minute':
            delta = timedelta(minutes=self.interval_length)
        else:
            raise ValueError(f"Unsupported interval type: {self.interval_type}")

        while current < end:
            interval_end = min(current + delta, end)
            intervals.append((current, interval_end))
            current = interval_end

        return intervals
        
    def _extract_interval_data(self, start_time: datetime, end_time: datetime) -> tuple:
        """Extract data for a specific time interval."""

        # Find block range for this time interval
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        start_block = self._get_block_number_by_timestamp(start_timestamp)
        end_block = self._get_block_number_by_timestamp(end_timestamp)

        if not start_block or not end_block:
            self.logger.error(f"Could not find blocks for interval {start_time} to {end_time}")
            return [], []

        # Process ALL blocks within the interval (no sampling)
        total_blocks = end_block - start_block + 1
        block_numbers = list(range(start_block, end_block + 1))

        self.logger.info(f"Processing {total_blocks} blocks ({start_block} to {end_block}) for interval {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}")
            
        # Extract data from all blocks
        transactions = []
        validators = []
        
        for block_num in block_numbers:
            try:
                block = self._get_block_by_number(block_num, True)
                if not block:
                    continue
                
                # Extract transaction features
                for tx in block.get('transactions', []):
                    if isinstance(tx, dict):  # Full transaction object
                        tx_features = self._extract_transaction_features(tx)
                        transactions.append(tx_features)
                        
                        # Check if this is a validator transaction
                        if self._is_validator_transaction(tx):
                            validators.append(tx_features)
                    
                time.sleep(self.fetch_delay)  # Rate limiting
                
            except Exception:
                continue
                
        return transactions, validators
        
    def _append_to_csv(self, file_path: str, data: Dict):
        """Append a single row of data to CSV file."""
        df = pd.DataFrame([data])
        
        if os.path.exists(file_path):
            df.to_csv(file_path, mode='a', header=False, index=False)
        else:
            df.to_csv(file_path, index=False)
            
    def run(self):
        """Main extraction process."""
        try:
            print("\n🚀 Starting extraction...")
            self._update_status("RUNNING", "0/0")
            
            # Generate time intervals
            intervals = self._generate_time_intervals()
            print(f"📊 Processing {len(intervals)} time interval{'s' if len(intervals) != 1 else ''}")
            
            # Set up output file names based on first interval
            if intervals:
                first_interval_str = intervals[0][0].strftime('%Y%m%d_%H%M%S')
                whale_file = os.path.join(self.data_directory, f"{first_interval_str}_whale_transactions.csv")
                validator_file = os.path.join(self.data_directory, f"{first_interval_str}_validator_transactions.csv")
            
            # Process each interval
            total_transactions = 0
            total_validators = 0
            total_intervals = len(intervals)
            
            # Create progress bar for interval processing  
            with tqdm(intervals, desc="Intervals", unit="int",
                      bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                for i, (start_time, end_time) in enumerate(pbar):

                    # Extract data for this interval
                    transactions, validators = self._extract_interval_data(start_time, end_time)
                    
                    # Aggregate whale transactions into a summary
                    whale_result = self.summarize_whale_transactions(transactions)
                    whale_result['interval_start'] = start_time.isoformat()
                    whale_result['interval_end'] = end_time.isoformat()

                    # Aggregate validator transactions
                    validators_result = self.summarize_validator_transactions(validators)
                    validators_result['interval_start'] = start_time.isoformat()
                    validators_result['interval_end'] = end_time.isoformat()
                    
                    # Append aggregated data to CSV files
                    if whale_result:
                        self._append_to_csv(whale_file, whale_result)
                    if validators_result:
                        self._append_to_csv(validator_file, validators_result)
                    
                    total_transactions += len(transactions)
                    total_validators += len(validators)
                    
                    # Update status file with interval progress
                    completed_intervals = i + 1
                    self._update_status("RUNNING", f"{completed_intervals}/{total_intervals}")
                    
            
            # Final status update
            if self.check_completed(validator_file):
                self.aggregate_results()
                self._update_status("COMPLETED", f"{completed_intervals}/{total_intervals}")
            else:
                self._update_status("FAILED", "Incomplete extraction")
            print(f"\nExtracted {total_transactions:,} transactions and {total_validators:,} validator records")
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            self._update_status("FAILED", str(e))
            raise

    def summarize_whale_transactions(self, transactions: List[Dict]) -> Dict:
        """Summarize whale transactions across all data. Pass the data as all data for one interval."""
        if not transactions:
            return {}
            
        df = pd.DataFrame(transactions)
        whale_threshold = 1e18  # 1 ETH in wei
        whale_txs = df[df['value'] >= whale_threshold]
        
        if whale_txs.empty:
            return {
                'whale_count': 0,
                'whale_avg_value_eth': 0,
                'whale_total_value_eth': 0
            }
            
        return {
            'whale_count': len(whale_txs),
            'whale_avg_value_eth': whale_txs['value'].mean() / 1e18,
            'whale_total_value_eth': whale_txs['value'].sum() / 1e18
        }

    def summarize_validator_transactions(self, validators: List[Dict]) -> Dict:
        """Summarize validator transaction data."""
        if not validators:
            return {
                'validator_count': 0,
                'validator_total_value_eth': 0,
                'validator_avg_value_eth': 0,
                'validator_avg_gas_price': 0
            }
            
        df = pd.DataFrame(validators)
        return {
            'validator_count': len(df),
            'validator_total_value_eth': df['value'].sum() / 1e18,  # Convert wei to ETH
            'validator_avg_value_eth': df['value'].mean() / 1e18,
            'validator_avg_gas_price': df['gas_price'].mean()
        }

    def check_completed(self, file_path: str) -> bool:
        with open(file_path) as f:
            for line in f:
                pass
            last_line = line.strip()
            # Convert interval_end to ISO format to match CSV format
            expected_end = self.end_dt.isoformat()
            if expected_end in last_line:
                return True
            else:
                self.logger.error(f"Last line does not match expected end interval: {expected_end} :: in :: {last_line.strip()}")
                return True
        return False
    
    def aggregate_results(self):
        """Aggregate all result files into a single combined CSV."""
        validator_results: pd.DataFrame = pd.DataFrame()
        whale_results: pd.DataFrame = pd.DataFrame()

        for file in os.listdir(self.data_directory):
            if file.endswith("validator_transactions.csv"):
                file_path = os.path.join(self.data_directory, file)
                df = pd.read_csv(file_path)
                validator_results = pd.concat([validator_results, df], ignore_index=True)

            elif file.endswith("whale_transactions.csv"):
                file_path = os.path.join(self.data_directory, file)
                df = pd.read_csv(file_path)
                whale_results = pd.concat([whale_results, df], ignore_index=True)

        # Merge results into one
        if not whale_results.empty and not validator_results.empty:
            aggregate_results: pd.DataFrame = pd.merge(
                whale_results,
                validator_results,
                how='inner',
                on=['interval_start', 'interval_end']
            )

            # Save aggregated results
            output_file = os.path.join(self.data_directory, 'aggregated_results.csv')
            aggregate_results.to_csv(output_file, index=False)
            self.logger.info(f"Aggregated results saved to {output_file}")
        else:
            self.logger.warning("No data to aggregate")


def main():
    """Main entry point for VM extraction."""
    try:
        print("=" * 60)
        print("Ethereum Feature Extractor")
        print("=" * 60)
        
        extractor = EthereumExtractor()
        
        # Show configuration summary
        print(f"Period: {extractor.interval_start} to {extractor.interval_end}")
        print(f"Provider: {extractor.provider_url}")
        print(f"Data directory: {os.path.abspath(extractor.data_directory)}")
        
        extractor.run()
        
        print("\n" + "=" * 60)
        print("Extraction completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"Extraction failed: {e}")
        logging.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
