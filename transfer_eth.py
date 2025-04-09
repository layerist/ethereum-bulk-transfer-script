import os
import time
import logging
from web3 import Web3
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import Timeout
from typing import List, Tuple

# Configure logging
logging.basicConfig(
    filename='transfer_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants and configuration
INFURA_URL = os.getenv('INFURA_URL', 'YOUR_INFURA_URL')
RECIPIENT_ADDRESS = os.getenv('RECIPIENT_ADDRESS', 'RECIPIENT_ETH_ADDRESS')
GAS_PRICE_GWEI = int(os.getenv('GAS_PRICE_GWEI', 0))  # 0 means use dynamic price
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 10))
RETRY_LIMIT = int(os.getenv('RETRY_LIMIT', 3))
WALLET_FILE = os.getenv('WALLET_FILE', 'wallets.txt')

# Initialize Web3
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    logging.critical("Failed to connect to the Ethereum network.")
    raise ConnectionError("Unable to connect to the Ethereum network.")

def load_wallets(file_path: str) -> List[Tuple[str, str]]:
    """Load wallet addresses and private keys from a file."""
    try:
        with open(file_path, 'r') as file:
            wallets = [tuple(line.strip().split(',')) for line in file if ',' in line]
        if not wallets:
            raise ValueError("Wallet file contains no valid entries.")
        return wallets
    except Exception as e:
        logging.critical(f"Failed to load wallets: {e}")
        raise

def get_gas_price() -> int:
    """Return gas price in Wei (custom or current network)."""
    return web3.to_wei(GAS_PRICE_GWEI, 'gwei') if GAS_PRICE_GWEI > 0 else web3.eth.gas_price

def calculate_fee(gas_limit: int) -> int:
    """Calculate estimated fee for a transaction."""
    return get_gas_price() * gas_limit

def send_eth(wallet_address: str, private_key: str, index: int, retries: int = 0) -> None:
    """Attempt to send ETH from a wallet with exponential backoff."""
    try:
        balance = web3.eth.get_balance(wallet_address)
        gas_price = get_gas_price()

        estimated_gas = web3.eth.estimate_gas({
            'from': wallet_address,
            'to': RECIPIENT_ADDRESS,
            'value': balance // 2
        })
        fee = calculate_fee(estimated_gas)

        if balance <= fee:
            logging.warning(f"[{index}] Skipping {wallet_address} - Insufficient balance. "
                            f"Balance: {web3.from_wei(balance, 'ether')} ETH, Required: {web3.from_wei(fee, 'ether')} ETH")
            return

        value_to_send = balance - fee
        nonce = web3.eth.get_transaction_count(wallet_address)

        tx = {
            'nonce': nonce,
            'to': RECIPIENT_ADDRESS,
            'value': value_to_send,
            'gas': estimated_gas,
            'gasPrice': gas_price,
            'chainId': web3.eth.chain_id
        }

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)

        logging.info(f"[{index}] Sent {web3.from_wei(value_to_send, 'ether')} ETH from {wallet_address}. TX: {web3.to_hex(tx_hash)}")

    except Timeout:
        if retries < RETRY_LIMIT:
            wait = 2 ** retries
            logging.warning(f"[{index}] Timeout for {wallet_address}. Retrying in {wait}s ({retries + 1}/{RETRY_LIMIT})...")
            time.sleep(wait)
            return send_eth(wallet_address, private_key, index, retries + 1)
        logging.error(f"[{index}] Failed after {RETRY_LIMIT} retries: {wallet_address}")
    except ValueError as e:
        logging.error(f"[{index}] Value error for {wallet_address}: {e}")
    except Exception as e:
        logging.error(f"[{index}] Unexpected error for {wallet_address}: {e}")

def process_wallets(wallets: List[Tuple[str, str]]) -> None:
    """Process ETH transfers in parallel using threading."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(send_eth, wallet, key, idx): (wallet, idx)
            for idx, (wallet, key) in enumerate(wallets)
        }
        for future in as_completed(futures):
            wallet, idx = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"[{idx}] Exception processing wallet {wallet}: {e}")

if __name__ == "__main__":
    try:
        start = time.time()
        wallets = load_wallets(WALLET_FILE)
        process_wallets(wallets)
        elapsed = time.time() - start
        logging.info(f"Transfer completed in {elapsed:.2f} seconds.")
    except Exception as e:
        logging.critical(f"Fatal error: {e}")
