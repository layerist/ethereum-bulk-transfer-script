import os
import time
import logging
from web3 import Web3
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import Timeout
from typing import List, Tuple, Callable

# Setup logging
logging.basicConfig(
    filename='transfer_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuration
INFURA_URL = os.getenv('INFURA_URL', 'YOUR_INFURA_URL')
RECIPIENT_ADDRESS = os.getenv('RECIPIENT_ADDRESS', 'RECIPIENT_ETH_ADDRESS')
GAS_PRICE_GWEI = int(os.getenv('GAS_PRICE_GWEI', 0))  # 0 = dynamic
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 10))
RETRY_LIMIT = int(os.getenv('RETRY_LIMIT', 3))
WALLET_FILE = os.getenv('WALLET_FILE', 'wallets.txt')
GAS_BUFFER_MULTIPLIER = float(os.getenv('GAS_BUFFER_MULTIPLIER', 1.1))  # Safety margin

# Initialize Web3
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    logging.critical("Failed to connect to the Ethereum network.")
    raise ConnectionError("Web3 connection failed.")

def load_wallets(file_path: str) -> List[Tuple[str, str]]:
    """Load wallets from file as (address, private_key) pairs."""
    try:
        with open(file_path, 'r') as f:
            wallets = [tuple(line.strip().split(',')) for line in f if ',' in line]
        if not wallets:
            raise ValueError("Wallet file is empty or malformed.")
        return wallets
    except Exception as e:
        logging.critical(f"Failed to load wallets: {e}")
        raise

def get_gas_price() -> int:
    """Return gas price in Wei, using dynamic or fixed value."""
    return web3.to_wei(GAS_PRICE_GWEI, 'gwei') if GAS_PRICE_GWEI > 0 else web3.eth.gas_price

def estimate_fee(address: str, value: int) -> Tuple[int, int]:
    """Estimate transaction gas limit and fee with buffer."""
    tx = {
        'from': address,
        'to': RECIPIENT_ADDRESS,
        'value': value,
    }
    gas_limit = int(web3.eth.estimate_gas(tx) * GAS_BUFFER_MULTIPLIER)
    gas_price = get_gas_price()
    return gas_limit, gas_limit * gas_price

def retry_with_backoff(fn: Callable, retries: int, *args, **kwargs):
    """Retry a function with exponential backoff."""
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < retries:
                wait = 2 ** attempt
                logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                logging.error(f"Exceeded retry limit for function {fn.__name__}: {e}")
                raise

def send_eth(wallet_address: str, private_key: str, index: int) -> None:
    """Send ETH from wallet to recipient, accounting for gas fee."""
    try:
        balance = web3.eth.get_balance(wallet_address)
        if balance == 0:
            logging.warning(f"[{index}] Wallet {wallet_address} has zero balance.")
            return

        gas_limit, fee = retry_with_backoff(estimate_fee, RETRY_LIMIT, wallet_address, 1)

        if balance <= fee:
            logging.warning(f"[{index}] Insufficient balance in {wallet_address}. Balance: {web3.from_wei(balance, 'ether')} ETH, Fee: {web3.from_wei(fee, 'ether')} ETH")
            return

        value = balance - fee
        nonce = web3.eth.get_transaction_count(wallet_address)
        gas_price = get_gas_price()

        tx = {
            'nonce': nonce,
            'to': RECIPIENT_ADDRESS,
            'value': value,
            'gas': gas_limit,
            'gasPrice': gas_price,
            'chainId': web3.eth.chain_id
        }

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)

        logging.info(f"[{index}] Sent {web3.from_wei(value, 'ether')} ETH from {wallet_address}. TX: {web3.to_hex(tx_hash)}")

    except ValueError as e:
        logging.error(f"[{index}] ValueError from {wallet_address}: {e}")
    except Timeout:
        logging.error(f"[{index}] Timeout while processing {wallet_address}.")
    except Exception as e:
        logging.error(f"[{index}] Unexpected error for {wallet_address}: {e}")

def process_wallets(wallets: List[Tuple[str, str]]) -> None:
    """Send ETH from all wallets concurrently."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(send_eth, address, key, i): (address, i)
            for i, (address, key) in enumerate(wallets)
        }
        for future in as_completed(futures):
            addr, idx = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"[{idx}] Failed to process {addr}: {e}")

if __name__ == "__main__":
    try:
        start = time.time()
        wallets = load_wallets(WALLET_FILE)
        process_wallets(wallets)
        duration = time.time() - start
        logging.info(f"Transfers completed in {duration:.2f} seconds.")
    except Exception as e:
        logging.critical(f"Script terminated due to error: {e}")
