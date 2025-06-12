import os
import time
import logging
from typing import List, Tuple, Callable
from web3 import Web3
from requests.exceptions import Timeout
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------- Logging Setup ----------------------
logging.basicConfig(
    filename='transfer_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------- Configuration ----------------------
INFURA_URL = os.getenv('INFURA_URL', 'YOUR_INFURA_URL')
RECIPIENT_ADDRESS = os.getenv('RECIPIENT_ADDRESS', 'RECIPIENT_ETH_ADDRESS')
GAS_PRICE_GWEI = int(os.getenv('GAS_PRICE_GWEI', 0))  # 0 = use dynamic
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 10))
RETRY_LIMIT = int(os.getenv('RETRY_LIMIT', 3))
WALLET_FILE = os.getenv('WALLET_FILE', 'wallets.txt')
GAS_BUFFER_MULTIPLIER = float(os.getenv('GAS_BUFFER_MULTIPLIER', 1.1))  # 10% buffer

# ---------------------- Web3 Initialization ----------------------
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    logging.critical("Unable to connect to Ethereum node.")
    raise ConnectionError("Web3 connection failed.")

# ---------------------- Utility Functions ----------------------
def load_wallets(file_path: str) -> List[Tuple[str, str]]:
    """Load wallets from a file in the format 'address,private_key'."""
    try:
        with open(file_path, 'r') as f:
            wallets = [tuple(line.strip().split(',')) for line in f if ',' in line]
        if not wallets:
            raise ValueError("Wallet file is empty or invalid format.")
        return wallets
    except Exception as e:
        logging.critical(f"Error loading wallets: {e}")
        raise

def get_gas_price() -> int:
    """Fetch current gas price in wei, or use fixed value."""
    try:
        return web3.to_wei(GAS_PRICE_GWEI, 'gwei') if GAS_PRICE_GWEI > 0 else web3.eth.gas_price
    except Exception as e:
        logging.error(f"Failed to get gas price: {e}")
        raise

def estimate_fee(address: str, value: int) -> Tuple[int, int]:
    """Estimate gas limit and total fee with a buffer."""
    try:
        tx = {
            'from': address,
            'to': RECIPIENT_ADDRESS,
            'value': value,
        }
        gas_limit = int(web3.eth.estimate_gas(tx) * GAS_BUFFER_MULTIPLIER)
        gas_price = get_gas_price()
        return gas_limit, gas_limit * gas_price
    except Exception as e:
        logging.error(f"Gas estimation failed for {address}: {e}")
        raise

def retry_with_backoff(fn: Callable, retries: int, *args, **kwargs):
    """Retry a function with exponential backoff on failure."""
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < retries:
                wait = 2 ** attempt
                logging.warning(f"{fn.__name__} failed (attempt {attempt + 1}/{retries}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                logging.error(f"{fn.__name__} failed after {retries} retries: {e}")
                raise

# ---------------------- ETH Transfer Function ----------------------
def send_eth(wallet_address: str, private_key: str, index: int) -> None:
    """Transfer all ETH from a wallet minus gas fee."""
    try:
        balance = retry_with_backoff(web3.eth.get_balance, RETRY_LIMIT, wallet_address)
        if balance == 0:
            logging.warning(f"[{index}] Wallet {wallet_address} has zero balance.")
            return

        gas_limit, fee = retry_with_backoff(estimate_fee, RETRY_LIMIT, wallet_address, 1)
        if balance <= fee:
            logging.warning(f"[{index}] Insufficient balance in {wallet_address}. Balance: {web3.from_wei(balance, 'ether')} ETH, Estimated fee: {web3.from_wei(fee, 'ether')} ETH.")
            return

        value = balance - fee
        nonce = retry_with_backoff(web3.eth.get_transaction_count, RETRY_LIMIT, wallet_address)
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

        logging.info(
            f"[{index}] Sent {web3.from_wei(value, 'ether')} ETH from {wallet_address} "
            f"with gas {gas_limit} at {web3.from_wei(gas_price, 'gwei')} gwei. "
            f"Transaction hash: {web3.to_hex(tx_hash)}"
        )

    except ValueError as e:
        logging.error(f"[{index}] ValueError in wallet {wallet_address}: {e}")
    except Timeout:
        logging.error(f"[{index}] Timeout while sending from {wallet_address}.")
    except Exception as e:
        logging.error(f"[{index}] Unexpected error in {wallet_address}: {e}")

# ---------------------- Wallet Processing ----------------------
def process_wallets(wallets: List[Tuple[str, str]]) -> None:
    """Transfer ETH from multiple wallets concurrently."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(send_eth, address, key, i): (address, i)
            for i, (address, key) in enumerate(wallets)
        }
        for future in as_completed(futures):
            address, idx = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"[{idx}] Failed to process wallet {address}: {e}")

# ---------------------- Main Execution ----------------------
if __name__ == "__main__":
    try:
        start_time = time.time()
        wallets = load_wallets(WALLET_FILE)
        process_wallets(wallets)
        duration = time.time() - start_time
        logging.info(f"All transfers completed in {duration:.2f} seconds.")
    except Exception as e:
        logging.critical(f"Script terminated due to error: {e}")
