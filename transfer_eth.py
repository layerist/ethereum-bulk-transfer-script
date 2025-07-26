import os
import time
import logging
import signal
from typing import List, Tuple, Callable, Any
from web3 import Web3
from web3.exceptions import TransactionNotFound
from web3.types import Wei
from requests.exceptions import Timeout
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------- Logging Setup ----------------------
LOG_FILE = "transfer_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ---------------------- Configuration ----------------------
INFURA_URL = os.getenv("INFURA_URL")
RECIPIENT_ADDRESS = os.getenv("RECIPIENT_ADDRESS")
GAS_PRICE_GWEI = int(os.getenv("GAS_PRICE_GWEI", 0))  # 0 = dynamic
USE_EIP1559 = bool(int(os.getenv("USE_EIP1559", 0)))  # 1 = True
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 10))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", 3))
WALLET_FILE = os.getenv("WALLET_FILE", "wallets.txt")
GAS_BUFFER_MULTIPLIER = float(os.getenv("GAS_BUFFER_MULTIPLIER", 1.1))

# Validate critical configs
if not INFURA_URL or not RECIPIENT_ADDRESS:
    raise EnvironmentError("Missing required ENV variables: INFURA_URL or RECIPIENT_ADDRESS")

# ---------------------- Web3 Initialization ----------------------
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    logging.critical("Unable to connect to Ethereum node. Check INFURA_URL.")
    raise ConnectionError("Web3 connection failed.")

# ---------------------- Utility Functions ----------------------
def load_wallets(file_path: str) -> List[Tuple[str, str]]:
    """Load wallets from a file: 'address,private_key'"""
    wallets: List[Tuple[str, str]] = []
    try:
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(",")
                if len(parts) == 2 and len(parts[1].strip()) > 30:
                    wallets.append((parts[0].strip(), parts[1].strip()))
        if not wallets:
            raise ValueError("Wallet file is empty or invalid.")
        return wallets
    except Exception as e:
        logging.critical(f"Error loading wallets: {e}")
        raise

def get_gas_price() -> Wei:
    """Return gas price in Wei (legacy)"""
    try:
        return web3.to_wei(GAS_PRICE_GWEI, "gwei") if GAS_PRICE_GWEI > 0 else web3.eth.gas_price
    except Exception as e:
        logging.error(f"Failed to get gas price: {e}")
        raise

def get_eip1559_fees() -> dict:
    """Get EIP-1559 fees"""
    try:
        base_fee = web3.eth.fee_history(1, "latest")["baseFeePerGas"][-1]
        priority_fee = web3.to_wei(2, "gwei")  # Adjust as needed
        max_fee = int(base_fee * 2 + priority_fee)
        return {"maxFeePerGas": max_fee, "maxPriorityFeePerGas": priority_fee}
    except Exception as e:
        logging.error(f"Failed to fetch EIP-1559 fees: {e}")
        raise

def estimate_fee(sender_address: str, value: int) -> Tuple[int, Wei]:
    """Estimate gas and total fee with buffer"""
    try:
        tx = {"from": sender_address, "to": RECIPIENT_ADDRESS, "value": value}
        base_gas = web3.eth.estimate_gas(tx)
        gas_limit = max(int(base_gas * GAS_BUFFER_MULTIPLIER), 21000)
        gas_price = get_gas_price()
        return gas_limit, gas_limit * gas_price
    except Exception as e:
        logging.error(f"Gas estimation failed for {sender_address}: {e}")
        raise

def retry_with_backoff(fn: Callable[..., Any], retries: int, *args, **kwargs) -> Any:
    """Retry a function with exponential backoff"""
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < retries:
                wait_time = 2 ** attempt
                logging.warning(f"{fn.__name__} failed (attempt {attempt+1}/{retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"{fn.__name__} failed after {retries} retries: {e}")
                raise

# ---------------------- ETH Transfer ----------------------
def send_eth(wallet_address: str, private_key: str, index: int) -> None:
    """Send all ETH from wallet (leave gas fee)"""
    try:
        balance = retry_with_backoff(web3.eth.get_balance, RETRY_LIMIT, wallet_address)
        if balance == 0:
            logging.info(f"[{index}] {wallet_address} has zero balance, skipping.")
            return

        gas_limit, estimated_fee = retry_with_backoff(estimate_fee, RETRY_LIMIT, wallet_address, 1)
        if balance <= estimated_fee:
            logging.info(f"[{index}] Insufficient funds in {wallet_address}. Balance: {web3.from_wei(balance, 'ether')} ETH, Fee: {web3.from_wei(estimated_fee, 'ether')} ETH")
            return

        value = balance - estimated_fee
        nonce = retry_with_backoff(web3.eth.get_transaction_count, RETRY_LIMIT, wallet_address)

        if USE_EIP1559:
            fee_data = get_eip1559_fees()
            tx = {
                "nonce": nonce,
                "to": RECIPIENT_ADDRESS,
                "value": value,
                "gas": gas_limit,
                "chainId": web3.eth.chain_id,
                **fee_data
            }
        else:
            gas_price = get_gas_price()
            tx = {
                "nonce": nonce,
                "to": RECIPIENT_ADDRESS,
                "value": value,
                "gas": gas_limit,
                "gasPrice": gas_price,
                "chainId": web3.eth.chain_id
            }

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)

        logging.info(f"[{index}] Sent {web3.from_wei(value, 'ether')} ETH from {wallet_address}. TX: {web3.to_hex(tx_hash)}")
    except ValueError as e:
        logging.error(f"[{index}] RPC error for {wallet_address}: {e}")
    except Timeout:
        logging.error(f"[{index}] Timeout while sending from {wallet_address}.")
    except Exception as e:
        logging.exception(f"[{index}] Unexpected error: {e}")

# ---------------------- Wallet Processing ----------------------
def process_wallets(wallets: List[Tuple[str, str]]) -> None:
    """Send ETH from all wallets concurrently"""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_wallet = {executor.submit(send_eth, addr, key, i): (addr, i) for i, (addr, key) in enumerate(wallets)}
        for future in as_completed(future_to_wallet):
            addr, i = future_to_wallet[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"[{i}] Wallet {addr} failed: {e}")

# ---------------------- Graceful Exit ----------------------
def handle_interrupt(sig, frame):
    logging.warning("Script interrupted. Exiting gracefully...")
    exit(0)

signal.signal(signal.SIGINT, handle_interrupt)

# ---------------------- Main ----------------------
if __name__ == "__main__":
    try:
        start = time.time()
        wallets = load_wallets(WALLET_FILE)
        process_wallets(wallets)
        elapsed = time.time() - start
        logging.info(f"All transfers completed in {elapsed:.2f} seconds.")
    except Exception as e:
        logging.critical(f"Fatal error: {e}")
