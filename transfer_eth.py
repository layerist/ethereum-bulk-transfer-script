import os
import time
import logging
import signal
from typing import List, Tuple, Callable, Any, Dict
from web3 import Web3
from web3.exceptions import TransactionNotFound
from web3.types import Wei, TxReceipt
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
def bool_from_env(name: str, default: bool = False) -> bool:
    """Parse environment variable as boolean (0/1, true/false)."""
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}

INFURA_URL: str = os.getenv("INFURA_URL", "")
RECIPIENT_ADDRESS: str = os.getenv("RECIPIENT_ADDRESS", "")
GAS_PRICE_GWEI: int = int(os.getenv("GAS_PRICE_GWEI", "0"))
USE_EIP1559: bool = bool_from_env("USE_EIP1559", False)
MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "10"))
RETRY_LIMIT: int = int(os.getenv("RETRY_LIMIT", "3"))
WALLET_FILE: str = os.getenv("WALLET_FILE", "wallets.txt")
GAS_BUFFER_MULTIPLIER: float = float(os.getenv("GAS_BUFFER_MULTIPLIER", "1.1"))
WAIT_FOR_RECEIPT: bool = bool_from_env("WAIT_FOR_RECEIPT", False)
RECEIPT_TIMEOUT: int = int(os.getenv("RECEIPT_TIMEOUT", "120"))

# Validate configs
if not INFURA_URL or not RECIPIENT_ADDRESS:
    raise EnvironmentError("Missing required ENV variables: INFURA_URL or RECIPIENT_ADDRESS")

# ---------------------- Web3 Initialization ----------------------
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    logging.critical("Unable to connect to Ethereum node. Check INFURA_URL.")
    raise ConnectionError("Web3 connection failed.")

# ---------------------- Utility Functions ----------------------
def eth_fmt(wei: int) -> str:
    """Format Wei as ETH string."""
    return f"{web3.from_wei(wei, 'ether'):.6f} ETH"

def load_wallets(file_path: str) -> List[Tuple[str, str]]:
    """Load wallets from a file: 'address,private_key'"""
    wallets: List[Tuple[str, str]] = []
    try:
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.split(",")]
                if len(parts) == 2 and len(parts[1]) > 30:
                    wallets.append((parts[0], parts[1]))
        if not wallets:
            raise ValueError("Wallet file is empty or invalid.")
        return wallets
    except Exception as e:
        logging.critical(f"Error loading wallets: {e}")
        raise

def get_gas_price() -> Wei:
    """Return gas price in Wei (legacy)."""
    return web3.to_wei(GAS_PRICE_GWEI, "gwei") if GAS_PRICE_GWEI > 0 else web3.eth.gas_price

def get_eip1559_fees() -> Dict[str, Wei]:
    """Get EIP-1559 fees."""
    base_fee = web3.eth.fee_history(1, "latest")["baseFeePerGas"][-1]
    priority_fee = web3.to_wei(2, "gwei")  # tweak as needed
    max_fee = int(base_fee * 2 + priority_fee)
    return {"maxFeePerGas": max_fee, "maxPriorityFeePerGas": priority_fee}

def estimate_fee(sender_address: str) -> Tuple[int, Wei]:
    """Estimate gas limit and total fee with buffer."""
    tx = {"from": sender_address, "to": RECIPIENT_ADDRESS, "value": 1}
    base_gas = web3.eth.estimate_gas(tx)
    gas_limit = max(int(base_gas * GAS_BUFFER_MULTIPLIER), 21000)
    gas_price = get_gas_price()
    return gas_limit, gas_limit * gas_price

def retry_with_backoff(fn: Callable[..., Any], retries: int, *args, **kwargs) -> Any:
    """Retry a function with exponential backoff (2^n seconds)."""
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < retries:
                wait_time = min(2 ** attempt, 30)  # cap at 30s
                logging.warning(
                    f"{fn.__name__} failed (attempt {attempt+1}/{retries}): {e}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                logging.error(f"{fn.__name__} failed after {retries} retries: {e}")
                raise

# ---------------------- ETH Transfer ----------------------
def send_eth(wallet_address: str, private_key: str, index: int) -> None:
    """Send all ETH from wallet (leave gas fee)."""
    try:
        balance = retry_with_backoff(web3.eth.get_balance, RETRY_LIMIT, wallet_address)
        if balance == 0:
            logging.info(f"[{index}] {wallet_address} has zero balance, skipping.")
            return

        gas_limit, estimated_fee = retry_with_backoff(estimate_fee, RETRY_LIMIT, wallet_address)
        if balance <= estimated_fee:
            logging.info(f"[{index}] Insufficient funds in {wallet_address}. "
                         f"Balance: {eth_fmt(balance)}, Fee: {eth_fmt(estimated_fee)}")
            return

        value = balance - estimated_fee
        nonce = retry_with_backoff(web3.eth.get_transaction_count, RETRY_LIMIT, wallet_address)

        if USE_EIP1559:
            tx = {
                "nonce": nonce,
                "to": RECIPIENT_ADDRESS,
                "value": value,
                "gas": gas_limit,
                "chainId": web3.eth.chain_id,
                **get_eip1559_fees()
            }
        else:
            tx = {
                "nonce": nonce,
                "to": RECIPIENT_ADDRESS,
                "value": value,
                "gas": gas_limit,
                "gasPrice": get_gas_price(),
                "chainId": web3.eth.chain_id
            }

        signed_tx = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        tx_hex = web3.to_hex(tx_hash)

        logging.info(f"[{index}] Sent {eth_fmt(value)} from {wallet_address}. TX: {tx_hex}")

        if WAIT_FOR_RECEIPT:
            try:
                receipt: TxReceipt = web3.eth.wait_for_transaction_receipt(
                    tx_hash, timeout=RECEIPT_TIMEOUT
                )
                status = "Success" if receipt["status"] == 1 else "Failed"
                logging.info(f"[{index}] TX {tx_hex} confirmed. Status: {status}")
            except Exception as e:
                logging.error(f"[{index}] Failed waiting for TX {tx_hex} receipt: {e}")

    except (ValueError, Timeout) as e:
        logging.error(f"[{index}] RPC/Timeout error for {wallet_address}: {e}")
    except Exception as e:
        logging.exception(f"[{index}] Unexpected error: {e}")

# ---------------------- Wallet Processing ----------------------
def process_wallets(wallets: List[Tuple[str, str]]) -> None:
    """Send ETH from all wallets concurrently."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_wallet = {
            executor.submit(send_eth, addr, key, i): (addr, i)
            for i, (addr, key) in enumerate(wallets)
        }
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
