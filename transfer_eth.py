import os
import time
import logging
import signal
import threading
from typing import List, Tuple, Callable, Any, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

from requests.exceptions import Timeout
from web3 import Web3
from web3.exceptions import TransactionNotFound

# ============================================================
#                      LOGGING SETUP
# ============================================================
LOG_FILE = "transfer_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("eth-transfer")

log_lock = threading.Lock()

def ts_log(level: str, msg: str):
    with log_lock:
        getattr(logger, level)(msg)


# ============================================================
#                      CONFIGURATION
# ============================================================
def bool_from_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}

INFURA_URL            = os.getenv("INFURA_URL", "")
RECIPIENT_ADDRESS     = os.getenv("RECIPIENT_ADDRESS", "")
GAS_PRICE_GWEI        = int(os.getenv("GAS_PRICE_GWEI", "0"))
USE_EIP1559           = bool_from_env("USE_EIP1559", True)
MAX_WORKERS           = int(os.getenv("MAX_WORKERS", "10"))
RETRY_LIMIT           = int(os.getenv("RETRY_LIMIT", "3"))
WALLET_FILE           = os.getenv("WALLET_FILE", "wallets.txt")
GAS_BUFFER_MULTIPLIER = float(os.getenv("GAS_BUFFER_MULTIPLIER", "1.15"))
WAIT_FOR_RECEIPT      = bool_from_env("WAIT_FOR_RECEIPT", False)
RECEIPT_TIMEOUT       = int(os.getenv("RECEIPT_TIMEOUT", "120"))
PRIORITY_FEE_GWEI     = int(os.getenv("PRIORITY_FEE_GWEI", "2"))
TX_DELAY_SECONDS      = float(os.getenv("TX_DELAY_SECONDS", "0.5"))
DRY_RUN               = bool_from_env("DRY_RUN", False)

if not INFURA_URL or not RECIPIENT_ADDRESS:
    raise EnvironmentError("Missing environment variables: INFURA_URL or RECIPIENT_ADDRESS")


# ============================================================
#                      WEB3 INITIALIZATION
# ============================================================
web3 = Web3(Web3.HTTPProvider(INFURA_URL))
if not web3.is_connected():
    logger.critical("Failed to connect to Ethereum node.")
    raise ConnectionError("Web3 connection failed")

RECIPIENT_ADDRESS = web3.to_checksum_address(RECIPIENT_ADDRESS)
CHAIN_ID = web3.eth.chain_id
logger.info(f"Connected to Ethereum chain ID: {CHAIN_ID}")


# ============================================================
#                      UTILITIES
# ============================================================
def eth_fmt(wei: int) -> str:
    return f"{web3.from_wei(wei, 'ether'):.6f} ETH"


def load_wallets(file_path: str) -> List[Tuple[str, str]]:
    wallets = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.split(",")]
                if len(parts) != 2:
                    continue

                addr, key = parts
                if web3.is_address(addr) and len(key) > 30:
                    wallets.append((web3.to_checksum_address(addr), key))

        if not wallets:
            raise ValueError("No valid wallets found.")
        return wallets

    except Exception as e:
        logger.critical(f"Error loading wallets: {e}")
        raise


def retry(fn: Callable[..., Any], *args, retries: int = RETRY_LIMIT, **kwargs) -> Any:
    """Generic retry wrapper with exponential backoff."""
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt < retries:
                delay = min(2 ** attempt, 30)
                ts_log("warning", f"{fn.__name__} failed ({attempt+1}/{retries}): {e}. Retrying in {delay}s")
                time.sleep(delay)
            else:
                ts_log("error", f"{fn.__name__} failed after {retries} attempts: {e}")
                raise


def get_legacy_gas_price() -> int:
    if GAS_PRICE_GWEI > 0:
        return web3.to_wei(GAS_PRICE_GWEI, "gwei")
    return web3.eth.gas_price


def get_eip1559_fees() -> Dict[str, int]:
    """Optimized and safe EIP-1559 fee builder."""
    try:
        history = web3.eth.fee_history(5, "latest")
        base_fee = history["baseFeePerGas"][-1]
    except Exception:
        base_fee = web3.eth.gas_price

    try:
        priority_fee = (
            web3.eth.max_priority_fee
            if hasattr(web3.eth, "max_priority_fee")
            else web3.to_wei(PRIORITY_FEE_GWEI, "gwei")
        )
    except Exception:
        priority_fee = web3.to_wei(PRIORITY_FEE_GWEI, "gwei")

    max_fee = int(base_fee * 2 + priority_fee)
    return {"maxFeePerGas": max_fee, "maxPriorityFeePerGas": priority_fee}


def estimate_fee(sender: str) -> Tuple[int, int]:
    """Return (gas_limit, total_fee_wei)."""
    tx = {"from": sender, "to": RECIPIENT_ADDRESS, "value": 1}
    base_gas = web3.eth.estimate_gas(tx)
    gas_limit = max(int(base_gas * GAS_BUFFER_MULTIPLIER), 21_000)

    if USE_EIP1559:
        fees = get_eip1559_fees()
        return gas_limit, gas_limit * fees["maxFeePerGas"]

    gp = get_legacy_gas_price()
    return gas_limit, gas_limit * gp


# ============================================================
#                      ETH TRANSFER
# ============================================================
def send_eth(address: str, private_key: str, index: int) -> None:
    ts_log("info", f"[{index}] Processing {address}...")

    try:
        balance = retry(web3.eth.get_balance, address)
        if balance == 0:
            ts_log("info", f"[{index}] {address} has zero balance.")
            return

        gas_limit, est_fee = retry(estimate_fee, address)
        if balance <= est_fee:
            ts_log("info", f"[{index}] Insufficient funds. Bal={eth_fmt(balance)}, Fee={eth_fmt(est_fee)}")
            return

        value = balance - est_fee
        nonce = retry(web3.eth.get_transaction_count, address)

        tx: Dict[str, Any] = {
            "nonce": nonce,
            "to": RECIPIENT_ADDRESS,
            "value": value,
            "gas": gas_limit,
            "chainId": CHAIN_ID,
        }

        tx.update(get_eip1559_fees() if USE_EIP1559 else {"gasPrice": get_legacy_gas_price()})

        if DRY_RUN:
            ts_log("info", f"[{index}] DRY RUN: would send {eth_fmt(value)}")
            return

        signed = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = web3.eth.send_raw_transaction(signed.rawTransaction)
        tx_hex = web3.to_hex(tx_hash)

        ts_log("info", f"[{index}] Sent {eth_fmt(value)}. TX={tx_hex}")

        if WAIT_FOR_RECEIPT:
            try:
                receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=RECEIPT_TIMEOUT)
                status = "SUCCESS" if receipt["status"] == 1 else "FAILED"
                ts_log("info", f"[{index}] TX {tx_hex} confirmed: {status}")
            except Exception as e:
                ts_log("error", f"[{index}] Receipt wait failed for {tx_hex}: {e}")

        time.sleep(TX_DELAY_SECONDS)

    except (ValueError, Timeout) as e:
        ts_log("error", f"[{index}] RPC/Timeout error: {e}")
    except Exception as e:
        ts_log("exception", f"[{index}] Unexpected error: {e}")


# ============================================================
#                      CONCURRENT EXECUTION
# ============================================================
def process_wallets(wallets: List[Tuple[str, str]]) -> None:
    total = len(wallets)
    ts_log("info", f"Processing {total} wallets using {MAX_WORKERS} threads...")

    success = 0
    failed = 0
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(send_eth, addr, key, i): i for i, (addr, key) in enumerate(wallets)}

        for future in as_completed(futures):
            try:
                future.result()
                with lock:
                    success += 1
            except Exception:
                with lock:
                    failed += 1

    ts_log("info", f"Completed: {success} succeeded, {failed} failed.")


# ============================================================
#                      GRACEFUL EXIT
# ============================================================
def handle_interrupt(sig, frame):
    logger.warning("Interrupted. Stopping...")
    raise SystemExit(0)

signal.signal(signal.SIGINT, handle_interrupt)


# ============================================================
#                      MAIN
# ============================================================
def main() -> None:
    try:
        start = time.time()
        wallets = load_wallets(WALLET_FILE)
        process_wallets(wallets)
        logger.info(f"Finished in {time.time() - start:.2f} seconds.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise


if __name__ == "__main__":
    main()
