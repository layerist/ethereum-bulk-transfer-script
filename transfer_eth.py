#!/usr/bin/env python3
"""
Concurrent ETH sweeper with EIP-1559 support.
Designed for safety, observability, and RPC resilience.
"""

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
#                      LOGGING
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
_log_lock = threading.Lock()


def ts_log(level: str, msg: str) -> None:
    with _log_lock:
        getattr(logger, level)(msg)


# ============================================================
#                      CONFIG
# ============================================================
def bool_from_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    return default if val is None else val.strip().lower() in {"1", "true", "yes", "on"}


INFURA_URL            = os.getenv("INFURA_URL", "")
RECIPIENT_ADDRESS     = os.getenv("RECIPIENT_ADDRESS", "")
GAS_PRICE_GWEI        = int(os.getenv("GAS_PRICE_GWEI", "0"))
PRIORITY_FEE_GWEI     = int(os.getenv("PRIORITY_FEE_GWEI", "2"))
USE_EIP1559           = bool_from_env("USE_EIP1559", True)
MAX_WORKERS           = int(os.getenv("MAX_WORKERS", "10"))
RETRY_LIMIT           = int(os.getenv("RETRY_LIMIT", "3"))
WALLET_FILE           = os.getenv("WALLET_FILE", "wallets.txt")
GAS_BUFFER_MULTIPLIER = float(os.getenv("GAS_BUFFER_MULTIPLIER", "1.15"))
WAIT_FOR_RECEIPT      = bool_from_env("WAIT_FOR_RECEIPT", False)
RECEIPT_TIMEOUT       = int(os.getenv("RECEIPT_TIMEOUT", "120"))
TX_DELAY_SECONDS      = float(os.getenv("TX_DELAY_SECONDS", "0.5"))
DRY_RUN               = bool_from_env("DRY_RUN", False)

if not INFURA_URL or not RECIPIENT_ADDRESS:
    raise EnvironmentError("INFURA_URL and RECIPIENT_ADDRESS must be set")


# ============================================================
#                      WEB3
# ============================================================
web3 = Web3(Web3.HTTPProvider(INFURA_URL, request_kwargs={"timeout": 20}))
if not web3.is_connected():
    raise ConnectionError("Failed to connect to Ethereum RPC")

RECIPIENT_ADDRESS = web3.to_checksum_address(RECIPIENT_ADDRESS)
CHAIN_ID = web3.eth.chain_id

logger.info("Connected to chain ID %s", CHAIN_ID)


# ============================================================
#                      HELPERS
# ============================================================
def eth_fmt(wei: int) -> str:
    return f"{web3.from_wei(wei, 'ether'):.6f} ETH"


def retry(fn: Callable[..., Any], *args, retries: int = RETRY_LIMIT, **kwargs) -> Any:
    for attempt in range(retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt >= retries:
                raise
            delay = min(2 ** attempt, 30)
            ts_log("warning", f"{fn.__name__} failed ({attempt+1}/{retries}): {e}, retrying in {delay}s")
            time.sleep(delay)


def load_wallets(path: str) -> List[Tuple[str, str]]:
    wallets: List[Tuple[str, str]] = []

    with open(path, "r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            try:
                addr, key = map(str.strip, line.split(",", 1))
                if not web3.is_address(addr) or len(key) < 32:
                    raise ValueError("Invalid format")
                wallets.append((web3.to_checksum_address(addr), key))
            except Exception:
                ts_log("warning", f"Invalid wallet entry at line {ln}")

    if not wallets:
        raise ValueError("No valid wallets loaded")

    return wallets


# ============================================================
#                      GAS
# ============================================================
def legacy_gas_price() -> int:
    return (
        web3.to_wei(GAS_PRICE_GWEI, "gwei")
        if GAS_PRICE_GWEI > 0
        else retry(web3.eth.gas_price)
    )


def eip1559_fees() -> Dict[str, int]:
    try:
        base_fee = retry(web3.eth.get_block, "latest")["baseFeePerGas"]
    except Exception:
        base_fee = legacy_gas_price()

    try:
        priority = retry(web3.eth.max_priority_fee)
    except Exception:
        priority = web3.to_wei(PRIORITY_FEE_GWEI, "gwei")

    max_fee = int(base_fee * 2 + priority)
    return {
        "maxFeePerGas": max_fee,
        "maxPriorityFeePerGas": priority,
    }


def estimate_total_fee(sender: str) -> Tuple[int, int]:
    base_gas = retry(
        web3.eth.estimate_gas,
        {"from": sender, "to": RECIPIENT_ADDRESS, "value": 1},
    )

    gas_limit = max(int(base_gas * GAS_BUFFER_MULTIPLIER), 21_000)

    if USE_EIP1559:
        fees = eip1559_fees()
        return gas_limit, gas_limit * fees["maxFeePerGas"]

    gp = legacy_gas_price()
    return gas_limit, gas_limit * gp


# ============================================================
#                      TRANSFER
# ============================================================
def send_eth(address: str, private_key: str, index: int) -> bool:
    try:
        ts_log("info", f"[{index}] Processing {address}")

        balance = retry(web3.eth.get_balance, address)
        if balance == 0:
            ts_log("info", f"[{index}] Zero balance")
            return True

        gas_limit, fee = estimate_total_fee(address)
        if balance <= fee:
            ts_log("info", f"[{index}] Insufficient balance: {eth_fmt(balance)}")
            return True

        value = balance - fee
        nonce = retry(web3.eth.get_transaction_count, address, "pending")

        tx: Dict[str, Any] = {
            "chainId": CHAIN_ID,
            "nonce": nonce,
            "to": RECIPIENT_ADDRESS,
            "value": value,
            "gas": gas_limit,
        }

        tx.update(eip1559_fees() if USE_EIP1559 else {"gasPrice": legacy_gas_price()})

        if DRY_RUN:
            ts_log("info", f"[{index}] DRY RUN → {eth_fmt(value)}")
            return True

        signed = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = retry(web3.eth.send_raw_transaction, signed.rawTransaction)
        tx_hex = web3.to_hex(tx_hash)

        ts_log("info", f"[{index}] Sent {eth_fmt(value)} → {tx_hex}")

        if WAIT_FOR_RECEIPT:
            receipt = retry(
                web3.eth.wait_for_transaction_receipt,
                tx_hash,
                timeout=RECEIPT_TIMEOUT,
            )
            status = "SUCCESS" if receipt["status"] == 1 else "FAILED"
            ts_log("info", f"[{index}] Receipt {status}")

        time.sleep(TX_DELAY_SECONDS)
        return True

    except (Timeout, TransactionNotFound, ValueError) as e:
        ts_log("error", f"[{index}] RPC error: {e}")
    except Exception as e:
        ts_log("exception", f"[{index}] Unexpected error: {e}")

    return False


# ============================================================
#                      EXECUTION
# ============================================================
def process_wallets(wallets: List[Tuple[str, str]]) -> None:
    ts_log("info", f"Starting {len(wallets)} wallets with {MAX_WORKERS} threads")

    success = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(send_eth, addr, key, i): i
            for i, (addr, key) in enumerate(wallets)
        }

        for future in as_completed(futures):
            if future.result():
                success += 1
            else:
                failed += 1

    ts_log("info", f"Done: {success} ok, {failed} failed")


# ============================================================
#                      SIGNALS
# ============================================================
_stop_event = threading.Event()


def handle_interrupt(sig, frame):
    logger.warning("SIGINT received, stopping...")
    _stop_event.set()


signal.signal(signal.SIGINT, handle_interrupt)


# ============================================================
#                      MAIN
# ============================================================
def main() -> None:
    start = time.time()
    wallets = load_wallets(WALLET_FILE)
    process_wallets(wallets)
    logger.info("Finished in %.2fs", time.time() - start)


if __name__ == "__main__":
    main()
