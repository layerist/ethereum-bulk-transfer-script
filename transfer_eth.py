#!/usr/bin/env python3
"""
Concurrent ETH sweeper with EIP-1559 support.

Design goals:
- RPC resilience
- Deterministic behavior
- Clear observability
- Safe concurrent execution
"""

from __future__ import annotations

import os
import time
import signal
import logging
import threading
from typing import List, Tuple, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from requests.exceptions import Timeout
from web3 import Web3
from web3.exceptions import TransactionNotFound


# ============================================================
#                         LOGGING
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

logger = logging.getLogger("eth-sweeper")
_log_lock = threading.Lock()


def log(level: str, msg: str) -> None:
    with _log_lock:
        getattr(logger, level)(msg)


# ============================================================
#                         CONFIG
# ============================================================
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    return default if v is None else v.strip().lower() in {"1", "true", "yes", "on"}


INFURA_URL            = os.getenv("INFURA_URL", "")
RECIPIENT_ADDRESS     = os.getenv("RECIPIENT_ADDRESS", "")
WALLET_FILE           = os.getenv("WALLET_FILE", "wallets.txt")

MAX_WORKERS           = int(os.getenv("MAX_WORKERS", "10"))
RETRY_LIMIT           = int(os.getenv("RETRY_LIMIT", "3"))
TX_DELAY_SECONDS      = float(os.getenv("TX_DELAY_SECONDS", "0.5"))

USE_EIP1559           = env_bool("USE_EIP1559", True)
PRIORITY_FEE_GWEI     = int(os.getenv("PRIORITY_FEE_GWEI", "2"))
GAS_PRICE_GWEI        = int(os.getenv("GAS_PRICE_GWEI", "0"))
GAS_BUFFER_MULTIPLIER = float(os.getenv("GAS_BUFFER_MULTIPLIER", "1.15"))

WAIT_FOR_RECEIPT      = env_bool("WAIT_FOR_RECEIPT", False)
RECEIPT_TIMEOUT       = int(os.getenv("RECEIPT_TIMEOUT", "120"))
DRY_RUN               = env_bool("DRY_RUN", False)

if not INFURA_URL or not RECIPIENT_ADDRESS:
    raise EnvironmentError("INFURA_URL and RECIPIENT_ADDRESS must be set")


# ============================================================
#                         WEB3
# ============================================================
web3 = Web3(Web3.HTTPProvider(INFURA_URL, request_kwargs={"timeout": 20}))
if not web3.is_connected():
    raise ConnectionError("Failed to connect to Ethereum RPC")

CHAIN_ID = web3.eth.chain_id
RECIPIENT_ADDRESS = web3.to_checksum_address(RECIPIENT_ADDRESS)

logger.info("Connected to chain ID %s", CHAIN_ID)


# ============================================================
#                         UTILS
# ============================================================
def eth_fmt(wei: int) -> str:
    return f"{web3.from_wei(wei, 'ether'):.6f} ETH"


def retry(
    fn: Callable[..., Any],
    *args,
    retries: int = RETRY_LIMIT,
    retry_on: tuple[type, ...] = (Timeout, TransactionNotFound),
    **kwargs,
) -> Any:
    for attempt in range(1, retries + 2):
        try:
            return fn(*args, **kwargs)
        except retry_on as e:
            if attempt > retries:
                raise
            delay = min(2 ** (attempt - 1), 30)
            log("warning", f"{fn.__name__} failed ({attempt}/{retries}): {e} → retry in {delay}s")
            time.sleep(delay)


# ============================================================
#                         WALLETS
# ============================================================
def load_wallets(path: str) -> List[Tuple[str, str]]:
    wallets: List[Tuple[str, str]] = []

    with open(path, "r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            try:
                addr, pk = map(str.strip, line.split(",", 1))
                if not web3.is_address(addr):
                    raise ValueError("Invalid address")
                if not pk.startswith("0x") or len(pk) != 66:
                    raise ValueError("Invalid private key")
                wallets.append((web3.to_checksum_address(addr), pk))
            except Exception as e:
                log("warning", f"Invalid wallet entry at line {ln}: {e}")

    if not wallets:
        raise ValueError("No valid wallets loaded")

    return wallets


# ============================================================
#                         GAS
# ============================================================
def legacy_gas_price() -> int:
    if GAS_PRICE_GWEI > 0:
        return web3.to_wei(GAS_PRICE_GWEI, "gwei")
    return retry(web3.eth.gas_price)


def eip1559_fees() -> Dict[str, int]:
    try:
        block = retry(web3.eth.get_block, "latest")
        base_fee = block["baseFeePerGas"]
    except Exception:
        base_fee = legacy_gas_price()

    try:
        priority = retry(web3.eth.max_priority_fee)
    except Exception:
        priority = web3.to_wei(PRIORITY_FEE_GWEI, "gwei")

    max_fee = int(base_fee * 2 + priority)

    return {
        "maxPriorityFeePerGas": priority,
        "maxFeePerGas": max_fee,
    }


def estimate_fee(sender: str) -> tuple[int, Dict[str, int]]:
    gas_estimate = retry(
        web3.eth.estimate_gas,
        {"from": sender, "to": RECIPIENT_ADDRESS, "value": 1},
    )

    gas_limit = max(int(gas_estimate * GAS_BUFFER_MULTIPLIER), 21_000)

    if USE_EIP1559:
        fees = eip1559_fees()
        return gas_limit, fees

    gp = legacy_gas_price()
    return gas_limit, {"gasPrice": gp}


# ============================================================
#                         TRANSFER
# ============================================================
def send_eth(address: str, private_key: str, idx: int) -> bool:
    try:
        log("info", f"[{idx}] Wallet {address}")

        balance = retry(web3.eth.get_balance, address)
        if balance == 0:
            log("info", f"[{idx}] Zero balance")
            return True

        gas_limit, fee_fields = estimate_fee(address)

        fee_cost = (
            gas_limit * fee_fields["maxFeePerGas"]
            if USE_EIP1559
            else gas_limit * fee_fields["gasPrice"]
        )

        if balance <= fee_cost:
            log("info", f"[{idx}] Insufficient balance ({eth_fmt(balance)})")
            return True

        nonce = retry(web3.eth.get_transaction_count, address, "pending")
        value = balance - fee_cost

        tx: Dict[str, Any] = {
            "chainId": CHAIN_ID,
            "nonce": nonce,
            "to": RECIPIENT_ADDRESS,
            "value": value,
            "gas": gas_limit,
            **fee_fields,
        }

        if DRY_RUN:
            log("info", f"[{idx}] DRY RUN → {eth_fmt(value)}")
            return True

        signed = web3.eth.account.sign_transaction(tx, private_key)
        tx_hash = retry(web3.eth.send_raw_transaction, signed.rawTransaction)
        tx_hex = web3.to_hex(tx_hash)

        log("info", f"[{idx}] Sent {eth_fmt(value)} → {tx_hex}")

        if WAIT_FOR_RECEIPT:
            receipt = retry(
                web3.eth.wait_for_transaction_receipt,
                tx_hash,
                timeout=RECEIPT_TIMEOUT,
            )
            status = "SUCCESS" if receipt["status"] == 1 else "FAILED"
            log("info", f"[{idx}] Receipt {status}")

        time.sleep(TX_DELAY_SECONDS)
        return True

    except (Timeout, TransactionNotFound) as e:
        log("error", f"[{idx}] RPC error: {e}")
    except ValueError as e:
        log("error", f"[{idx}] TX rejected: {e}")
    except Exception as e:
        log("exception", f"[{idx}] Unexpected error: {e}")

    return False


# ============================================================
#                         EXECUTION
# ============================================================
_stop_event = threading.Event()


def handle_sigint(sig, frame):
    logger.warning("SIGINT received → stopping new submissions")
    _stop_event.set()


signal.signal(signal.SIGINT, handle_sigint)


def process_wallets(wallets: List[Tuple[str, str]]) -> None:
    log("info", f"Processing {len(wallets)} wallets with {MAX_WORKERS} workers")

    ok = failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            pool.submit(send_eth, addr, pk, i)
            for i, (addr, pk) in enumerate(wallets)
            if not _stop_event.is_set()
        ]

        for f in as_completed(futures):
            if f.result():
                ok += 1
            else:
                failed += 1

    log("info", f"Completed: {ok} success, {failed} failed")


# ============================================================
#                         MAIN
# ============================================================
def main() -> None:
    start = time.time()
    wallets = load_wallets(WALLET_FILE)
    process_wallets(wallets)
    logger.info("Finished in %.2fs", time.time() - start)


if __name__ == "__main__":
    main()
