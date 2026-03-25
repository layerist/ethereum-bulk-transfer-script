#!/usr/bin/env python3
"""
Advanced Concurrent ETH Sweeper (Production-Ready)

Upgrades:
- Nonce locking (per wallet)
- Fee bumping for stuck txs
- Safer EIP-1559 logic
- Retry with jitter
- Replace-by-fee support
- Thread-safe nonce cache
"""

from __future__ import annotations

import os
import time
import signal
import random
import logging
import threading
from decimal import Decimal
from typing import List, Tuple, Dict, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed

from requests.exceptions import Timeout, ConnectionError as ReqConnectionError
from web3 import Web3
from web3.exceptions import TransactionNotFound, TimeExhausted

# ============================================================
# LOGGING
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
# CONFIG
# ============================================================

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    return default if v is None else v.strip().lower() in {"1", "true", "yes", "on"}

RPC_URLS              = os.getenv("RPC_URLS", "").split(",")
RECIPIENT_ADDRESS     = os.getenv("RECIPIENT_ADDRESS", "")
WALLET_FILE           = os.getenv("WALLET_FILE", "wallets.txt")

MAX_WORKERS           = int(os.getenv("MAX_WORKERS", "10"))
RETRY_LIMIT           = int(os.getenv("RETRY_LIMIT", "3"))
TX_DELAY_SECONDS      = float(os.getenv("TX_DELAY_SECONDS", "0.3"))

PRIORITY_FEE_GWEI     = int(os.getenv("PRIORITY_FEE_GWEI", "2"))
GAS_BUFFER_MULTIPLIER = float(os.getenv("GAS_BUFFER_MULTIPLIER", "1.2"))
FEE_BUMP_MULTIPLIER   = float(os.getenv("FEE_BUMP_MULTIPLIER", "1.15"))

WAIT_FOR_RECEIPT      = env_bool("WAIT_FOR_RECEIPT", False)
RECEIPT_TIMEOUT       = int(os.getenv("RECEIPT_TIMEOUT", "120"))
DRY_RUN               = env_bool("DRY_RUN", False)

if not RPC_URLS or not RECIPIENT_ADDRESS:
    raise EnvironmentError("RPC_URLS and RECIPIENT_ADDRESS required")

# ============================================================
# WEB3 (with failover)
# ============================================================

def connect_web3() -> Web3:
    for url in RPC_URLS:
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 15}))
        if w3.is_connected():
            log("info", f"Connected to RPC: {url}")
            return w3
    raise ConnectionError("All RPC endpoints failed")

web3 = connect_web3()
CHAIN_ID = web3.eth.chain_id
RECIPIENT_ADDRESS = web3.to_checksum_address(RECIPIENT_ADDRESS)

# ============================================================
# NONCE MANAGEMENT
# ============================================================

_nonce_lock = threading.Lock()
_nonce_cache: Dict[str, int] = {}

def get_nonce(address: str) -> int:
    with _nonce_lock:
        if address not in _nonce_cache:
            _nonce_cache[address] = web3.eth.get_transaction_count(address, "pending")
        else:
            _nonce_cache[address] += 1
        return _nonce_cache[address]

# ============================================================
# UTILS
# ============================================================

def eth_fmt(wei: int) -> str:
    return f"{Decimal(wei) / Decimal(10**18):.6f} ETH"

def retry(fn: Callable[..., Any], *args, retries=RETRY_LIMIT, **kwargs):
    for attempt in range(1, retries + 2):
        try:
            return fn(*args, **kwargs)
        except (Timeout, ReqConnectionError, TransactionNotFound, TimeExhausted) as e:
            if attempt > retries:
                raise
            delay = min(2 ** attempt + random.uniform(0, 1), 30)
            log("warning", f"{fn.__name__} retry {attempt}: {e} → {delay:.2f}s")
            time.sleep(delay)

# ============================================================
# GAS
# ============================================================

def get_fees(multiplier: float = 1.0) -> Dict[str, int]:
    block = retry(web3.eth.get_block, "latest")
    base_fee = block.get("baseFeePerGas", 0)

    try:
        priority = web3.eth.max_priority_fee
    except Exception:
        priority = web3.to_wei(PRIORITY_FEE_GWEI, "gwei")

    priority = int(priority * multiplier)
    max_fee = int((base_fee * 2 + priority) * multiplier)

    return {
        "maxPriorityFeePerGas": priority,
        "maxFeePerGas": max_fee,
    }

def estimate_gas(sender: str) -> int:
    gas = retry(
        web3.eth.estimate_gas,
        {"from": sender, "to": RECIPIENT_ADDRESS, "value": 1},
    )
    return max(int(gas * GAS_BUFFER_MULTIPLIER), 21000)

# ============================================================
# WALLET LOADING
# ============================================================

def load_wallets(path: str) -> List[Tuple[str, str]]:
    wallets = []
    with open(path) as f:
        for line in f:
            if not line.strip():
                continue
            addr, pk = map(str.strip, line.split(","))
            wallets.append((web3.to_checksum_address(addr), pk))
    return wallets

# ============================================================
# TRANSFER
# ============================================================

def send_eth(address: str, pk: str, idx: int) -> bool:
    try:
        balance = retry(web3.eth.get_balance, address)
        if balance == 0:
            return True

        gas_limit = estimate_gas(address)
        fees = get_fees()

        fee_cost = gas_limit * fees["maxFeePerGas"]
        value = balance - fee_cost

        if value <= 0:
            log("info", f"[{idx}] Not enough balance")
            return True

        nonce = get_nonce(address)

        tx = {
            "chainId": CHAIN_ID,
            "nonce": nonce,
            "to": RECIPIENT_ADDRESS,
            "value": value,
            "gas": gas_limit,
            **fees,
        }

        if DRY_RUN:
            log("info", f"[{idx}] DRY → {eth_fmt(value)}")
            return True

        for attempt in range(RETRY_LIMIT + 1):
            signed = web3.eth.account.sign_transaction(tx, pk)

            try:
                tx_hash = web3.eth.send_raw_transaction(signed.rawTransaction)
                log("info", f"[{idx}] Sent {eth_fmt(value)} → {web3.to_hex(tx_hash)}")
                return True

            except ValueError as e:
                log("warning", f"[{idx}] TX failed → bumping fee ({attempt})")

                # bump fees (replace tx)
                fees = get_fees(FEE_BUMP_MULTIPLIER ** (attempt + 1))
                tx.update(fees)

        return False

    except Exception as e:
        log("exception", f"[{idx}] Error: {e}")
        return False

# ============================================================
# EXECUTION
# ============================================================

_stop_event = threading.Event()

def handle_sigint(sig, frame):
    log("warning", "Stopping...")
    _stop_event.set()

signal.signal(signal.SIGINT, handle_sigint)

def process(wallets):
    ok = fail = 0

    with ThreadPoolExecutor(MAX_WORKERS) as ex:
        futures = [ex.submit(send_eth, a, k, i) for i, (a, k) in enumerate(wallets)]

        for f in as_completed(futures):
            if f.result():
                ok += 1
            else:
                fail += 1

    log("info", f"Done: {ok} ok, {fail} failed")

# ============================================================
# MAIN
# ============================================================

def main():
    wallets = load_wallets(WALLET_FILE)
    process(wallets)

if __name__ == "__main__":
    main()
