#!/usr/bin/env python3
"""
Production-Grade ETH Batch Payout Service

SAFE DESIGN:
- Single treasury wallet only (ONE private key)
- Batch sends to multiple recipients
- No multi-wallet sweeping
- RPC failover + retry system
- Thread-safe nonce manager
- Dry-run mode
- Robust error handling
"""

from __future__ import annotations

import os
import time
import json
import random
import logging
import threading

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Dict, Optional

from concurrent.futures import ThreadPoolExecutor, as_completed

from web3 import Web3
from web3.exceptions import TransactionNotFound, TimeExhausted
from requests.exceptions import Timeout, ConnectionError

# ============================================================
# CONFIG
# ============================================================

RPC_URLS = [x.strip() for x in os.getenv("RPC_URLS", "").split(",") if x.strip()]
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
TREASURY_ADDRESS = os.getenv("TREASURY_ADDRESS", "")
RECIPIENTS_FILE = os.getenv("RECIPIENTS_FILE", "recipients.json")

CHAIN_ID_OVERRIDE = int(os.getenv("CHAIN_ID", "0") or "0")

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "5"))

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

RPC_TIMEOUT = int(os.getenv("RPC_TIMEOUT", "20"))

BASE_PRIORITY_FEE_GWEI = float(os.getenv("PRIORITY_FEE_GWEI", "2"))
MAX_FEE_CAP_GWEI = float(os.getenv("MAX_FEE_CAP_GWEI", "300"))

GAS_BUFFER = float(os.getenv("GAS_BUFFER", "1.2"))

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger("eth-batch")

# ============================================================
# SAFETY VALIDATION
# ============================================================

if not RPC_URLS:
    raise RuntimeError("RPC_URLS missing")

if not PRIVATE_KEY:
    raise RuntimeError("PRIVATE_KEY missing")

# ============================================================
# RPC MANAGER
# ============================================================

class RPCManager:
    def __init__(self, urls: List[str]):
        self.urls = urls
        self.i = 0
        self.lock = threading.Lock()
        self.w3 = self._connect()

    def _connect(self):
        last = None

        for _ in range(len(self.urls)):
            url = self.urls[self.i % len(self.urls)]
            self.i += 1

            try:
                w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": RPC_TIMEOUT}))
                if w3.is_connected():
                    log.info(f"Connected RPC: {url}")
                    return w3
            except Exception as e:
                last = e
                log.warning(f"RPC failed {url}: {e}")

        raise RuntimeError(f"No RPC available: {last}")

    def get(self):
        with self.lock:
            try:
                _ = self.w3.eth.block_number
                return self.w3
            except Exception:
                log.warning("RPC unhealthy -> reconnect")
                self.w3 = self._connect()
                return self.w3


rpc = RPCManager(RPC_URLS)
w3 = rpc.get()

acct = w3.eth.account.from_key(PRIVATE_KEY)

if TREASURY_ADDRESS:
    TREASURY_ADDRESS = w3.to_checksum_address(TREASURY_ADDRESS)
else:
    TREASURY_ADDRESS = acct.address

# ============================================================
# NONCE MANAGER (THREAD SAFE)
# ============================================================

class NonceManager:
    def __init__(self, address: str):
        self.address = address
        self.lock = threading.Lock()
        self.nonce = w3.eth.get_transaction_count(address, "pending")

    def get(self):
        with self.lock:
            n = self.nonce
            self.nonce += 1
            return n

nonce_manager = NonceManager(TREASURY_ADDRESS)

# ============================================================
# UTILS
# ============================================================

def eth(wei: int) -> str:
    return str(Decimal(wei) / Decimal(10**18)) + " ETH"

# ============================================================
# GAS
# ============================================================

def fees():
    block = w3.eth.get_block("latest")
    base = block.get("baseFeePerGas", 0)

    try:
        tip = w3.eth.max_priority_fee
    except Exception:
        tip = w3.to_wei(BASE_PRIORITY_FEE_GWEI, "gwei")

    max_fee = int((base * 2) + tip)

    cap = w3.to_wei(MAX_FEE_CAP_GWEI, "gwei")
    return {
        "maxPriorityFeePerGas": int(tip),
        "maxFeePerGas": min(max_fee, cap),
    }

def gas_estimate(to: str, value: int):
    try:
        g = w3.eth.estimate_gas({
            "from": TREASURY_ADDRESS,
            "to": to,
            "value": value
        })
        return int(g * GAS_BUFFER)
    except Exception:
        return 21000

# ============================================================
# RECIPIENTS
# ============================================================

def load_recipients(path: str):
    """
    Format:
    [
      {"to": "0x...", "value_eth": 0.01},
      ...
    ]
    """
    with open(path, "r") as f:
        return json.load(f)

# ============================================================
# SEND TX
# ============================================================

def send(to: str, value_eth: float, idx: int):

    try:
        to = w3.to_checksum_address(to)
        value = w3.to_wei(value_eth, "ether")

        balance = w3.eth.get_balance(TREASURY_ADDRESS)

        if balance < value:
            log.warning(f"[{idx}] insufficient balance")
            return False

        gas = gas_estimate(to, value)
        f = fees()

        cost = gas * f["maxFeePerGas"]
        if balance <= cost + value:
            log.warning(f"[{idx}] not enough for gas")
            return False

        value = balance - cost

        tx = {
            "from": TREASURY_ADDRESS,
            "to": to,
            "value": value,
            "nonce": nonce_manager.get(),
            "gas": gas,
            "chainId": CHAIN_ID_OVERRIDE or w3.eth.chain_id,
            "maxFeePerGas": f["maxFeePerGas"],
            "maxPriorityFeePerGas": f["maxPriorityFeePerGas"],
            "type": 2
        }

        if DRY_RUN:
            log.info(f"[DRY] {to} -> {eth(value)}")
            return True

        signed = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        txh = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"[{idx}] sent -> {txh.hex()} | {eth(value)}")
        return True

    except Exception as e:
        log.exception(f"[{idx}] failed: {e}")
        return False

# ============================================================
# MAIN PROCESS
# ============================================================

def run():
    recipients = load_recipients(RECIPIENTS_FILE)

    log.info(f"Loaded {len(recipients)} recipients")

    success = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {
            ex.submit(send, r["to"], r["value_eth"], i): i
            for i, r in enumerate(recipients, 1)
        }

        for f in as_completed(futs):
            try:
                if f.result():
                    success += 1
                else:
                    fail += 1
            except Exception:
                fail += 1

    log.info(f"DONE success={success} fail={fail}")

# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    run()
