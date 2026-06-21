#!/usr/bin/env python3
"""
Production-Grade ETH Batch Payout Service

SAFE DESIGN:
- Single treasury wallet only
- Batch ETH payouts to multiple recipients
- No sweeping / no "send all balance"
- Sequential nonce-safe sending
- RPC failover for read/send calls
- Dry-run mode
- Strict recipient validation
- Decimal-safe ETH amounts
- Preflight balance check
- JSONL audit log
- Optional receipt waiting

Recipients format:
[
  {"to": "0x...", "value_eth": "0.01"},
  {"to": "0x...", "value_eth": "0.025"}
]

Environment:
  RPC_URLS="https://rpc1,https://rpc2"
  PRIVATE_KEY="..."
  TREASURY_ADDRESS="0x..."              optional, must match private key if set
  RECIPIENTS_FILE="recipients.json"

  DRY_RUN=true|false
  CHAIN_ID=1                            optional override
  RETRY_LIMIT=5
  RPC_TIMEOUT=20

  PRIORITY_FEE_GWEI=2
  MAX_FEE_CAP_GWEI=300
  GAS_BUFFER=1.20

  WAIT_RECEIPTS=true|false
  RECEIPT_TIMEOUT=180
  CONFIRMATIONS=1

  STATE_FILE="payout_state.jsonl"
  ALLOW_DUPLICATE_RECIPIENTS=false
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, List, Optional, Tuple

from web3 import Web3
from web3.exceptions import TimeExhausted


# ============================================================
# CONFIG
# ============================================================

WEI = Decimal("1000000000000000000")

RPC_URLS = [x.strip() for x in os.getenv("RPC_URLS", "").split(",") if x.strip()]
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()
TREASURY_ADDRESS_ENV = os.getenv("TREASURY_ADDRESS", "").strip()
RECIPIENTS_FILE = Path(os.getenv("RECIPIENTS_FILE", "recipients.json"))

CHAIN_ID_OVERRIDE = int(os.getenv("CHAIN_ID", "0") or "0")

RETRY_LIMIT = int(os.getenv("RETRY_LIMIT", "5"))
RPC_TIMEOUT = int(os.getenv("RPC_TIMEOUT", "20"))

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

BASE_PRIORITY_FEE_GWEI = Decimal(os.getenv("PRIORITY_FEE_GWEI", "2"))
MAX_FEE_CAP_GWEI = Decimal(os.getenv("MAX_FEE_CAP_GWEI", "300"))
GAS_BUFFER = Decimal(os.getenv("GAS_BUFFER", "1.20"))

WAIT_RECEIPTS = os.getenv("WAIT_RECEIPTS", "true").lower() == "true"
RECEIPT_TIMEOUT = int(os.getenv("RECEIPT_TIMEOUT", "180"))
CONFIRMATIONS = int(os.getenv("CONFIRMATIONS", "1"))

STATE_FILE = Path(os.getenv("STATE_FILE", "payout_state.jsonl"))
ALLOW_DUPLICATE_RECIPIENTS = os.getenv("ALLOW_DUPLICATE_RECIPIENTS", "false").lower() == "true"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger("eth-batch")


# ============================================================
# MODELS
# ============================================================

@dataclass(frozen=True)
class Recipient:
    idx: int
    to: str
    value_wei: int
    value_eth_raw: str


@dataclass(frozen=True)
class FeeParams:
    max_priority_fee_per_gas: int
    max_fee_per_gas: int
    base_fee_per_gas: int


# ============================================================
# BASIC UTILS
# ============================================================

def require_env() -> None:
    if not RPC_URLS:
        raise RuntimeError("RPC_URLS missing")

    if not PRIVATE_KEY:
        raise RuntimeError("PRIVATE_KEY missing")

    if not RECIPIENTS_FILE.exists():
        raise RuntimeError(f"Recipients file not found: {RECIPIENTS_FILE}")


def wei_to_eth_str(wei: int) -> str:
    return f"{Decimal(wei) / WEI:.18f}".rstrip("0").rstrip(".") + " ETH"


def gwei_to_wei(value: Decimal) -> int:
    return int(value * Decimal(10**9))


def eth_to_wei(value: Any) -> int:
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"Invalid ETH amount: {value!r}") from e

    if dec <= 0:
        raise ValueError(f"Amount must be positive: {value!r}")

    wei = dec * WEI

    if wei != wei.to_integral_value():
        raise ValueError(f"Amount has more than 18 decimals: {value!r}")

    return int(wei)


def write_state(event: dict) -> None:
    event = {
        "ts": int(time.time()),
        **event,
    }

    with STATE_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def short_addr(addr: str) -> str:
    return f"{addr[:8]}...{addr[-6:]}"


def is_already_known_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        "already known" in msg
        or "already imported" in msg
        or "known transaction" in msg
        or "transaction already in mempool" in msg
    )


def is_nonce_too_low_error(exc: Exception) -> bool:
    return "nonce too low" in str(exc).lower()


# ============================================================
# RPC FAILOVER
# ============================================================

class RPCPool:
    def __init__(self, urls: List[str]):
        self.urls = urls
        self.index = 0
        self.w3 = self._connect_any()

    def _provider(self, url: str) -> Web3:
        return Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": RPC_TIMEOUT}))

    def _connect_any(self) -> Web3:
        last_exc: Optional[Exception] = None

        for _ in range(len(self.urls)):
            url = self.urls[self.index % len(self.urls)]
            self.index += 1

            try:
                w3 = self._provider(url)
                if w3.is_connected():
                    log.info("Connected RPC: %s", self._safe_url(url))
                    return w3
            except Exception as e:
                last_exc = e
                log.warning("RPC connect failed %s: %s", self._safe_url(url), e)

        raise RuntimeError(f"No RPC available: {last_exc}")

    def rotate(self) -> Web3:
        self.w3 = self._connect_any()
        return self.w3

    def call(self, name: str, fn: Callable[[Web3], Any], retries: int = RETRY_LIMIT) -> Any:
        last_exc: Optional[Exception] = None

        for attempt in range(1, retries + 1):
            try:
                return fn(self.w3)
            except Exception as e:
                last_exc = e
                log.warning(
                    "RPC call failed name=%s attempt=%s/%s err=%s",
                    name,
                    attempt,
                    retries,
                    e,
                )

                try:
                    self.rotate()
                except Exception as rotate_exc:
                    log.warning("RPC rotation failed: %s", rotate_exc)

                time.sleep(min(0.25 * attempt + random.random() * 0.25, 3.0))

        raise RuntimeError(f"RPC call failed after {retries} retries: {name}: {last_exc}")

    @staticmethod
    def _safe_url(url: str) -> str:
        # не светим токены/пароли в логах
        if "@" in url:
            prefix, rest = url.split("@", 1)
            return "***@" + rest
        if "?" in url:
            return url.split("?", 1)[0] + "?***"
        return url


# ============================================================
# RECIPIENTS
# ============================================================

def load_recipients(path: Path) -> List[Recipient]:
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    if not isinstance(raw, list):
        raise ValueError("Recipients file must contain JSON array")

    recipients: List[Recipient] = []
    seen: set[str] = set()

    for idx, item in enumerate(raw, 1):
        if not isinstance(item, dict):
            raise ValueError(f"Recipient #{idx} must be object")

        to_raw = str(item.get("to", "")).strip()
        amount_raw = item.get("value_eth")

        if not Web3.is_address(to_raw):
            raise ValueError(f"Recipient #{idx} has invalid address: {to_raw!r}")

        to = Web3.to_checksum_address(to_raw)
        value_wei = eth_to_wei(amount_raw)

        if not ALLOW_DUPLICATE_RECIPIENTS:
            duplicate_key = to.lower()
            if duplicate_key in seen:
                raise ValueError(
                    f"Duplicate recipient address #{idx}: {to}. "
                    f"Set ALLOW_DUPLICATE_RECIPIENTS=true if this is intentional."
                )
            seen.add(duplicate_key)

        recipients.append(
            Recipient(
                idx=idx,
                to=to,
                value_wei=value_wei,
                value_eth_raw=str(amount_raw),
            )
        )

    return recipients


# ============================================================
# GAS / FEES
# ============================================================

def get_fee_params(rpc: RPCPool) -> FeeParams:
    def _get(w3: Web3) -> FeeParams:
        block = w3.eth.get_block("latest")
        base_fee = int(block.get("baseFeePerGas") or 0)

        try:
            tip = int(w3.eth.max_priority_fee)
        except Exception:
            tip = gwei_to_wei(BASE_PRIORITY_FEE_GWEI)

        calculated_max_fee = base_fee * 2 + tip
        cap = gwei_to_wei(MAX_FEE_CAP_GWEI)

        if calculated_max_fee > cap:
            raise RuntimeError(
                "Calculated maxFeePerGas exceeds cap: "
                f"calculated={calculated_max_fee} wei, cap={cap} wei. "
                "Increase MAX_FEE_CAP_GWEI or wait for lower base fee."
            )

        if calculated_max_fee < tip:
            raise RuntimeError("maxFeePerGas cannot be lower than maxPriorityFeePerGas")

        return FeeParams(
            max_priority_fee_per_gas=tip,
            max_fee_per_gas=calculated_max_fee,
            base_fee_per_gas=base_fee,
        )

    return rpc.call("get_fee_params", _get)


def estimate_gas(rpc: RPCPool, treasury: str, to: str, value_wei: int) -> int:
    def _estimate(w3: Web3) -> int:
        gas = w3.eth.estimate_gas(
            {
                "from": treasury,
                "to": to,
                "value": value_wei,
            }
        )

        buffered = int(Decimal(gas) * GAS_BUFFER)
        return max(buffered, 21_000)

    return rpc.call("estimate_gas", _estimate)


# ============================================================
# PREFLIGHT
# ============================================================

def preflight(
    rpc: RPCPool,
    treasury: str,
    recipients: List[Recipient],
) -> Tuple[int, FeeParams, List[int]]:
    chain_id = CHAIN_ID_OVERRIDE or rpc.call("chain_id", lambda w3: w3.eth.chain_id)
    fees = get_fee_params(rpc)

    gas_limits: List[int] = []
    total_value = 0
    total_max_gas_cost = 0

    for r in recipients:
        gas = estimate_gas(rpc, treasury, r.to, r.value_wei)
        gas_limits.append(gas)

        total_value += r.value_wei
        total_max_gas_cost += gas * fees.max_fee_per_gas

    balance = rpc.call("get_balance", lambda w3: w3.eth.get_balance(treasury, "pending"))
    required = total_value + total_max_gas_cost

    log.info("Chain ID: %s", chain_id)
    log.info("Treasury: %s", treasury)
    log.info("Balance: %s", wei_to_eth_str(balance))
    log.info("Recipients: %s", len(recipients))
    log.info("Total payout: %s", wei_to_eth_str(total_value))
    log.info("Max reserved gas: %s", wei_to_eth_str(total_max_gas_cost))
    log.info("Max required: %s", wei_to_eth_str(required))
    log.info(
        "Fees: base=%s gwei, priority=%s gwei, max=%s gwei",
        Decimal(fees.base_fee_per_gas) / Decimal(10**9),
        Decimal(fees.max_priority_fee_per_gas) / Decimal(10**9),
        Decimal(fees.max_fee_per_gas) / Decimal(10**9),
    )

    if balance < required:
        raise RuntimeError(
            "Insufficient treasury balance for full batch. "
            f"balance={wei_to_eth_str(balance)}, required={wei_to_eth_str(required)}"
        )

    return chain_id, fees, gas_limits


# ============================================================
# TX SEND / RECEIPT
# ============================================================

def wait_receipt(rpc: RPCPool, tx_hash_hex: str) -> bool:
    tx_hash = Web3.to_bytes(hexstr=tx_hash_hex)

    try:
        receipt = rpc.call(
            "wait_for_receipt",
            lambda w3: w3.eth.wait_for_transaction_receipt(
                tx_hash,
                timeout=RECEIPT_TIMEOUT,
            ),
            retries=1,
        )
    except TimeExhausted:
        log.warning("Receipt timeout: %s", tx_hash_hex)
        return False

    status = int(receipt.get("status", 0))
    block_number = int(receipt.get("blockNumber", 0))

    if status != 1:
        log.error("Transaction failed on-chain: %s", tx_hash_hex)
        return False

    if CONFIRMATIONS > 1:
        while True:
            latest = rpc.call("latest_block", lambda w3: w3.eth.block_number)
            confirmations = latest - block_number + 1

            if confirmations >= CONFIRMATIONS:
                break

            time.sleep(3)

    return True


def send_one(
    rpc: RPCPool,
    private_key: str,
    treasury: str,
    recipient: Recipient,
    nonce: int,
    chain_id: int,
    fees: FeeParams,
    gas_limit: int,
) -> Tuple[bool, bool]:
    """
    Returns:
      (ok, nonce_consumed)

    nonce_consumed=True means tx was signed/sent/dry-run and next nonce should be used.
    """

    tx = {
        "from": treasury,
        "to": recipient.to,
        "value": recipient.value_wei,
        "nonce": nonce,
        "gas": gas_limit,
        "chainId": chain_id,
        "maxFeePerGas": fees.max_fee_per_gas,
        "maxPriorityFeePerGas": fees.max_priority_fee_per_gas,
        "type": 2,
    }

    tx_cost = recipient.value_wei + gas_limit * fees.max_fee_per_gas

    write_state(
        {
            "event": "prepared",
            "idx": recipient.idx,
            "to": recipient.to,
            "value_wei": recipient.value_wei,
            "value_eth": recipient.value_eth_raw,
            "nonce": nonce,
            "gas": gas_limit,
            "max_fee_per_gas": fees.max_fee_per_gas,
            "max_priority_fee_per_gas": fees.max_priority_fee_per_gas,
            "max_total_cost_wei": tx_cost,
            "dry_run": DRY_RUN,
        }
    )

    if DRY_RUN:
        log.info(
            "[DRY][%s] %s -> %s | nonce=%s gas=%s",
            recipient.idx,
            short_addr(recipient.to),
            wei_to_eth_str(recipient.value_wei),
            nonce,
            gas_limit,
        )
        return True, True

    signed = Web3().eth.account.sign_transaction(tx, private_key)

    raw_tx = getattr(signed, "raw_transaction", None)
    if raw_tx is None:
        raw_tx = getattr(signed, "rawTransaction")

    tx_hash_hex = signed.hash.hex()

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            rpc.call(
                "send_raw_transaction",
                lambda w3: w3.eth.send_raw_transaction(raw_tx),
                retries=1,
            )

            log.info(
                "[%s] sent %s -> %s | %s | nonce=%s",
                recipient.idx,
                tx_hash_hex,
                short_addr(recipient.to),
                wei_to_eth_str(recipient.value_wei),
                nonce,
            )

            write_state(
                {
                    "event": "sent",
                    "idx": recipient.idx,
                    "to": recipient.to,
                    "value_wei": recipient.value_wei,
                    "tx_hash": tx_hash_hex,
                    "nonce": nonce,
                }
            )

            if WAIT_RECEIPTS:
                ok = wait_receipt(rpc, tx_hash_hex)

                write_state(
                    {
                        "event": "receipt",
                        "idx": recipient.idx,
                        "to": recipient.to,
                        "tx_hash": tx_hash_hex,
                        "ok": ok,
                    }
                )

                return ok, True

            return True, True

        except Exception as e:
            if is_already_known_error(e):
                log.warning("[%s] tx already known: %s", recipient.idx, tx_hash_hex)

                if WAIT_RECEIPTS:
                    ok = wait_receipt(rpc, tx_hash_hex)
                    return ok, True

                return True, True

            if is_nonce_too_low_error(e):
                log.error(
                    "[%s] nonce too low for nonce=%s. "
                    "This may mean the tx was already accepted or nonce was used externally.",
                    recipient.idx,
                    nonce,
                )

                write_state(
                    {
                        "event": "nonce_too_low",
                        "idx": recipient.idx,
                        "to": recipient.to,
                        "tx_hash": tx_hash_hex,
                        "nonce": nonce,
                        "error": str(e),
                    }
                )

                return False, True

            log.warning(
                "[%s] send attempt %s/%s failed: %s",
                recipient.idx,
                attempt,
                RETRY_LIMIT,
                e,
            )

            time.sleep(min(0.5 * attempt + random.random() * 0.5, 5.0))

    write_state(
        {
            "event": "send_unknown",
            "idx": recipient.idx,
            "to": recipient.to,
            "value_wei": recipient.value_wei,
            "tx_hash": tx_hash_hex,
            "nonce": nonce,
        }
    )

    log.error(
        "[%s] send status unknown after retries. tx_hash=%s nonce=%s. "
        "Stop and check manually before continuing.",
        recipient.idx,
        tx_hash_hex,
        nonce,
    )

    return False, True


# ============================================================
# MAIN
# ============================================================

def run() -> int:
    require_env()

    rpc = RPCPool(RPC_URLS)
    first_w3 = rpc.w3

    acct = first_w3.eth.account.from_key(PRIVATE_KEY)
    treasury = Web3.to_checksum_address(TREASURY_ADDRESS_ENV or acct.address)

    if treasury.lower() != acct.address.lower():
        raise RuntimeError(
            "TREASURY_ADDRESS does not match PRIVATE_KEY address. "
            f"TREASURY_ADDRESS={treasury}, private_key_address={acct.address}"
        )

    recipients = load_recipients(RECIPIENTS_FILE)

    if not recipients:
        log.warning("No recipients loaded")
        return 0

    log.info("Loaded recipients: %s", len(recipients))

    chain_id, fees, gas_limits = preflight(rpc, treasury, recipients)

    nonce = rpc.call(
        "get_pending_nonce",
        lambda w3: w3.eth.get_transaction_count(treasury, "pending"),
    )

    log.info("Starting nonce: %s", nonce)

    success = 0
    fail = 0

    for recipient, gas_limit in zip(recipients, gas_limits):
        ok, nonce_consumed = send_one(
            rpc=rpc,
            private_key=PRIVATE_KEY,
            treasury=treasury,
            recipient=recipient,
            nonce=nonce,
            chain_id=chain_id,
            fees=fees,
            gas_limit=gas_limit,
        )

        if nonce_consumed:
            nonce += 1

        if ok:
            success += 1
        else:
            fail += 1
            log.error("Stopping batch after failure at recipient #%s", recipient.idx)
            break

    log.info("DONE success=%s fail=%s dry_run=%s", success, fail, DRY_RUN)

    return 0 if fail == 0 else 1


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except KeyboardInterrupt:
        log.warning("Interrupted")
        raise SystemExit(130)
    except Exception as e:
        log.exception("Fatal error: %s", e)
        raise SystemExit(1)
