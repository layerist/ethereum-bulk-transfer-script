#!/usr/bin/env python3
"""
Reliable ETH Batch Payout Service v2

Purpose:
- Send fixed ETH amounts from one treasury wallet to multiple recipients.
- Never sweep balances and never calculate "send all" amounts.
- Send transactions sequentially to keep nonce handling deterministic.

Recipients JSON:
[
  {"to": "0x...", "value_eth": "0.01"},
  {"to": "0x...", "value_eth": "0.025"}
]

Required environment:
  RPC_URLS="https://rpc1,https://rpc2"
  PRIVATE_KEY="..."

Optional environment:
  TREASURY_ADDRESS="0x..."          must match PRIVATE_KEY
  RECIPIENTS_FILE="recipients.json"
  STATE_FILE="payout_state.jsonl"
  LOCK_FILE="payout.lock"

  DRY_RUN=false
  CHAIN_ID=0                          0 = auto-detect
  RETRY_LIMIT=5
  RPC_TIMEOUT=20
  RPC_COOLDOWN_SECONDS=15

  PRIORITY_FEE_GWEI=2
  MAX_PRIORITY_FEE_GWEI=20
  MAX_FEE_CAP_GWEI=300
  BASE_FEE_MULTIPLIER=2
  GAS_BUFFER=1.20
  MIN_GAS_LIMIT=21000
  REFRESH_FEES_EVERY=10               transactions; 0 = never

  WAIT_RECEIPTS=true
  RECEIPT_TIMEOUT=180
  RECEIPT_POLL_INTERVAL=3
  CONFIRMATIONS=1
  CONFIRMATION_TIMEOUT=600

  ALLOW_DUPLICATE_RECIPIENTS=false
  MAX_RECIPIENTS=10000
  MAX_TOTAL_ETH=0                     0 = disabled
  CONTINUE_AFTER_RECEIPT_TIMEOUT=false
  LOG_LEVEL=INFO
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from typing import Any, Callable, Optional, Sequence, TypeVar
from urllib.parse import urlsplit, urlunsplit

from web3 import Web3
from web3.exceptions import TransactionNotFound

T = TypeVar("T")
WEI_PER_ETH = Decimal("1000000000000000000")
WEI_PER_GWEI = Decimal("1000000000")


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true/false, got {raw!r}")


def env_int(name: str, default: int, minimum: Optional[int] = None) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


def env_decimal(name: str, default: str, minimum: Optional[Decimal] = None) -> Decimal:
    raw = os.getenv(name, default).strip()
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a decimal, got {raw!r}") from exc
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}, got {value}")
    return value


@dataclass(frozen=True)
class Config:
    rpc_urls: tuple[str, ...]
    private_key: str
    treasury_address: str
    recipients_file: Path
    state_file: Path
    lock_file: Path
    dry_run: bool
    chain_id_override: int
    retry_limit: int
    rpc_timeout: int
    rpc_cooldown_seconds: float
    priority_fee_gwei: Decimal
    max_priority_fee_gwei: Decimal
    max_fee_cap_gwei: Decimal
    base_fee_multiplier: Decimal
    gas_buffer: Decimal
    min_gas_limit: int
    refresh_fees_every: int
    wait_receipts: bool
    receipt_timeout: int
    receipt_poll_interval: float
    confirmations: int
    confirmation_timeout: int
    allow_duplicate_recipients: bool
    max_recipients: int
    max_total_eth: Decimal
    continue_after_receipt_timeout: bool

    @classmethod
    def from_env(cls) -> "Config":
        urls = tuple(x.strip() for x in os.getenv("RPC_URLS", "").split(",") if x.strip())
        cfg = cls(
            rpc_urls=urls,
            private_key=os.getenv("PRIVATE_KEY", "").strip(),
            treasury_address=os.getenv("TREASURY_ADDRESS", "").strip(),
            recipients_file=Path(os.getenv("RECIPIENTS_FILE", "recipients.json")),
            state_file=Path(os.getenv("STATE_FILE", "payout_state.jsonl")),
            lock_file=Path(os.getenv("LOCK_FILE", "payout.lock")),
            dry_run=env_bool("DRY_RUN", False),
            chain_id_override=env_int("CHAIN_ID", 0, 0),
            retry_limit=env_int("RETRY_LIMIT", 5, 1),
            rpc_timeout=env_int("RPC_TIMEOUT", 20, 1),
            rpc_cooldown_seconds=float(env_decimal("RPC_COOLDOWN_SECONDS", "15", Decimal("0"))),
            priority_fee_gwei=env_decimal("PRIORITY_FEE_GWEI", "2", Decimal("0")),
            max_priority_fee_gwei=env_decimal("MAX_PRIORITY_FEE_GWEI", "20", Decimal("0")),
            max_fee_cap_gwei=env_decimal("MAX_FEE_CAP_GWEI", "300", Decimal("0.000000001")),
            base_fee_multiplier=env_decimal("BASE_FEE_MULTIPLIER", "2", Decimal("1")),
            gas_buffer=env_decimal("GAS_BUFFER", "1.20", Decimal("1")),
            min_gas_limit=env_int("MIN_GAS_LIMIT", 21_000, 21_000),
            refresh_fees_every=env_int("REFRESH_FEES_EVERY", 10, 0),
            wait_receipts=env_bool("WAIT_RECEIPTS", True),
            receipt_timeout=env_int("RECEIPT_TIMEOUT", 180, 1),
            receipt_poll_interval=float(env_decimal("RECEIPT_POLL_INTERVAL", "3", Decimal("0.1"))),
            confirmations=env_int("CONFIRMATIONS", 1, 1),
            confirmation_timeout=env_int("CONFIRMATION_TIMEOUT", 600, 1),
            allow_duplicate_recipients=env_bool("ALLOW_DUPLICATE_RECIPIENTS", False),
            max_recipients=env_int("MAX_RECIPIENTS", 10_000, 1),
            max_total_eth=env_decimal("MAX_TOTAL_ETH", "0", Decimal("0")),
            continue_after_receipt_timeout=env_bool("CONTINUE_AFTER_RECEIPT_TIMEOUT", False),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.rpc_urls:
            raise ValueError("RPC_URLS is missing")
        if not self.private_key:
            raise ValueError("PRIVATE_KEY is missing")
        if not self.recipients_file.is_file():
            raise ValueError(f"Recipients file not found: {self.recipients_file}")
        if self.priority_fee_gwei > self.max_priority_fee_gwei:
            raise ValueError("PRIORITY_FEE_GWEI exceeds MAX_PRIORITY_FEE_GWEI")
        if self.max_priority_fee_gwei > self.max_fee_cap_gwei:
            raise ValueError("MAX_PRIORITY_FEE_GWEI exceeds MAX_FEE_CAP_GWEI")
        if self.state_file.resolve() == self.recipients_file.resolve():
            raise ValueError("STATE_FILE must differ from RECIPIENTS_FILE")


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("eth-batch-v2")


# -----------------------------------------------------------------------------
# Models and helpers
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Recipient:
    idx: int
    to: str
    value_wei: int
    value_eth_raw: str


@dataclass(frozen=True)
class FeeParams:
    base_fee_per_gas: int
    max_priority_fee_per_gas: int
    max_fee_per_gas: int


@dataclass(frozen=True)
class SendResult:
    ok: bool
    nonce_consumed: bool
    tx_hash: Optional[str]
    status: str


def decimal_to_wei(value: Decimal, unit: Decimal) -> int:
    result = value * unit
    if result != result.to_integral_value():
        raise ValueError(f"Value has too many decimal places: {value}")
    return int(result)


def eth_to_wei(value: Any) -> int:
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid ETH amount: {value!r}") from exc
    if not dec.is_finite() or dec <= 0:
        raise ValueError(f"ETH amount must be finite and positive: {value!r}")
    return decimal_to_wei(dec, WEI_PER_ETH)


def gwei_to_wei(value: Decimal) -> int:
    return decimal_to_wei(value, WEI_PER_GWEI)


def wei_to_eth_str(value: int) -> str:
    text = f"{Decimal(value) / WEI_PER_ETH:.18f}".rstrip("0").rstrip(".")
    return f"{text} ETH"


def wei_to_gwei_str(value: int) -> str:
    return f"{Decimal(value) / WEI_PER_GWEI:f}".rstrip("0").rstrip(".")


def short_addr(address: str) -> str:
    return f"{address[:8]}...{address[-6:]}"


def redact_url(url: str) -> str:
    try:
        p = urlsplit(url)
        host = p.hostname or ""
        if p.port:
            host += f":{p.port}"
        if p.username or p.password:
            host = f"***@{host}"
        path = p.path
        # API keys are frequently stored in the final path segment.
        if len(path.rstrip("/").split("/")[-1]) >= 20:
            segments = path.split("/")
            segments[-1] = "***"
            path = "/".join(segments)
        return urlunsplit((p.scheme, host, path, "***" if p.query else "", ""))
    except Exception:
        return "<redacted-rpc-url>"


def error_text(exc: BaseException) -> str:
    return str(exc).lower()


def is_already_known(exc: BaseException) -> bool:
    text = error_text(exc)
    return any(x in text for x in (
        "already known", "already imported", "known transaction",
        "transaction already in mempool",
    ))


def is_nonce_too_low(exc: BaseException) -> bool:
    text = error_text(exc)
    return "nonce too low" in text or "nonce has already been used" in text


def is_replacement_underpriced(exc: BaseException) -> bool:
    text = error_text(exc)
    return "replacement transaction underpriced" in text or "replacement fee too low" in text


class StopRequested(RuntimeError):
    pass


STOP_REQUESTED = False


def request_stop(signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log.warning("Signal %s received; stopping after current safe step", signum)


# -----------------------------------------------------------------------------
# Single-process lock and audit log
# -----------------------------------------------------------------------------

class ProcessLock:
    def __init__(self, path: Path):
        self.path = path
        self.acquired = False

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise RuntimeError(
                f"Lock file already exists: {self.path}. Another payout process may be running. "
                "Remove it only after confirming no process is active."
            ) from exc
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"pid": os.getpid(), "started_at": int(time.time())}) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        self.acquired = True
        atexit.register(self.release)

    def release(self) -> None:
        if self.acquired:
            try:
                self.path.unlink(missing_ok=True)
            finally:
                self.acquired = False


class AuditLog:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event: str, **fields: Any) -> None:
        record = {"ts": int(time.time()), "event": event, **fields}
        line = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())


# -----------------------------------------------------------------------------
# RPC pool
# -----------------------------------------------------------------------------

@dataclass
class RPCNode:
    url: str
    w3: Web3
    failures: int = 0
    cooldown_until: float = 0.0
    chain_id: Optional[int] = None


class RPCPool:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.nodes = [
            RPCNode(
                url=url,
                w3=Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": cfg.rpc_timeout})),
            )
            for url in cfg.rpc_urls
        ]
        self.cursor = 0
        self.expected_chain_id: Optional[int] = None

    def validate_nodes(self) -> int:
        valid: list[RPCNode] = []
        detected_ids: set[int] = set()
        for node in self.nodes:
            try:
                if not node.w3.is_connected():
                    raise ConnectionError("is_connected() returned false")
                chain_id = int(node.w3.eth.chain_id)
                node.chain_id = chain_id
                if self.cfg.chain_id_override and chain_id != self.cfg.chain_id_override:
                    raise RuntimeError(
                        f"wrong chain ID {chain_id}; expected {self.cfg.chain_id_override}"
                    )
                detected_ids.add(chain_id)
                valid.append(node)
                log.info("Validated RPC %s chain_id=%s", redact_url(node.url), chain_id)
            except Exception as exc:
                log.warning("Ignoring RPC %s: %s", redact_url(node.url), exc)

        if not valid:
            raise RuntimeError("No valid RPC nodes available")
        if len(detected_ids) != 1:
            raise RuntimeError(f"RPC_URLS point to different chains: {sorted(detected_ids)}")

        self.nodes = valid
        self.expected_chain_id = self.cfg.chain_id_override or next(iter(detected_ids))
        return self.expected_chain_id

    def _choose_node(self) -> RPCNode:
        now = time.monotonic()
        candidates = [n for n in self.nodes if n.cooldown_until <= now]
        if not candidates:
            candidates = self.nodes
        candidates.sort(key=lambda n: (n.failures, n.cooldown_until))
        node = candidates[self.cursor % len(candidates)]
        self.cursor += 1
        return node

    def call(self, name: str, fn: Callable[[Web3], T], retries: Optional[int] = None) -> T:
        attempts = retries or self.cfg.retry_limit
        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            if STOP_REQUESTED:
                raise StopRequested("Stop requested")
            node = self._choose_node()
            try:
                if node.chain_id is not None and self.expected_chain_id is not None:
                    if node.chain_id != self.expected_chain_id:
                        raise RuntimeError("RPC chain ID changed")
                result = fn(node.w3)
                node.failures = max(0, node.failures - 1)
                return result
            except StopRequested:
                raise
            except Exception as exc:
                last_exc = exc
                node.failures += 1
                node.cooldown_until = time.monotonic() + self.cfg.rpc_cooldown_seconds
                log.warning(
                    "RPC call failed name=%s rpc=%s attempt=%s/%s error=%s",
                    name, redact_url(node.url), attempt, attempts, exc,
                )
                if attempt < attempts:
                    time.sleep(min(0.35 * (2 ** (attempt - 1)) + random.random() * 0.25, 5.0))
        assert last_exc is not None
        raise RuntimeError(f"RPC call failed after {attempts} attempts: {name}: {last_exc}") from last_exc

    def send_raw(self, raw_tx: bytes) -> str:
        result = self.call(
            "send_raw_transaction",
            lambda w3: w3.eth.send_raw_transaction(raw_tx),
            retries=1,
        )
        return result.hex()


# -----------------------------------------------------------------------------
# Recipients, fees, gas and preflight
# -----------------------------------------------------------------------------

def load_recipients(cfg: Config) -> list[Recipient]:
    try:
        raw = json.loads(cfg.recipients_file.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in {cfg.recipients_file} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc

    if not isinstance(raw, list):
        raise ValueError("Recipients file must contain a JSON array")
    if len(raw) > cfg.max_recipients:
        raise ValueError(f"Too many recipients: {len(raw)} > MAX_RECIPIENTS={cfg.max_recipients}")

    recipients: list[Recipient] = []
    seen: set[str] = set()
    total_wei = 0
    for idx, item in enumerate(raw, 1):
        if not isinstance(item, dict):
            raise ValueError(f"Recipient #{idx} must be an object")
        unknown = set(item) - {"to", "value_eth"}
        if unknown:
            raise ValueError(f"Recipient #{idx} contains unknown fields: {sorted(unknown)}")
        if "to" not in item or "value_eth" not in item:
            raise ValueError(f"Recipient #{idx} must contain 'to' and 'value_eth'")

        to_raw = str(item["to"]).strip()
        if not Web3.is_address(to_raw):
            raise ValueError(f"Recipient #{idx} has invalid address: {to_raw!r}")
        to = Web3.to_checksum_address(to_raw)
        value_wei = eth_to_wei(item["value_eth"])

        key = to.lower()
        if not cfg.allow_duplicate_recipients and key in seen:
            raise ValueError(
                f"Duplicate recipient #{idx}: {to}. Set ALLOW_DUPLICATE_RECIPIENTS=true only if intentional."
            )
        seen.add(key)
        total_wei += value_wei
        recipients.append(Recipient(idx, to, value_wei, str(item["value_eth"])))

    if cfg.max_total_eth > 0:
        max_total_wei = decimal_to_wei(cfg.max_total_eth, WEI_PER_ETH)
        if total_wei > max_total_wei:
            raise ValueError(
                f"Total payout {wei_to_eth_str(total_wei)} exceeds MAX_TOTAL_ETH={cfg.max_total_eth}"
            )
    return recipients


def get_fee_params(rpc: RPCPool, cfg: Config) -> FeeParams:
    def fetch(w3: Web3) -> FeeParams:
        block = w3.eth.get_block("latest")
        base_fee_raw = block.get("baseFeePerGas")
        if base_fee_raw is None:
            raise RuntimeError("Latest block has no baseFeePerGas; this script requires EIP-1559")
        base_fee = int(base_fee_raw)
        fallback_tip = gwei_to_wei(cfg.priority_fee_gwei)
        try:
            suggested_tip = int(w3.eth.max_priority_fee)
        except Exception:
            suggested_tip = fallback_tip
        tip = max(fallback_tip, suggested_tip)
        tip_cap = gwei_to_wei(cfg.max_priority_fee_gwei)
        tip = min(tip, tip_cap)

        multiplied = (Decimal(base_fee) * cfg.base_fee_multiplier).to_integral_value(rounding=ROUND_CEILING)
        max_fee = int(multiplied) + tip
        fee_cap = gwei_to_wei(cfg.max_fee_cap_gwei)
        if max_fee > fee_cap:
            raise RuntimeError(
                f"Required maxFeePerGas {wei_to_gwei_str(max_fee)} gwei exceeds "
                f"MAX_FEE_CAP_GWEI={cfg.max_fee_cap_gwei}"
            )
        return FeeParams(base_fee, tip, max_fee)

    return rpc.call("get_fee_params", fetch)


def estimate_gas(rpc: RPCPool, cfg: Config, treasury: str, recipient: Recipient) -> int:
    estimated = rpc.call(
        f"estimate_gas[{recipient.idx}]",
        lambda w3: int(w3.eth.estimate_gas({
            "from": treasury,
            "to": recipient.to,
            "value": recipient.value_wei,
        })),
    )
    buffered = int((Decimal(estimated) * cfg.gas_buffer).to_integral_value(rounding=ROUND_CEILING))
    return max(buffered, cfg.min_gas_limit)


def preflight(
    rpc: RPCPool,
    cfg: Config,
    treasury: str,
    recipients: Sequence[Recipient],
) -> tuple[FeeParams, list[int]]:
    fees = get_fee_params(rpc, cfg)
    gas_limits = [estimate_gas(rpc, cfg, treasury, r) for r in recipients]
    total_value = sum(r.value_wei for r in recipients)
    max_gas_cost = sum(gas * fees.max_fee_per_gas for gas in gas_limits)
    required = total_value + max_gas_cost
    balance = int(rpc.call("get_pending_balance", lambda w3: w3.eth.get_balance(treasury, "pending")))

    log.info("Treasury: %s", treasury)
    log.info("Pending balance: %s", wei_to_eth_str(balance))
    log.info("Recipients: %s", len(recipients))
    log.info("Total payout: %s", wei_to_eth_str(total_value))
    log.info("Maximum reserved gas: %s", wei_to_eth_str(max_gas_cost))
    log.info("Maximum required: %s", wei_to_eth_str(required))
    log.info(
        "Fees: base=%s gwei priority=%s gwei max=%s gwei",
        wei_to_gwei_str(fees.base_fee_per_gas),
        wei_to_gwei_str(fees.max_priority_fee_per_gas),
        wei_to_gwei_str(fees.max_fee_per_gas),
    )
    if balance < required:
        raise RuntimeError(
            f"Insufficient pending balance: balance={wei_to_eth_str(balance)}, "
            f"required={wei_to_eth_str(required)}"
        )
    return fees, gas_limits


# -----------------------------------------------------------------------------
# Transaction status and sending
# -----------------------------------------------------------------------------

def get_receipt_once(rpc: RPCPool, tx_hash: str) -> Optional[Any]:
    def fetch(w3: Web3) -> Optional[Any]:
        try:
            return w3.eth.get_transaction_receipt(tx_hash)
        except TransactionNotFound:
            return None
    return rpc.call("get_transaction_receipt", fetch)


def wait_for_receipt(rpc: RPCPool, cfg: Config, tx_hash: str) -> str:
    deadline = time.monotonic() + cfg.receipt_timeout
    while time.monotonic() < deadline:
        if STOP_REQUESTED:
            return "interrupted"
        receipt = get_receipt_once(rpc, tx_hash)
        if receipt is not None:
            status = int(receipt.get("status", 0))
            if status != 1:
                return "reverted"
            block_number = int(receipt["blockNumber"])
            if cfg.confirmations <= 1:
                return "confirmed"

            confirm_deadline = time.monotonic() + cfg.confirmation_timeout
            while time.monotonic() < confirm_deadline:
                if STOP_REQUESTED:
                    return "interrupted"
                latest = int(rpc.call("latest_block", lambda w3: w3.eth.block_number))
                if latest - block_number + 1 >= cfg.confirmations:
                    # Re-read the receipt to catch a rare reorg/removal.
                    final_receipt = get_receipt_once(rpc, tx_hash)
                    if final_receipt is None:
                        return "reorged"
                    if int(final_receipt.get("status", 0)) != 1:
                        return "reverted"
                    return "confirmed"
                time.sleep(cfg.receipt_poll_interval)
            return "confirmation_timeout"
        time.sleep(cfg.receipt_poll_interval)
    return "receipt_timeout"


def tx_is_known(rpc: RPCPool, tx_hash: str) -> bool:
    def lookup(w3: Web3) -> bool:
        try:
            w3.eth.get_transaction(tx_hash)
            return True
        except TransactionNotFound:
            return False
    try:
        return bool(rpc.call("get_transaction", lookup, retries=1))
    except Exception:
        return False


def reconcile_nonce(rpc: RPCPool, treasury: str, nonce: int, tx_hash: str) -> bool:
    """Return True only when there is evidence that this nonce is already consumed."""
    if tx_is_known(rpc, tx_hash):
        return True
    try:
        pending_nonce = int(rpc.call(
            "reconcile_pending_nonce",
            lambda w3: w3.eth.get_transaction_count(treasury, "pending"),
        ))
        latest_nonce = int(rpc.call(
            "reconcile_latest_nonce",
            lambda w3: w3.eth.get_transaction_count(treasury, "latest"),
        ))
    except Exception:
        return False
    return pending_nonce > nonce or latest_nonce > nonce


def build_signed_transaction(
    private_key: str,
    treasury: str,
    recipient: Recipient,
    nonce: int,
    chain_id: int,
    fees: FeeParams,
    gas_limit: int,
) -> tuple[dict[str, int | str], bytes, str]:
    tx: dict[str, int | str] = {
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
    signed = Web3().eth.account.sign_transaction(tx, private_key)
    raw = getattr(signed, "raw_transaction", None)
    if raw is None:
        raw = getattr(signed, "rawTransaction")
    raw_bytes = bytes(raw)
    tx_hash = Web3.keccak(raw_bytes).hex()
    return tx, raw_bytes, tx_hash


def send_one(
    rpc: RPCPool,
    cfg: Config,
    audit: AuditLog,
    treasury: str,
    recipient: Recipient,
    nonce: int,
    chain_id: int,
    fees: FeeParams,
    gas_limit: int,
) -> SendResult:
    tx, raw_tx, tx_hash = build_signed_transaction(
        cfg.private_key, treasury, recipient, nonce, chain_id, fees, gas_limit
    )
    audit.write(
        "prepared",
        idx=recipient.idx,
        to=recipient.to,
        value_wei=recipient.value_wei,
        value_eth=recipient.value_eth_raw,
        nonce=nonce,
        gas=gas_limit,
        max_fee_per_gas=fees.max_fee_per_gas,
        max_priority_fee_per_gas=fees.max_priority_fee_per_gas,
        tx_hash=tx_hash,
        dry_run=cfg.dry_run,
    )

    if cfg.dry_run:
        log.info(
            "[DRY][%s] %s -> %s nonce=%s gas=%s tx_hash=%s",
            recipient.idx, short_addr(recipient.to), wei_to_eth_str(recipient.value_wei),
            nonce, gas_limit, tx_hash,
        )
        audit.write("dry_run", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash)
        return SendResult(True, True, tx_hash, "dry_run")

    last_error: Optional[BaseException] = None
    for attempt in range(1, cfg.retry_limit + 1):
        if STOP_REQUESTED:
            return SendResult(False, False, tx_hash, "interrupted_before_send")
        try:
            returned_hash = rpc.send_raw(raw_tx)
            if returned_hash.lower() != tx_hash.lower():
                raise RuntimeError(
                    f"RPC returned unexpected tx hash {returned_hash}; locally signed hash is {tx_hash}"
                )
            log.info(
                "[%s] sent %s -> %s amount=%s nonce=%s",
                recipient.idx, tx_hash, short_addr(recipient.to),
                wei_to_eth_str(recipient.value_wei), nonce,
            )
            audit.write("sent", idx=recipient.idx, to=recipient.to, nonce=nonce, tx_hash=tx_hash)
            if not cfg.wait_receipts:
                return SendResult(True, True, tx_hash, "broadcast")

            receipt_status = wait_for_receipt(rpc, cfg, tx_hash)
            audit.write("receipt", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash, status=receipt_status)
            if receipt_status == "confirmed":
                return SendResult(True, True, tx_hash, receipt_status)
            if receipt_status in {"receipt_timeout", "confirmation_timeout"}:
                log.warning("[%s] %s for %s", recipient.idx, receipt_status, tx_hash)
                return SendResult(
                    cfg.continue_after_receipt_timeout,
                    True,
                    tx_hash,
                    receipt_status,
                )
            log.error("[%s] transaction status=%s tx=%s", recipient.idx, receipt_status, tx_hash)
            return SendResult(False, True, tx_hash, receipt_status)

        except Exception as exc:
            last_error = exc
            if is_already_known(exc):
                audit.write("already_known", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash)
                if not cfg.wait_receipts:
                    return SendResult(True, True, tx_hash, "already_known")
                status = wait_for_receipt(rpc, cfg, tx_hash)
                audit.write("receipt", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash, status=status)
                return SendResult(status == "confirmed", True, tx_hash, status)

            if is_nonce_too_low(exc) or is_replacement_underpriced(exc):
                consumed = reconcile_nonce(rpc, treasury, nonce, tx_hash)
                status = "nonce_consumed" if consumed else "nonce_conflict_unresolved"
                audit.write(
                    status,
                    idx=recipient.idx,
                    nonce=nonce,
                    tx_hash=tx_hash,
                    error=str(exc),
                )
                log.error(
                    "[%s] nonce conflict nonce=%s consumed=%s tx=%s error=%s",
                    recipient.idx, nonce, consumed, tx_hash, exc,
                )
                return SendResult(False, consumed, tx_hash, status)

            log.warning(
                "[%s] send attempt %s/%s failed: %s",
                recipient.idx, attempt, cfg.retry_limit, exc,
            )
            if attempt < cfg.retry_limit:
                # Before rebroadcasting, verify whether the previous call actually succeeded.
                if tx_is_known(rpc, tx_hash):
                    audit.write("found_after_send_error", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash)
                    if not cfg.wait_receipts:
                        return SendResult(True, True, tx_hash, "found_after_send_error")
                    status = wait_for_receipt(rpc, cfg, tx_hash)
                    audit.write("receipt", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash, status=status)
                    return SendResult(status == "confirmed", True, tx_hash, status)
                time.sleep(min(0.5 * (2 ** (attempt - 1)) + random.random() * 0.5, 8.0))

    known = tx_is_known(rpc, tx_hash)
    consumed = known or reconcile_nonce(rpc, treasury, nonce, tx_hash)
    audit.write(
        "send_unknown",
        idx=recipient.idx,
        nonce=nonce,
        tx_hash=tx_hash,
        nonce_consumed=consumed,
        error=str(last_error) if last_error else None,
    )
    log.error(
        "[%s] send result unknown tx=%s nonce=%s consumed=%s; stopping for manual review",
        recipient.idx, tx_hash, nonce, consumed,
    )
    return SendResult(False, consumed, tx_hash, "send_unknown")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def run() -> int:
    cfg = Config.from_env()
    lock = ProcessLock(cfg.lock_file)
    lock.acquire()
    audit = AuditLog(cfg.state_file)

    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_stop)

    account = Web3().eth.account.from_key(cfg.private_key)
    treasury = Web3.to_checksum_address(cfg.treasury_address or account.address)
    if treasury.lower() != account.address.lower():
        raise RuntimeError(
            f"TREASURY_ADDRESS={treasury} does not match PRIVATE_KEY address={account.address}"
        )

    recipients = load_recipients(cfg)
    if not recipients:
        log.warning("No recipients loaded")
        return 0

    rpc = RPCPool(cfg)
    chain_id = rpc.validate_nodes()
    fees, gas_limits = preflight(rpc, cfg, treasury, recipients)
    nonce = int(rpc.call(
        "get_pending_nonce",
        lambda w3: w3.eth.get_transaction_count(treasury, "pending"),
    ))

    audit.write(
        "batch_start",
        chain_id=chain_id,
        treasury=treasury,
        recipients=len(recipients),
        starting_nonce=nonce,
        dry_run=cfg.dry_run,
    )
    log.info("Chain ID: %s", chain_id)
    log.info("Starting nonce: %s", nonce)

    success = 0
    failed = 0
    for position, (recipient, gas_limit) in enumerate(zip(recipients, gas_limits), 1):
        if STOP_REQUESTED:
            audit.write("batch_interrupted", next_idx=recipient.idx, nonce=nonce)
            return 130

        if cfg.refresh_fees_every and position > 1 and (position - 1) % cfg.refresh_fees_every == 0:
            fees = get_fee_params(rpc, cfg)
            log.info(
                "Refreshed fees: priority=%s gwei max=%s gwei",
                wei_to_gwei_str(fees.max_priority_fee_per_gas),
                wei_to_gwei_str(fees.max_fee_per_gas),
            )
            # A refreshed fee may increase reserved cost; verify current balance for this tx.
            balance = int(rpc.call("balance_before_send", lambda w3: w3.eth.get_balance(treasury, "pending")))
            needed = recipient.value_wei + gas_limit * fees.max_fee_per_gas
            if balance < needed:
                raise RuntimeError(
                    f"Insufficient balance before recipient #{recipient.idx}: "
                    f"balance={wei_to_eth_str(balance)}, needed={wei_to_eth_str(needed)}"
                )

        result = send_one(
            rpc, cfg, audit, treasury, recipient, nonce, chain_id, fees, gas_limit
        )
        if result.nonce_consumed:
            nonce += 1
        if result.ok:
            success += 1
            continue

        failed += 1
        log.error(
            "Stopping batch at recipient #%s status=%s tx=%s",
            recipient.idx, result.status, result.tx_hash,
        )
        break

    audit.write(
        "batch_end",
        success=success,
        failed=failed,
        next_nonce=nonce,
        interrupted=STOP_REQUESTED,
        dry_run=cfg.dry_run,
    )
    log.info("DONE success=%s failed=%s dry_run=%s", success, failed, cfg.dry_run)
    return 0 if failed == 0 and not STOP_REQUESTED else (130 if STOP_REQUESTED else 1)


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except StopRequested:
        log.warning("Stopped")
        raise SystemExit(130)
    except KeyboardInterrupt:
        log.warning("Interrupted")
        raise SystemExit(130)
    except Exception as exc:
        log.exception("Fatal error: %s", exc)
        raise SystemExit(1)
