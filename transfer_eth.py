#!/usr/bin/env python3
"""
Reliable ETH Batch Payout Service v6

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
  STATE_FILE="payout_state_v6.jsonl"
  LOCK_FILE="payout.lock"              persistent .guard sidecar; do not delete while running

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

  WAIT_RECEIPTS=true                  false = broadcast only, not confirmed
  RECEIPT_TIMEOUT=180
  RECEIPT_POLL_INTERVAL=3
  CONFIRMATIONS=1
  CONFIRMATION_TIMEOUT=600

  ALLOW_DUPLICATE_RECIPIENTS=false
  MAX_RECIPIENTS=10000
  MAX_TOTAL_ETH=0                     0 = disabled
  CONTINUE_AFTER_RECEIPT_TIMEOUT=false  legacy option; ignored for safety
  RESUME=true                         skip recipients already confirmed in this exact batch
                                      and reconcile crash-window prepared transactions on-chain;
                                      retry only at the original prepared nonce;
                                      false intentionally bypasses duplicate protection
  STALE_LOCK_SECONDS=300            remove dead local lock after this age; 0 = never
  LOG_LEVEL=INFO
"""

from __future__ import annotations

import atexit
import hashlib
import json
import logging
import math
import re
import os
import random
import signal
import socket
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
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
    rpc_urls: tuple[str, ...] = field(repr=False)
    private_key: str = field(repr=False)
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
    resume: bool
    stale_lock_seconds: int

    @classmethod
    def from_env(cls) -> "Config":
        urls = tuple(dict.fromkeys(x.strip() for x in os.getenv("RPC_URLS", "").split(",") if x.strip()))
        cfg = cls(
            rpc_urls=urls,
            private_key=os.getenv("PRIVATE_KEY", "").strip(),
            treasury_address=os.getenv("TREASURY_ADDRESS", "").strip(),
            recipients_file=Path(os.getenv("RECIPIENTS_FILE", "recipients.json")),
            state_file=Path(os.getenv("STATE_FILE", "payout_state_v6.jsonl")),
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
            resume=env_bool("RESUME", True),
            stale_lock_seconds=env_int("STALE_LOCK_SECONDS", 300, 0),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        for name, value in (("RPC_COOLDOWN_SECONDS", self.rpc_cooldown_seconds),
                            ("RECEIPT_POLL_INTERVAL", self.receipt_poll_interval)):
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
        # Validate fixed-point configuration before connecting to a wallet or RPC.
        for name, value in (
            ("PRIORITY_FEE_GWEI", self.priority_fee_gwei),
            ("MAX_PRIORITY_FEE_GWEI", self.max_priority_fee_gwei),
            ("MAX_FEE_CAP_GWEI", self.max_fee_cap_gwei),
        ):
            try:
                gwei_to_wei(value)
            except (ValueError, InvalidOperation) as exc:
                raise ValueError(f"{name} must be specified in whole wei") from exc
        if self.max_total_eth:
            try:
                decimal_to_wei(self.max_total_eth, WEI_PER_ETH)
            except (ValueError, InvalidOperation) as exc:
                raise ValueError("MAX_TOTAL_ETH must be specified in whole wei") from exc
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
        resolved = {
            "RECIPIENTS_FILE": self.recipients_file.resolve(),
            "STATE_FILE": self.state_file.resolve(),
            "LOCK_FILE": self.lock_file.resolve(),
            "LOCK_GUARD": lock_guard_path(self.lock_file).resolve(),
        }
        if len(set(resolved.values())) != len(resolved):
            raise ValueError(
                "Recipient, state, lock and lock guard paths must all be distinct"
            )

        existing = [path for path in resolved.values() if path.exists()]
        for i, path in enumerate(existing):
            if any(path.samefile(other) for other in existing[:i]):
                raise ValueError("Recipient, state, lock and lock guard files must not be hard links to each other")

        for url in self.rpc_urls:
            parsed = urlsplit(url)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise ValueError(f"Invalid RPC URL: {redact_url(url)}")
            try:
                parsed.port  # Validate the optional port, if present.
            except ValueError as exc:
                raise ValueError(f"Invalid RPC URL: {redact_url(url)}") from exc


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("eth-batch-v6")


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
    """Convert amounts exactly, independent of the active Decimal precision."""
    if not value.is_finite():
        raise ValueError("Amount must be finite")
    if unit == WEI_PER_ETH:
        unit_decimals = 18
    elif unit == WEI_PER_GWEI:
        unit_decimals = 9
    else:
        raise ValueError("Unsupported base unit")

    if not value:
        return 0
    sign, digits, exponent = value.as_tuple()
    scaled_exponent = exponent + unit_decimals
    if scaled_exponent < 0:
        remove = -scaled_exponent
        if remove >= len(digits) or any(digits[-remove:]):
            raise ValueError(f"Value has too many decimal places: {value}")
        digits = digits[:-remove]
        scaled_exponent = 0
    if len(digits) + scaled_exponent > 78:
        raise ValueError(f"Amount exceeds uint256 wei range: {value}")
    result = int("".join(map(str, digits))) * 10 ** scaled_exponent
    if result > 2**256 - 1:
        raise ValueError(f"Amount exceeds uint256 wei range: {value}")
    return -result if sign else result


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


def _format_base_units(value: int, decimals: int) -> str:
    """Convert integer base units without rounding large balances."""
    sign = "-" if value < 0 else ""
    whole, fractional = divmod(abs(value), 10 ** decimals)
    if not fractional:
        return f"{sign}{whole}"
    return f"{sign}{whole}.{fractional:0{decimals}d}".rstrip("0")


def wei_to_eth_str(value: int) -> str:
    return f"{_format_base_units(value, 18)} ETH"


def wei_to_gwei_str(value: int) -> str:
    return _format_base_units(value, 9)


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
        if ":" in (p.hostname or ""):
            host = f"[{p.hostname}]" + (f":{p.port}" if p.port else "")
            if p.username or p.password:
                host = f"***@{host}"
        return urlunsplit((p.scheme, host, "/***" if p.path not in {"", "/"} else p.path,
                           "***" if p.query else "", ""))
    except Exception:
        return "<redacted-rpc-url>"


def safe_error(exc: BaseException) -> str:
    """Avoid exposing configured RPC URLs and signing keys in logs or state."""
    message = str(exc)
    for url in os.getenv("RPC_URLS", "").split(","):
        url = url.strip()
        if url:
            message = message.replace(url, redact_url(url))
            try:
                parsed = urlsplit(url)
                secrets = [parsed.username, parsed.password, parsed.query]
                secrets.extend(part for part in parsed.path.split("/") if len(part) >= 12)
                for secret in secrets:
                    if secret and len(secret) >= 4:
                        message = message.replace(secret, "***")
            except ValueError:
                pass
    key = os.getenv("PRIVATE_KEY", "").strip()
    if key:
        message = message.replace(key, "***")
        if key.lower().startswith("0x"):
            message = message.replace(key[2:], "***")
    return message


def normalize_tx_hash(value: Any) -> str:
    """Accept both HexBytes conventions and legacy unprefixed audit hashes."""
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).hex()
    if not isinstance(value, str):
        raise ValueError("Transaction hash must be bytes or a hex string")
    if value[:2].lower() == "0x":
        value = value[2:]
    if not re.fullmatch(r"[0-9a-fA-F]{64}", value):
        raise ValueError("Invalid 32-byte transaction hash")
    return "0x" + value.lower()


def error_text(exc: BaseException) -> str:
    return str(exc).lower()


def is_already_known(exc: BaseException) -> bool:
    text = error_text(exc)
    return bool(re.search(
        r"\b(?:already known|already imported|known transaction|transaction already in mempool)\b",
        text,
    )) and not re.search(r"\b(?:not (?:a )?known transaction|not already known)\b", text)


def is_nonce_too_low(exc: BaseException) -> bool:
    text = error_text(exc)
    return "nonce too low" in text or "nonce has already been used" in text


def is_replacement_underpriced(exc: BaseException) -> bool:
    text = error_text(exc)
    return "replacement transaction underpriced" in text or "replacement fee too low" in text


def sleep_interruptibly(seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while True:
        if STOP_REQUESTED:
            raise StopRequested("Stop requested")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 0.25))


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

def lock_guard_path(path: Path) -> Path:
    path = path.resolve()
    return path.with_name(path.name + ".guard")


def fsync_directory(path: Path) -> None:
    """Persist directory entries on POSIX before broadcasting any transaction."""
    if os.name == "nt":
        return  # Python does not expose portable directory fsync on Windows.
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class ProcessLock:
    def __init__(self, path: Path, stale_after_seconds: int):
        self.path = path
        self.stale_after_seconds = stale_after_seconds
        self.acquired = False
        self.token = uuid.uuid4().hex
        self.guard_fd: Optional[int] = None

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        if os.name == "nt":
            # os.kill(pid, 0) is not a safe existence check on Windows.
            import ctypes
            from ctypes import wintypes
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
            kernel.OpenProcess.restype = wintypes.HANDLE
            kernel.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
            kernel.WaitForSingleObject.restype = wintypes.DWORD
            kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
            handle = kernel.OpenProcess(0x00100000, False, pid)  # SYNCHRONIZE
            if not handle:
                return ctypes.get_last_error() != 87  # ERROR_INVALID_PARAMETER
            try:
                return kernel.WaitForSingleObject(handle, 0) != 0  # WAIT_OBJECT_0
            finally:
                kernel.CloseHandle(handle)
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return True  # Unknown liveness: do not reclaim the lock.
        return True

    def _read_existing(self) -> dict[str, Any]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}

    def _remove_stale_local_lock(self) -> bool:
        if not self.path.exists():
            return True
        info = self._read_existing()
        try:
            pid = int(info.get("pid", 0) or 0)
            started_at = int(info.get("started_at", 0) or 0)
        except (TypeError, ValueError):
            return False
        host = str(info.get("host", ""))
        age = max(0, int(time.time()) - started_at) if started_at else None
        same_host = bool(host) and host == socket.gethostname()
        dead_local_process = same_host and pid > 0 and not self._pid_is_alive(pid)
        old_enough = (
            self.stale_after_seconds > 0
            and age is not None
            and age >= self.stale_after_seconds
        )
        if dead_local_process and old_enough:
            # Do not delete a replacement lock if it changed during inspection.
            if self._read_existing() != info:
                return False
            log.warning(
                "Removing stale lock %s pid=%s age=%ss", self.path, pid, age
            )
            self.path.unlink(missing_ok=True)
            return True
        return False

    def _acquire_guard(self) -> None:
        guard = lock_guard_path(self.path)
        fd = os.open(guard, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            if os.name == "nt":
                import msvcrt
                # Windows byte-range locks may extend beyond EOF.
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(fd)
            raise
        self.guard_fd = fd

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._acquire_guard()
        try:
            self._acquire_lock_file()
        except BaseException:
            self.release()
            raise

    def _acquire_lock_file(self) -> None:
        for attempt in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                break
            except FileExistsError as exc:
                if attempt == 0 and self._remove_stale_local_lock():
                    continue
                info = self._read_existing()
                raise RuntimeError(
                    f"Lock file already exists: {self.path}; details={info or 'unreadable'}. "
                    "Another payout process may be running. Remove it only after confirming no process is active."
                ) from exc
        else:
            raise RuntimeError(f"Unable to acquire lock: {self.path}")

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "pid": os.getpid(),
                    "host": socket.gethostname(),
                    "started_at": int(time.time()),
                    "token": self.token,
                }) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            self.path.unlink(missing_ok=True)
            raise
        self.acquired = True
        atexit.register(self.release)

    def release(self) -> None:
        try:
            if self.acquired:
                try:
                    if self._read_existing().get("token") == self.token:
                        self.path.unlink(missing_ok=True)
                finally:
                    self.acquired = False
        finally:
            if self.guard_fd is not None:
                fd, self.guard_fd = self.guard_fd, None
                os.close(fd)  # releases OS lock; never unlink the guard file
            atexit.unregister(self.release)


class AuditLog:
    def __init__(self, path: Path, context: Optional[dict[str, Any]] = None):
        self.path = path
        self.context = dict(context or {})
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def set_context(self, **fields: Any) -> None:
        self.context.update(fields)

    def write(self, event: str, **fields: Any) -> None:
        record = {"ts": int(time.time()), **self.context, "event": event, **fields}
        line = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        # New audit files contain payment metadata and must not be world-readable.
        new_file = not self.path.exists()
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        if new_file:
            fsync_directory(self.path.parent)

    def records_for_batch(self, batch_id: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        try:
            handle = self.path.open("r", encoding="utf-8")
        except FileNotFoundError:
            return []
        with handle:
            for line_no, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line, object_pairs_hook=unique_json_object)
                except ValueError as exc:
                    raise RuntimeError(
                        f"Malformed audit log line {line_no} in {self.path}: {exc}. "
                        "Refusing resume because ignoring state corruption could duplicate a payout."
                    ) from exc
                if not isinstance(item, dict) or not isinstance(item.get("event"), str):
                    raise RuntimeError(f"Invalid audit record at line {line_no} in {self.path}; refusing resume")
                if item.get("batch_id") == batch_id:
                    records.append(item)
        return records


# -----------------------------------------------------------------------------
# RPC pool
# -----------------------------------------------------------------------------

@dataclass
class RPCNode:
    url: str = field(repr=False)
    w3: Web3 = field(repr=False)
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
        genesis_hashes: set[str] = set()
        for node in self.nodes:
            try:
                chain_id = int(node.w3.eth.chain_id)
                if chain_id <= 0:
                    raise ValueError("Invalid RPC chain ID")
                node.chain_id = chain_id
                if self.cfg.chain_id_override and chain_id != self.cfg.chain_id_override:
                    raise RuntimeError(
                        f"wrong chain ID {chain_id}; expected {self.cfg.chain_id_override}"
                    )
                genesis_hashes.add(normalize_tx_hash(node.w3.eth.get_block(0)["hash"]))
                detected_ids.add(chain_id)
                valid.append(node)
                log.info("Validated RPC %s chain_id=%s", redact_url(node.url), chain_id)
            except Exception as exc:
                log.warning("Ignoring RPC %s: %s", redact_url(node.url), safe_error(exc))

        if not valid:
            raise RuntimeError("No valid RPC nodes available")
        if len(detected_ids) != 1 or len(genesis_hashes) != 1:
            raise RuntimeError(f"RPC_URLS point to different chains or genesis blocks: {sorted(detected_ids)}")

        self.nodes = valid
        self.expected_chain_id = self.cfg.chain_id_override or next(iter(detected_ids))
        return self.expected_chain_id

    def _choose_node(self) -> RPCNode:
        now = time.monotonic()
        candidates = [n for n in self.nodes if n.cooldown_until <= now]
        if not candidates:
            # With a single RPC, ignoring cooldown would hammer a rate-limited
            # endpoint on each retry. Respect it even when all nodes failed.
            earliest = min(n.cooldown_until for n in self.nodes)
            delay = max(0.0, earliest - now)
            if delay:
                sleep_interruptibly(delay)
            if STOP_REQUESTED:
                raise StopRequested("Stop requested")
            candidates = [n for n in self.nodes if n.cooldown_until <= time.monotonic()]
            if not candidates:
                candidates = [min(self.nodes, key=lambda n: n.cooldown_until)]
        # Rotate across available nodes, including recovered nodes, so a
        # healthy but lagging endpoint cannot monopolize receipt polling.
        node = candidates[self.cursor % len(candidates)]
        self.cursor += 1
        return node

    def call(self, name: str, fn: Callable[[Web3], T], retries: Optional[int] = None) -> T:
        attempts = self.cfg.retry_limit if retries is None else retries
        if attempts < 1:
            raise ValueError("RPC call attempts must be >= 1")
        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            if STOP_REQUESTED:
                raise StopRequested("Stop requested")
            node = self._choose_node()
            try:
                if node.chain_id is not None and self.expected_chain_id is not None:
                    if node.chain_id != self.expected_chain_id:
                        raise RuntimeError("RPC chain ID mismatch")
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
                    name, redact_url(node.url), attempt, attempts, safe_error(exc),
                )
                if attempt < attempts:
                    sleep_interruptibly(min(0.35 * (2 ** min(attempt - 1, 5)) + random.random() * 0.25, 5.0))
        assert last_exc is not None
        raise RuntimeError(f"RPC call failed after {attempts} attempts: {name}: {safe_error(last_exc)}") from last_exc

    def send_raw(self, raw_tx: bytes) -> str:
        result = self.call(
            "send_raw_transaction",
            lambda w3: w3.eth.send_raw_transaction(raw_tx),
            retries=1,
        )
        return normalize_tx_hash(result)


# -----------------------------------------------------------------------------
# Recipients, fees, gas and preflight
# -----------------------------------------------------------------------------

def unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key!r}")
        result[key] = value
    return result


def load_recipients(cfg: Config) -> list[Recipient]:
    try:
        raw = json.loads(cfg.recipients_file.read_text(encoding="utf-8-sig"),
                         parse_float=Decimal, object_pairs_hook=unique_json_object)
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
        if int(to, 16) == 0:
            raise ValueError(f"Recipient #{idx} is the zero address")
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


def ceil_scaled_int(value: int, multiplier: Decimal) -> int:
    numerator, denominator = multiplier.as_integer_ratio()
    return (value * numerator + denominator - 1) // denominator


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

        max_fee = ceil_scaled_int(base_fee, cfg.base_fee_multiplier) + tip
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
    buffered = ceil_scaled_int(estimated, cfg.gas_buffer)
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


def make_batch_id(chain_id: int, treasury: str, recipients: Sequence[Recipient]) -> str:
    payload = {
        "chain_id": chain_id,
        "treasury": treasury.lower(),
        "recipients": [
            {"idx": r.idx, "to": r.to.lower(), "value_wei": r.value_wei}
            for r in recipients
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def receipt_status(receipt: Any, tx_hash: str) -> int:
    if normalize_tx_hash(receipt["transactionHash"]) != normalize_tx_hash(tx_hash):
        raise RuntimeError("RPC receipt transaction hash mismatch")
    status = int(receipt["status"])
    if status not in (0, 1):
        raise RuntimeError("Invalid RPC receipt status")
    return status


def receipt_canonical_on_node(w3: Web3, receipt: Any) -> bool:
    block = w3.eth.get_block(int(receipt["blockNumber"]))
    return normalize_tx_hash(block["hash"]) == normalize_tx_hash(receipt["blockHash"])


def _query_tx_state_across_nodes(
    rpc: RPCPool,
    treasury: str,
    nonce: int,
    tx_hash: str,
) -> tuple[str, Optional[Any]]:
    """
    Conservatively reconcile a transaction across every validated RPC node.

    Returns:
      ("confirmed", receipt)  - successful receipt observed
      ("reverted", receipt)   - failed receipt observed
      ("known", None)         - tx is known/pending but has no receipt yet
      ("not_broadcast", None) - every responsive RPC says tx is unknown and
                                no observed nonce has advanced beyond this nonce
      ("ambiguous", None)     - inconsistent/unavailable evidence; manual review
    """
    responsive = 0
    known = False
    successful_receipt: Optional[Any] = None
    reverted_receipt: Optional[Any] = None
    pending_nonces: list[int] = []
    latest_nonces: list[int] = []

    for node in rpc.nodes:
        if STOP_REQUESTED:
            raise StopRequested("Stop requested")
        try:
            try:
                receipt = node.w3.eth.get_transaction_receipt(tx_hash)
            except TransactionNotFound:
                receipt = None

            if receipt is not None:
                known = True
                status = receipt_status(receipt, tx_hash)
                if receipt_canonical_on_node(node.w3, receipt):
                    if status == 1:
                        successful_receipt = receipt
                    else:
                        reverted_receipt = receipt

            try:
                transaction = node.w3.eth.get_transaction(tx_hash)
                if transaction is not None:
                    if normalize_tx_hash(transaction["hash"]) != normalize_tx_hash(tx_hash):
                        raise RuntimeError("RPC transaction hash mismatch")
                    known = True
            except TransactionNotFound:
                pass

            pending = int(node.w3.eth.get_transaction_count(treasury, "pending"))
            latest = int(node.w3.eth.get_transaction_count(treasury, "latest"))
            if pending < latest or latest < 0:
                raise RuntimeError("Invalid RPC nonce observations")
            pending_nonces.append(pending)
            latest_nonces.append(latest)
            responsive += 1
        except Exception as exc:
            log.warning(
                "Resume reconciliation RPC failed rpc=%s tx=%s error=%s",
                redact_url(node.url), tx_hash, safe_error(exc),
            )

    # Absence is much harder to prove than presence. A failed/unreachable RPC
    # must never help us conclude that a transaction was never broadcast.
    all_nodes_responsive = responsive == len(rpc.nodes)

    if successful_receipt is not None and reverted_receipt is not None:
        # Nodes disagree on canonical-chain state. Never guess.
        return "ambiguous", None
    if successful_receipt is not None:
        return "confirmed", successful_receipt
    if reverted_receipt is not None:
        return "reverted", reverted_receipt
    if known:
        return "known", None

    # Safe automatic retry is allowed only when every responsive observation
    # shows that neither pending nor mined nonce has advanced past this nonce.
    if (
        all_nodes_responsive
        and pending_nonces
        and latest_nonces
        and len(pending_nonces) == len(rpc.nodes)
        and len(latest_nonces) == len(rpc.nodes)
        and max(pending_nonces) <= nonce
        and max(latest_nonces) <= nonce
    ):
        return "not_broadcast", None

    return "ambiguous", None


@dataclass
class RecoveryState:
    confirmed: set[int]
    safe_retry_nonces: dict[int, int]


def recover_batch_progress(
    audit: AuditLog,
    batch_id: str,
    recipients: Sequence[Recipient],
    resume: bool,
    rpc: RPCPool,
    treasury: str,
    cfg: Config,
) -> RecoveryState:
    """
    Recover only payouts proven complete.

    Critical invariant:
    a prior "prepared" record is NOT silently ignored. It may represent the
    crash window where eth_sendRawTransaction succeeded but the process died
    before the "sent" audit record was fsynced.
    """
    if not resume:
        return RecoveryState(set(), {})

    records = audit.records_for_batch(batch_id)
    if not records:
        return RecoveryState(set(), {})

    by_idx = {r.idx: r for r in recipients}
    confirmed: set[int] = set()
    evidence: dict[int, tuple[int, set[str]]] = {}
    confirmed_hashes: dict[int, set[str]] = {}
    nonce_owners: dict[int, int] = {}
    safe_retry_nonces: dict[int, int] = {}
    inconsistent_evidence: set[int] = set()
    evidence_events = {
        "prepared", "sent", "already_known", "found_after_send_error",
        "send_unknown", "nonce_consumed", "nonce_conflict_unresolved", "receipt",
        "resume_reconciled_confirmed", "resume_reconciled_not_broadcast",
    }

    for record in records:
        event = record.get("event")
        if event == "dry_run" or record.get("dry_run") is True:
            continue
        if event not in evidence_events:
            continue
        idx = record.get("idx")
        if type(idx) is not int or idx not in by_idx:
            raise RuntimeError("Invalid recipient index in transaction audit evidence")
        nonce = record.get("nonce")
        try:
            tx_hash = normalize_tx_hash(record.get("tx_hash"))
        except ValueError:
            inconsistent_evidence.add(idx)
            continue
        if type(nonce) is not int or nonce < 0:
            inconsistent_evidence.add(idx)
            continue
        recipient = by_idx[idx]
        if ("to" in record and (not isinstance(record["to"], str)
                               or record["to"].lower() != recipient.to.lower())) or (
            "value_wei" in record and (type(record["value_wei"]) is not int
                                       or record["value_wei"] != recipient.value_wei)
        ):
            inconsistent_evidence.add(idx)
            continue
        owner = nonce_owners.setdefault(nonce, idx)
        if owner != idx:
            inconsistent_evidence.update((owner, idx))
        original_nonce, hashes = evidence.setdefault(idx, (nonce, set()))
        if original_nonce != nonce:
            inconsistent_evidence.add(idx)
        hashes.add(tx_hash)
        if (event == "receipt" and record.get("status") == "confirmed") or event == "resume_reconciled_confirmed":
            confirmed_hashes.setdefault(idx, set()).add(tx_hash)
            confirmed.add(idx)

    inconsistent_evidence.update(idx for idx, hashes in confirmed_hashes.items() if len(hashes) > 1)
    if inconsistent_evidence:
        raise RuntimeError(
            "Conflicting or incomplete audit transaction evidence for recipients "
            + ", ".join(f"#{idx}" for idx in sorted(inconsistent_evidence))
            + ". Refusing automatic retry to prevent duplicate payouts."
        )

    ambiguous: list[str] = []
    for idx, (nonce, hashes) in sorted(evidence.items()):
        if idx in confirmed:
            continue  # Existing durable confirmation remains authoritative.
        # Fee refreshes can create multiple hashes for one nonce. A previously
        # prepared hash may be the one that eventually mined; never forget it.
        states = [(tx_hash, *_query_tx_state_across_nodes(rpc, treasury, nonce, tx_hash))
                  for tx_hash in sorted(hashes)]
        mined = [(h, state) for h, state, _ in states if state in {"confirmed", "reverted"}]
        if len(mined) > 1:
            ambiguous.append(f"#{idx}:conflicting_mined_hashes")
            continue
        candidates = [h for h, state, _ in states if state == "confirmed"]
        if not candidates and not mined:
            known = [h for h, state, _ in states if state == "known"]
            if len(known) == 1:
                candidates = known
        if candidates:
            tx_hash = candidates[0]
            confirmation_status = wait_for_receipt(rpc, cfg, tx_hash)
            if confirmation_status == "confirmed":
                confirmed.add(idx)
                audit.write("resume_reconciled_confirmed", idx=idx, nonce=nonce, tx_hash=tx_hash)
                log.warning("Recovered recipient #%s as confirmed from on-chain state tx=%s", idx, tx_hash)
                continue
            ambiguous.append(f"#{idx}:reconcile_{confirmation_status}:{tx_hash}")
            continue
        if all(state == "not_broadcast" for _, state, _ in states):
            for tx_hash, _, _ in states:
                audit.write("resume_reconciled_not_broadcast", idx=idx, nonce=nonce, tx_hash=tx_hash)
            safe_retry_nonces[idx] = nonce
            log.warning("Recovered recipient #%s as safe-to-retry ONLY at original nonce=%s", idx, nonce)
            continue
        ambiguous.append(f"#{idx}:" + ",".join(f"{state}:{h}" for h, state, _ in states))

    if ambiguous:
        raise RuntimeError(
            "Previous run contains unresolved transaction state: "
            + ", ".join(ambiguous)
            + ". Refusing automatic resend to prevent duplicate payouts. "
              "Resolve these transactions on-chain before continuing."
        )

    return RecoveryState(confirmed, safe_retry_nonces)


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


def receipt_is_canonical(rpc: RPCPool, receipt: Any) -> bool:
    """Confirm that the receipt's inclusion block is still canonical."""
    block_number = int(receipt["blockNumber"])
    block = rpc.call("receipt_canonical_block", lambda w3: w3.eth.get_block(block_number))
    return block["hash"] == receipt["blockHash"]


def wait_for_receipt(rpc: RPCPool, cfg: Config, tx_hash: str) -> str:
    deadline = time.monotonic() + cfg.receipt_timeout
    confirm_deadline: Optional[float] = None
    inclusion: Optional[tuple[int, str]] = None

    def observe(w3: Web3) -> tuple[Optional[Any], bool, int]:
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
        except TransactionNotFound:
            return None, False, 0
        if receipt is None:
            return None, False, 0
        receipt_status(receipt, tx_hash)
        if not receipt_canonical_on_node(w3, receipt):
            return receipt, False, 0
        block_number = int(receipt["blockNumber"])
        if cfg.confirmations <= 1:
            return receipt, True, 1
        depth = int(w3.eth.block_number) - block_number + 1
        if depth >= cfg.confirmations:
            # Check again after reading the head so a changed inclusion cannot
            # borrow confirmation depth from an older block.
            if not receipt_canonical_on_node(w3, receipt):
                return receipt, False, 0
        return receipt, True, max(0, depth)

    try:
        while time.monotonic() < (confirm_deadline if confirm_deadline is not None else deadline):
            receipt, canonical, depth = rpc.call("observe_receipt", observe)
            if receipt is not None:
                if not canonical:
                    return "reorged"
                current = (int(receipt["blockNumber"]), normalize_tx_hash(receipt["blockHash"]))
                if inclusion is not None and inclusion != current:
                    return "reorged"
                if receipt_status(receipt, tx_hash) == 0:
                    return "reverted"
                if depth >= cfg.confirmations:
                    return "confirmed"
                if confirm_deadline is None:
                    inclusion = current
                    confirm_deadline = time.monotonic() + cfg.confirmation_timeout
            # A lagging RPC may temporarily not know an observed receipt.
            # Do not treat that alone as a reorg or reset the timeout.
            remaining = (confirm_deadline if confirm_deadline is not None else deadline) - time.monotonic()
            sleep_interruptibly(max(0.0, min(cfg.receipt_poll_interval, remaining)))
    except StopRequested:
        return "interrupted"
    return "confirmation_timeout" if confirm_deadline is not None else "receipt_timeout"


def tx_is_known(rpc: RPCPool, tx_hash: str) -> bool:
    """Return True if any validated RPC can prove the transaction is known."""
    for node in rpc.nodes:
        if STOP_REQUESTED:
            raise StopRequested("Stop requested")
        try:
            transaction = node.w3.eth.get_transaction(tx_hash)
            if transaction is not None and normalize_tx_hash(transaction["hash"]) == normalize_tx_hash(tx_hash):
                return True
        except TransactionNotFound:
            continue
        except Exception as exc:
            log.warning(
                "Transaction lookup failed rpc=%s tx=%s error=%s",
                redact_url(node.url), tx_hash, safe_error(exc),
            )
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
    except StopRequested:
        raise
    except Exception:
        return False
    return pending_nonce > nonce or latest_nonce > nonce


def observed_pending_nonce(rpc: RPCPool, treasury: str) -> int:
    """Use the highest observed pending nonce to avoid replacing known transfers.

    An unavailable node cannot establish that a nonce is free, so fail closed.
    """
    pending: list[int] = []
    for node in rpc.nodes:
        if STOP_REQUESTED:
            raise StopRequested("Stop requested")
        try:
            latest = int(node.w3.eth.get_transaction_count(treasury, "latest"))
            candidate = int(node.w3.eth.get_transaction_count(treasury, "pending"))
            if candidate < latest or latest < 0:
                raise RuntimeError("pending nonce is below mined nonce")
            pending.append(candidate)
        except Exception as exc:
            raise RuntimeError(
                f"Unable to verify nonce at RPC {redact_url(node.url)}; "
                "refusing to risk nonce replacement"
            ) from exc
    if not pending:
        raise RuntimeError("No RPC nodes available to verify nonce")
    if len(set(pending)) > 1:
        log.warning("RPC pending nonce observations differ: %s; using highest", pending)
    return max(pending)


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
    tx_hash = normalize_tx_hash(Web3.keccak(raw_bytes))
    return tx, raw_bytes, tx_hash


def _finish_send(
    rpc: RPCPool,
    cfg: Config,
    audit: AuditLog,
    recipient: Recipient,
    nonce: int,
    tx_hash: str,
    source: str,
) -> SendResult:
    """Handle an accepted transaction without accidentally rebroadcasting on lookup errors."""
    if not cfg.wait_receipts:
        return SendResult(True, True, tx_hash, source)

    try:
        status = wait_for_receipt(rpc, cfg, tx_hash)
    except Exception as exc:
        # The prepared/sent evidence has already been durably written. A
        # receipt RPC failure does not imply that sending failed.
        log.error("[%s] receipt lookup failed for %s: %s", recipient.idx, tx_hash, safe_error(exc))
        status = "receipt_lookup_error"
    audit.write("receipt", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash, status=status)
    if status in {"receipt_timeout", "confirmation_timeout"}:
        log.warning("[%s] %s for %s", recipient.idx, status, tx_hash)
        if cfg.continue_after_receipt_timeout:
            log.warning(
                "CONTINUE_AFTER_RECEIPT_TIMEOUT is ignored for safety; "
                "restart with RESUME=true after RPC state converges"
            )
    elif status != "confirmed":
        log.error("[%s] transaction status=%s tx=%s", recipient.idx, status, tx_hash)
    return SendResult(status == "confirmed", True, tx_hash, status)


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
    _tx, raw_tx, tx_hash = build_signed_transaction(
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
        audit.write("dry_run", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash, dry_run=True)
        return SendResult(True, False, tx_hash, "dry_run")

    last_error: Optional[BaseException] = None
    for attempt in range(1, cfg.retry_limit + 1):
        if STOP_REQUESTED:
            return SendResult(False, False, tx_hash, "interrupted_before_send")
        try:
            returned_hash = rpc.send_raw(raw_tx)
        except StopRequested:
            raise
        except Exception as exc:
            last_error = exc
            if is_already_known(exc):
                audit.write("already_known", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash)
                return _finish_send(rpc, cfg, audit, recipient, nonce, tx_hash, "already_known")

            if is_nonce_too_low(exc) or is_replacement_underpriced(exc):
                if tx_is_known(rpc, tx_hash):
                    audit.write("found_after_send_error", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash)
                    return _finish_send(rpc, cfg, audit, recipient, nonce, tx_hash, "found_after_send_error")
                consumed = reconcile_nonce(rpc, treasury, nonce, tx_hash)
                status = "nonce_consumed" if consumed else "nonce_conflict_unresolved"
                audit.write(
                    status,
                    idx=recipient.idx,
                    nonce=nonce,
                    tx_hash=tx_hash,
                    error=safe_error(exc),
                )
                log.error(
                    "[%s] nonce conflict nonce=%s consumed=%s tx=%s error=%s",
                    recipient.idx, nonce, consumed, tx_hash, safe_error(exc),
                )
                return SendResult(False, consumed, tx_hash, status)

            log.warning(
                "[%s] send attempt %s/%s failed: %s",
                recipient.idx, attempt, cfg.retry_limit, safe_error(exc),
            )
            if attempt < cfg.retry_limit:
                if tx_is_known(rpc, tx_hash):
                    audit.write("found_after_send_error", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash)
                    return _finish_send(
                        rpc, cfg, audit, recipient, nonce, tx_hash, "found_after_send_error"
                    )
                sleep_interruptibly(min(0.5 * (2 ** min(attempt - 1, 5)) + random.random() * 0.5, 8.0))
            continue

        if returned_hash.lower() != tx_hash.lower():
            # The remote node may have broadcast *something*; fail closed.
            audit.write(
                "send_unknown", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash,
                returned_hash=returned_hash, error="unexpected transaction hash",
            )
            log.error("[%s] RPC returned unexpected transaction hash; stopping", recipient.idx)
            return SendResult(False, False, tx_hash, "unexpected_tx_hash")

        log.info(
            "[%s] sent %s -> %s amount=%s nonce=%s",
            recipient.idx, tx_hash, short_addr(recipient.to),
            wei_to_eth_str(recipient.value_wei), nonce,
        )
        audit.write("sent", idx=recipient.idx, to=recipient.to, nonce=nonce, tx_hash=tx_hash)
        return _finish_send(rpc, cfg, audit, recipient, nonce, tx_hash, "broadcast")

    if tx_is_known(rpc, tx_hash):
        audit.write("found_after_send_error", idx=recipient.idx, nonce=nonce, tx_hash=tx_hash)
        return _finish_send(rpc, cfg, audit, recipient, nonce, tx_hash, "found_after_send_error")
    consumed = reconcile_nonce(rpc, treasury, nonce, tx_hash)
    audit.write(
        "send_unknown",
        idx=recipient.idx,
        nonce=nonce,
        tx_hash=tx_hash,
        nonce_consumed=consumed,
        error=safe_error(last_error) if last_error else None,
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
    lock = ProcessLock(cfg.lock_file, cfg.stale_lock_seconds)
    lock.acquire()
    try:
        return run_locked(cfg)
    finally:
        lock.release()


def run_locked(cfg: Config) -> int:
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
    self_payments = [r.idx for r in recipients if r.to.lower() == treasury.lower()]
    if self_payments:
        raise ValueError(
            "Treasury address appears as a recipient at indices "
            + ", ".join(map(str, self_payments))
        )
    if not recipients:
        log.warning("No recipients loaded")
        return 0

    rpc = RPCPool(cfg)
    chain_id = rpc.validate_nodes()
    batch_id = make_batch_id(chain_id, treasury, recipients)
    audit.set_context(batch_id=batch_id, chain_id=chain_id, treasury=treasury)
    recovery = recover_batch_progress(audit, batch_id, recipients, cfg.resume, rpc, treasury, cfg)
    completed_indices = recovery.confirmed
    pending_recipients = [r for r in recipients if r.idx not in completed_indices]
    if completed_indices:
        log.info("Resuming batch %s: already confirmed=%s", batch_id[:12], len(completed_indices))
    if not pending_recipients:
        log.info("Batch %s is already complete", batch_id[:12])
        audit.write("batch_already_complete", recipients=len(recipients))
        return 0
    fees, pending_gas_limits = preflight(rpc, cfg, treasury, pending_recipients)
    gas_by_idx = {r.idx: gas for r, gas in zip(pending_recipients, pending_gas_limits)}
    nonce = observed_pending_nonce(rpc, treasury)
    # Detect an inconsistent recovery plan before sending even the first payout.
    for offset, recipient in enumerate(pending_recipients):
        pinned = recovery.safe_retry_nonces.get(recipient.idx)
        if pinned is not None and pinned != nonce + offset:
            raise RuntimeError(f"Unsafe retry plan for recipient #{recipient.idx}: original nonce={pinned}, planned={nonce + offset}")

    audit.write(
        "batch_start",
        recipients=len(recipients),
        remaining=len(pending_recipients),
        starting_nonce=nonce,
        dry_run=cfg.dry_run,
    )
    log.info("Chain ID: %s", chain_id)
    log.info("Batch ID: %s", batch_id)
    log.info("Starting nonce: %s", nonce)

    success = 0
    failed = 0
    for position, recipient in enumerate(pending_recipients, 1):
        gas_limit = gas_by_idx[recipient.idx]
        if STOP_REQUESTED:
            audit.write("batch_interrupted", next_idx=recipient.idx, nonce=nonce)
            return 130

        pinned_nonce = recovery.safe_retry_nonces.get(recipient.idx)
        if pinned_nonce is not None and nonce != pinned_nonce:
            raise RuntimeError(
                f"Unsafe retry for recipient #{recipient.idx}: original nonce={pinned_nonce}, "
                f"current nonce={nonce}. A different nonce could duplicate the payment."
            )
        if not cfg.dry_run:
            network_nonce = observed_pending_nonce(rpc, treasury)
            if network_nonce < nonce and cfg.wait_receipts:
                raise RuntimeError(f"Pending nonce fell behind before recipient #{recipient.idx}: expected={nonce}, observed={network_nonce}; stopping")
            if network_nonce > nonce:
                raise RuntimeError(
                    f"Pending nonce advanced unexpectedly before recipient #{recipient.idx}: "
                    f"expected={nonce}, observed={network_nonce}. "
                    "Refusing to replace another pending transaction."
                )

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
        if result.nonce_consumed or cfg.dry_run:
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
    if failed == 0 and not STOP_REQUESTED:
        audit.write(
            "batch_complete" if cfg.wait_receipts or cfg.dry_run else "batch_broadcast_complete",
            success=success,
            total_recipients=len(recipients),
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
        log.error("Fatal error: %s", safe_error(exc))
        raise SystemExit(1)
