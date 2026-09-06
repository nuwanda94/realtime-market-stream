"""Token-bucket rate limiting and bounded-inflight backpressure.

Local-first, zero extra dependencies. Used by the ingestion publisher
(so a fast synthetic source cannot overwhelm Redpanda) and by the FastAPI
layer (so a noisy client cannot melt the lakehouse reader).

Design
------
* :class:`TokenBucket` — classic refill-per-second limiter.
* :class:`InflightGate` — blocks producers once unacked sends hit a cap.
* :class:`FlowController` — composes both from :class:`FlowControlSettings`.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Protocol

from realtime_market_stream.config.settings import FlowControlSettings


class EventPublisherLike(Protocol):
    """Subset of the ingestion EventPublisher used by the wrapper."""

    def send(self, topic: str, key: bytes | None, value: bytes) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


@dataclass
class TokenBucket:
    """Thread-safe token bucket.

    ``rate_per_sec=0`` disables limiting (every acquire succeeds immediately).
    """

    rate_per_sec: float
    burst: float | None = None
    _tokens: float = field(init=False)
    _updated: float = field(init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.rate_per_sec < 0:
            raise ValueError("rate_per_sec must be >= 0")
        capacity = float(self.burst) if self.burst is not None else max(self.rate_per_sec, 1.0)
        if capacity < 1:
            capacity = 1.0
        self.burst = capacity
        self._tokens = capacity
        self._updated = time.monotonic()

    @property
    def disabled(self) -> bool:
        return self.rate_per_sec <= 0

    def _refill_unlocked(self) -> None:
        if self.disabled:
            self._tokens = float(self.burst or 1.0)
            self._updated = time.monotonic()
            return
        now = time.monotonic()
        elapsed = now - self._updated
        self._tokens = min(float(self.burst or 1.0), self._tokens + elapsed * self.rate_per_sec)
        self._updated = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Take ``tokens`` if available; never block."""
        if tokens <= 0:
            return True
        with self._lock:
            if self.disabled:
                return True
            self._refill_unlocked()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def acquire(self, tokens: float = 1.0, timeout: float | None = None) -> bool:
        """Block until tokens are available or ``timeout`` elapses."""
        if self.disabled or tokens <= 0:
            return True
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill_unlocked()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                missing = tokens - self._tokens
                wait = missing / self.rate_per_sec if self.rate_per_sec > 0 else 0.0
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                wait = min(wait, remaining)
            time.sleep(max(wait, 0.001))

    def snapshot_tokens(self) -> float:
        with self._lock:
            self._refill_unlocked()
            return self._tokens


@dataclass
class InflightGate:
    """Backpressure gate for unacknowledged producer sends.

    ``max_inflight=0`` disables the gate.
    """

    max_inflight: int
    _count: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _slot: threading.Condition = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_inflight < 0:
            raise ValueError("max_inflight must be >= 0")
        self._slot = threading.Condition(self._lock)

    @property
    def disabled(self) -> bool:
        return self.max_inflight <= 0

    @property
    def inflight(self) -> int:
        with self._lock:
            return self._count

    def try_acquire(self) -> bool:
        with self._lock:
            if self.disabled or self._count < self.max_inflight:
                self._count += 1
                return True
            return False

    def acquire(self, timeout: float | None = None) -> bool:
        if self.disabled:
            with self._lock:
                self._count += 1
            return True
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._slot:
            while self._count >= self.max_inflight:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._slot.wait(timeout=remaining)
            self._count += 1
            return True

    def release(self) -> None:
        with self._slot:
            if self._count > 0:
                self._count -= 1
            self._slot.notify()


@dataclass
class FlowStats:
    """Counters exposed to tests and /metrics-style dumps."""

    rate_limited: int = 0
    backpressure_waits: int = 0
    dropped: int = 0
    acquired: int = 0


@dataclass
class FlowController:
    """Compose rate limit + inflight gate from settings."""

    settings: FlowControlSettings
    stats: FlowStats = field(default_factory=FlowStats)
    ingest_bucket: TokenBucket = field(init=False)
    api_bucket: TokenBucket = field(init=False)
    inflight: InflightGate = field(init=False)

    def __post_init__(self) -> None:
        ingest_rate = float(self.settings.ingest_max_per_sec)
        api_rate = float(self.settings.api_requests_per_sec)
        if not self.settings.enabled:
            ingest_rate = 0.0
            api_rate = 0.0
        self.ingest_bucket = TokenBucket(
            rate_per_sec=ingest_rate,
            burst=float(self.settings.ingest_burst or max(ingest_rate, 1.0)),
        )
        self.api_bucket = TokenBucket(
            rate_per_sec=api_rate,
            burst=float(self.settings.api_burst or max(api_rate, 1.0)),
        )
        max_in = self.settings.producer_max_inflight if self.settings.enabled else 0
        self.inflight = InflightGate(max_inflight=max_in)

    def allow_ingest(self, *, block: bool = True) -> bool:
        """Reserve one ingest publish slot."""
        if not self.settings.enabled:
            self.stats.acquired += 1
            return True
        ok = (
            self.ingest_bucket.acquire(timeout=self.settings.producer_block_timeout_sec)
            if block
            else self.ingest_bucket.try_acquire()
        )
        if ok:
            self.stats.acquired += 1
            return True
        self.stats.rate_limited += 1
        if not block:
            self.stats.dropped += 1
        return False

    def allow_api(self) -> bool:
        if not self.settings.enabled:
            return True
        if self.api_bucket.try_acquire():
            return True
        self.stats.rate_limited += 1
        return False

    def enter_inflight(self) -> bool:
        if self.inflight.disabled:
            return True
        waited = self.inflight.inflight >= self.inflight.max_inflight
        ok = self.inflight.acquire(timeout=self.settings.producer_block_timeout_sec)
        if waited:
            self.stats.backpressure_waits += 1
        if not ok:
            self.stats.dropped += 1
        return ok

    def leave_inflight(self) -> None:
        if not self.inflight.disabled:
            self.inflight.release()


class BackpressurePublisher:
    """Wrap an EventPublisher with rate limit + inflight backpressure.

    On timeout the send is skipped (counted as dropped) so a stalled broker
    cannot deadlock the ingestion loop. Callers that need durability should
    inspect :attr:`controller.stats.dropped`.
    """

    def __init__(self, inner: EventPublisherLike, controller: FlowController) -> None:
        self._inner = inner
        self.controller = controller

    def send(self, topic: str, key: bytes | None, value: bytes) -> None:
        if not self.controller.allow_ingest(block=True):
            return
        if not self.controller.enter_inflight():
            return
        try:
            self._inner.send(topic, key, value)
        finally:
            self.controller.leave_inflight()

    def flush(self) -> None:
        self._inner.flush()

    def close(self) -> None:
        self._inner.close()


def controller_from_settings(settings: FlowControlSettings | None = None) -> FlowController:
    return FlowController(settings=settings or FlowControlSettings())
