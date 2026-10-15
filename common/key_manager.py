import os
import time
import logging
import asyncio
import threading
from collections import defaultdict, deque
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)

class TracxnKeyManager:
    """
    Thread-safe, round-robin token manager for Tracxn API.
    Supports pooling up to 30+ tokens with automatic backoff on rate-limited tokens
    and local sliding-window rate limiting (capped at 90 calls/sec per key per /tracxn-api skill).
    """

    def __init__(self, tokens: Optional[List[str]] = None, max_rate_per_sec: int = 90):
        if tokens is not None:
            self.tokens = [str(t).strip() for t in tokens if str(t).strip()]
        else:
            raw_tokens = os.getenv("TRACXN_API_TOKENS", "")
            if raw_tokens:
                self.tokens = [t.strip() for t in raw_tokens.split(",") if t.strip()]
            else:
                fallback = os.getenv("TRACXN_API_TOKEN", "")
                self.tokens = [fallback] if fallback else []

        self._index = 0
        self._penalties: Dict[str, float] = {}
        self.max_rate_per_sec = max_rate_per_sec
        self._call_history: Dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def get_active_tokens_count(self) -> int:
        now = time.time()
        with self._lock:
            return sum(1 for t in self.tokens if self._penalties.get(t, 0) <= now)

    def mark_rate_limited(self, token: str, penalty_sec: float = 60.0) -> None:
        """
        Temporarily marks a token as penalized until time.time() + penalty_sec.
        Default 60s per /tracxn-api skill specification for rate limit backoff.
        """
        if token:
            with self._lock:
                self._penalties[token] = time.time() + penalty_sec
            logger.warning(f"Tracxn token ...{token[-6:]} penalized for {penalty_sec:.1f}s")

    def penalize_token(self, token: str, cooldown_seconds: float = 60.0) -> None:
        self.mark_rate_limited(token, penalty_sec=cooldown_seconds)

    def handle_http_error(self, token: str, status_code: int) -> None:
        """
        Applies penalties and rotation policies based on Tracxn HTTP response codes:
        - 429: Too Many Requests -> 60s cooldown per /tracxn-api skill.
        - 403: Forbidden -> 2.0s cooldown per /tracxn-api skill.
        - 401: Unauthorized / Expired -> 3600.0s cooldown (invalidated from active pool).
        - 5xx: Server error -> 5.0s cooldown.
        """
        if not token:
            return
        if status_code == 429:
            self.mark_rate_limited(token, penalty_sec=60.0)
        elif status_code == 403:
            self.mark_rate_limited(token, penalty_sec=2.0)
        elif status_code == 401:
            self.mark_rate_limited(token, penalty_sec=3600.0)
            logger.error(f"Tracxn token ...{token[-6:]} encountered 401 Unauthorized / Expired. Rotated out of active pool.")
        elif status_code >= 500:
            self.mark_rate_limited(token, penalty_sec=5.0)
        else:
            self.mark_rate_limited(token, penalty_sec=10.0)

    def _prune_history(self, token: str, now: float) -> None:
        q = self._call_history[token]
        cutoff = now - 1.0
        while q and q[0] <= cutoff:
            q.popleft()

    def can_consume(self, token: str) -> bool:
        """
        Checks whether token is within its per-second rate limit (max_rate_per_sec).
        """
        with self._lock:
            now = time.time()
            self._prune_history(token, now)
            return len(self._call_history[token]) < self.max_rate_per_sec

    def record_call(self, token: str) -> None:
        """
        Records an API call timestamp for the specified token.
        """
        with self._lock:
            now = time.time()
            self._prune_history(token, now)
            self._call_history[token].append(now)

    def acquire_sync(self, token: Optional[str] = None) -> str:
        """
        Synchronously acquires permission to make an API call using the given token (or next available).
        Blocks/sleeps if the token is currently at its per-second rate ceiling.
        """
        tok = token or self.get_next_token()
        while True:
            with self._lock:
                now = time.time()
                self._prune_history(tok, now)
                q = self._call_history[tok]
                if len(q) < self.max_rate_per_sec:
                    q.append(now)
                    return tok
                oldest = q[0]
                wait_time = max(0.01, 1.0 - (now - oldest) + 0.005)
            time.sleep(wait_time)

    async def acquire(self, token: Optional[str] = None) -> str:
        """
        Asynchronously acquires permission to make an API call using the given token (or next available).
        Sleeps asynchronously if the token is currently at its per-second rate ceiling.
        """
        tok = token or self.get_next_token()
        while True:
            with self._lock:
                now = time.time()
                self._prune_history(tok, now)
                q = self._call_history[tok]
                if len(q) < self.max_rate_per_sec:
                    q.append(now)
                    return tok
                oldest = q[0]
                wait_time = max(0.01, 1.0 - (now - oldest) + 0.005)
            await asyncio.sleep(wait_time)

    def get_next_token(self) -> str:
        """
        Returns the next available token in round-robin order.
        Prioritizes tokens that are not penalized and not currently saturated.
        """
        if not self.tokens:
            return ""

        now = time.time()
        n = len(self.tokens)

        # 1. Prefer unpenalized and unsaturated token
        with self._lock:
            for _ in range(n):
                token = self.tokens[self._index % n]
                self._index = (self._index + 1) % n

                if self._penalties.get(token, 0) <= now:
                    self._prune_history(token, now)
                    if len(self._call_history[token]) < self.max_rate_per_sec:
                        return token

            # 2. Fallback to unpenalized (even if near saturation)
            for _ in range(n):
                token = self.tokens[self._index % n]
                self._index = (self._index + 1) % n
                if self._penalties.get(token, 0) <= now:
                    return token

            # 3. Fallback to earliest penalized token
            earliest_token = min(self.tokens, key=lambda t: self._penalties.get(t, 0))
            return earliest_token

    def get_token(self) -> str:
        return self.get_next_token()

    @classmethod
    def from_env(cls, max_rate_per_sec: int = 90) -> "TracxnKeyManager":
        return cls(max_rate_per_sec=max_rate_per_sec)



class NIMKeyManager:
    """
    Thread-safe, round-robin key manager for Nvidia NIM API keys.
    Supports asynchronous and synchronous token rotation, rate-limit cooldowns,
    and invalid key deactivation across all configured keys.
    """

    def __init__(self, keys: Optional[List[str]] = None):
        if keys is not None:
            self.keys = [str(k).strip() for k in keys if str(k).strip()]
        else:
            raw_keys = os.getenv("NIM_API_KEYS", "")
            if raw_keys:
                self.keys = [k.strip() for k in raw_keys.split(",") if k.strip()]
            else:
                self.keys = []

        self._index = 0
        self._penalties: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._async_lock: Optional[asyncio.Lock] = None

    def _get_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    def get_total_keys(self) -> int:
        with self._lock:
            return len(self.keys)

    @property
    def total_tokens(self) -> int:
        return self.get_total_keys()

    def mark_rate_limited(self, key: str, penalty_sec: float = 25.0, cooldown_seconds: Optional[float] = None) -> None:
        sec = cooldown_seconds if cooldown_seconds is not None else penalty_sec
        if key:
            with self._lock:
                self._penalties[key] = time.time() + sec
            logger.warning(f"NIM key ...{key[-6:]} penalized for {sec:.1f}s")

    def penalize_key(self, key: str, cooldown_seconds: float = 25.0) -> None:
        self.mark_rate_limited(key, penalty_sec=cooldown_seconds)

    def mark_invalid(self, key: str) -> None:
        with self._lock:
            if key in self.keys:
                self.keys.remove(key)
            if key in self._penalties:
                del self._penalties[key]
        logger.warning(f"Permanently disabled invalid NIM key ...{key[-6:]}. Active keys remaining: {len(self.keys)}")

    def get_next_key(self) -> str:
        with self._lock:
            if not self.keys:
                return ""

            now = time.time()
            n = len(self.keys)

            for _ in range(n):
                key = self.keys[self._index % n]
                self._index = (self._index + 1) % n
                if self._penalties.get(key, 0) <= now:
                    return key

            earliest_key = min(self.keys, key=lambda k: self._penalties.get(k, 0))
            return earliest_key

    async def get_next_key_async(self):
        async with self._get_async_lock():
            with self._lock:
                if not self.keys:
                    return "", 0.0

                now = time.time()
                n = len(self.keys)

                for _ in range(n):
                    key = self.keys[self._index % n]
                    self._index = (self._index + 1) % n
                    if self._penalties.get(key, 0) <= now:
                        return key, 0.0

                earliest_key = min(self.keys, key=lambda k: self._penalties.get(k, 0))
                wait_time = max(0.5, self._penalties.get(earliest_key, 0) - now)
                return earliest_key, wait_time

    def get_key(self) -> str:
        return self.get_next_key()

    @classmethod
    def from_env(cls) -> "NIMKeyManager":
        return cls()
