import os
import time
import logging
import asyncio
import threading
from collections import defaultdict, deque
from typing import List, Optional, Dict, Set, Any
import aiohttp

logger = logging.getLogger(__name__)

class TracxnKeyManager:
    """
    Thread-safe, round-robin token manager for Tracxn API.
    Supports pooling up to 30+ tokens with automatic backoff on rate-limited tokens,
    permanent deactivation for invalid/expired tokens (401/403),
    startup pre-flight token validation, and local sliding-window rate limiting.
    """

    PREFLIGHT_URL = "https://platform.tracxn.com/utils/web-scraper/fetch/url/track"

    def __init__(
        self,
        tokens: Optional[List[str]] = None,
        max_rate_per_sec: int = 2,
        max_rate_per_min: int = 65,
    ):
        self.invalid_tokens: Set[str] = set()
        if tokens is not None:
            raw_tokens = [str(t).strip() for t in tokens if str(t).strip()]
        else:
            env_tokens = os.getenv("TRACXN_API_TOKENS", "")
            if env_tokens:
                raw_tokens = [t.strip() for t in env_tokens.split(",") if t.strip()]
            else:
                fallback = os.getenv("TRACXN_API_TOKEN", "")
                raw_tokens = [fallback] if fallback else []

        self.tokens: List[str] = [t for t in raw_tokens if t and t not in self.invalid_tokens]
        self._index = 0
        self._penalties: Dict[str, float] = {}
        self.max_rate_per_sec = max_rate_per_sec
        self.max_rate_per_min = max_rate_per_min
        self._call_history: Dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def get_active_tokens_count(self) -> int:
        now = time.time()
        with self._lock:
            return sum(1 for t in self.tokens if self._penalties.get(t, 0) <= now)

    def mark_invalid(self, token: str) -> None:
        """
        Permanently deactivates an invalid or expired token so it is never used again.
        """
        if not token:
            return
        with self._lock:
            self.invalid_tokens.add(token)
            if token in self.tokens:
                self.tokens.remove(token)
            if token in self._penalties:
                del self._penalties[token]
            if token in self._call_history:
                del self._call_history[token]
            if self.tokens:
                self._index = self._index % len(self.tokens)
            else:
                self._index = 0
        logger.warning(
            f"Permanently put Tracxn token ...{token[-6:]} to sleep (expired/invalid). "
            f"Active tokens remaining: {len(self.tokens)}"
        )

    def mark_rate_limited(self, token: str, penalty_sec: float = 60.0) -> None:
        """
        Temporarily marks a token as penalized until time.time() + penalty_sec.
        Default 60s per /tracxn-api skill specification for rate limit backoff.
        """
        if token and token not in self.invalid_tokens:
            with self._lock:
                self._penalties[token] = time.time() + penalty_sec
            logger.warning(f"Tracxn token ...{token[-6:]} penalized for {penalty_sec:.1f}s")

    def penalize_token(self, token: str, cooldown_seconds: float = 60.0) -> None:
        self.mark_rate_limited(token, penalty_sec=cooldown_seconds)

    def handle_http_error(self, token: str, status_code: int) -> None:
        """
        Applies penalties and rotation policies based on Tracxn HTTP response codes:
        - 401: Unauthorized / Expired -> Permanently disabled (put to sleep).
        - 403: Forbidden / Access Denied -> Permanently disabled (put to sleep).
        - 429: Too Many Requests -> 60s cooldown per /tracxn-api skill.
        - 5xx: Server error -> 5.0s cooldown.
        """
        if not token:
            return
        if status_code in (401, 403):
            self.mark_invalid(token)
        elif status_code == 429:
            self.mark_rate_limited(token, penalty_sec=60.0)
        elif status_code >= 500:
            self.mark_rate_limited(token, penalty_sec=5.0)
        else:
            self.mark_rate_limited(token, penalty_sec=10.0)

    async def validate_token(self, token: str, session: aiohttp.ClientSession) -> bool:
        """
        Probes a single token against the Tracxn API to verify if it is valid or expired.
        """
        headers = {
            "Content-Type": "application/json",
            "X-Request-Source": "Testing",
            "accessToken": token,
        }
        payload = {"jobId": "preflight_probe_test"}
        try:
            async with session.post(
                self.PREFLIGHT_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 201, 202):
                    return True
                elif resp.status in (401, 403):
                    self.mark_invalid(token)
                    return False
                elif resp.status == 429:
                    self.mark_rate_limited(token, penalty_sec=60.0)
                    return True
                else:
                    return True
        except Exception as e:
            logger.warning(f"Error probing Tracxn token ...{token[-6:]}: {e}. Retaining in pool.")
            return True

    async def validate_and_prune_all_tokens(
        self, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, List[str]]:
        """
        Tests all available Tracxn tokens at startup/restart.
        Permanently sleeps any invalid/expired tokens (401/403).
        Returns a dict with 'valid' and 'invalid' token lists.
        """
        with self._lock:
            candidate_tokens = list(self.tokens)

        if not candidate_tokens:
            logger.warning("No Tracxn tokens available to validate.")
            return {"valid": [], "invalid": list(self.invalid_tokens)}

        logger.info(f"Pre-flight testing {len(candidate_tokens)} Tracxn tokens...")

        should_close_session = False
        active_session = session
        if active_session is None:
            active_session = aiohttp.ClientSession()
            should_close_session = True

        valid_list = []
        invalid_list = []

        try:
            tasks = [self.validate_token(tok, active_session) for tok in candidate_tokens]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for tok, res in zip(candidate_tokens, results):
                if res is True:
                    valid_list.append(tok)
                else:
                    invalid_list.append(tok)
        finally:
            if should_close_session:
                await active_session.close()

        logger.info(
            f"Tracxn token pre-flight audit complete: {len(valid_list)} valid, "
            f"{len(invalid_list)} put to permanent sleep."
        )
        return {"valid": valid_list, "invalid": invalid_list}

    def validate_and_prune_all_tokens_sync(self) -> Dict[str, List[str]]:
        """
        Synchronous wrapper for validate_and_prune_all_tokens.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, self.validate_and_prune_all_tokens())
                return future.result()
        else:
            return asyncio.run(self.validate_and_prune_all_tokens())

    def _prune_history(self, token: str, now: float) -> None:
        q = self._call_history[token]
        cutoff = now - 60.0
        while q and q[0] <= cutoff:
            q.popleft()

    def can_consume(self, token: str) -> bool:
        """
        Checks whether token is within its rate limits (both 1s burst and 60s window).
        """
        if token in self.invalid_tokens:
            return False
        with self._lock:
            now = time.time()
            self._prune_history(token, now)
            q = self._call_history[token]
            if len(q) >= self.max_rate_per_min:
                return False
            calls_in_1s = sum(1 for t in reversed(q) if t > now - 1.0)
            return calls_in_1s < self.max_rate_per_sec

    def record_call(self, token: str) -> None:
        """
        Records an API call timestamp for the specified token.
        """
        if token in self.invalid_tokens:
            return
        with self._lock:
            now = time.time()
            self._prune_history(token, now)
            self._call_history[token].append(now)

    def reload_tokens_from_env(self, env_path: Optional[str] = None) -> int:
        """
        Dynamically re-reads TRACXN_API_TOKENS from .env file into the active pool.
        Clears penalty and invalid status for any tokens refreshed in the new environment.
        Returns the number of active valid tokens loaded.
        """
        try:
            target_env = env_path or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
            env_tokens = ""
            if os.path.exists(target_env):
                try:
                    from dotenv import dotenv_values
                    vals = dotenv_values(target_env)
                    env_tokens = vals.get("TRACXN_API_TOKENS", "") or ""
                except Exception:
                    pass
            if not env_tokens:
                env_tokens = os.getenv("TRACXN_API_TOKENS", "")

            raw_tokens = [t.strip() for t in env_tokens.split(",") if t.strip()] if env_tokens else []
            if not raw_tokens:
                fallback = os.getenv("TRACXN_API_TOKEN", "")
                raw_tokens = [fallback] if fallback else []

            with self._lock:
                new_tokens = [t for t in raw_tokens if t]
                # If fresh tokens were provided, clear invalid/penalty state
                for t in new_tokens:
                    self.invalid_tokens.discard(t)
                    if t in self._penalties and self._penalties[t] > time.time() + 60.0:
                        del self._penalties[t]

                self.tokens = [t for t in new_tokens if t and t not in self.invalid_tokens]
                if self.tokens:
                    self._index = self._index % len(self.tokens)
                else:
                    self._index = 0

            logger.info(f"🔄 Dynamic token reload: {len(self.tokens)} active tokens in pool.")
            return len(self.tokens)
        except Exception as e:
            logger.warning(f"Error reloading tokens from env: {e}")
            return len(self.tokens)

    def acquire_sync(self, token: Optional[str] = None, timeout: float = 45.0) -> str:
        """
        Synchronously acquires permission to make an API call using the given token (or next available).
        Blocks/sleeps if the token is currently at its rate ceiling, rotating across candidates first.
        Times out after `timeout` seconds (default 45s) and returns "" if no tokens available.
        """
        tok = token or self.get_next_token()
        start_time = time.time()
        reloaded = False
        while True:
            if not tok or len(self.tokens) == 0:
                if not reloaded:
                    self.reload_tokens_from_env()
                    reloaded = True
                    tok = token or self.get_next_token()
                if not tok:
                    if time.time() - start_time > timeout:
                        logger.error(f"TracxnKeyManager: Token acquisition timed out after {timeout}s (pool exhausted).")
                        return ""
                    time.sleep(0.5)
                    tok = token or self.get_next_token()
                    continue

            with self._lock:
                if tok in self.invalid_tokens or tok not in self.tokens:
                    tok = self.get_next_token()
                    if not tok:
                        continue
                now = time.time()
                penalty_until = self._penalties.get(tok, 0)
                if penalty_until > now:
                    wait_time = max(0.05, penalty_until - now)
                else:
                    self._prune_history(tok, now)
                    q = self._call_history[tok]
                    if len(q) >= self.max_rate_per_min:
                        wait_time = max(0.02, 60.0 - (now - q[0]) + 0.05)
                    else:
                        calls_in_1s = [t for t in q if t > now - 1.0]
                        if len(calls_in_1s) >= self.max_rate_per_sec:
                            wait_time = max(0.02, 1.0 - (now - calls_in_1s[0]) + 0.05)
                        else:
                            q.append(now)
                            return tok

            if time.time() - start_time > timeout:
                logger.error(f"TracxnKeyManager: Token acquisition timed out after {timeout}s.")
                return ""

            if token is None:
                next_tok = self.get_next_token()
                if next_tok != tok and self.can_consume(next_tok):
                    tok = next_tok
                    continue

            time.sleep(min(wait_time, 0.5))

    async def acquire(self, token: Optional[str] = None, timeout: float = 45.0) -> str:
        """
        Asynchronously acquires permission to make an API call using the given token (or next available).
        Sleeps asynchronously if all candidate tokens are currently saturated.
        Times out after `timeout` seconds (default 45s) and returns "" if no tokens available.
        """
        tok = token or self.get_next_token()
        start_time = time.time()
        reloaded = False
        while True:
            if not tok or len(self.tokens) == 0:
                if not reloaded:
                    self.reload_tokens_from_env()
                    reloaded = True
                    tok = token or self.get_next_token()
                if not tok:
                    if time.time() - start_time > timeout:
                        logger.error(f"TracxnKeyManager: Async token acquisition timed out after {timeout}s (pool exhausted).")
                        return ""
                    await asyncio.sleep(0.5)
                    tok = token or self.get_next_token()
                    continue

            with self._lock:
                if tok in self.invalid_tokens or tok not in self.tokens:
                    tok = self.get_next_token()
                    if not tok:
                        continue
                now = time.time()
                penalty_until = self._penalties.get(tok, 0)
                if penalty_until > now:
                    wait_time = max(0.05, penalty_until - now)
                else:
                    self._prune_history(tok, now)
                    q = self._call_history[tok]
                    if len(q) >= self.max_rate_per_min:
                        wait_time = max(0.02, 60.0 - (now - q[0]) + 0.05)
                    else:
                        calls_in_1s = [t for t in q if t > now - 1.0]
                        if len(calls_in_1s) >= self.max_rate_per_sec:
                            wait_time = max(0.02, 1.0 - (now - calls_in_1s[0]) + 0.05)
                        else:
                            q.append(now)
                            return tok

            if time.time() - start_time > timeout:
                logger.error(f"TracxnKeyManager: Async token acquisition timed out after {timeout}s.")
                return ""

            if token is None:
                next_tok = self.get_next_token()
                if next_tok != tok and self.can_consume(next_tok):
                    tok = next_tok
                    continue

            await asyncio.sleep(min(wait_time, 0.5))

    def get_next_token(self) -> str:
        """
        Returns the next available token in round-robin order.
        Prioritizes tokens that are not penalized, not invalid, and not currently saturated.
        """
        with self._lock:
            if not self.tokens:
                return ""

            now = time.time()
            n = len(self.tokens)

            # 1. Prefer unpenalized and unsaturated token
            for _ in range(n):
                token = self.tokens[self._index % n]
                self._index = (self._index + 1) % n

                if token not in self.invalid_tokens and self._penalties.get(token, 0) <= now:
                    self._prune_history(token, now)
                    q = self._call_history[token]
                    if len(q) < self.max_rate_per_min:
                        calls_in_1s = sum(1 for t in reversed(q) if t > now - 1.0)
                        if calls_in_1s < self.max_rate_per_sec:
                            return token

            # 2. Fallback to unpenalized (even if near saturation)
            for _ in range(n):
                token = self.tokens[self._index % n]
                self._index = (self._index + 1) % n
                if token not in self.invalid_tokens and self._penalties.get(token, 0) <= now:
                    return token

            # 3. Fallback to earliest penalized valid token
            valid_candidates = [t for t in self.tokens if t not in self.invalid_tokens]
            if not valid_candidates:
                return ""
            earliest_token = min(valid_candidates, key=lambda t: self._penalties.get(t, 0))
            return earliest_token

    def get_token(self) -> str:
        return self.get_next_token()

    @classmethod
    def from_env(cls, max_rate_per_sec: int = 2, max_rate_per_min: int = 65) -> "TracxnKeyManager":
        return cls(max_rate_per_sec=max_rate_per_sec, max_rate_per_min=max_rate_per_min)


class NIMKeyManager:
    """
    Thread-safe, round-robin key manager for Nvidia NIM API keys.
    Supports asynchronous and synchronous token rotation, rate-limit cooldowns,
    permanent deactivation for invalid/expired keys (401/403),
    and startup pre-flight testing across all configured keys.
    """

    PREFLIGHT_URL = "https://integrate.api.nvidia.com/v1/models"

    # SHA-256 hash digests of historically deactivated NIM keys to avoid storing plaintext tokens in git
    KNOWN_EXPIRED_NIM_KEY_HASHES: Set[str] = {
        "7a0f8c299bd0beaef86255b7d145f18334bb3ad95181928673e16b5f8ee9729d",  # HTTP 403 Forbidden
        "dd9fb9f95bda0f5c1f6343111510b8c77e53e55df5d8648e00d1892326e2f16e",  # HTTP 403 Forbidden
        "0cc5c3fbb30514be83b82bad53dc68b50113cc97e4cc36c930831db81b469a8d",  # Stalling: >50s socket timeouts
    }

    @classmethod
    def is_key_expired_or_invalid(cls, key: str) -> bool:
        if not key:
            return True
        import hashlib
        k_hash = hashlib.sha256(str(key).strip().encode("utf-8")).hexdigest()
        return k_hash in cls.KNOWN_EXPIRED_NIM_KEY_HASHES

    def __init__(self, keys: Optional[List[str]] = None):
        self.invalid_keys: Set[str] = set()

        # Load expired keys from environment variable if present
        env_expired = os.getenv("EXPIRED_NIM_KEYS", "")
        if env_expired:
            for k in env_expired.split(","):
                if k.strip():
                    self.invalid_keys.add(k.strip())

        if keys is not None:
            raw_keys = [str(k).strip() for k in keys if str(k).strip()]
        else:
            raw_env_keys = os.getenv("NIM_API_KEYS", "")
            if raw_env_keys:
                raw_keys = [k.strip() for k in raw_env_keys.split(",") if k.strip()]
            else:
                raw_keys = []

        # Auto-blacklist any keys whose SHA-256 hash matches known expired keys
        for k in raw_keys:
            if self.is_key_expired_or_invalid(k):
                self.invalid_keys.add(k)

        self.keys: List[str] = [k for k in raw_keys if k and k not in self.invalid_keys]
        self._index = 0
        self._penalties: Dict[str, float] = {}
        self.max_in_flight_per_key: int = 2
        self._in_flight: Dict[str, int] = {}
        self._last_used: Dict[str, float] = {}
        self._min_interval: float = 0.5  # seconds between calls on the SAME key
        self._lock = threading.Lock()
        self._async_lock: Optional[asyncio.Lock] = None
        self._async_condition: Optional[asyncio.Condition] = None

    def _get_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    def _get_async_condition(self) -> asyncio.Condition:
        if self._async_condition is None:
            self._async_condition = asyncio.Condition()
        return self._async_condition

    async def _notify_key_available(self) -> None:
        if self._async_condition is not None:
            async with self._async_condition:
                self._async_condition.notify_all()

    def release_key(self, key: str) -> None:
        """
        Releases an in-flight reservation on a NIM key so it can be leased by other workers.
        """
        if not key:
            return
        with self._lock:
            if key in self._in_flight:
                self._in_flight[key] -= 1
                if self._in_flight[key] <= 0:
                    del self._in_flight[key]
        if self._async_condition is not None:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._notify_key_available())
            except RuntimeError:
                pass

    def get_total_keys(self) -> int:
        with self._lock:
            return len(self.keys)

    def get_active_keys_count(self) -> int:
        now = time.time()
        with self._lock:
            return sum(1 for k in self.keys if self._penalties.get(k, 0) <= now)

    @property
    def total_tokens(self) -> int:
        return self.get_total_keys()

    def mark_rate_limited(self, key: str, penalty_sec: float = 3.0, cooldown_seconds: Optional[float] = None) -> None:
        sec = cooldown_seconds if cooldown_seconds is not None else penalty_sec
        if key and key not in self.invalid_keys:
            with self._lock:
                self._penalties[key] = time.time() + sec
            logger.warning(f"NIM key ...{key[-6:]} penalized for {sec:.1f}s")

    def penalize_key(self, key: str, cooldown_seconds: float = 3.0) -> None:
        self.mark_rate_limited(key, penalty_sec=cooldown_seconds)

    def mark_invalid(self, key: str) -> None:
        """
        Permanently deactivates an invalid or expired NIM key so it is never used again.
        """
        if not key:
            return
        with self._lock:
            self.invalid_keys.add(key)
            self._in_flight.pop(key, None)
            if key in self.keys:
                self.keys.remove(key)
            if key in self._penalties:
                del self._penalties[key]
            if key in self._last_used:
                del self._last_used[key]
            if self.keys:
                self._index = self._index % len(self.keys)
            else:
                self._index = 0
        logger.warning(
            f"Permanently put NIM key ...{key[-6:]} to sleep (expired/invalid). "
            f"Active keys remaining: {len(self.keys)}"
        )

    async def validate_key(self, key: str, session: aiohttp.ClientSession) -> bool:
        """
        Probes a single NIM key against the Nvidia API models endpoint.
        """
        headers = {
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        }
        try:
            async with session.get(
                self.PREFLIGHT_URL,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 201, 202):
                    return True
                elif resp.status in (401, 403):
                    self.mark_invalid(key)
                    return False
                elif resp.status == 429:
                    self.mark_rate_limited(key, penalty_sec=25.0)
                    return True
                else:
                    return True
        except Exception as e:
            logger.warning(f"Error probing NIM key ...{key[-6:]}: {e}. Retaining in pool.")
            return True

    async def validate_and_prune_all_keys(
        self, session: Optional[aiohttp.ClientSession] = None
    ) -> Dict[str, List[str]]:
        """
        Tests all available NIM keys at startup/restart.
        Permanently sleeps any invalid/expired keys (401/403).
        Returns a dict with 'valid' and 'invalid' key lists.
        """
        with self._lock:
            candidate_keys = list(self.keys)

        if not candidate_keys:
            logger.warning("No NIM keys available to validate.")
            return {"valid": [], "invalid": list(self.invalid_keys)}

        logger.info(f"Pre-flight testing {len(candidate_keys)} NIM keys...")

        should_close_session = False
        active_session = session
        if active_session is None:
            active_session = aiohttp.ClientSession()
            should_close_session = True

        valid_list = []
        invalid_list = []

        try:
            tasks = [self.validate_key(k, active_session) for k in candidate_keys]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for k, res in zip(candidate_keys, results):
                if res is True:
                    valid_list.append(k)
                else:
                    invalid_list.append(k)
        finally:
            if should_close_session:
                await active_session.close()

        logger.info(
            f"NIM key pre-flight audit complete: {len(valid_list)} valid, "
            f"{len(invalid_list)} put to permanent sleep."
        )
        return {"valid": valid_list, "invalid": invalid_list}

    def validate_and_prune_all_keys_sync(self) -> Dict[str, List[str]]:
        """
        Synchronous wrapper for validate_and_prune_all_keys.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, self.validate_and_prune_all_keys())
                return future.result()
        else:
            return asyncio.run(self.validate_and_prune_all_keys())

    def get_next_key(self) -> str:
        with self._lock:
            if not self.keys:
                return ""

            now = time.time()
            n = len(self.keys)

            # Pass 1: find a key under capacity, not penalized, and respecting min_interval
            for _ in range(n):
                key = self.keys[self._index % n]
                self._index = (self._index + 1) % n
                if key not in self.invalid_keys and self._penalties.get(key, 0) <= now:
                    if self._in_flight.get(key, 0) < self.max_in_flight_per_key:
                        last_t = self._last_used.get(key, 0)
                        if now - last_t >= self._min_interval:
                            self._in_flight[key] = self._in_flight.get(key, 0) + 1
                            self._last_used[key] = now
                            return key

            # Pass 2: any key under capacity
            available_under_capacity = [
                k for k in self.keys
                if k not in self.invalid_keys
                and self._penalties.get(k, 0) <= now
                and self._in_flight.get(k, 0) < self.max_in_flight_per_key
            ]
            if available_under_capacity:
                chosen = min(available_under_capacity, key=lambda k: self._last_used.get(k, 0))
                self._in_flight[chosen] = self._in_flight.get(chosen, 0) + 1
                self._last_used[chosen] = now
                return chosen

            return ""

    async def get_next_key_async(self, timeout: float = 60.0):
        """
        Leases a NIM key with controlled concurrency (up to max_in_flight_per_key)
        and rate-limiting / penalty awareness.
        If all keys are at capacity or penalized, waits on async condition until a key is released.
        """
        cond = self._get_async_condition()
        start_time = time.time()

        while time.time() - start_time < timeout:
            async with cond:
                now = time.time()
                with self._lock:
                    if not self.keys:
                        return "", 0.0

                    n = len(self.keys)

                    # Pass 1: key not penalized, under capacity, respecting min_interval
                    for _ in range(n):
                        key = self.keys[self._index % n]
                        self._index = (self._index + 1) % n
                        if key not in self.invalid_keys and self._penalties.get(key, 0) <= now:
                            if self._in_flight.get(key, 0) < self.max_in_flight_per_key:
                                last_t = self._last_used.get(key, 0)
                                if now - last_t >= self._min_interval:
                                    self._in_flight[key] = self._in_flight.get(key, 0) + 1
                                    self._last_used[key] = now
                                    return key, 0.0

                    # Pass 2: free keys that are under capacity but used recently (< min_interval)
                    available_under_capacity = [
                        k for k in self.keys
                        if k not in self.invalid_keys
                        and self._penalties.get(k, 0) <= now
                        and self._in_flight.get(k, 0) < self.max_in_flight_per_key
                    ]
                    if available_under_capacity:
                        earliest_key = min(available_under_capacity, key=lambda k: self._last_used.get(k, 0))
                        wait_time = max(0.05, self._min_interval - (now - self._last_used.get(earliest_key, 0)))
                        self._in_flight[earliest_key] = self._in_flight.get(earliest_key, 0) + 1
                        self._last_used[earliest_key] = now + wait_time
                        return earliest_key, wait_time

                    # Pass 3: All keys are either currently at capacity or penalized
                    valid_candidates = [k for k in self.keys if k not in self.invalid_keys]
                    if not valid_candidates:
                        return "", 0.0

                    # Calculate minimum wait until next penalty expires or default wait
                    penalized = [self._penalties[k] for k in valid_candidates if self._penalties.get(k, 0) > now]
                    min_wait = max(0.05, min(penalized) - now) if penalized else 1.0

                # Release lock and wait for release_key notification or penalty expiration
                wait_sec = min(min_wait, 1.0)
                try:
                    await asyncio.wait_for(cond.wait(), timeout=wait_sec)
                except asyncio.TimeoutError:
                    pass

        return "", 0.0

    def get_key(self) -> str:
        return self.get_next_key()

    @classmethod
    def from_env(cls) -> "NIMKeyManager":
        return cls()

    @classmethod
    def for_sheet(
        cls,
        sheet_num: int,
        total_sheets: int = 4,
        keys: Optional[List[str]] = None
    ) -> "NIMKeyManager":
        sharded = get_sharded_nim_keys(sheet_num, total_sheets=total_sheets, keys=keys)
        logger.info(
            f"Initialized dedicated NIMKeyManager for Sheet {sheet_num}/{total_sheets} "
            f"with {len(sharded)} isolated keys (0 cross-process collisions)."
        )
        return cls(keys=sharded)


def get_sharded_nim_keys(
    sheet_num: int,
    total_sheets: int = 4,
    keys: Optional[List[str]] = None,
    invalid_keys: Optional[Set[str]] = None
) -> List[str]:
    """
    Partitions the full NIM key pool into balanced, interleaved subsets for each sheet.
    Filters out permanently expired/invalid keys (e.g. 401/403).
    Uses round-robin interleaving so that fast and slow keys are distributed evenly across
    all sheets, completely avoiding clustered stalling.
    Ensures 0 key collision across parallel sheet processes.
    """
    if sheet_num < 1 or sheet_num > total_sheets:
        raise ValueError(f"sheet_num must be between 1 and {total_sheets}, got {sheet_num}")

    if keys is not None:
        raw_keys = [str(k).strip() for k in keys if str(k).strip()]
    else:
        raw_env = os.getenv("NIM_API_KEYS", "")
        if raw_env:
            raw_keys = [k.strip() for k in raw_env.split(",") if k.strip()]
        else:
            raw_keys = []

    if not raw_keys:
        return []

    bad_keys = set(invalid_keys or [])
    valid_candidates = [
        k for k in raw_keys
        if k and k not in bad_keys and not NIMKeyManager.is_key_expired_or_invalid(k)
    ]
    if not valid_candidates:
        valid_candidates = [k for k in raw_keys if k]

    # Interleaved round-robin distribution:
    # Sheet 1 gets indices 0, 4, 8, ...
    # Sheet 2 gets indices 1, 5, 9, ...
    # Sheet 3 gets indices 2, 6, 10, ...
    # Sheet 4 gets indices 3, 7, 11, ...
    assigned = [k for idx, k in enumerate(valid_candidates) if idx % total_sheets == (sheet_num - 1)]
    return assigned
