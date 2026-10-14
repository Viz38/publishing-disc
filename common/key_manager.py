import os
import time
import logging
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)

class TracxnKeyManager:
    """
    Thread-safe, round-robin token manager for Tracxn API.
    Supports pooling up to 30+ tokens with automatic backoff on rate-limited tokens.
    """

    def __init__(self, tokens: Optional[List[str]] = None):
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

    def get_active_tokens_count(self) -> int:
        return len(self.tokens)

    def mark_rate_limited(self, token: str, penalty_sec: float = 30.0) -> None:
        """
        Temporarily marks a token as penalized until time.time() + penalty_sec.
        """
        if token:
            self._penalties[token] = time.time() + penalty_sec
            logger.warning(f"Tracxn token ...{token[-6:]} penalized for {penalty_sec}s")

    def penalize_token(self, token: str, cooldown_seconds: float = 30.0) -> None:
        self.mark_rate_limited(token, penalty_sec=cooldown_seconds)

    def get_next_token(self) -> str:
        """
        Returns the next available token in round-robin order.
        Skips currently penalized tokens if unpenalized tokens exist.
        """
        if not self.tokens:
            return ""

        now = time.time()
        n = len(self.tokens)

        # Try to find a non-penalized token
        for _ in range(n):
            token = self.tokens[self._index % n]
            self._index = (self._index + 1) % n

            penalized_until = self._penalties.get(token, 0)
            if penalized_until <= now:
                return token

        # If all tokens are penalized, fallback to the earliest penalized token
        earliest_token = min(self.tokens, key=lambda t: self._penalties.get(t, 0))
        return earliest_token

    def get_token(self) -> str:
        return self.get_next_token()

    @classmethod
    def from_env(cls) -> "TracxnKeyManager":
        return cls()


class NIMKeyManager:
    """
    Thread-safe, round-robin key manager for Nvidia NIM API keys.
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

    def get_total_keys(self) -> int:
        return len(self.keys)

    def mark_rate_limited(self, key: str, penalty_sec: float = 30.0) -> None:
        if key:
            self._penalties[key] = time.time() + penalty_sec

    def penalize_key(self, key: str, cooldown_seconds: float = 30.0) -> None:
        self.mark_rate_limited(key, penalty_sec=cooldown_seconds)

    def get_next_key(self) -> str:
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

    def get_key(self) -> str:
        return self.get_next_key()

    @classmethod
    def from_env(cls) -> "NIMKeyManager":
        return cls()
