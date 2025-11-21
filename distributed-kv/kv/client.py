"""
KV Client (Clerk) implementation with retry logic.
"""

import time
import random
import uuid
import threading
from typing import Optional, Tuple
from .rpc_types import GetArgs, GetReply, PutArgs, PutReply
from .errors import ErrNoKey, ErrVersion, ErrMaybe, ErrTimeout
from .server import KVServer


class Clerk:
    """
    Client stub for KV service with retry logic for unreliable networks.
    """

    def __init__(
        self, server: KVServer, max_retries: int = 10, retry_delay: float = 0.01
    ):
        self.server = server
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.client_id = str(uuid.uuid4())
        self.seq_num = 0
        self.seq_lock = threading.Lock()

    def _next_seq_num(self) -> int:
        """Get next sequence number for RPC deduplication."""
        with self.seq_lock:
            self.seq_num += 1
            return self.seq_num

    def _backoff_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay."""
        base_delay = self.retry_delay
        max_delay = 1.0
        delay = min(base_delay * (2**attempt), max_delay)
        # Add jitter
        jitter = delay * 0.1 * random.random()
        return delay + jitter

    def get(self, key: str) -> Tuple[str, int]:
        """
        Get value and version for key.
        Returns (value, version) or raises ErrNoKey.
        """
        args = GetArgs(key=key)
        seq_num = self._next_seq_num()

        for attempt in range(self.max_retries):
            try:
                reply = self.server.get(args, self.client_id, seq_num)

                if reply.err == "ErrNoKey":
                    raise ErrNoKey(f"Key '{key}' not found")
                elif reply.err:
                    raise Exception(f"Unexpected error: {reply.err}")

                return reply.value, reply.version

            except Exception as e:
                if "dropped" in str(e):
                    # Network drop - retry
                    if attempt < self.max_retries - 1:
                        time.sleep(self._backoff_delay(attempt))
                        continue
                    else:
                        raise ErrTimeout("Get operation timed out after retries")
                else:
                    # Other error - propagate immediately
                    raise

        raise ErrTimeout("Get operation timed out after retries")

    def put(self, key: str, value: str, version: int) -> None:
        """
        Put value with version check.
        Raises ErrNoKey, ErrVersion, ErrMaybe, or ErrTimeout.
        """
        args = PutArgs(key=key, value=value, version=version)
        seq_num = self._next_seq_num()
        first_attempt = True

        for attempt in range(self.max_retries):
            try:
                reply = self.server.put(args, self.client_id, seq_num)

                if reply.err == "ErrNoKey":
                    raise ErrNoKey(f"Key '{key}' not found and version != 0")
                elif reply.err == "ErrVersion":
                    if first_attempt:
                        # First attempt with ErrVersion means actual version mismatch
                        raise ErrVersion(f"Version mismatch for key '{key}'")
                    else:
                        # Retry attempt with ErrVersion means operation might have succeeded
                        raise ErrMaybe("Put operation might have succeeded")
                elif reply.err:
                    raise Exception(f"Unexpected error: {reply.err}")

                # Success
                return

            except (ErrNoKey, ErrVersion, ErrMaybe):
                # These errors should be propagated immediately
                raise
            except Exception as e:
                if "dropped" in str(e):
                    # Network drop - retry (but no longer first attempt)
                    first_attempt = False
                    if attempt < self.max_retries - 1:
                        time.sleep(self._backoff_delay(attempt))
                        continue
                    else:
                        # After retries, we don't know if Put succeeded
                        raise ErrMaybe(
                            "Put operation might have succeeded after timeout"
                        )
                else:
                    # Other error - propagate immediately
                    raise

        # Exhausted retries
        raise ErrMaybe("Put operation might have succeeded after timeout")

    def conditional_put(self, key: str, value: str, expected_version: int) -> bool:
        """
        Conditional put that returns True if successful, False if version mismatch.
        Raises other errors as appropriate.
        """
        try:
            self.put(key, value, expected_version)
            return True
        except ErrVersion:
            return False

    def create_if_missing(self, key: str, value: str) -> bool:
        """
        Create key with value if it doesn't exist.
        Returns True if created, False if already exists.
        """
        try:
            self.put(key, value, 0)
            return True
        except ErrNoKey:
            # This shouldn't happen when version=0, but handle it
            return False
        except ErrVersion:
            # Key already exists (version 0 != current version)
            return False
