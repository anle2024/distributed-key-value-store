"""
Key-Value server implementation.
"""

import threading
import time
import random
from typing import Dict, Tuple, Optional
from .rpc_types import GetArgs, GetReply, PutArgs, PutReply
from .errors import ErrNoKey, ErrVersion


class KVServer:
    """Thread-safe Key-Value server with versioned writes."""

    def __init__(self, unreliable: bool = False, drop_rate: float = 0.1):
        self.data: Dict[str, Tuple[str, int]] = {}  # key -> (value, version)
        self.lock = threading.Lock()
        self.unreliable = unreliable
        self.drop_rate = drop_rate
        self.request_history: Dict[str, Tuple[str, int]] = (
            {}
        )  # client_id:seq -> (method, timestamp)
        self.reply_cache: Dict[str, object] = {}  # client_id:seq -> cached reply

    def _should_drop(self) -> bool:
        """Simulate network drops if unreliable mode is enabled."""
        if not self.unreliable:
            return False
        return random.random() < self.drop_rate

    def _make_cache_key(self, client_id: str, seq_num: int) -> str:
        """Create cache key for duplicate detection."""
        return f"{client_id}:{seq_num}"

    def get(self, args: GetArgs, client_id: str = "", seq_num: int = 0) -> GetReply:
        """
        Get operation: returns (value, version) or ErrNoKey.
        """
        # Simulate request drop
        if self._should_drop():
            raise Exception("Request dropped")

        cache_key = self._make_cache_key(client_id, seq_num)

        with self.lock:
            # Check for duplicate request
            if cache_key in self.reply_cache:
                cached_reply = self.reply_cache[cache_key]
                # Simulate reply drop
                if self._should_drop():
                    raise Exception("Reply dropped")
                return cached_reply

            if args.key not in self.data:
                reply = GetReply(err="ErrNoKey")
            else:
                value, version = self.data[args.key]
                reply = GetReply(value=value, version=version)

            # Cache the reply for duplicate detection
            if client_id and seq_num > 0:
                self.reply_cache[cache_key] = reply

            # Simulate reply drop
            if self._should_drop():
                raise Exception("Reply dropped")

            return reply

    def put(self, args: PutArgs, client_id: str = "", seq_num: int = 0) -> PutReply:
        """
        Put operation with conditional versioning:
        - If key missing and version != 0 -> ErrNoKey
        - If key missing and version == 0 -> create with version 1
        - If version != current_version -> ErrVersion
        - Else update and increment version
        """
        # Simulate request drop
        if self._should_drop():
            raise Exception("Request dropped")

        cache_key = self._make_cache_key(client_id, seq_num)

        with self.lock:
            # Check for duplicate request
            if cache_key in self.reply_cache:
                cached_reply = self.reply_cache[cache_key]
                # Simulate reply drop
                if self._should_drop():
                    raise Exception("Reply dropped")
                return cached_reply

            if args.key not in self.data:
                # Key doesn't exist
                if args.version != 0:
                    reply = PutReply(err="ErrNoKey")
                else:
                    # Create new key with version 1
                    self.data[args.key] = (args.value, 1)
                    reply = PutReply()
            else:
                # Key exists
                current_value, current_version = self.data[args.key]
                if args.version != current_version:
                    reply = PutReply(err="ErrVersion")
                else:
                    # Update with incremented version
                    self.data[args.key] = (args.value, current_version + 1)
                    reply = PutReply()

            # Cache the reply for duplicate detection
            if client_id and seq_num > 0:
                self.reply_cache[cache_key] = reply

            # Simulate reply drop
            if self._should_drop():
                raise Exception("Reply dropped")

            return reply

    def set_unreliable(self, unreliable: bool, drop_rate: float = 0.1):
        """Configure unreliable network simulation."""
        with self.lock:
            self.unreliable = unreliable
            self.drop_rate = drop_rate

    def get_stats(self) -> Dict[str, int]:
        """Get server statistics."""
        with self.lock:
            return {"num_keys": len(self.data), "cached_replies": len(self.reply_cache)}
