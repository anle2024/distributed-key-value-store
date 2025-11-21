"""
Distributed lock implementation using KV service.
"""

import time
import threading
import uuid
import sys
import os
from typing import Optional

# Add parent directory to path for absolute imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kv.client import Clerk
from kv.errors import ErrNoKey, ErrVersion, ErrMaybe


class Lock:
    """
    Distributed lock built on top of KV Clerk.
    Uses optimistic concurrency control with versioning.
    """

    def __init__(self, clerk: Clerk, lock_name: str, retry_delay: float = 0.01):
        self.clerk = clerk
        self.lock_name = lock_name
        self.retry_delay = retry_delay
        self.owner_id = str(uuid.uuid4())
        self.is_held = False
        self.local_lock = threading.Lock()

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire the distributed lock.

        Args:
            timeout: Maximum time to wait for lock (None = wait forever)

        Returns:
            True if lock acquired, False if timeout
        """
        start_time = time.time()

        with self.local_lock:
            if self.is_held:
                return True  # Already holding the lock

        while True:
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            try:
                # Try to create the lock if it doesn't exist
                if self.clerk.create_if_missing(self.lock_name, self.owner_id):
                    with self.local_lock:
                        self.is_held = True
                    return True

                # Lock exists, try to acquire it
                try:
                    current_value, current_version = self.clerk.get(self.lock_name)

                    # If lock is held by us, we already have it
                    if current_value == self.owner_id:
                        with self.local_lock:
                            self.is_held = True
                        return True

                    # Try to take the lock from current holder
                    if self.clerk.conditional_put(
                        self.lock_name, self.owner_id, current_version
                    ):
                        with self.local_lock:
                            self.is_held = True
                        return True

                except ErrNoKey:
                    # Lock was deleted between checks, try creating again
                    continue

            except ErrMaybe:
                # Operation might have succeeded, check if we got the lock
                try:
                    current_value, _ = self.clerk.get(self.lock_name)
                    if current_value == self.owner_id:
                        with self.local_lock:
                            self.is_held = True
                        return True
                except ErrNoKey:
                    # Lock doesn't exist, try creating again
                    continue

            # Failed to acquire, backoff and retry
            time.sleep(self.retry_delay)

    def release(self) -> None:
        """
        Release the distributed lock.

        Raises:
            Exception if we don't hold the lock
        """
        with self.local_lock:
            if not self.is_held:
                raise Exception("Cannot release lock we don't hold")
            self.is_held = False

        while True:
            try:
                current_value, current_version = self.clerk.get(self.lock_name)

                if current_value != self.owner_id:
                    # Someone else has the lock, we already released it
                    return

                # Release by putting empty string
                self.clerk.put(self.lock_name, "", current_version)
                return

            except ErrNoKey:
                # Lock doesn't exist, already released
                return
            except ErrVersion:
                # Version changed, try again
                continue
            except ErrMaybe:
                # Operation might have succeeded, check the state
                try:
                    current_value, _ = self.clerk.get(self.lock_name)
                    if current_value != self.owner_id:
                        # Lock released or taken by someone else
                        return
                    # We might still hold it, try again
                    continue
                except ErrNoKey:
                    # Lock was deleted, consider released
                    return

    def is_locked(self) -> bool:
        """Check if we currently hold the lock."""
        with self.local_lock:
            return self.is_held

    def check_lock_state(self) -> Optional[str]:
        """
        Check the current holder of the lock.

        Returns:
            Current owner ID, empty string if released, or None if lock doesn't exist
        """
        try:
            value, _ = self.clerk.get(self.lock_name)
            return value if value else None
        except ErrNoKey:
            return None

    def __enter__(self):
        """Context manager support."""
        if not self.acquire():
            raise Exception("Failed to acquire lock")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support."""
        self.release()
