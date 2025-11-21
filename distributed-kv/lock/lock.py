"""
Distributed lock implementation using KV service.
"""

import time
import threading
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
        # Use clerk's client_id so multiple lock instances from same clerk share ownership
        self.owner_id = clerk.client_id
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

                # Lock exists, check if we can acquire it
                try:
                    current_value, current_version = self.clerk.get(self.lock_name)

                    # If lock is held by us, we already have it
                    if current_value == self.owner_id:
                        with self.local_lock:
                            self.is_held = True
                        return True

                    # If lock is empty (released), try to acquire it
                    if current_value == "":
                        if self.clerk.conditional_put(
                            self.lock_name, self.owner_id, current_version
                        ):
                            with self.local_lock:
                                self.is_held = True
                            return True

                    # Lock is held by someone else - we cannot steal it
                    # In a proper distributed lock, we must wait for release
                    # If no timeout specified, add a brief delay to prevent busy waiting
                    if timeout is None:
                        time.sleep(self.retry_delay)

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

            # Failed to acquire, check timeout before backoff
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            # Backoff and retry
            time.sleep(self.retry_delay)

            # Check timeout again after backoff
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

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
