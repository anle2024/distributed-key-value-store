"""
Test cases for reliable distributed lock operations.
"""

import pytest
import threading
import time
import sys
import os

# Add the lab2 directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kv import KVServer, Clerk
from lock import Lock


class TestLockReliable:

    def setup_method(self):
        """Set up test environment."""
        self.server = KVServer(unreliable=False)
        self.clerk = Clerk(self.server)
        self.lock = Lock(self.clerk, "test_lock")

    def test_single_client_acquire_release(self):
        """Test basic lock acquire and release."""
        # Initially not locked
        assert not self.lock.is_locked()
        assert self.lock.check_lock_state() is None

        # Acquire lock
        assert self.lock.acquire() == True
        assert self.lock.is_locked() == True

        # Check lock state
        lock_owner = self.lock.check_lock_state()
        assert lock_owner == self.lock.owner_id

        # Release lock
        self.lock.release()
        assert self.lock.is_locked() == False

        # Check lock state after release
        lock_state = self.lock.check_lock_state()
        assert lock_state == "" or lock_state is None

    def test_double_acquire_same_client(self):
        """Test that same client can acquire lock it already holds."""
        assert self.lock.acquire() == True
        assert self.lock.acquire() == True  # Should succeed immediately
        assert self.lock.is_locked() == True

        self.lock.release()
        assert self.lock.is_locked() == False

    def test_multiple_clients_competition(self):
        """Test multiple clients competing for the same lock."""
        num_clients = 5
        lock_holders = []
        errors = []

        def try_acquire_lock(client_id: int):
            try:
                clerk = Clerk(self.server)
                lock = Lock(clerk, "shared_lock", retry_delay=0.001)

                if lock.acquire(timeout=2.0):
                    lock_holders.append(client_id)
                    # Hold lock briefly
                    time.sleep(0.01)
                    lock.release()
                else:
                    errors.append(f"Client {client_id} failed to acquire lock")

            except Exception as e:
                errors.append(f"Client {client_id} error: {str(e)}")

        # Start multiple threads
        threads = []
        for i in range(num_clients):
            t = threading.Thread(target=try_acquire_lock, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # Check results
        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert (
            len(lock_holders) == num_clients
        ), f"Expected all {num_clients} clients to get lock, got {len(lock_holders)}"

        # All clients should have gotten the lock at some point
        assert set(lock_holders) == set(range(num_clients))

    def test_lock_timeout(self):
        """Test lock acquisition timeout."""
        # One client holds the lock
        clerk1 = Clerk(self.server)
        lock1 = Lock(clerk1, "timeout_lock")
        assert lock1.acquire() == True

        # Another client tries to acquire with timeout
        clerk2 = Clerk(self.server)
        lock2 = Lock(clerk2, "timeout_lock")

        start_time = time.time()
        result = lock2.acquire(timeout=0.1)
        elapsed = time.time() - start_time

        assert result == False, "Should have timed out"
        assert elapsed >= 0.1, f"Should have waited at least 0.1s, waited {elapsed}s"
        assert (
            elapsed < 0.2
        ), f"Should not have waited much longer than timeout, waited {elapsed}s"

        # Release first lock
        lock1.release()

        # Now second client should be able to acquire
        assert lock2.acquire(timeout=0.1) == True
        lock2.release()

    def test_context_manager(self):
        """Test lock as context manager."""
        # Use lock with context manager
        with Lock(self.clerk, "context_lock") as lock:
            assert lock.is_locked() == True
            lock_state = lock.check_lock_state()
            assert lock_state == lock.owner_id

        # After context, lock should be released
        # Create new lock instance to check state
        check_lock = Lock(self.clerk, "context_lock")
        assert (
            check_lock.check_lock_state() == "" or check_lock.check_lock_state() is None
        )

    def test_context_manager_with_exception(self):
        """Test that lock is released even if exception occurs."""
        lock_name = "exception_lock"

        try:
            with Lock(self.clerk, lock_name) as lock:
                assert lock.is_locked() == True
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected

        # Lock should still be released
        check_lock = Lock(self.clerk, lock_name)
        assert (
            check_lock.check_lock_state() == "" or check_lock.check_lock_state() is None
        )

        # Should be able to acquire the lock now
        assert check_lock.acquire() == True
        check_lock.release()

    def test_release_without_holding(self):
        """Test error when releasing lock we don't hold."""
        with pytest.raises(Exception, match="Cannot release lock we don't hold"):
            self.lock.release()

    def test_concurrent_acquire_attempts(self):
        """Test many threads trying to acquire same lock simultaneously."""
        num_threads = 10
        successes = []
        failures = []

        def acquire_attempt(thread_id: int):
            try:
                clerk = Clerk(self.server)
                lock = Lock(clerk, "concurrent_lock", retry_delay=0.001)

                if lock.acquire(timeout=1.0):
                    successes.append(thread_id)
                    # Brief critical section
                    time.sleep(0.005)
                    lock.release()
                else:
                    failures.append(thread_id)

            except Exception as e:
                failures.append(f"Thread {thread_id}: {str(e)}")

        # Start all threads at roughly the same time
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=acquire_attempt, args=(i,))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # All threads should eventually succeed
        assert (
            len(successes) == num_threads
        ), f"Expected {num_threads} successes, got {len(successes)}. Failures: {failures}"
        assert len(failures) == 0, f"Unexpected failures: {failures}"

    def test_lock_reentrance_different_instances(self):
        """Test behavior when same client creates multiple lock instances."""
        lock1 = Lock(self.clerk, "reentrant_lock")
        lock2 = Lock(
            self.clerk, "reentrant_lock"
        )  # Different instance, same clerk, same lock name

        # First instance acquires
        assert lock1.acquire() == True

        # Second instance (same client) should be able to acquire immediately
        # since it's the same underlying clerk/client
        assert lock2.acquire() == True

        # Both should report being locked
        assert lock1.is_locked() == True
        assert lock2.is_locked() == True

        # Release from first instance
        lock1.release()

        # Lock should still be held (by second instance's perspective)
        # But actually released since they share the same underlying lock
        lock_state = lock2.check_lock_state()
        # The behavior here depends on implementation - both instances track the same lock
