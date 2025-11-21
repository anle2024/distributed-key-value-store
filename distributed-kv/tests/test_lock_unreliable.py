"""
Test cases for distributed lock with unreliable network.
"""

import pytest
import threading
import time
import sys
import os

# Add the lab2 directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kv import KVServer, Clerk, ErrMaybe
from lock import Lock


class TestLockUnreliable:

    def setup_method(self):
        """Set up test environment with unreliable network."""
        self.server = KVServer(unreliable=True, drop_rate=0.2)
        self.clerk = Clerk(self.server, max_retries=15, retry_delay=0.001)
        self.lock = Lock(self.clerk, "test_lock_unreliable", retry_delay=0.001)

    def test_acquire_release_with_drops(self):
        """Test basic lock operations work despite network drops."""
        # Should eventually acquire despite drops
        assert self.lock.acquire(timeout=5.0) == True
        assert self.lock.is_locked() == True

        # Should eventually release despite drops
        self.lock.release()
        assert self.lock.is_locked() == False

    def test_multiple_clients_unreliable_network(self):
        """Test multiple clients with unreliable network."""
        num_clients = 5
        successful_acquisitions = []
        errors = []

        def client_lock_operations(client_id: int):
            try:
                clerk = Clerk(self.server, max_retries=20, retry_delay=0.001)
                lock = Lock(clerk, "unreliable_shared_lock", retry_delay=0.001)

                # Try to acquire lock
                if lock.acquire(timeout=10.0):
                    successful_acquisitions.append(client_id)

                    # Hold lock briefly
                    time.sleep(0.01)

                    # Release lock
                    lock.release()
                else:
                    errors.append(f"Client {client_id} failed to acquire lock")

            except Exception as e:
                errors.append(f"Client {client_id} error: {str(e)}")

        # Start multiple client threads
        threads = []
        for i in range(num_clients):
            t = threading.Thread(target=client_lock_operations, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all clients
        for t in threads:
            t.join()

        # Most clients should succeed despite unreliable network
        min_expected_successes = int(
            num_clients * 0.8
        )  # Allow for some failures due to timeouts

        assert (
            len(successful_acquisitions) >= min_expected_successes
        ), f"Expected at least {min_expected_successes} successful lock acquisitions, got {len(successful_acquisitions)}. Errors: {errors}"

        print(f"Successful acquisitions: {len(successful_acquisitions)}/{num_clients}")

    def test_lock_with_high_contention_unreliable(self):
        """Test lock behavior under high contention with unreliable network."""
        # Increase drop rate for this test
        self.server.set_unreliable(True, 0.3)

        num_threads = 8
        lock_acquisitions = []
        total_attempts = []

        def contending_client(client_id: int):
            attempts = 0
            try:
                clerk = Clerk(self.server, max_retries=25, retry_delay=0.001)

                for round_num in range(3):  # Multiple rounds per client
                    attempts += 1
                    lock = Lock(clerk, "high_contention_lock", retry_delay=0.001)

                    if lock.acquire(timeout=8.0):
                        lock_acquisitions.append(f"{client_id}_{round_num}")
                        time.sleep(0.005)  # Brief hold
                        lock.release()

                total_attempts.append(attempts)

            except Exception as e:
                print(f"Client {client_id} error: {e}")

        # Start all threads
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=contending_client, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Should get reasonable success rate
        total_expected = num_threads * 3
        min_successes = int(
            total_expected * 0.6
        )  # Allow for network drops and timeouts

        assert (
            len(lock_acquisitions) >= min_successes
        ), f"Expected at least {min_successes} lock acquisitions out of {total_expected}, got {len(lock_acquisitions)}"

        print(f"Lock acquisitions: {len(lock_acquisitions)}/{total_expected}")

    def test_lock_recovery_after_network_partition(self):
        """Test lock recovery after simulated network partition."""
        # Start with normal operation
        self.server.set_unreliable(False)
        assert self.lock.acquire() == True
        self.lock.release()

        # Simulate severe network partition
        self.server.set_unreliable(True, 0.9)

        # Try operations during partition - should mostly fail or take long
        partition_clerk = Clerk(self.server, max_retries=5, retry_delay=0.001)
        partition_lock = Lock(partition_clerk, "partition_test_lock", retry_delay=0.001)

        start_time = time.time()
        result = partition_lock.acquire(timeout=1.0)
        elapsed = time.time() - start_time

        # During partition, operation should timeout or take long time
        if not result:
            assert elapsed >= 1.0, "Should have timed out"
        else:
            # If it succeeded, it should have taken significant time due to retries
            partition_lock.release()

        # Restore network
        self.server.set_unreliable(True, 0.1)  # Back to normal unreliable rate

        # Operations should work again
        recovery_clerk = Clerk(self.server, max_retries=15, retry_delay=0.001)
        recovery_lock = Lock(recovery_clerk, "recovery_lock", retry_delay=0.001)

        assert recovery_lock.acquire(timeout=5.0) == True
        recovery_lock.release()

    def test_concurrent_acquire_with_maybe_errors(self):
        """Test that ErrMaybe is handled correctly in lock operations."""
        # Set moderate drop rate to induce some ErrMaybe errors
        self.server.set_unreliable(True, 0.4)

        successes = []
        timeouts = []

        def maybe_error_client(client_id: int):
            try:
                clerk = Clerk(self.server, max_retries=10, retry_delay=0.001)
                lock = Lock(clerk, "maybe_error_lock", retry_delay=0.001)

                if lock.acquire(timeout=5.0):
                    successes.append(client_id)
                    time.sleep(0.01)
                    lock.release()
                else:
                    timeouts.append(client_id)

            except Exception as e:
                print(f"Client {client_id} unexpected error: {e}")

        # Start several clients
        threads = []
        for i in range(6):
            t = threading.Thread(target=maybe_error_client, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Should get some successes despite ErrMaybe errors
        total_clients = 6
        min_successes = int(total_clients * 0.5)  # Allow for higher failure rate

        assert (
            len(successes) >= min_successes
        ), f"Expected at least {min_successes} successes out of {total_clients}, got {len(successes)}. Timeouts: {len(timeouts)}"

        print(f"Successes: {len(successes)}, Timeouts: {len(timeouts)}")

    def test_lock_state_consistency_unreliable(self):
        """Test that lock state remains consistent despite network issues."""
        # Create multiple lock instances for same lock name
        clerks = [
            Clerk(self.server, max_retries=15, retry_delay=0.001) for _ in range(3)
        ]
        locks = [Lock(clerk, "consistency_lock", retry_delay=0.001) for clerk in clerks]

        # First lock acquires
        assert locks[0].acquire(timeout=5.0) == True

        # Other locks should see it as held
        for i in range(1, 3):
            lock_state = locks[i].check_lock_state()
            # Should either see the owner or be unable to check due to network
            if lock_state is not None:
                assert (
                    lock_state == locks[0].owner_id or lock_state != locks[i].owner_id
                )

        # Release the lock
        locks[0].release()

        # Eventually, other locks should be able to acquire
        acquired_by = None
        for i in range(1, 3):
            if locks[i].acquire(timeout=3.0):
                acquired_by = i
                break

        assert (
            acquired_by is not None
        ), "One of the other locks should have been able to acquire"

        # Clean up
        if acquired_by is not None:
            locks[acquired_by].release()
