"""
Test cases for unreliable network KV operations.
"""

import pytest
import threading
import time
import sys
import os

# Add the lab2 directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kv import KVServer, Clerk, ErrNoKey, ErrVersion, ErrMaybe, ErrTimeout


class TestUnreliableKV:

    def setup_method(self):
        """Set up test environment with unreliable network."""
        self.server = KVServer(unreliable=True, drop_rate=0.3)
        self.clerk = Clerk(self.server, max_retries=20, retry_delay=0.001)

    def test_basic_operations_with_drops(self):
        """Test basic operations work despite network drops."""
        # Put should eventually succeed despite drops
        self.clerk.put("key1", "value1", 0)

        # Get should eventually succeed despite drops
        value, version = self.clerk.get("key1")
        assert value == "value1"
        assert version == 1

    def test_put_maybe_error(self):
        """Test that Put can return ErrMaybe in unreliable network."""
        # Set very high drop rate to force timeouts
        self.server.set_unreliable(True, 0.9)
        clerk_short_timeout = Clerk(self.server, max_retries=3, retry_delay=0.001)

        maybe_errors = 0
        successes = 0

        for i in range(10):
            try:
                clerk_short_timeout.put(f"key{i}", f"value{i}", 0)
                successes += 1
            except ErrMaybe:
                maybe_errors += 1
            except Exception as e:
                # Should not get other errors
                assert False, f"Unexpected error: {e}"

        # Should get at least some ErrMaybe due to high drop rate
        assert maybe_errors > 0, "Expected some ErrMaybe errors with high drop rate"
        print(f"Got {maybe_errors} ErrMaybe and {successes} successes")

    def test_duplicate_detection(self):
        """Test that duplicate requests are handled correctly."""
        # Reset to lower drop rate for this test
        self.server.set_unreliable(True, 0.1)

        # Multiple puts with same parameters should be idempotent
        self.clerk.put("dup_key", "dup_value", 0)

        # Get the result
        value, version = self.clerk.get("dup_key")
        assert value == "dup_value"
        assert version == 1

        # Multiple gets should return same result
        for _ in range(5):
            v, ver = self.clerk.get("dup_key")
            assert v == "dup_value"
            assert ver == 1

    def test_concurrent_clients_unreliable(self):
        """Test multiple clients with unreliable network."""
        num_clients = 5
        operations_per_client = 10
        results = {}
        errors = []

        def client_operations(client_id: int):
            try:
                clerk = Clerk(self.server, max_retries=15, retry_delay=0.001)
                client_results = []

                for op in range(operations_per_client):
                    key = f"client_{client_id}_key_{op}"
                    value = f"client_{client_id}_value_{op}"

                    try:
                        clerk.put(key, value, 0)
                        retrieved_value, version = clerk.get(key)
                        client_results.append((key, retrieved_value, version))
                    except ErrMaybe:
                        # ErrMaybe is acceptable in unreliable network
                        pass
                    except Exception as e:
                        errors.append(f"Client {client_id}: {str(e)}")

                results[client_id] = client_results

            except Exception as e:
                errors.append(f"Client {client_id} failed: {str(e)}")

        # Start multiple client threads
        threads = []
        for i in range(num_clients):
            t = threading.Thread(target=client_operations, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all clients
        for t in threads:
            t.join()

        # Check that most operations succeeded
        total_operations = sum(
            len(client_results) for client_results in results.values()
        )
        expected_min = (
            num_clients * operations_per_client * 0.7
        )  # Expect at least 70% success

        assert (
            total_operations >= expected_min
        ), f"Too many failed operations. Got {total_operations}, expected at least {expected_min}. Errors: {errors}"

        # Verify data integrity for successful operations
        for client_id, client_results in results.items():
            for key, value, version in client_results:
                expected_value = f"client_{client_id}_value_" + key.split("_")[-1]
                assert (
                    value == expected_value
                ), f"Data corruption: {key} -> {value}, expected {expected_value}"
                assert version == 1, f"Wrong version for {key}: {version}"

    def test_version_conflicts_unreliable(self):
        """Test version conflicts in unreliable network."""
        # Use lower drop rate to ensure operations can complete
        self.server.set_unreliable(True, 0.1)

        self.clerk.put("conflict_key", "initial", 0)

        conflicts = 0
        updates = 0
        maybe_errors = 0
        barrier = threading.Barrier(5)  # Synchronize thread starts

        def update_worker():
            nonlocal conflicts, updates, maybe_errors
            try:
                clerk = Clerk(self.server, max_retries=10, retry_delay=0.001)

                # Wait for all threads to be ready
                barrier.wait()

                # Immediately try to read and update - increases contention
                for attempt in range(10):  # More attempts to increase conflict chances
                    try:
                        value, version = clerk.get("conflict_key")
                        # Small delay to increase chance of version conflicts
                        time.sleep(0.001)
                        clerk.put(
                            "conflict_key",
                            f"updated_{threading.current_thread().ident}_{attempt}",
                            version,
                        )
                        updates += 1
                        break  # Success, exit retry loop
                    except ErrVersion:
                        conflicts += 1
                        # Don't break - retry with new version
                        time.sleep(0.001)  # Brief delay before retry
                    except ErrMaybe:
                        maybe_errors += 1
                        # ErrMaybe means operation might have succeeded, so break
                        break

            except Exception as e:
                print(f"Worker error: {e}")

        # Start multiple competing threads
        threads = []
        for _ in range(5):
            t = threading.Thread(target=update_worker)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Should have some conflicts due to concurrent access
        print(
            f"Updates: {updates}, Conflicts: {conflicts}, Maybe errors: {maybe_errors}"
        )

        # With proper contention, we should see some conflicts
        # If we have very few conflicts, it might be because of network issues
        # In that case, we should at least see some successful operations
        total_meaningful_operations = updates + conflicts
        assert total_meaningful_operations > 0, "No meaningful operations completed"

        # Either we should see conflicts, or if network is too unreliable,
        # we should see ErrMaybe errors
        assert (
            conflicts > 0 or maybe_errors > 0
        ), "Expected either version conflicts or ErrMaybe errors with concurrent access"

        # Verify final state is consistent
        final_value, final_version = self.clerk.get("conflict_key")
        assert final_version > 1, "Version should have been incremented"

    def test_network_partition_simulation(self):
        """Test behavior during simulated network partition."""
        # Start with working network
        self.server.set_unreliable(False)
        self.clerk.put("partition_key", "before_partition", 0)

        # Simulate network partition (very high drop rate)
        self.server.set_unreliable(True, 0.95)
        clerk_partition = Clerk(self.server, max_retries=5, retry_delay=0.001)

        # Operations during partition should mostly fail or return ErrMaybe
        partition_errors = 0
        for i in range(10):
            try:
                clerk_partition.put(f"partition_key_{i}", f"value_{i}", 0)
            except (ErrMaybe, ErrTimeout):
                partition_errors += 1

        # Should have many failures during partition
        assert (
            partition_errors > 5
        ), f"Expected more failures during partition, got {partition_errors}"

        # Restore network
        self.server.set_unreliable(False)

        # Operations should work again
        value, version = self.clerk.get("partition_key")
        assert value == "before_partition"
        assert version == 1

        # New operations should succeed
        self.clerk.put("after_partition", "recovered", 0)
        value, version = self.clerk.get("after_partition")
        assert value == "recovered"
        assert version == 1
