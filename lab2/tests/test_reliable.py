"""
Test cases for reliable KV operations.
"""

import pytest
import threading
import time
import sys
import os

# Add the lab2 directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kv import KVServer, Clerk, ErrNoKey, ErrVersion


class TestReliableKV:

    def setup_method(self):
        """Set up test environment."""
        self.server = KVServer(unreliable=False)
        self.clerk = Clerk(self.server)

    def test_basic_put_get(self):
        """Test basic Put and Get operations."""
        # Test creating new key
        self.clerk.put("key1", "value1", 0)
        value, version = self.clerk.get("key1")
        assert value == "value1"
        assert version == 1

        # Test updating existing key
        self.clerk.put("key1", "value2", 1)
        value, version = self.clerk.get("key1")
        assert value == "value2"
        assert version == 2

    def test_get_nonexistent_key(self):
        """Test Get on non-existent key."""
        with pytest.raises(ErrNoKey):
            self.clerk.get("nonexistent")

    def test_put_nonexistent_key_wrong_version(self):
        """Test Put on non-existent key with non-zero version."""
        with pytest.raises(ErrNoKey):
            self.clerk.put("newkey", "value", 1)

    def test_put_version_mismatch(self):
        """Test Put with wrong version on existing key."""
        self.clerk.put("key1", "value1", 0)

        # Wrong version should fail
        with pytest.raises(ErrVersion):
            self.clerk.put("key1", "value2", 5)

        # Verify original value unchanged
        value, version = self.clerk.get("key1")
        assert value == "value1"
        assert version == 1

    def test_multiple_keys(self):
        """Test operations on multiple keys."""
        # Create multiple keys
        for i in range(5):
            self.clerk.put(f"key{i}", f"value{i}", 0)

        # Verify all keys
        for i in range(5):
            value, version = self.clerk.get(f"key{i}")
            assert value == f"value{i}"
            assert version == 1

    def test_concurrent_puts_same_key(self):
        """Test concurrent Put operations on the same key."""
        self.clerk.put("shared", "initial", 0)

        results = []
        errors = []

        def update_key(client_id: int, new_value: str):
            try:
                clerk = Clerk(self.server)
                for attempt in range(10):
                    try:
                        value, version = clerk.get("shared")
                        clerk.put("shared", f"{new_value}_{client_id}", version)
                        results.append(f"{new_value}_{client_id}")
                        break
                    except ErrVersion:
                        # Retry on version conflict
                        time.sleep(0.001)
                        continue
            except Exception as e:
                errors.append(str(e))

        # Start multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=update_key, args=(i, "updated"))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # Check results
        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(results) == 5, f"Expected 5 updates, got {len(results)}"

        # Verify final state
        final_value, final_version = self.clerk.get("shared")
        assert final_version == 6  # initial + 5 updates
        assert final_value in results

    def test_concurrent_puts_different_keys(self):
        """Test concurrent Put operations on different keys."""
        results = {}
        errors = []

        def create_keys(start_idx: int, count: int):
            try:
                clerk = Clerk(self.server)
                for i in range(start_idx, start_idx + count):
                    clerk.put(f"key_{i}", f"value_{i}", 0)
                    results[f"key_{i}"] = f"value_{i}"
            except Exception as e:
                errors.append(str(e))

        # Start multiple threads creating different keys
        threads = []
        for i in range(5):
            t = threading.Thread(target=create_keys, args=(i * 10, 10))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # Check results
        assert len(errors) == 0, f"Unexpected errors: {errors}"
        assert len(results) == 50, f"Expected 50 keys, got {len(results)}"

        # Verify all keys exist
        for key, expected_value in results.items():
            value, version = self.clerk.get(key)
            assert value == expected_value
            assert version == 1

    def test_conditional_put(self):
        """Test conditional put helper method."""
        self.clerk.put("key1", "value1", 0)

        # Successful conditional put
        assert self.clerk.conditional_put("key1", "value2", 1) == True
        value, version = self.clerk.get("key1")
        assert value == "value2"
        assert version == 2

        # Failed conditional put
        assert self.clerk.conditional_put("key1", "value3", 1) == False
        value, version = self.clerk.get("key1")
        assert value == "value2"  # unchanged
        assert version == 2

    def test_create_if_missing(self):
        """Test create if missing helper method."""
        # Create new key
        assert self.clerk.create_if_missing("newkey", "newvalue") == True
        value, version = self.clerk.get("newkey")
        assert value == "newvalue"
        assert version == 1

        # Try to create existing key
        assert self.clerk.create_if_missing("newkey", "othervalue") == False
        value, version = self.clerk.get("newkey")
        assert value == "newvalue"  # unchanged
        assert version == 1

    def test_server_stats(self):
        """Test server statistics."""
        stats = self.server.get_stats()
        assert stats["num_keys"] == 0

        # Add some keys
        for i in range(3):
            self.clerk.put(f"key{i}", f"value{i}", 0)

        stats = self.server.get_stats()
        assert stats["num_keys"] == 3
