#!/usr/bin/env python3
"""
Run script for Lab 2 tests and demos.
"""
import sys
import os
import time
import threading

# Add the lab2 directory to the Python path
sys.path.insert(0, os.path.dirname(__file__))

from kv import KVServer, Clerk, ErrNoKey, ErrVersion, ErrMaybe
from lock import Lock


def demo_basic_kv():
    """Demonstrate basic KV operations."""
    print("\n=== Basic KV Operations Demo ===")

    server = KVServer()
    clerk = Clerk(server)

    print("1. Creating new key 'hello' with value 'world'")
    clerk.put("hello", "world", 0)
    value, version = clerk.get("hello")
    print(f"   Result: value='{value}', version={version}")

    print("2. Updating key 'hello' with value 'universe'")
    clerk.put("hello", "universe", 1)
    value, version = clerk.get("hello")
    print(f"   Result: value='{value}', version={version}")

    print("3. Trying to get non-existent key")
    try:
        clerk.get("nonexistent")
    except ErrNoKey:
        print("   Got ErrNoKey as expected")

    print("4. Trying to update with wrong version")
    try:
        clerk.put("hello", "wrong", 5)
    except ErrVersion:
        print("   Got ErrVersion as expected")


def demo_concurrent_access():
    """Demonstrate concurrent access to KV store."""
    print("\n=== Concurrent Access Demo ===")

    server = KVServer()

    results = []

    def worker(worker_id):
        clerk = Clerk(server)
        for i in range(5):
            key = f"worker_{worker_id}_key_{i}"
            value = f"value_{i}"
            clerk.put(key, value, 0)
            retrieved_value, version = clerk.get(key)
            results.append((worker_id, key, retrieved_value, version))

    print("Starting 3 concurrent workers...")
    threads = []
    for i in range(3):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f"Completed {len(results)} operations successfully")
    for worker_id, key, value, version in results[:6]:  # Show first 6
        print(f"   Worker {worker_id}: {key} -> {value} (v{version})")


def demo_unreliable_network():
    """Demonstrate unreliable network behavior."""
    print("\n=== Unreliable Network Demo ===")

    server = KVServer(unreliable=True, drop_rate=0.3)
    clerk = Clerk(server, max_retries=10, retry_delay=0.01)

    print("Testing with 30% packet drop rate...")

    successes = 0
    maybe_errors = 0

    for i in range(10):
        try:
            clerk.put(f"unreliable_key_{i}", f"value_{i}", 0)
            value, version = clerk.get(f"unreliable_key_{i}")
            successes += 1
        except ErrMaybe:
            maybe_errors += 1
            print(f"   Got ErrMaybe for operation {i}")
        except Exception as e:
            print(f"   Unexpected error for operation {i}: {e}")

    print(f"Results: {successes} successes, {maybe_errors} maybe errors")


def demo_distributed_lock():
    """Demonstrate distributed lock functionality."""
    print("\n=== Distributed Lock Demo ===")

    server = KVServer()
    clerk = Clerk(server)

    print("1. Single client lock acquire/release")
    lock = Lock(clerk, "demo_lock")
    print(f"   Lock acquired: {lock.acquire()}")
    print(f"   Lock state: {lock.check_lock_state()}")
    print(f"   Is locked: {lock.is_locked()}")
    lock.release()
    print(f"   After release - Lock state: {lock.check_lock_state()}")

    print("\n2. Multiple clients competing for lock")

    acquired_order = []

    def lock_worker(worker_id):
        worker_clerk = Clerk(server)
        worker_lock = Lock(worker_clerk, "shared_demo_lock")

        if worker_lock.acquire(timeout=2.0):
            acquired_order.append(worker_id)
            print(f"   Worker {worker_id} acquired lock")
            time.sleep(0.1)  # Hold lock briefly
            worker_lock.release()
            print(f"   Worker {worker_id} released lock")
        else:
            print(f"   Worker {worker_id} timed out")

    threads = []
    for i in range(3):
        t = threading.Thread(target=lock_worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print(f"Lock acquisition order: {acquired_order}")


def demo_context_manager():
    """Demonstrate lock context manager."""
    print("\n=== Lock Context Manager Demo ===")

    server = KVServer()
    clerk = Clerk(server)

    print("Using lock as context manager...")
    with Lock(clerk, "context_lock") as lock:
        print(f"   Inside context: lock is held = {lock.is_locked()}")
        time.sleep(0.1)

    # Check that lock was released
    check_lock = Lock(clerk, "context_lock")
    print(f"   After context: lock state = {check_lock.check_lock_state()}")


def run_manual_tests():
    """Run a series of manual tests for verification."""
    print("Running manual verification tests...")

    demo_basic_kv()
    demo_concurrent_access()
    demo_unreliable_network()
    demo_distributed_lock()
    demo_context_manager()

    print("\n=== All demos completed successfully! ===")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            print("To run pytest tests, use:")
            print("  pytest tests/test_reliable.py -v")
            print("  pytest tests/test_unreliable.py -v")
            print("  pytest tests/test_lock_reliable.py -v")
            print("  pytest tests/test_lock_unreliable.py -v")
            print("\nOr run all tests:")
            print("  pytest tests/ -v")
        elif sys.argv[1] == "demo":
            run_manual_tests()
        else:
            print("Usage: python run.py [demo|test]")
    else:
        run_manual_tests()
