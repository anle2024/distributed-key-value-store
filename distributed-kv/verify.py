#!/usr/bin/env python3
"""
Quick verification script to test the implementation.
"""
import sys
import os
import traceback

# Add the lab2 directory to the Python path
sys.path.insert(0, os.path.dirname(__file__))


def verify_implementation():
    """Run quick verification tests."""
    try:
        # Test basic imports
        print("Testing imports...")
        from kv import KVServer, Clerk, ErrNoKey, ErrVersion, ErrMaybe
        from lock import Lock

        print("✓ All imports successful")

        # Test basic KV operations
        print("\nTesting basic KV operations...")
        server = KVServer()
        clerk = Clerk(server)

        clerk.put("test", "value", 0)
        value, version = clerk.get("test")
        assert value == "value" and version == 1
        print("✓ Basic Put/Get works")

        # Test version checking
        try:
            clerk.put("test", "new", 5)  # Wrong version
            assert False, "Should have gotten ErrVersion"
        except ErrVersion:
            print("✓ Version checking works")

        # Test lock
        print("\nTesting distributed lock...")
        lock = Lock(clerk, "test_lock")
        assert lock.acquire() == True
        assert lock.is_locked() == True
        lock.release()
        assert lock.is_locked() == False
        print("✓ Basic lock operations work")

        # Test unreliable network
        print("\nTesting unreliable network...")
        unreliable_server = KVServer(unreliable=True, drop_rate=0.5)
        unreliable_clerk = Clerk(unreliable_server, max_retries=10)

        try:
            unreliable_clerk.put("unreliable", "test", 0)
            value, version = unreliable_clerk.get("unreliable")
            print("✓ Unreliable network operations work")
        except ErrMaybe:
            print("✓ ErrMaybe correctly raised in unreliable network")

        print("\n🎉 All verification tests passed!")
        print("\nThe implementation is ready to use.")
        print("\nNext steps:")
        print("1. Install pytest: pip install pytest")
        print("2. Run demos: python run.py demo")
        print("3. Run tests: pytest tests/ -v")

        return True

    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    verify_implementation()
