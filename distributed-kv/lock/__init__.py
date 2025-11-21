"""Lock package for Lab 2 implementation."""

import sys
import os

# Add parent directory to path for absolute imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lock.lock import Lock

__all__ = ["Lock"]
