"""
Distributed Key-Value Store Implementation
"""

try:
    # Try relative imports first (when used as a package)
    from . import kv
    from . import lock
except ImportError:
    # Fall back to absolute imports (when run directly or by pytest)
    import kv
    import lock

__version__ = "1.0.0"
__author__ = "Distributed Systems Implementation"
