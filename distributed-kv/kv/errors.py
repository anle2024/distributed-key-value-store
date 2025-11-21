"""
Error definitions for the KV service.
"""


class KVError(Exception):
    """Base class for KV service errors."""

    pass


class ErrNoKey(KVError):
    """Error returned when a key does not exist."""

    pass


class ErrVersion(KVError):
    """Error returned when version mismatch occurs."""

    pass


class ErrMaybe(KVError):
    """Error returned when operation might have succeeded due to unreliable network."""

    pass


class ErrTimeout(KVError):
    """Error returned when RPC times out."""

    pass
