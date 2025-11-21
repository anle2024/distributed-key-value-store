"""
RPC types for the KV service.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class GetArgs:
    key: str


@dataclass
class GetReply:
    value: str = ""
    version: int = 0
    err: str = ""


@dataclass
class PutArgs:
    key: str
    value: str
    version: int


@dataclass
class PutReply:
    err: str = ""


# RPC message envelope for unreliable network simulation
@dataclass
class RPCMessage:
    method: str
    args: object
    reply: object
    seq_num: int
    client_id: str
