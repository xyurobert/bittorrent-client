from typing import TypeAlias

Addr: TypeAlias = tuple[str, int]
"""An (ip address, port) tuple"""

def from_bytes_network(b: bytes) -> int:
  return int.from_bytes(b, byteorder="big")

def to_bytes_network(i: int, length: int) -> bytes:
  return i.to_bytes(length, byteorder="big")
