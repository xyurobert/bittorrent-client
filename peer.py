import socket
import time
from typing import Optional

from common import Addr


class Conn:
    """We maintain a separate Conn object for every socket we have to a peer. There can be multiple connections per peer"""

    def __init__(
        self,
        addr: Addr,
        our_port: int,
        sock: socket.socket,
        we_handshaked: bool = False,
    ):
        """
        Initializes a connection to a peer.

        Parameters:
        - `addr`: The address of the peer as an `Addr` tuple (IP, port).
        - `our_port`: The port on our side that the peer thinks it's connected to.
        - `sock`: The socket object for this connection.
        - `we_handshaked`: Whether we've already sent a handshake message to the peer.
        """
        self.addr = addr
        self.our_port = our_port
        self.sock = sock
        self.we_handshaked = we_handshaked
        self.they_handshaked = False

        self.prev_time: float = time.time()

        self.am_choking_them: bool = True
        self.am_interested_in_them: bool = False
        self.they_are_choking_me: bool = True
        self.they_are_interested_in_me: bool = False

        self.time_added = time.time()
        """When this peer was added"""
        self.upload_time = 0.0
        """How much total time this peer has spent uploading"""
        self.uploaded = 0
        """How many bytes this peer has uploaded to us"""
        self.last_request: float = time.time()
        """When the last request to this peer was made, for calculating upload rate"""

        self.last_sent: float = 0
        """Last time we sent them anything. Used for avoiding sending heartbeat"""

    def upload_rate(self) -> float:
        """How many bytes per second this peer uploads to us"""
        return self.uploaded / self.upload_time if self.upload_time > 0 else 0
    
    def receive(self, length: int) -> bytes:
        """Call recv() in a loop until it returns either None or empty"""
        res = b""
        while len(res) < length:
            data = self.sock.recv(length - len(res))
            if data is None or len(data) == 0:
                return res
            res += data
        return res
    
    def __repr__(self):
        ip, port = self.addr
        return f"Conn({ip}:{port})"


class Peer:
    """
    Represents a peer in the torrent swarm, which may have multiple active connections.
    Tracks all associated `Conn` objects and provides peer-level state and management.
    """
    def __init__(
        self,
        addr: Addr,
        conns: list[Conn],
        id: Optional[bytes] = None,
    ):
        """
        Initialize a peer.

        Parameters:
        - `addr`: The address of the peer as an `Addr` tuple (IP, port).
        - `conns`: A list of `Conn` objects representing the active connections to this peer.
        - `id`: The unique ID of the peer (optional, set during the handshake).
        """
        self.addr = addr
        self.conns = conns
        self.id = id

    def curr_conn(self) -> Conn:
        return self.conns[0]

    def next_conn(self):
        self.conns.append(self.conns.pop(0))

    def add_conn(self, conn: Conn):
        self.conns.insert(0, conn)

    def remove_conn(self, conn: Conn):
        self.conns = [c for c in self.conns if c.our_port != conn.our_port]

    def __str__(self):
        ip, port = self.addr
        return (
            "Peer("
            f"{ip}:{port}, {len(self.conns)} conns)"
        )

    def __repr__(self):
        return str(self)
