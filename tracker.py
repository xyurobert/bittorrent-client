"""For communicating with the tracker. To run the code, see __main__.py"""

import bencodepy # type: ignore
import ipaddress
import logging
import random
import socket
from typing import Any, Literal, Optional
from urllib.parse import quote

from common import Addr, from_bytes_network, to_bytes_network

logger = logging.getLogger(__name__)


HTTP_START = b"HTTP/1.1"

UDP_PROTOCOL_ID = 0x41727101980
"""Magic constant from https://www.bittorrent.org/beps/bep_0015.html"""

UDP_TIMEOUT = 15

UDP_RETRIES = 4

UDP_CONN_ACTION = to_bytes_network(0, 4)

UDP_ANNOUNCE_ACTION = to_bytes_network(1, 4)


class HttpResponse:
    def __init__(
        self, code: int, code_name: str, headers: dict[str, str], body: Optional[bytes]
    ):
        self.code = code
        self.code_name = code_name
        self.headers = headers
        self.body = body

    def __str__(self) -> str:
        return (
            f"{HTTP_START.decode()} {self.code} {self.code_name}\n"
            + "\n".join(f"{header}: {value}" for header, value in self.headers.items())
            + str(f"\n{self.body!r}" if self.body is not None else b"")
        )

    @staticmethod
    def parse(resp: bytes) -> Optional["HttpResponse"]:
        """Parsing an HTTP response. Returns None for responses it couldn't parse"""

        def log_invalid():
            logger.error(f"Invalid response: {resp!r}")

        if not resp.startswith(HTTP_START):
            log_invalid()
            return None

        rest = resp.strip(HTTP_START + b" ")

        # The first line has the code in it
        split = rest.split(b"\r\n", maxsplit=1)
        if len(split) != 2:
            log_invalid()
            return None

        # Extract code from first line
        code_line, rest = split
        if b" " not in code_line:
            log_invalid()
            return None
        code_raw, code_name = code_line.split(b" ")

        if not code_raw.isdigit():
            log_invalid()
            return None
        code = int(code_raw)

        headers: dict[str, str] = {}
        body: Optional[bytes] = None

        while len(rest) > 0:
            # There are two \r\ns between the headers and the body
            if rest.startswith(b"\r\n"):
                body = rest.removeprefix(b"\r\n")
                break

            if b"\r\n" not in rest:
                first_line_raw = rest
                rest = b""
            else:
                first_line_raw, rest = rest.split(b"\r\n", maxsplit=1)

            first_line = first_line_raw.decode()
            if ": " not in first_line:
                logger.error(f"Invalid header line {first_line}, response: {resp!r}")
                return None

            name, value = first_line.split(": ")
            if name in headers:
                # If multiple headers with the same name, join together with commas
                # See https://stackoverflow.com/a/4371395/11882002
                headers[name] = f"{headers[name]}, {value}"
            else:
                headers[name] = value

        return HttpResponse(int(code), code_name.decode(), headers, body)


def announce_tcp(
    tracker_addr: Addr,
    tracker_path: str,
    tracker_hostname: str,
    *,
    info_hash: bytes,
    peer_id: bytes,
    our_port: int,
    uploaded: int,
    downloaded: int,
    left: int,
    event: Optional[Literal["started", "completed", "stopped"]],
    numwant: Optional[int],
) -> tuple[int, list[Addr]]:
    """
    Talk to the tracker using TCP.
    Returns tracker interval and list of peers
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(tracker_addr)

        params: dict[str, Optional[str | bytes]] = {
            "info_hash": info_hash,
            "peer_id": peer_id,
            "ip": socket.gethostbyname(socket.gethostname()),
            "port": str(our_port),
            "uploaded": str(uploaded),
            "downloaded": str(downloaded),
            "left": str(left),
            "compact": "1",
            "no_peer_id": "0",
            "event": event,
            "numwant": None if numwant is None else str(numwant),
        }

        get_details = "&".join(
            f"{param}={quote(value)}"
            for param, value in params.items()
            if value is not None
        )

        request = f"GET {tracker_path}?{get_details} HTTP/1.1\r\nHost: {tracker_hostname}\r\n\r\n"
        # logger.debug(f"[request_tracker] Sent request")
        sock.sendall(request.encode("utf-8"))

        raw_response = sock.recv(1024)
        response = HttpResponse.parse(raw_response)
        if response is None:
            logger.error("[request_tracker] Tracker gave invalid response")
            exit(1)

        body_raw = response.body
        if body_raw is None:
            logger.critical(f"[request_tracker] Tracker response body empty")
            exit(1)

        info: dict[bytes, Any] = bencodepy.decode(body_raw)

        if b"failure code" in info:
            logger.critical(f"Failure code: {info[b'failure code']}")
            exit(1)

        peers = info.get(b"peers")
        if peers is None:
            logger.critical(f"No peers in info: {info}")
            exit(1)

        return info[b"interval"], get_peers(peers)


def announce_udp(
    tracker_addr: tuple[str, int],
    tracker_path: str,
    tracker_hostname: str,
    *,
    info_hash: bytes,
    peer_id: bytes,
    our_port: int,
    uploaded: int,
    downloaded: int,
    left: int,
    event: Optional[Literal["started", "completed", "stopped"]],
    numwant: Optional[int],
) -> tuple[int, list[Addr]]:
    """
    Talk to the tracker using UDP. See https://www.bittorrent.org/beps/bep_0015.html
    and https://www.rasterbar.com/products/libtorrent/udp_tracker_protocol.html.
    Returns tracker interval and list of peers
    """
    logger.debug(
        f"[announce_udp] {tracker_addr=} {tracker_path=} {downloaded=} {left=} {numwant=}"
    )
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(UDP_TIMEOUT)
        # sock.bind(("0.0.0.0", 0))

        transaction_id = to_bytes_network(random.randint(0, 2**32 - 1), 4)

        sock.sendto(
            to_bytes_network(UDP_PROTOCOL_ID, 8) + UDP_CONN_ACTION + transaction_id,
            tracker_addr,
        )

        conn_resp = request_udp(sock, 16, "Connect request")
        assert conn_resp[:4] == UDP_CONN_ACTION, conn_resp
        assert conn_resp[4:8] == transaction_id, conn_resp

        logger.debug(f"[announce_udp] {conn_resp=} {transaction_id=}")

        connection_id = conn_resp[8:]
        transaction_id = to_bytes_network(random.randint(0, 2**32 - 1), 4)

        if event is None:
            event_int = 0
        elif event == "completed":
            event_int = 1
        elif event == "started":
            event_int = 2
        else:
            event_int = 3

        # It looks like the key must be consistent, hence the seed based on peer ID
        random.seed(from_bytes_network(peer_id))
        key = random.randint(0, 2**32 - 1)

        numwant = 50 if numwant is None else numwant

        _, my_port = sock.getsockname()

        pieces = [
            connection_id,
            UDP_ANNOUNCE_ACTION,
            transaction_id,
            info_hash,
            peer_id,
            to_bytes_network(downloaded, 8),
            to_bytes_network(left, 8),
            to_bytes_network(uploaded, 8),
            to_bytes_network(event_int, 4),
            to_bytes_network(0, 4),
            to_bytes_network(key, 4),
            numwant.to_bytes(4, byteorder="big", signed=True),
            to_bytes_network(my_port, 2),
            # + to_bytes_network(2, 1)  # Request string extension
            # + to_bytes_network(len(tracker_path), 1)
            # + tracker_path.encode(),
        ]
        logger.debug("pieces: " + "\n".join(map(str, pieces)))
        sock.sendto(b"".join(pieces), tracker_addr)

        announce_resp = request_udp(sock, 1024, "Announce request")
        assert announce_resp[:4] == UDP_ANNOUNCE_ACTION, announce_header
        assert announce_resp[4:8] == transaction_id, announce_header

        interval = from_bytes_network(announce_resp[8:12])
        num_leechers = from_bytes_network(announce_resp[12:16])
        num_seeders = from_bytes_network(announce_resp[16:20])
        peer_data = announce_resp[20:]

        logger.debug(f"[announce_udp] {interval=} {num_leechers=} {num_seeders=}")

        peer_data_len = (num_leechers + num_seeders) * 6
        assert (
            len(peer_data) == peer_data_len
        ), f"{len(peer_data)}, {peer_data_len}, {peer_data!r}"

        return interval, get_peers(peer_data)


def request_udp(sock: socket.socket, length: int, req_name: str) -> bytes:
    for i in range(UDP_RETRIES):
        try:
            data, _ = sock.recvfrom(length)
            break
        except TimeoutError as e:
            if i == UDP_RETRIES - 1:
                raise e
            logger.debug(f"[announce_udp] {req_name} timed out {i+1} time(s)")

    return data


def get_peers(data: bytes) -> list[Addr]:
    """Get the list of peer addresses from the tracker response"""
    # [4-byte IP address][2-byte port] for each peer
    peers: list[Addr] = []
    for i in range(0, len(data), 6):
        ip_bytes = data[i : i + 4]
        port_bytes = data[i + 4 : i + 6]

        # Binary IP to string
        ip = ".".join(str(byte) for byte in ip_bytes)

        # Binary port to integer
        port = int.from_bytes(port_bytes, byteorder="big")

        peers.append((ip, port))

    return peers
