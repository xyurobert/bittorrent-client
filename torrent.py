from alive_progress import alive_bar  # type: ignore
import os
import bencodepy  # type: ignore
from collections import defaultdict
import hashlib
from itertools import groupby
import logging
from math import ceil
import random
import selectors
import socket
import threading
import time
from typing import Any, Literal, Optional
from urllib.parse import urlparse

import callbacks
from common import Addr, from_bytes_network
import tracker
from peer import Conn, Peer
from piece import FilePieces, Piece, MAX_SUBPIECE_SIZE

PEER_TIMEOUT = 120
"""How many seconds we should wait without any messages from a peer before considering it dead"""

HEART_BEAT_INTERVAL = 90
"""How many seconds we should wait before sending keep-alive messages to avoid other peers thinking we're dead"""

PEER_ID_LEN = 20

PEER_ID_START = b"-ZZ1234-"
"""For making Azureus style [peer IDs](https://wiki.theory.org/BitTorrentSpecification#peer_id)"""

PSTR = b"BitTorrent protocol"
"""String used in handshake (https://wiki.theory.org/BitTorrentSpecification#Handshake)"""

sel = selectors.DefaultSelector()
"""Holds the sockets of our peers"""

logger = logging.getLogger(__name__)

"""Setting subpiece size to 2^15 per spec"""
SUBPIECE_SIZE = 16384

DEFAULT_TRACKER_INTERVAL = 900

REQUEST_INTERVAL = 0.1
"""How often to ask for pieces"""

REQUEST_TIMEOUT = 5
"""How long to wait for a response to a piece request"""

PEER_THRESHOLD = 20
"""If we only have these many peers, ask the tracker for more"""

UNCHOKED_PEER_THRESHOLD = 2
"""If we only have these many peers not choking us, ask the tracker for more"""

CHOKE_UPDATE_INTERVAL = 10
"""Update list of choked peers every 10 seconds"""

OPTIMISTIC_UNCHOKE_INTERVAL = 30
"""Update optimistic unchoke every 30 seconds"""

NUM_UNCHOKED = 7

NEWLY_CONNECTED_THRESHOLD = 30
"""How many seconds after creation a peer should be considered newly connected.
Used for optimistic unchoking"""

MAX_PEERS_ADD = 3
"""Maximum number of peers to send connection requests to at a time"""

MIN_TRACKER_REQ_INTERVAL = 3
"""Wait at least these many seconds before requesting the tracker for more peers"""

ENDGAME_THRESHOLD = 20
"""If we only have these many pieces left, go into endgame mode"""

CONN_SPLIT_THRESHOLD = 3000
"""Give a peer a new connection for these many bytes/s of upload rate"""

USELESS_THRESHOLD = 15
"""If the upload rate after these many seconds is 0, drop them"""

BLACKLIST_TIMEOUT = 10
"""If a peer was removed these many seconds ago, don't add it back"""


class Torrent:
    def __init__(
        self,
        our_ports: list[int],
        torrent_file_path: str,
        output_path: Optional[str],
        max_pieces: int,
        restrict_peers: Optional[list[Addr]],
        selfish: bool,
    ):
        """
        # Parameters
        - `our_ports`: What ports we're listening on
        - `output_path`: Directory to put files in
        - `restrict_peers`: Restrict to these peers (empty to use peers given by tracker)
        """

        self.piece_first_search_time: dict[int, float] = defaultdict(
            lambda: time.time()
        )

        assert len(our_ports) > 0, "Must be listening on at least one port"
        self.our_ports = our_ports
        """What ports we're listening on"""

        self.uploaded: int = 0
        """How many bytes we've uploaded so far"""

        self.downloaded: int = 0
        """How many bytes we've downloaded so far (including pieces that failed the integrity check)"""

        self.max_pieces: int = max_pieces
        """Max pieces to request at once"""

        self.restrict_peers: Optional[list[Addr]] = restrict_peers

        self.peers: dict[Addr, Peer] = {}
        """All our peers, indexed by their address"""

        self.unchoked: list[Conn] = []
        """Peers whom we're currently not choking"""

        self.optimistic_unchoke: Optional[Conn] = None
        """The peer we're optimistically unchoking"""

        self.piece_to_peer: dict[int, set[Addr]] = defaultdict(set)
        """Which peers have each piece"""

        self.selfish = selfish
        """Whether to stop when we're done downloading"""

        self.bar: Any = None
        """Progress bar"""

        self.recently_removed: list[tuple[Addr, float]] = []
        """Holds addresses that were recently removed and the time when they were removed"""

        """
        Initializes the Torrent object and extracts metadata from the .torrent file.
        """

        with open(torrent_file_path, "rb") as torrent_file:
            data = torrent_file.read()

        # decode data with bencodepy
        decoded_data = bencodepy.decode(data)

        # gets info and hash of the info, used to send to tracker and peers to verify during handshake process
        info = decoded_data[b"info"]
        encoded_info_hash = hashlib.sha1(
            bencodepy.encode(decoded_data[b"info"])
        ).digest()

        self.info = info
        self.encoded_info_hash = encoded_info_hash
        self.peer_ids = {
            port: (
                PEER_ID_START
                + "".join(
                    str(random.randint(0, 9))
                    for _ in range(PEER_ID_LEN - len(PEER_ID_START))
                ).encode()
            )
            for port in our_ports
        }
        self.current_pieces = None
        self.endgame_subpieces: set[tuple[int, int]] = set()

        self.piece_length: int = decoded_data[b"info"][b"piece length"]
        """Used to determine offset and piece"""

        if output_path is None:
            output_path = "."  # dump in current directory by default

        # Determine if it's a single-file or multi-file torrent
        if b"files" in info:
            # Multi-file torrent
            file_list = info[b"files"]
            file_names = [file[b"path"][-1] for file in file_list]
            file_lengths = [file[b"length"] for file in file_list]
        else:
            # Single-file torrent
            file_names = [info[b"name"]]
            file_lengths = [info[b"length"]]

        self.files: list[FilePieces] = []
        """The files to write to/read from"""

        cum_size = 0
        for file_name, length in zip(file_names, file_lengths):
            file = FilePieces(
                os.path.join(output_path, file_name.decode("utf-8")),
                cum_size,
                length,
            )
            self.files.append(file)
            cum_size += length

        self.total_byte_size: int = cum_size
        """Total length of the file(s), in bytes"""

        self.num_pieces = ceil(self.total_byte_size / self.piece_length)
        self.piece_list: list[bool] = [False] * self.num_pieces

        # creating list of hashes of each piece
        self.pieces_hashed = []
        hashes = info[b"pieces"]
        for i in range(0, len(hashes), 20):
            self.pieces_hashed.append(hashes[i : i + 20])

        self.piece_object_list: list[Piece] = []
        for i in range(self.num_pieces):
            length = min(
                self.total_byte_size - i * self.piece_length, self.piece_length
            )
            self.piece_object_list.append(Piece(i, length))

        self.pieces_in_transit: set[tuple[int, int]] = set()

        # parsing through the URL
        parsed_url = urlparse(str(decoded_data[b"announce"].decode("utf-8")))
        self.udp = parsed_url.scheme == "udp"
        assert parsed_url.hostname is not None
        assert parsed_url.port is not None

        self.tracker_hostname: str = parsed_url.hostname
        self.tracker_addr: Addr = (
            socket.gethostbyname(parsed_url.hostname),
            parsed_url.port,
        )
        self.tracker_path: str = parsed_url.path
        self.tracker_interval: int = DEFAULT_TRACKER_INTERVAL
        """How long to wait before talking to the tracker again"""

        # Initial request, gets all of valid peers for specific file
        peer_addrs = self.request_tracker(our_ports[0], event="started")

        # For debugging/leeching purposes, if we only want to connect to certain peers
        if restrict_peers is None:
            logger.debug(f"Peers are {peer_addrs}")
        else:
            peer_addrs = restrict_peers

        self.read_already_downloaded_files()

        # start downloading from peers!
        self.select_loop(set(peer_addrs))

    def all_conns(self) -> list[Conn]:
        return [conn for peer in self.peers.values() for conn in peer.conns]

    # checks to see if file already exists
    def read_already_downloaded_files(self):
        for piece in self.piece_object_list:
            have_all = all(
                file.already_downloaded
                for file, _, _ in FilePieces.files_for(
                    self.files, self.piece_length * piece.index, piece.size
                )
            )
            if have_all:
                for subpiece_ind in range(ceil(piece.size // MAX_SUBPIECE_SIZE)):
                    begin = subpiece_ind * MAX_SUBPIECE_SIZE
                    block = FilePieces.read(
                        self.files,
                        piece.index,
                        piece.size,
                        begin,
                        subpiece_len(piece.size, subpiece_ind),
                    )
                    piece.add_subpiece(begin, block)
                self.piece_list[piece.index] = True

    def send_handshake(self, conn: Conn):
        peer = self.peers[conn.addr]
        logger.debug(f"[send_handshake] Sending handshake to {peer}")

        reserved = b"\x00" * 8

        handshake = (
            len(PSTR).to_bytes(1, byteorder="big")
            + PSTR
            + reserved
            + self.encoded_info_hash
            + self.peer_ids[conn.our_port]
        )

        conn.sock.sendall(handshake)
        conn.we_handshaked = True
        self.send_bitfield(conn)
        self.send_interested(conn)

    def add_peer(self, addr: Addr, our_port: int) -> Optional[Peer]:
        """Try to create a connection to a peer, and if it succeeds, add it to the peers dictionary"""
        if any(a == addr for a, _ in self.recently_removed):
            # Don't make a connection again if it was just removed
            return None
        try:
            peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_sock.settimeout(3)
            peer_sock.connect(addr)
            logger.debug(
                f"[add_peer] Connection {addr} (our port {our_port}) successful"
            )

            conn = Conn(addr, our_port, peer_sock)
            peer = self.register_conn(conn)

            if peer is not None:
                self.send_handshake(conn)
                return peer
        except Exception as e:
            # logger.debug(f"[get_peers] Could not connect to peer {addr}, error: {e}")
            pass

        return None

    def register_conn(self, conn: Conn) -> Optional[Peer]:
        """Register a new connection (a Peer for it may already exist)"""
        if conn.addr in self.peers:
            peer = self.peers[conn.addr]
            if any(c.our_port == conn.our_port for c in peer.conns):
                logger.error(
                    f"[register_peer] Already have a connection for {conn.addr} with our port {conn.our_port}"
                )
                return None
        else:
            peer = Peer(conn.addr, [])
            self.peers[conn.addr] = peer

        try:
            conn.sock.setblocking(False)
            peer.add_conn(conn)
            sel.register(
                conn.sock,
                selectors.EVENT_READ,
                lambda *_: self.handle_message_callback(conn),
            )
            return peer
        except Exception as e:
            logger.debug(
                f"[register_conn] Could not register conn {conn.addr} for our port {conn.our_port}, error: {e}"
            )
            return None

    def remove_conn(self, conn: Conn, reason: Optional[str] = None):
        end = f": {reason}" if reason else ""
        logger.debug(f"Removing connection for peer {conn.addr}{end}")

        self.recently_removed.append((conn.addr, time.time()))

        self.unchoked = [c for c in self.unchoked if c != conn]
        if self.optimistic_unchoke == conn:
            self.optimistic_unchoke = None

        try:
            sel.unregister(conn.sock)
            conn.sock.close()
            removed_all = False
            peer = self.peers.get(conn.addr)
            if peer is not None:
                peer.remove_conn(conn)
                if len(peer.conns) == 0:
                    removed_all = True
        finally:
            if removed_all:
                # No more connections left, so get rid of the whole peer
                for peer_list in self.piece_to_peer.values():
                    peer_list.discard(conn.addr)

                self.unchoked = [c for c in self.unchoked if c.addr != conn.addr]
                if (
                    self.optimistic_unchoke is not None
                    and self.optimistic_unchoke.addr == conn.addr
                ):
                    self.optimistic_unchoke = None

                del self.peers[conn.addr]

    def process_request(self, conn: Conn, index: int, begin: int, length: int):
        peer = self.peers[conn.addr]

        logger.info(f"Got a request! {str(peer)}, {index=}, {begin=}, {length=}")

        # Don't send if we're choking them
        if conn.am_choking_them:
            return

        # don't send if we don't have the piece
        if not self.piece_list[index]:
            return

        # Only sending at most 2**14 bytes a time, so we don't send too much data
        if length > 2**14:
            length = 2**14

        block = FilePieces.read(self.files, index, self.piece_length, begin, length)
        # Might want to check block here for debugging

        # block_int = int.from_bytes(block, byteorder="big")
        # block = block_int.to_bytes(len(block), 'big') # TODO need to convert block byte order?
        request = b"\x07" + index.to_bytes(4, "big") + begin.to_bytes(4, "big") + block

        try:
            conn.sock.sendall(len(request).to_bytes(4, "big") + request)
            self.uploaded += len(block)
            logger.info(f"Sent a piece over to {peer} (index: {index}, begin: {begin})")
        except:
            logger.error(f"Failed to send piece to {peer}")
            self.remove_conn(conn, "Failed to send piece to peer")

    def process_cancel(self, conn: Conn, index: int, begin: int, length: int):
        # TODO figure out how to handle the cancel message
        logger.info(f"Got a cancel request! {conn=}, {index=}, {begin=}, {length=}")

    def process_piece(self, conn: Conn, index: int, begin: int, block: bytes):
        """
        Processes a piece (or subpiece) of data received from a peer.

        Parameters:
        - `conn`: The connection object representing the peer who sent the piece.
        - `index`: The index of the piece being received.
        - `begin`: The byte offset within the piece where this block starts.
        - `block`: The actual data block received from the peer.

        This method validates the piece, updates download progress, integrates
        it into the file if complete, and handles edge cases like endgame mode.
        """

        self.downloaded += len(block)

        # Validate the piece index
        if index >= self.num_pieces:
            self.remove_conn(conn, f"Out-of-bounds piece index: {index}")
            conn.uploaded -= len(
                block
            )  # They didn't truly upload anything useful to us
            return

        # Calculate the expected length of the subpiece
        expected_len = subpiece_len(
            self.piece_object_list[index].size, begin // MAX_SUBPIECE_SIZE
        )

        if len(block) < expected_len:
            # Band-aid fix in case they give us a smaller block than we asked for
            return

        # If the received block is larger than expected, truncate it
        if expected_len < len(block):
            logger.debug(
                f"[process_piece] Expected subpiece of length {expected_len}, got {len(block)}"
            )
            block = block[:expected_len]

        conn.upload_time += time.time() - conn.last_request
        conn.uploaded += len(block)

        # If the piece is already marked as complete, skip further processing
        piece = self.piece_object_list[index]

        if self.piece_list[index]:
            return

        # Calculate the subpiece index (relative to the piece) based on the offset
        piece.add_subpiece(begin, block)
        subpiece_index = begin // SUBPIECE_SIZE

        if (index, subpiece_index) in self.pieces_in_transit:
            self.pieces_in_transit.remove((index, subpiece_index))

        if (index, subpiece_index) in self.endgame_subpieces:
            self.endgame_subpieces.remove((index, subpiece_index))
            for peer in list(self.peers.values()):
                self.send_cancel(peer, index, subpiece_index)

        # Check if the piece is now complete
        if piece.is_complete():
            if piece.matches_sha1(self.pieces_hashed[index]):
                logger.debug(f"Piece {index} is complete and matches hash ")
                self.bar()

                # Combining everything
                real_block = b"".join(piece.segments)  # type: ignore

                FilePieces.write(self.files, index, self.piece_length, real_block)

                self.piece_list[index] = True
                self.send_have_to_peers(index)
            else:
                # Invalid pieces
                logger.debug(f"NOOO Piece {index} does not match hash ")
                # reset piece
                piece.reset()
                conn.uploaded -= len(block)  # They didn't truly upload the block to us

    def process_bitfield(self, conn: Conn, bitfield: int):
        i = 0
        while bitfield != 0 and i < self.num_pieces:
            if bitfield & 1:
                self.piece_to_peer[i].add(conn.addr)
            bitfield >>= 1
            i += 1
        self.send_interested(conn)

    def try_send_msg(
        self,
        conn: Conn,
        id: Optional[int],
        body: bytes,
        length: Optional[int] = None,
        msg_type: Optional[str] = None,
    ) -> bool:
        request = id.to_bytes(1, "big") + body if id is not None else body
        length = len(request) if length is None else length

        try:
            conn.sock.sendall(length.to_bytes(4, "big") + request)
            conn.last_sent = time.time()
            return True
        except:
            request_excerpt = request if len(request) < 100 else request[:100] + b"..."
            self.remove_conn(
                conn,
                (
                    f"Couldn't send {msg_type or 'message'} to {conn.addr}: {request_excerpt!r}"
                ),
            )
            return False

    def send_bitfield(self, conn: Conn):
        """
        Sends a bitfield message to a peer, indicating which pieces of the torrent the client has.

        Parameters:
        - `conn`: The connection object representing the peer to whom the bitfield will be sent.

        The bitfield message is structured as:
        - <length prefix><message ID><bitfield>
            - `length prefix`: Length of the message in bytes (including the bitfield).
            - `message ID`: 5 (indicating a bitfield message).
            - `bitfield`: A sequence of bits where each bit corresponds to a piece:
                - 1 if the piece is complete.
                - 0 if the piece is missing.
        """
        # bitfield: <len=0001+X><id=5><bitfield>
        logger.debug(f"Sending bitfield to {conn.addr}")
        bitfield = 0
        for i, piece in enumerate(self.piece_list):
            if piece:
                bitfield |= 1 << i

        bitfield_len = ceil(len(self.piece_list) // 8)

        self.try_send_msg(
            conn,
            id=5,
            body=bitfield.to_bytes(bitfield_len, "big"),
            length=1 + bitfield_len,
            msg_type="bitfield",
        )

    def send_heartbeat_to_peers(self):
        logger.debug(f"Sending heartbeat to {len(self.peers)} peers")
        now = time.time()
        for peer in list(self.peers.values()):
            # todo only send heartbeat to connections that need it
            for conn in peer.conns:
                if now - conn.last_sent >= HEART_BEAT_INTERVAL:
                    self.try_send_msg(conn, id=None, body=b"", msg_type="heartbeat")

    def send_have_to_peers(self, index: int):
        for peer in list(self.peers.values()):
            peer.next_conn()
            self.try_send_msg(
                peer.curr_conn(), id=4, body=index.to_bytes(4, "big"), msg_type="have"
            )

    def send_choke(self, conn: Conn):
        logger.debug(f"Sending choke to {conn.addr}")
        self.try_send_msg(conn, id=0, body=b"", msg_type="choke")

    def send_unchoke(self, conn: Conn):
        self.try_send_msg(conn, id=1, body=b"", msg_type="unchoke")

    def send_interested(self, conn: Conn):
        self.try_send_msg(conn, id=2, body=b"", msg_type="interested")

    # send a request to a specific peer
    def send_request(self, conn: Conn, piece: int, subpiece: int) -> bool:
        # request: <len=0013><id=6><index><begin><length>

        # calculating the offset
        begin = subpiece * MAX_SUBPIECE_SIZE
        length = subpiece_len(self.piece_object_list[piece].size, subpiece)
        res = self.try_send_msg(
            conn,
            id=6,
            body=piece.to_bytes(4, "big")
            + begin.to_bytes(4, "big")
            + length.to_bytes(4, "big"),
            msg_type="piece request",
        )
        if not res:
            return False

        self.pieces_in_transit.add((piece, subpiece))

        def remove_timed_out_request():
            nonlocal piece, subpiece
            if (piece, subpiece) in self.pieces_in_transit:
                self.pieces_in_transit.remove((piece, subpiece))
                self.piece_to_peer[piece].discard(conn.addr)
                # Add to upload time if request timed out because that's bad
                conn.upload_time += time.time() - conn.last_request

        callbacks.register(remove_timed_out_request, REQUEST_TIMEOUT)
        return True
    
    def send_cancel(self, peer: Peer, piece: int, subpiece: int):
        begin = subpiece * MAX_SUBPIECE_SIZE
        length = subpiece_len(self.piece_object_list[piece].size, subpiece)
        self.try_send_msg(
            peer,
            id=8,
            body=piece.to_bytes(4, "big")
            + begin.to_bytes(4, "big")
            + length.to_bytes(4, "big"),
            msg_type="cancel",
        )

    def select_pieces(self) -> list[int]:
        """Find the rarest pieces and select one of them randomly"""
        pieces_sorted = sorted(
            filter(
                lambda piece: not self.piece_list[piece]
                and any(
                    (piece, subpiece) not in self.pieces_in_transit
                    for subpiece in self.piece_object_list[
                        piece
                    ].get_missing_subpieces()
                )
                and len(self.piece_to_peer[piece]) > 0,
                self.piece_to_peer.keys(),
            ),
            key=lambda piece: len(self.piece_to_peer[piece]),
            reverse=True,
        )

        if len(pieces_sorted) == 0:
            return []

        rarest_pieces: list[int] = []
        for _, group in groupby(
            pieces_sorted, lambda piece: len(self.piece_to_peer[piece])
        ):
            group_list = list(group)
            random.shuffle(group_list)
            for piece in group_list:
                if len(rarest_pieces) >= self.max_pieces:
                    break
                rarest_pieces.append(piece)

        return rarest_pieces

    def send_piece_requests_to_peers(self):

        # get all valid connections
        valid_unchoked_conns = [
            conn for conn in self.all_conns() if not conn.they_are_choking_me
        ]

        logger.debug(
            f"Number of conns that are valid and not choking us: {len(valid_unchoked_conns)}"
        )

        # exit if no valid connections, or all pieces have been found
        if len(valid_unchoked_conns) == 0:
            return

        self.current_pieces = self.select_pieces()
        if len(self.current_pieces) == 0:
            logger.error("No more pieces to request")
            return

        # check how many pieces are currently in transit and how many are still left
        pieces_in_transit = len(set(piece for piece, _ in self.pieces_in_transit))
        pieces_left = self.piece_list.count(False)
        endgame = pieces_left < ENDGAME_THRESHOLD and pieces_left < pieces_in_transit

        # loop through each piece we are trying to download
        for piece in self.current_pieces:

            # identify subpieces needed for the piece
            subpieces = [
                subpiece
                for subpiece in self.piece_object_list[piece].get_missing_subpieces()
                if not (piece, subpiece) in self.pieces_in_transit
            ]

            if endgame:
                for peer in list(self.peers.values()):
                    for subpiece in subpieces:
                        peer.next_conn()
                        if not self.send_request(peer.curr_conn(), piece, subpiece):
                            break
                        self.endgame_subpieces.add((piece, subpiece))
            else:
                conns = []
                for peer_addr in self.piece_to_peer[piece]:
                    peer = self.peers.get(peer_addr)
                    if peer is None:
                        continue
                    conns.extend(
                        conn for conn in peer.conns if not conn.they_are_choking_me
                    )
                # Shuffle to avoid picking the same connection over and over again,
                # in case they all have the same upload rate
                random.shuffle(conns)
                # logger.critical(
                #     str(sum(1 if conn.upload_rate() > 0 else 0 for conn in conns))
                # )

                # pick the connection with the highest upload rate
                best = max(conns, key=lambda conn: conn.upload_rate(), default=None)

                if best is None:
                    logger.critical(
                        f"No peers to request piece {piece} from ({len(self.peers)=})"
                    )
                    continue

                # send a request to get each subpiece
                for subpiece in subpieces:
                    if not self.send_request(best, piece, subpiece):
                        break

    def update_chokes(self):
        prev_unchoked = self.unchoked

        best_conns = sorted(
            self.all_conns(), key=lambda conn: conn.upload_rate(), reverse=True
        )
        downloaders = [conn for conn in best_conns if conn.they_are_interested_in_me][
            : NUM_UNCHOKED - 1
        ]
        max_downloader_rate = (
            downloaders[0].upload_rate() if len(downloaders) > 0 else 0
        )
        uninterested_but_better = [
            conn for conn in best_conns if conn.upload_rate() >= max_downloader_rate
        ][: NUM_UNCHOKED - len(downloaders)]
        # Unchoke at most NUM_UNCHOKED at a time
        self.unchoked = downloaders + uninterested_but_better

        newly_choked = [conn for conn in prev_unchoked if conn not in self.unchoked]
        for conn in newly_choked:
            if self.optimistic_unchoke != conn:
                conn.am_choking_them = True
                self.send_choke(conn)

        newly_unchoked = [peer for peer in self.unchoked if peer not in prev_unchoked]
        for conn in newly_unchoked:
            conn.am_choking_them = False
            self.send_unchoke(conn)

        logger.debug(f"[update_chokes] {self.unchoked=}")

    def update_optimistic_unchoke(self):
        all_conns = self.all_conns()
        if len(all_conns) == 0:
            return

        now = time.time()

        none_interested = all(not conn.they_are_interested_in_me for conn in all_conns)
        candidates = [
            conn
            for conn in all_conns
            if conn not in self.unchoked
            and (none_interested or conn.they_are_interested_in_me)
        ]
        # Make newly connected peers 3x more likely to be the optimistic unchoke
        weights = [
            3 if now - conn.time_added < NEWLY_CONNECTED_THRESHOLD else 1
            for conn in candidates
            if conn not in self.unchoked
        ]

        prev = self.optimistic_unchoke

        us = [conn for conn in all_conns if conn.addr[0] == "127.0.0.1"]
        if len(us) > 0:
            self.optimistic_unchoke = us[0]
        elif len(candidates) == 0:
            self.optimistic_unchoke = all_conns[0]
        else:
            self.optimistic_unchoke = random.choices(candidates, weights=weights, k=1)[
                0
            ]

        if self.optimistic_unchoke != prev:
            self.optimistic_unchoke.am_choking_them = False
            self.send_unchoke(self.optimistic_unchoke)
            logger.info(f"Optimistically unchoking {self.optimistic_unchoke}")

            if prev is not None and prev not in self.unchoked:
                prev.am_choking_them = True
                self.send_choke(prev)

    def setup_server_sock(self, port: int) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock.bind(("", port))
        # Allow at most 256 clients. Maybe change this later if torrents usually have more
        # Or read in how many clients are expected from the torrent file
        sock.listen(256)

        sock.setblocking(False)
        sel.register(sock, selectors.EVENT_READ, lambda sock: self.accept(sock, port))

        return sock

    def select_loop(self, init_peers: set[Addr]):

        # creates a set of peers to connect to
        conn_queue = set(
            (random.choice(self.our_ports), peer_addr) for peer_addr in init_peers
        )

        def update_tracker(our_ports: list[int] = self.our_ports):
            def run():
                # request the tracker for more peers
                want_peers = self.restrict_peers is None
                peer_list = self.request_tracker(
                    our_ports[0], event=None, numwant=None if want_peers else 0
                )
                for our_port in our_ports[1:]:
                    self.request_tracker(our_port, event=None, numwant=0)
                for peer in peer_list:
                    if peer not in self.peers:
                        conn_queue.add((random.choice(our_ports), peer))

            threading.Thread(target=run).start()

        # setup server socket for accepting incoming connections
        server_socks = list(map(self.setup_server_sock, self.our_ports))

        # progress bar
        with alive_bar(self.num_pieces) as bar:
            self.bar = bar

            # set up period tasks to ensure things run smoothly
            callbacks.register(
                self.send_heartbeat_to_peers, HEART_BEAT_INTERVAL, repeat=True
            )
            callbacks.register(self.update_chokes, CHOKE_UPDATE_INTERVAL, repeat=True)
            callbacks.register(
                self.update_optimistic_unchoke, OPTIMISTIC_UNCHOKE_INTERVAL, repeat=True
            )

            # this specific callback is requesting pieces from peers
            req_callback = callbacks.register(
                self.send_piece_requests_to_peers, REQUEST_INTERVAL, repeat=True
            )

            start = time.time()
            last = start
            last_tracker_request = start
            done = False

            while True:

                #check if all pieces have been received
                if not done and all(self.piece_list):
                    logger.info("Got all pieces!")
                    callbacks.remove(req_callback)
                    done = True
                    for our_port in self.our_ports:
                        self.request_tracker(our_port, event="completed")
                    if self.selfish:
                        exit(0)

                events = sel.select(timeout=0)
                for key, _mask in events:
                    callback = key.data
                    callback(key.fileobj)

                now = time.time()

                # Regularly contact tracker after set interval to refresh peers
                if now - last_tracker_request > self.tracker_interval:
                    update_tracker([self.our_ports[0]])
                    last_tracker_request = now

                # Removing a peer that has timed out
                for conn in self.all_conns():
                    if now - conn.prev_time > PEER_TIMEOUT:
                        self.remove_conn(conn, "Peer timed out")

                self.recently_removed = [
                    (addr, remove_time)
                    for addr, remove_time in self.recently_removed
                    if now - remove_time < BLACKLIST_TIMEOUT
                ]

                num_unchoked = len(
                    set(
                        conn.addr
                        for conn in self.all_conns()
                        if not conn.they_are_choking_me
                    )
                )

                # attempt to connect to peers that are in the connection set
                if len(conn_queue) > 0:
                    addrs = [
                        conn_queue.pop()
                        for _ in range(min(len(conn_queue), MAX_PEERS_ADD))
                    ]
                    
                    # For each address, create a new thread to establish a connection
                    for our_port, addr in addrs:
                        threading.Thread(
                            target=lambda: self.add_peer(addr, our_port)
                        ).start()

                # making sure we have enough peers
                elif ( 
                    not done
                    and (
                        len(self.peers) < PEER_THRESHOLD
                        or num_unchoked < UNCHOKED_PEER_THRESHOLD
                    )
                    and (
                        self.restrict_peers is None
                        or len(self.peers) < len(self.restrict_peers)
                    )
                    and now - last_tracker_request > MIN_TRACKER_REQ_INTERVAL
                ):
                    # if we don't have enough peers, get new list of peers from the tracker server
                    if self.restrict_peers is None:
                        update_tracker([random.choice(self.our_ports)])
                    else:
                        for our_port in self.our_ports:
                            for peer in self.restrict_peers:
                                if peer not in self.peers:
                                    conn_queue.add((our_port, peer))
                    last_tracker_request = now

                # check to see if any of the callbacks we established earlier needs to be called
                callbacks.update()

                if now - last > 10 and not done:
                    # This includes blocks that turned out to be invalid
                    logger.info(
                        f"Download rate: {self.downloaded/(now - start)}"
                        f", total downloaded: {self.downloaded}"
                        f", upload rate: {self.uploaded/(now - start)}"
                        f", total uploaded: {self.uploaded}"
                    )
                    last = now

    def accept(self, sock: socket.socket, our_port: int):
        """Accept a connection from a peer. Every peer gets the read callback."""
        conn_sock, addr = sock.accept()
        # if any(a == addr for a, _ in self.recently_removed):
        #     # Don't make a connection again if it was just removed
        #     conn_sock.close()
        #     return
        conn = Conn(addr, our_port, conn_sock)
        peer = self.register_conn(conn)

        if peer is not None:
            logging.debug(f"[accept] Accepted connection from {addr}")
            return peer

    def handle_message_callback(self, conn: Conn):
        peer = next(peer for peer in self.peers.values() if peer.addr == conn.addr)
        try:
            # Avoid BlockingIOErrors from reading when all the data hasn't been sent yet
            conn.sock.settimeout(1)

            conn.prev_time = time.time()

            if not conn.they_handshaked:
                # We are expecting a handshake
                pstrlen = conn.sock.recv(1)
                if pstrlen is None:
                    logger.error(
                        f"[handle_message_callback] Received empty message from {peer}"
                    )
                    self.remove_conn(conn)
                    return
                pstr = conn.receive(from_bytes_network(pstrlen))

                if pstr != PSTR:
                    self.remove_conn(conn, "Didn't receive pstr in handshake")
                    return

                conn.they_handshaked = True
                logger.debug(f"Received handshake from {conn}, body: {pstr!r}")

                if len(conn.receive(8)) < 8:
                    self.remove_conn(conn, "Did not get reserved bytes for handshake")
                    return
                info_hash = conn.receive(20)
                if len(info_hash) < 20:
                    self.remove_conn(conn, "Did not get info hash for handshake")
                    return

                if info_hash != self.encoded_info_hash:
                    self.remove_conn(
                        conn, f"info hash didn't match (got {info_hash!r})"
                    )
                    return

                # If we're the recipient of the handshake, respond
                if not conn.we_handshaked:
                    # Must respond as soon as we read info_hash part of handshake
                    self.send_handshake(conn)

                peer_id = conn.receive(20)
                if len(peer_id) < 20:
                    self.remove_conn(conn, "No peer id for handshake")
                    return

                if peer.id is None:
                    peer.id = peer_id
                elif peer.id != peer_id:
                    self.remove_conn(
                        conn,
                        f"(handshake) Expected peer id {peer.id!r}, got {peer_id!r}",
                    )
                    return

                return

            # read the message length first
            length_bytes = conn.receive(4)
            if length_bytes is None:
                logger.error(
                    f"[handle_message_callback] Received empty message from {conn}"
                )
                self.remove_conn(conn)
                return
            if len(length_bytes) < 4:
                logger.debug(
                    f"[handle_message_callback] Did not receive 4 bytes for length from {conn}"
                )
                self.remove_conn(conn)
                return

            length = int.from_bytes(length_bytes, byteorder="big")
            body = conn.receive(length)
            if len(body) < length:
                self.remove_conn(
                    conn,
                    f"Body was supposed to be {length}, only got {len(body)} before timing out",
                )
                return

            if length == 0:
                # Keep alive message
                pass
            else:
                msg_id = body[0]
                # logger.debug(f"{length=}, {msg_id=}")

                """
                choke: <len=0001><id=0>
                The choke message is fixed-length and has no payload.

                unchoke: <len=0001><id=1>
                The unchoke message is fixed-length and has no payload.

                interested: <len=0001><id=2>
                The interested message is fixed-length and has no payload.

                not interested: <len=0001><id=3>
                The not interested message is fixed-length and has no payload.
                """

                if msg_id == 0:
                    conn.they_are_choking_me = True
                    logger.debug(f"Got choked by {peer}")
                elif msg_id == 1:
                    conn.they_are_choking_me = False
                    logger.debug(f"Got unchoked by {peer} :)")
                elif msg_id == 2:
                    conn.they_are_interested_in_me = True
                elif msg_id == 3:
                    conn.they_are_interested_in_me = False
                elif msg_id == 4:
                    # have: <len=0005><id=4><piece index>
                    # The have message is fixed length. The payload is the zero-based index of a piece that has just been successfully downloaded and verified via the hash.
                    piece_index = int.from_bytes(body[1:], byteorder="big")
                    if piece_index >= self.num_pieces:
                        self.remove_conn(
                            conn,
                            f"Said that it has out-of-bounds piece {piece_index}",
                        )
                        return
                    self.piece_to_peer[piece_index].add(peer.addr)
                elif msg_id == 5:
                    # bitfield: <len=0001+(bitfield length)><id=5><bitfield>
                    self.process_bitfield(
                        conn, int.from_bytes(body[1:], byteorder="big")
                    )
                elif msg_id == 6 or msg_id == 8:
                    """
                    cancel: <len=0013><id=8><index><begin><length>
                    The cancel message is fixed length, and is used to cancel block requests. The payload is identical to that of the "request" message. It is typically used during "End Game" (see the Algorithms section below).

                    request: <len=0013><id=6><index><begin><length>
                    The request message is fixed length, and is used to request a block. The payload contains the following information:

                    index: integer specifying the zero-based piece index
                    begin: integer specifying the zero-based byte offset within the piece
                    length: integer specifying the requested length.
                    """
                    index = int.from_bytes(body[1:5], byteorder="big")
                    begin = int.from_bytes(body[5:9], byteorder="big")
                    req_length = int.from_bytes(body[9:13], byteorder="big")

                    if msg_id == 6:
                        self.process_request(conn, index, begin, req_length)
                    else:
                        self.process_cancel(conn, index, begin, req_length)
                elif msg_id == 7:
                    # piece: <len=0009+X><id=7><index><begin><block>
                    # The piece message is variable length, where X is the length of the block. The payload contains the following information:
                    # index: integer specifying the zero-based piece index
                    # begin: integer specifying the zero-based byte offset within the piece
                    # block: block of data, which is a subset of the piece specified by index.

                    self.process_piece(
                        conn,
                        int.from_bytes(body[1:5], byteorder="big"),
                        int.from_bytes(body[5:9], byteorder="big"),
                        body[9:],
                    )
                else:
                    self.remove_conn(
                        conn,
                        f"[handle_message_callback] Unknown msg_id {msg_id}, body={body[:50]!r}",
                    )
                    return

            # Make it non-blocking again
            conn.sock.setblocking(False)
        except TimeoutError:
            self.remove_conn(conn, f"[handle_message_callback] Peer {peer} timed out")
        except (OSError, BrokenPipeError) as e:
            self.remove_conn(
                conn, f"[handle_message_callback] Peer {peer} caused error {e}"
            )

    def request_tracker(
        self,
        our_port: int,
        event: Optional[Literal["started", "completed", "stopped"]] = None,
        numwant: Optional[int] = None,
    ) -> list[Addr]:
        """
        Communicates with the tracker to retrieve information about peers or update tracker state.

        Parameters:
        - `our_port`: The port on which the client is listening for peer connections.
        - `event`: A string indicating the event to notify the tracker of (optional). Possible values:
            - `"started"`: The client has started downloading.
            - `"completed"`: The client has finished downloading all pieces.
            - `"stopped"`: The client is shutting down or stopping this torrent.
        - `numwant`: The number of peers the client is requesting from the tracker (optional).
                    If not provided, the tracker determines the number.

        Returns:
        - A list of peer addresses (`Addr`), where each `Addr` is a tuple of (IP, port).
        """

        announce_fun = tracker.announce_udp if self.udp else tracker.announce_tcp
        interval, peers = announce_fun(
            self.tracker_addr,
            self.tracker_path,
            self.tracker_hostname,
            info_hash=self.encoded_info_hash,
            peer_id=self.peer_ids[our_port],
            our_port=our_port,
            uploaded=self.uploaded,
            downloaded=self.downloaded,
            left=max(
                0,
                self.total_byte_size
                - self.piece_length * sum(1 if has else 0 for has in self.piece_list),
            ),
            event=event,
            numwant=numwant,
        )
        self.tracker_interval = interval
        return peers


def calculate_offset(index: int, begin: int, piece_length: int) -> int:
    return index * piece_length + begin


def subpiece_len(piece_len: int, subpiece_index: int) -> int:
    """Get the length of the subpiece at the given index"""
    return min(MAX_SUBPIECE_SIZE, piece_len - subpiece_index * MAX_SUBPIECE_SIZE)
