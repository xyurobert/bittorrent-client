import argparse
import logging

from torrent import Torrent

logger = logging.getLogger(__name__)
LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARN,
    "error": logging.ERROR,
}

parser = argparse.ArgumentParser(prog="tracker")
parser.add_argument(
    "--port",
    "-p",
    type=int,
    default=[],
    help="Port to listen on",
    action="append",
)
parser.add_argument(
    "file",
    default="./debian1.torrent",
    nargs="?",
    help=".torrent/metainfo file",
)
parser.add_argument("--log-level", "-l", choices=LOG_LEVELS.keys(), default="debug")
parser.add_argument(
    "--output",
    "-o",
    help="Output file (if single file) or directory (if multiple files)",
)
parser.add_argument(
    "--max-pieces",
    help="Maximum number of pieces to request at once",
    type=int,
    default=3,
)
parser.add_argument(
    "--restrict-peers",
    "-r",
    help="Restrict peers to given peers instead of getting from tracker (but accept connections)",
)
parser.add_argument(
    "--selfish",
    help="Stop after downloading file",
    action="store_true",
)

args = parser.parse_args()

logging.basicConfig(level=LOG_LEVELS[args.log_level])

if args.restrict_peers is not None:
    restrict_peers = []

    if len(args.restrict_peers) > 0:
        for peer in args.restrict_peers.split(","):
            addr, port = peer.split(":")
            restrict_peers.append((addr, int(port)))
else:
    restrict_peers = None

ports = (
    [6881, 6882]
    if len(args.port) == 0
    else args.port
)

try:
    torrent = Torrent(
        ports,
        args.file,
        args.output,
        args.max_pieces,
        restrict_peers,
        selfish=args.selfish,
    )
except KeyboardInterrupt:
    print("Cancelled")
    exit(-1)
