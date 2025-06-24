"""
To create a fake UDP torrent file
"""

import bencodepy
import hashlib

orig_file = open("debian1.torrent", "rb")
torrent = bencodepy.decode(orig_file.read())
torrent[b"announce"] = b"udp://127.0.0.1:1212"
out_file = open("fakeudp.torrent", "wb")
out_file.write(bencodepy.encode(torrent))

print(hashlib.sha1(bencodepy.encode(torrent[b"info"])).digest().hex())
