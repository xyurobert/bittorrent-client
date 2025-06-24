import hashlib
import logging
from math import ceil
import os
from typing import Optional

MAX_SUBPIECE_SIZE = 2**14

logger = logging.getLogger(__name__)


class Piece:

    def __init__(self, index: int, size: int):
        """
        Initialize a piece.

        Parameters:
        - `index`: The index of this piece in the torrent.
        - `size`: The total size of the piece in bytes.
        """
        self.index: int = index
        self.size: int = size

        self.segments: list[Optional[bytes]] = [None] * ceil(size // MAX_SUBPIECE_SIZE)

        self.filled: bool = False

        self.is_handled = False

    def add_subpiece(self, offset: int, data: bytes) -> None:
        """
        Add a subpiece to the piece.

        Parameters:
        - `offset`: The byte offset of the subpiece within the piece.
        - `data`: The subpiece data to add.
        """

        # calculate index and store data
        self.segments[offset // MAX_SUBPIECE_SIZE] = data

        if all(segment is not None for segment in self.segments):
            self.filled = True

    def get_missing_subpieces(self) -> list[int]:
        """
        Get a list of indices for subpieces that are not yet downloaded.

        Returns:
        - A list of subpiece indices that are still `None`.
        """
        missing_lst = []
        for i in range(len(self.segments)):
            if self.segments[i] is None:
                missing_lst.append(i)

        return missing_lst

    def is_complete(self) -> bool:
        return self.filled

    def matches_sha1(self, correct_sha1: bytes) -> bool:
        if self.filled:
            return hashlib.sha1(b"".join(self.segments)).digest() == correct_sha1 # type: ignore
        else:
            return False

    def reset(self) -> None:
        self.segments = [None] * len(self.segments)
        self.filled = False

    def get_full_piece(self) -> bytes:
        return b"".join(self.segments) # type: ignore


class FilePieces:
    """
    Represents a single file in the torrent and handles reading/writing
    pieces and subpieces to/from disk.
    """
    def __init__(self, path: str, start: int, length: int):

        """
        Initialize the file.

        Parameters:
        - `path`: The file path on disk.
        - `start`: The starting byte offset of this file in the torrent.
        - `length`: The length of this file in bytes.
        """

        self.path = path
        self.start = start
        """At what offset in the torrent this file starts (bytes)"""
        self.length = length
        """Bytes"""
        self.end = start + length
        """At what offset in the torrent this file ends (bytes)"""

        if os.path.isfile(path):
            logger.warn(f"File already exists: {path}")
            self._file = open(path, "rb")
            self.already_downloaded = True
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self._file = open(path, "w+b")
            self.already_downloaded = False

    def __repr__(self):
        return f"FilePieces({self.path}, {self.start}, {self.length})"

    @staticmethod
    def read(
        files: list["FilePieces"],
        piece_ind: int,
        piece_len: int,
        subpiece_start: int,
        subpiece_len: int,
    ) -> bytes:
        """
        Read a subpiece from the appropriate file(s).

        Parameters:
        - `files`: List of all FilePieces objects, sorted by their start offsets.
        - `piece_ind`: The index of the piece containing the subpiece.
        - `piece_len`: The length of the full piece.
        - `subpiece_start`: The byte offset of the subpiece within the piece.
        - `subpiece_len`: The length of the subpiece to read.

        Returns:
        - The subpiece data as bytes.
        """
        block = b""
        for file, start, length in FilePieces.files_for(
            files, piece_ind * piece_len + subpiece_start, subpiece_len
        ):
            file._file.seek(start)
            block += file._file.read(length)
        return block

    @staticmethod
    def write(
        files: list["FilePieces"],
        piece_ind: int,
        piece_len: int,
        piece: bytes,
    ) -> None:
        """
        Write a complete piece to the appropriate file(s).

        Parameters:
        - `files`: List of all FilePieces objects, sorted by their start offsets.
        - `piece_ind`: The index of the piece to write.
        - `piece_len`: The length of the piece.
        - `piece`: The full piece data as bytes.
        """
        block_offset = 0
        for file, start, length in FilePieces.files_for(
            files, piece_ind * piece_len, len(piece)
        ):
            file._file.seek(start)
            file._file.write(piece[block_offset : block_offset + length])
            block_offset += length

        assert block_offset == len(piece), (
            f"{piece_ind=}, {piece_len=}, {len(piece)=}, {block_offset=}",
            FilePieces.files_for(files, piece_ind * piece_len, len(piece)),
        )

    @staticmethod
    def files_for(
        files: list["FilePieces"], start: int, length: int
    ) -> list[tuple["FilePieces", int, int]]:
        """
        # Parameters
        - `files`: Must be sorted by start offset
        - `start`: Start offset of the piece or subpiece
        - `length`: Length of the piece or subpiece
        # Returns
        Because the block may span multiple files, must return a list with each
        file, the start offset within that file to start reading/writing at, and
        how much to read/write from that file"""

        assert len(files) > 0, "No files given"

        end = start + length
        for i in range(len(files)):
            file = files[i]
            if start < file.start or file.end <= start:
                continue
            res = [(file, start - file.start, min(file.end - start, length))]
            for j in range(i + 1, len(files)):
                if start + length <= files[j].end:
                    res.append(
                        (
                            files[j],
                            0,
                            min(files[j].end, end) - files[j].start,
                        )
                    )
                else:
                    break
            return res

        raise AssertionError(
            f"Start offset {start} was beyond end ({files[len(files) - 1].end})"
        )
