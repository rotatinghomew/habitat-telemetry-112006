"""Gateway archive reader.

An archive is gzip, then ASCII ``HL01``, then frames with a 6-byte big-endian
header: 2-byte device, 1-byte boot, 2-byte unsigned counter, 1-byte payload
length.  The payload length delimits each frame.  Compression framing is *not*
part of a frame's identity, so an identical frame re-delivered inside a second
archive still resolves to the same ``(device, boot, seq)``.
"""

from __future__ import annotations

import gzip
import hashlib
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .model import Frame

MAGIC = b"HL01"
HEADER = struct.Struct(">2scHB")


class ArchiveError(ValueError):
    """The archive itself cannot be read as a gateway deliverable."""


@dataclass
class ParsedArchive:
    archive: str
    file: str
    sha256: str
    arrived: str | None
    frames: list[Frame] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    raw_bytes: int = 0

    def duplicate_frames(self) -> list[Frame]:
        """Frames whose identity repeats inside this archive."""

        seen: set[tuple[str, str, int]] = set()
        repeated: list[Frame] = []
        for frame in self.frames:
            key = (frame.device, frame.boot, frame.seq)
            if key in seen:
                repeated.append(frame)
            seen.add(key)
        return repeated


def read_archive(
    path: Path,
    archive: str | None = None,
    arrived: str | None = None,
    file: str | None = None,
) -> ParsedArchive:
    path = Path(path)
    try:
        compressed = path.read_bytes()
    except FileNotFoundError as exc:
        raise ArchiveError(f"缺少原档：{path}") from exc
    digest = hashlib.sha256(compressed).hexdigest()
    try:
        raw = gzip.decompress(compressed)
    except OSError as exc:
        raise ArchiveError(f"原档不是有效的 gzip：{path.name}") from exc
    if raw[: len(MAGIC)] != MAGIC:
        raise ArchiveError(f"原档缺少 HL01 头：{path.name}")

    result = ParsedArchive(
        archive=archive or path.stem,
        file=file or path.name,
        sha256=digest,
        arrived=arrived,
        raw_bytes=len(raw),
    )
    position = len(MAGIC)
    while position < len(raw):
        if len(raw) - position < HEADER.size:
            result.errors.append(
                {
                    "kind": "truncated_header",
                    "offset": position,
                    "detail": f"剩余 {len(raw) - position} 字节不足一个 6 字节头",
                }
            )
            break
        device_raw, boot_raw, seq, size = HEADER.unpack_from(raw, position)
        frame_start = position
        position += HEADER.size
        end = position + size
        if end > len(raw):
            result.errors.append(
                {
                    "kind": "truncated_payload",
                    "offset": frame_start,
                    "detail": f"帧头声明载荷 {size} 字节，仅剩 {len(raw) - position}",
                    "device": device_raw.decode("ascii", "replace"),
                    "boot": boot_raw.decode("ascii", "replace"),
                    "seq": seq,
                }
            )
            break
        payload = raw[position:end]
        position = end
        try:
            device = device_raw.decode("ascii")
            boot = boot_raw.decode("ascii")
        except UnicodeDecodeError:
            result.errors.append(
                {
                    "kind": "bad_identity_encoding",
                    "offset": frame_start,
                    "detail": "设备或启动段不是 ASCII",
                }
            )
            continue
        result.frames.append(
            Frame(
                device=device,
                boot=boot,
                seq=seq,
                payload=payload,
                byte_offset=frame_start,
                archive_sha256=digest,
                archive=result.archive,
            )
        )
    if position != len(raw):
        result.errors.append(
            {
                "kind": "trailing_bytes",
                "offset": position,
                "detail": f"帧束之后还有 {len(raw) - position} 字节无法解释",
            }
        )
    return result
