"""Compatibility re-exports for the lake package."""

from .archive import ArchiveError, ParsedArchive, read_archive
from .columns import COLUMNS, Table, build_table
from .decode import DecodeError, decode_payload
from .engine import evaluate
from .model import Frame, Outcome, PayloadValue
from .pipeline import (
    LakeError,
    decode_basis,
    duty,
    ingest,
    load_issued,
    patrol,
    promote_replay,
    replay,
    request_fingerprint,
    verify,
)
from .sources import Material, MaterialError, load_material
from .store import Lake

__all__ = [
    "ArchiveError",
    "COLUMNS",
    "DecodeError",
    "Frame",
    "Lake",
    "LakeError",
    "Material",
    "MaterialError",
    "Outcome",
    "ParsedArchive",
    "PayloadValue",
    "Table",
    "build_table",
    "decode_basis",
    "decode_payload",
    "duty",
    "evaluate",
    "ingest",
    "load_issued",
    "load_material",
    "patrol",
    "promote_replay",
    "read_archive",
    "replay",
    "request_fingerprint",
    "verify",
]
