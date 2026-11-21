"""Core value objects for the telemetry lake.

The source dictionary is the authority for the meanings encoded here.  In
particular a frame's *identity* is ``(device, boot, seq)``: the frame counter is
only unique inside one boot segment.  The archive digest is provenance, not
identity, so re-delivering the same bytes (or the same frame in another
archive) must not create a second observation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping

TZ = timezone(timedelta(hours=8))

MISSING_SENTINEL_FW1 = 0x7FFF
MISSING_SENTINEL_FW2 = 0x7FFFFF


# ---------------------------------------------------------------------------
# Material (the signed / published source rows)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FirmwareNotice:
    notice: str
    device: str
    boot: str
    begin: int
    end: int
    firmware: int
    issued: date
    replaces: str | None = None

    def covers(self, seq: int) -> bool:
        return self.begin <= seq < self.end


@dataclass(frozen=True)
class OffsetNotice:
    device: str
    boot: str
    begin: int
    end: int
    offset_cm: Decimal
    issued: date

    def covers(self, seq: int) -> bool:
        return self.begin <= seq < self.end


@dataclass(frozen=True)
class Deployment:
    device: str
    boot: str
    starts: datetime
    plot: str
    rev: int
    published: date


# ---------------------------------------------------------------------------
# Frames and resolutions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Frame:
    device: str
    boot: str
    seq: int
    payload: bytes
    byte_offset: int
    archive_sha256: str
    archive: str

    @property
    def frame_id(self) -> str:
        return frame_id(self.device, self.boot, self.seq)

    @property
    def payload_hex(self) -> str:
        return self.payload.hex()


def frame_id(device: str, boot: str, seq: int) -> str:
    return f"{device}/{boot}/{seq}"


@dataclass(frozen=True)
class FirmwareResolution:
    """Result of applying signed notices at one knowledge time."""

    firmware: int | None
    notices: tuple[str, ...] = ()
    conflict: bool = False
    reason: str | None = None
    detail: str | None = None
    candidates: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True)
class OffsetResolution:
    offset_cm: Decimal | None
    notices: tuple[str, ...] = ()
    reason: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class DeploymentResolution:
    deployment: Deployment | None
    reason: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class PayloadValue:
    """A resolved payload value or a confirmed missing reading."""

    missing: bool
    value_cm: Decimal | None
    raw_value: int
    status: int
    missing_reason: str | None = None
    detail: str | None = None

    @property
    def value_float(self) -> float | None:
        if self.value_cm is None:
            return None
        return float(self.value_cm)


@dataclass
class PendingIssue:
    """One reason why a frame is not truth yet.

    ``chase`` names the kind(s) of issued material a duty operator should
    follow up, not a person or a pipeline stage.
    """

    reason: str
    detail: str
    chase: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {"reason": self.reason, "detail": self.detail, "chase": list(self.chase)}


@dataclass
class Outcome:
    """Per-frame outcome of one release/replay evaluation."""

    frame: Frame
    status: str  # decoded | missing | pending | out_of_scope
    issues: list[PendingIssue] = field(default_factory=list)
    firmware: int | None = None
    firmware_notices: tuple[str, ...] = ()
    offset_cm: Decimal | None = None
    offset_notices: tuple[str, ...] = ()
    deployment: Deployment | None = None
    observed_at: datetime | None = None
    plot: str | None = None
    day: date | None = None
    payload: PayloadValue | None = None
    sources: tuple[str, ...] = ()

    @property
    def resolved(self) -> bool:
        return self.status in ("decoded", "missing")

    @property
    def frame_id(self) -> str:
        return self.frame.frame_id

    @property
    def issue(self) -> PendingIssue | None:
        return self.issues[0] if self.issues else None

    @property
    def reasons(self) -> list[str]:
        return [issue.reason for issue in self.issues]

    @property
    def chase_materials(self) -> list[str]:
        return sorted({material for issue in self.issues for material in issue.chase})

    def partition_key(self) -> tuple[str, str] | None:
        if self.plot is not None and self.day is not None:
            return self.plot, self.day.isoformat()
        return None

    def as_outcome_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "frame_id": self.frame_id,
            "device": self.frame.device,
            "boot": self.frame.boot,
            "seq": self.frame.seq,
            "status": self.status,
            "sources": list(self.sources or (self.frame.archive_sha256,)),
            "firmware": self.firmware,
            "firmware_notices": list(self.firmware_notices),
            "offset_cm": _decimal_json(self.offset_cm),
            "offset_notices": list(self.offset_notices),
            "plot": self.plot,
            "day": self.day.isoformat() if self.day else None,
            "observed_at": self.observed_at.isoformat() if self.observed_at else None,
            "value_cm": (
                _decimal_json(self.payload.value_cm)
                if self.payload is not None
                else None
            ),
            "missing": bool(self.payload.missing) if self.payload else False,
            "missing_reason": self.payload.missing_reason if self.payload else None,
            "raw_value": self.payload.raw_value if self.payload else None,
            "status_byte": self.payload.status if self.payload else None,
            "issues": [issue.as_dict() for issue in self.issues],
            "reasons": self.reasons,
            "chase": self.chase_materials,
            "deployment_rev": self.deployment.rev if self.deployment else None,
            "deployment_published": (
                self.deployment.published.isoformat() if self.deployment else None
            ),
        }
        return data


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decimal_json(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def decimal_from_text(text: str) -> Decimal:
    return Decimal(text.strip())


def unique_sorted(values: Iterable[str]) -> list[str]:
    return sorted(set(values))


def stable_json(value: Any) -> Any:
    """Make a structure deterministic before hashing/writing."""

    if isinstance(value, Mapping):
        return {str(k): stable_json(value[k]) for k in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [stable_json(v) for v in value]
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def local_day(value: datetime) -> date:
    return value.astimezone(TZ).date()
