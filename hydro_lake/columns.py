"""Publication documents for one ``(plot, day)`` partition."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

FORMAT = "hydro-columns-v1"

COLUMNS: tuple[str, ...] = (
    "frame_id",
    "device",
    "boot",
    "seq",
    "observed_at",
    "plot",
    "day",
    "status",
    "value_cm",
    "missing",
    "missing_reason",
    "raw_value",
    "status_byte",
    "firmware",
    "offset_cm",
    "sources",
    "firmware_notices",
    "offset_notices",
    "deployment_rev",
    "delivered_release",
)


@dataclass(frozen=True)
class Table:
    plot: str
    day: str
    columns: Mapping[str, Sequence[Any]]
    complete: bool
    missing_by_reason: Mapping[str, int]
    basis: Mapping[str, Any]
    pending: Sequence[Mapping[str, Any]] = field(default_factory=tuple)
    release: str | None = None

    @property
    def row_count(self) -> int:
        if not self.columns:
            return 0
        return len(next(iter(self.columns.values())))

    def rows(self) -> list[dict[str, Any]]:
        return [
            {name: self.columns[name][index] for name in self.columns}
            for index in range(self.row_count)
        ]

    def select(self, names: Iterable[str]) -> dict[str, list[Any]]:
        return {name: list(self.columns[name]) for name in names}

    def as_dict(self) -> dict[str, Any]:
        return {
            "format": FORMAT,
            "plot": self.plot,
            "day": self.day,
            "release": self.release,
            "complete": self.complete,
            "missing_by_reason": dict(self.missing_by_reason),
            "basis": dict(self.basis),
            "row_count": self.row_count,
            "pending": [dict(item) for item in self.pending],
            "columns": {name: list(self.columns[name]) for name in COLUMNS},
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Table":
        columns = {name: list(data["columns"].get(name, [])) for name in COLUMNS}
        return cls(
            plot=data["plot"],
            day=data["day"],
            columns=columns,
            complete=bool(data.get("complete", False)),
            missing_by_reason=dict(data.get("missing_by_reason", {})),
            basis=dict(data.get("basis", {})),
            pending=tuple(data.get("pending", ())),
            release=data.get("release"),
        )


def decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def row_from_outcome(outcome: Any, release: str | None = None) -> dict[str, Any]:
    payload = outcome.payload
    return {
        "frame_id": outcome.frame_id,
        "device": outcome.frame.device,
        "boot": outcome.frame.boot,
        "seq": outcome.frame.seq,
        "observed_at": outcome.observed_at.isoformat() if outcome.observed_at else None,
        "plot": outcome.plot,
        "day": outcome.day.isoformat() if outcome.day else None,
        "status": outcome.status,
        "value_cm": decimal_text(payload.value_cm) if payload else None,
        "missing": bool(payload and payload.missing),
        "missing_reason": payload.missing_reason if payload else None,
        "raw_value": payload.raw_value if payload else None,
        "status_byte": payload.status if payload else None,
        "firmware": outcome.firmware,
        "offset_cm": decimal_text(outcome.offset_cm),
        "sources": list(outcome.sources),
        "firmware_notices": list(outcome.firmware_notices),
        "offset_notices": list(outcome.offset_notices),
        "deployment_rev": outcome.deployment.rev if outcome.deployment else None,
        "delivered_release": release,
    }


def build_table(
    plot: str,
    day: str,
    rows: Sequence[Mapping[str, Any]],
    pending: Sequence[Mapping[str, Any]],
    basis: Mapping[str, Any],
    release: str | None,
) -> Table:
    ordered = sorted(
        rows, key=lambda row: (row.get("observed_at") or "", row.get("frame_id") or "")
    )
    data: dict[str, list[Any]] = {name: [] for name in COLUMNS}
    counts: dict[str, int] = {}
    for row in ordered:
        for name in COLUMNS:
            data[name].append(row.get(name))
        if row.get("delivered_release") is None:
            data["delivered_release"][-1] = release
        if row.get("missing"):
            reason = row.get("missing_reason") or "unknown"
            counts[reason] = counts.get(reason, 0) + 1
    return Table(
        plot=plot,
        day=day,
        columns=data,
        complete=len(pending) == 0,
        missing_by_reason=counts,
        basis=dict(basis),
        pending=tuple(dict(item) for item in pending),
        release=release,
    )
