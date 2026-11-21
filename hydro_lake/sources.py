"""Source material loading and as-of resolution.

Everything here is driven by the signed / published material.  Payload bytes are
never inspected to guess a format: a frame is only decoded under the firmware
that the signed notices say was in force for its ``(device, boot, seq)`` at the
knowledge time being evaluated.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from .model import (
    Deployment,
    DeploymentResolution,
    FirmwareNotice,
    FirmwareResolution,
    OffsetNotice,
    OffsetResolution,
    TZ,
    decimal_from_text,
)


class MaterialError(ValueError):
    """The supplied material is structurally unusable."""


@dataclass(frozen=True)
class Material:
    firmware: tuple[FirmwareNotice, ...]
    offsets: tuple[OffsetNotice, ...]
    deployments: tuple[Deployment, ...]
    digests: dict[str, str]
    paths: dict[str, Path]

    def firmware_at(self, as_of: date) -> tuple[FirmwareNotice, ...]:
        return tuple(n for n in self.firmware if n.issued <= as_of)

    def offsets_at(self, as_of: date) -> tuple[OffsetNotice, ...]:
        return tuple(n for n in self.offsets if n.issued <= as_of)

    def deployments_at(self, as_of: date) -> tuple[Deployment, ...]:
        return tuple(d for d in self.deployments if d.published <= as_of)


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise MaterialError(f"缺少资料文件：{path}") from exc
    rows = list(csv.DictReader(io.StringIO(text)))
    if rows and rows[0].get(None) is not None:
        raise MaterialError(f"{path.name} 的列数多于表头")
    return rows


def _parse_date(text: str, what: str) -> date:
    try:
        return date.fromisoformat(text.strip())
    except (AttributeError, ValueError) as exc:
        raise MaterialError(f"{what} 不是 ISO 日期：{text!r}") from exc


def _parse_int(text: str, what: str) -> int:
    try:
        return int(text.strip())
    except (AttributeError, ValueError) as exc:
        raise MaterialError(f"{what} 不是整数：{text!r}") from exc


def _parse_datetime(text: str, what: str) -> datetime:
    raw = text.strip()
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise MaterialError(f"{what} 不是 ISO 时刻：{text!r}") from exc
    if value.tzinfo is None:
        raise MaterialError(f"{what} 缺少时区：{text!r}")
    return value.astimezone(TZ)


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_material(directory: Path) -> Material:
    directory = Path(directory)
    firmware_rows = _read_csv(directory / "signed-firmware.csv")
    offset_rows = _read_csv(directory / "offsets.csv")
    deployment_rows = _read_csv(directory / "deployment.csv")

    firmware: list[FirmwareNotice] = []
    for index, row in enumerate(firmware_rows, start=2):
        notice = (row.get("notice") or "").strip()
        if not notice:
            raise MaterialError(f"signed-firmware.csv 第 {index} 行缺少 notice")
        replaces = (row.get("replaces") or "").strip() or None
        firmware_number = _parse_int(row.get("firmware", ""), f"firmware@{notice}")
        if firmware_number not in (1, 2):
            raise MaterialError(f"notice {notice} 的固件号未知：{firmware_number}")
        begin = _parse_int(row.get("begin", ""), f"begin@{notice}")
        end = _parse_int(row.get("end", ""), f"end@{notice}")
        if end < begin:
            raise MaterialError(f"notice {notice} 的计数区间倒置：{begin}-{end}")
        firmware.append(
            FirmwareNotice(
                notice=notice,
                device=(row.get("device") or "").strip(),
                boot=(row.get("boot") or "").strip(),
                begin=begin,
                end=end,
                firmware=firmware_number,
                issued=_parse_date(row.get("issued", ""), f"issued@{notice}"),
                replaces=replaces,
            )
        )

    offsets: list[OffsetNotice] = []
    for index, row in enumerate(offset_rows, start=2):
        device = (row.get("device") or "").strip()
        boot = (row.get("boot") or "").strip()
        if not device or not boot:
            raise MaterialError(f"offsets.csv 第 {index} 行缺少 device/boot")
        begin = _parse_int(row.get("begin", ""), f"begin@{device}/{boot}")
        end = _parse_int(row.get("end", ""), f"end@{device}/{boot}")
        if end < begin:
            raise MaterialError(f"offsets.csv 第 {index} 行计数区间倒置：{begin}-{end}")
        offsets.append(
            OffsetNotice(
                device=device,
                boot=boot,
                begin=begin,
                end=end,
                offset_cm=decimal_from_text(row.get("offset_cm", "")),
                issued=_parse_date(row.get("issued", ""), f"issued@{device}/{boot}"),
            )
        )

    deployments: list[Deployment] = []
    for index, row in enumerate(deployment_rows, start=2):
        device = (row.get("device") or "").strip()
        boot = (row.get("boot") or "").strip()
        if not device or not boot:
            raise MaterialError(f"deployment.csv 第 {index} 行缺少 device/boot")
        deployments.append(
            Deployment(
                device=device,
                boot=boot,
                starts=_parse_datetime(row.get("starts", ""), f"starts@{device}/{boot}"),
                plot=(row.get("plot") or "").strip(),
                rev=_parse_int(row.get("rev", ""), f"rev@{device}/{boot}"),
                published=_parse_date(
                    row.get("published", ""), f"published@{device}/{boot}"
                ),
            )
        )

    digests = {
        name: _digest(directory / name)
        for name in ("signed-firmware.csv", "offsets.csv", "deployment.csv")
    }
    return Material(
        firmware=tuple(firmware),
        offsets=tuple(offsets),
        deployments=tuple(deployments),
        digests=digests,
        paths={
            name: directory / name
            for name in ("signed-firmware.csv", "offsets.csv", "deployment.csv")
        },
    )


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def _subtract(
    intervals: Sequence[tuple[int, int]], cut_begin: int, cut_end: int
) -> list[tuple[int, int]]:
    """Remove ``[cut_begin, cut_end)`` from every interval."""

    result: list[tuple[int, int]] = []
    for begin, end in intervals:
        if cut_end <= begin or cut_begin >= end:
            result.append((begin, end))
            continue
        if begin < cut_begin:
            result.append((begin, cut_begin))
        if end > cut_end:
            result.append((cut_end, end))
    return result


def _notice_dict(notice: FirmwareNotice) -> dict[str, object]:
    return {
        "notice": notice.notice,
        "begin": notice.begin,
        "end": notice.end,
        "firmware": notice.firmware,
        "issued": notice.issued.isoformat(),
        "replaces": notice.replaces,
    }


def resolve_firmware(
    material: Material, device: str, boot: str, seq: int, as_of: date
) -> FirmwareResolution:
    known = [
        n for n in material.firmware_at(as_of) if n.device == device and n.boot == boot
    ]
    covering = [n for n in known if n.covers(seq)]
    if not covering:
        return FirmwareResolution(
            firmware=None,
            reason="unsigned_interval",
            detail=f"{device}/{boot}/{seq} 在 {as_of.isoformat()} 前没有覆盖该计数的固件签发",
        )

    # ``replaces`` removes only the named notice over the overlapping counter
    # range.  Other valid notices keep their own ranges, so a newer replacement
    # never silently reinterprets a frame outside its stated interval.
    effective: list[tuple[int, int, FirmwareNotice]] = []
    for notice in covering:
        pieces = [(notice.begin, notice.end)]
        for replacement in known:
            if replacement.replaces != notice.notice:
                continue
            overlap_begin = max(notice.begin, replacement.begin)
            overlap_end = min(notice.end, replacement.end)
            if overlap_begin < overlap_end:
                pieces = _subtract(pieces, overlap_begin, overlap_end)
        effective.extend((begin, end, notice) for begin, end in pieces)

    live = [(begin, end, notice) for begin, end, notice in effective if begin <= seq < end]
    firmwares = sorted({notice.firmware for _, _, notice in live})
    notices = tuple(sorted({notice.notice for _, _, notice in live}))
    if len(firmwares) > 1:
        detail = "；".join(
            f"{notice.notice}: 固件{notice.firmware} [{begin},{end})"
            for begin, end, notice in sorted(live, key=lambda item: item[2].notice)
        )
        return FirmwareResolution(
            firmware=None,
            notices=notices,
            conflict=True,
            reason="firmware_notice_conflict",
            detail=f"{device}/{boot}/{seq} 的生效签发互相矛盾：{detail}",
            candidates=tuple(_notice_dict(notice) for _, _, notice in live),
        )
    if not firmwares:
        return FirmwareResolution(
            firmware=None,
            reason="unsigned_interval",
            detail=f"{device}/{boot}/{seq} 的生效签发已被替换材料覆盖，没有有效固件",
            candidates=tuple(_notice_dict(n) for n in covering),
        )
    return FirmwareResolution(
        firmware=firmwares[0],
        notices=notices,
        candidates=tuple(
            _notice_dict(notice) for _, _, notice in sorted(live, key=lambda i: i[2].notice)
        ),
    )


def resolve_offset(
    material: Material, device: str, boot: str, seq: int, as_of: date
) -> OffsetResolution:
    covering = [
        o
        for o in material.offsets_at(as_of)
        if o.device == device and o.boot == boot and o.covers(seq)
    ]
    if not covering:
        # The dictionary does not make calibration mandatory before a value can
        # be published; it only says offsets apply when known.  Record that the
        # baseline was assumed so a later notice shows up as a replay change.
        return OffsetResolution(
            offset_cm=Decimal("0"),
            reason="offset_absent_default_zero",
            detail=f"{device}/{boot}/{seq} 在 {as_of.isoformat()} 前没有校准签发，按 0 cm 处理",
        )
    newest = max(o.issued for o in covering)
    newest_rows = [o for o in covering if o.issued == newest]
    values = sorted({o.offset_cm for o in newest_rows})
    if len(values) > 1:
        detail = "；".join(
            f"{o.issued.isoformat()} offset={o.offset_cm}cm [{o.begin},{o.end})"
            for o in sorted(newest_rows, key=lambda o: (o.begin, o.end))
        )
        return OffsetResolution(
            offset_cm=None,
            reason="offset_notice_conflict",
            detail=f"{device}/{boot}/{seq} 较新签发的校准互相矛盾：{detail}",
        )
    return OffsetResolution(
        offset_cm=values[0],
        notices=tuple(
            sorted({f"{o.issued.isoformat()}@{o.begin}-{o.end}" for o in newest_rows})
        ),
    )


def resolve_deployment(
    material: Material, device: str, boot: str, as_of: date
) -> DeploymentResolution:
    rows = [
        d for d in material.deployments_at(as_of) if d.device == device and d.boot == boot
    ]
    if not rows:
        return DeploymentResolution(
            deployment=None,
            reason="deployment_unknown",
            detail=f"{device}/{boot} 在 {as_of.isoformat()} 前没有已公布部署",
        )
    highest = max(d.rev for d in rows)
    newest = [d for d in rows if d.rev == highest]
    starts = {d.starts for d in newest}
    plots = {d.plot for d in newest}
    if len(starts) > 1 or len(plots) > 1:
        detail = "；".join(
            f"rev{d.rev} {d.plot} {d.starts.isoformat()}"
            for d in sorted(newest, key=lambda d: (d.starts, d.plot))
        )
        return DeploymentResolution(
            deployment=None,
            reason="deployment_revision_conflict",
            detail=f"{device}/{boot} 同一高修订有多个部署：{detail}",
        )
    chosen = sorted(newest, key=lambda d: d.published)[-1]
    return DeploymentResolution(deployment=chosen)
