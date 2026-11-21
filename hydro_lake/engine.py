"""Frame evaluation: signed material + archive bytes -> observations.

The order of resolution is deliberate:

1. identity is ``(device, boot, seq)``; duplicates are only merged once both
   device and boot are known from the frame header;
2. firmware comes *only* from signed notices in force at ``as_of``;
3. deployment (plot, boot start) comes from the highest revision published by
   ``as_of``; the observation time is ``starts + (seq - 1) seconds``;
4. offset comes from the newest calibration notice in force;
5. only then are the payload bytes interpreted.

Anything that cannot be settled stays ``pending`` with a reason and the kind of
material a duty operator has to chase.  A pending frame never becomes truth and
never blocks sibling frames in the same archive from being published.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Sequence

from .archive import ParsedArchive
from .decode import DecodeError, apply_offset, decode_payload
from .model import Outcome, Frame, PendingIssue, local_day
from .sources import Material, resolve_deployment, resolve_firmware, resolve_offset

CHASE_FIRMWARE = "signed-firmware"
CHASE_DEPLOYMENT = "deployment"
CHASE_OFFSET = "offsets"
CHASE_PRODUCER = "gateway-producer"

#: Not a blocker; kept in the outcome so a later calibration is visible.
NON_BLOCKING = {"offset_assumed_zero"}


def _frame_source(frame: Frame) -> str:
    return f"{frame.archive}:{frame.archive_sha256}"


def _identity_groups(archives: Sequence[ParsedArchive]) -> dict[str, list[Frame]]:
    groups: dict[str, list[Frame]] = {}
    for archive in archives:
        for frame in archive.frames:
            groups.setdefault(frame.frame_id, []).append(frame)
    return groups


def evaluate(
    material: Material, archives: Sequence[ParsedArchive], as_of: date
) -> dict[str, Outcome]:
    """Evaluate every distinct frame identity in ``archives`` at ``as_of``."""

    return {
        key: _evaluate_identity(material, key, instances, as_of)
        for key, instances in _identity_groups(archives).items()
    }


def _evaluate_identity(
    material: Material, key: str, instances: Sequence[Frame], as_of: date
) -> Outcome:
    frame = instances[0]
    sources = tuple(sorted({_frame_source(f) for f in instances}))
    outcome = Outcome(frame=frame, status="pending", sources=sources)

    payloads = {f.payload for f in instances}
    if len(payloads) > 1:
        detail = "；".join(
            sorted(f"{_frame_source(f)} 载荷 {f.payload.hex()}" for f in instances)
        )
        outcome.issues.append(
            PendingIssue(
                reason="frame_identity_conflict",
                detail=f"{key} 在不同交付中以不同字节出现：{detail}",
                chase=(CHASE_PRODUCER,),
            )
        )

    # -- placement (plot / day) ------------------------------------------------
    deployment = resolve_deployment(material, frame.device, frame.boot, as_of)
    if deployment.deployment is None:
        outcome.issues.append(
            PendingIssue(
                reason=deployment.reason or "deployment_unknown",
                detail=deployment.detail or "部署未判清",
                chase=(CHASE_DEPLOYMENT,),
            )
        )
    else:
        chosen = deployment.deployment
        outcome.deployment = chosen
        outcome.plot = chosen.plot
        outcome.observed_at = chosen.starts + timedelta(seconds=frame.seq - 1)
        outcome.day = local_day(outcome.observed_at)

    # -- signed firmware -------------------------------------------------------
    firmware = resolve_firmware(material, frame.device, frame.boot, frame.seq, as_of)
    outcome.firmware = firmware.firmware
    outcome.firmware_notices = firmware.notices
    if firmware.firmware is None:
        outcome.issues.append(
            PendingIssue(
                reason=firmware.reason or "unsigned_interval",
                detail=firmware.detail or "固件签发未判清",
                chase=(CHASE_FIRMWARE,),
            )
        )

    # -- calibration -----------------------------------------------------------
    offset = resolve_offset(material, frame.device, frame.boot, frame.seq, as_of)
    outcome.offset_cm = offset.offset_cm
    outcome.offset_notices = offset.notices
    if offset.offset_cm is None:
        outcome.issues.append(
            PendingIssue(
                reason=offset.reason or "offset_notice_conflict",
                detail=offset.detail or "校准签发未判清",
                chase=(CHASE_OFFSET,),
            )
        )
    elif offset.reason == "offset_absent_default_zero":
        outcome.issues.append(
            PendingIssue(
                reason="offset_assumed_zero",
                detail=offset.detail or "没有校准签发",
                chase=(CHASE_OFFSET,),
            )
        )

    # -- payload ---------------------------------------------------------------
    decoded_missing = False
    if firmware.firmware is not None:
        try:
            value = decode_payload(firmware.firmware, frame.payload)
        except DecodeError as exc:
            outcome.issues.append(
                PendingIssue(reason=exc.reason, detail=exc.detail, chase=(CHASE_PRODUCER,))
            )
        else:
            decoded_missing = value.missing
            outcome.payload = value if value.missing else apply_offset(value, offset.offset_cm)

    if blocking(outcome):
        outcome.status = "pending"
    elif decoded_missing:
        outcome.status = "missing"
    else:
        outcome.status = "decoded"
    return outcome


def blocking(outcome: Outcome) -> list[PendingIssue]:
    return [issue for issue in outcome.issues if issue.reason not in NON_BLOCKING]


def group_observations(
    outcomes: Iterable[Outcome],
) -> dict[tuple[str, str], list[Outcome]]:
    """Resolved observations by ``(plot, day)``; pending ones are not placed."""

    partitions: dict[tuple[str, str], list[Outcome]] = {}
    for outcome in outcomes:
        key = outcome.partition_key()
        if outcome.resolved and key is not None:
            partitions.setdefault(key, []).append(outcome)
    for items in partitions.values():
        items.sort(key=lambda o: (o.observed_at, o.frame_id))
    return partitions


