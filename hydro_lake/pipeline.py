"""Ingest, replay, promotion, patrol and duty-desk service.

The pipeline is split into a pure evaluation step (signed material + original
bytes -> per-frame outcomes) and a state step (outcomes -> a complete new
generation).  A replay re-runs the pure step against the archived originals and
a chosen knowledge time, is stored separately, and only an explicit promotion
turns it into a new generation.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

from .archive import ArchiveError, ParsedArchive, read_archive
from .columns import COLUMNS, Table, build_table, row_from_outcome
from .engine import evaluate, group_observations
from .model import Outcome, unique_sorted
from .sources import Material, load_material
from .store import Lake, _atomic_write, _dump, fingerprint, write_immutable

RULESET = "hydro-lake/1"


class LakeError(ValueError):
    """A request cannot be safely applied to the lake."""


# ---------------------------------------------------------------------------
# Request identity
# ---------------------------------------------------------------------------


def _request_payload(
    kind: str,
    material: Material,
    archives: Sequence[Mapping[str, Any]],
    as_of: date,
    ruleset: str,
    against: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": kind,
        "ruleset": ruleset,
        "as_of": as_of.isoformat(),
        "material": material.digests,
        "archives": sorted(
            ({"archive": item.get("archive"), "sha256": item["sha256"]} for item in archives),
            key=lambda item: (str(item.get("archive")), item["sha256"]),
        ),
    }
    if against is not None:
        payload["against"] = against
    return payload


def request_fingerprint(
    material: Material,
    archives: Sequence[Mapping[str, Any]],
    as_of: date,
    ruleset: str,
) -> str:
    return fingerprint(_request_payload("ingest", material, archives, as_of, ruleset))


def replay_fingerprint(
    material: Material,
    archives: Sequence[Mapping[str, Any]],
    as_of: date,
    ruleset: str,
    against: str | None,
) -> str:
    return fingerprint(
        _request_payload("replay", material, archives, as_of, ruleset, against)
    )


def decode_basis(
    material: Material,
    archives: Sequence[Mapping[str, Any]],
    as_of: date,
    ruleset: str,
) -> dict[str, Any]:
    return {
        "ruleset": ruleset,
        "as_of": as_of.isoformat(),
        "material": dict(material.digests),
        "archives": sorted(
            (
                {
                    "archive": item.get("archive"),
                    "file": item.get("file"),
                    "sha256": item["sha256"],
                }
                for item in archives
            ),
            key=lambda item: (str(item.get("archive")), item["sha256"]),
        ),
    }


# ---------------------------------------------------------------------------
# Archive registry and reading
# ---------------------------------------------------------------------------


def register_archives(
    lake: Lake, data_dir: Path, entries: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Store original bytes immutably and verify each registry digest."""

    verified: list[dict[str, Any]] = []
    for entry in entries:
        filename = entry.get("file")
        sha = entry.get("sha256")
        if not filename or not sha:
            raise LakeError(f"原档登记缺少 file/sha256：{entry}")
        source = (Path(data_dir) / filename).resolve()
        if not source.exists():
            raise LakeError(f"缺少原档文件：{source}")
        actual = hashlib.sha256(source.read_bytes()).hexdigest()
        if actual != sha:
            raise LakeError(f"原档摘要不符：{filename} 登记 {sha}，实际 {actual}")
        lake.store_archive(source, sha)
        parsed = read_archive(
            source,
            archive=entry.get("archive"),
            arrived=entry.get("arrived"),
            file=filename,
        )
        verified.append(
            {
                "archive": entry.get("archive") or parsed.archive,
                "file": filename,
                "sha256": sha,
                "arrived": entry.get("arrived"),
                "frames": len(parsed.frames),
                "errors": parsed.errors,
            }
        )
    return verified


def resolve_archives(
    lake: Lake,
    data_dir: Path,
    extra: Sequence[Mapping[str, Any]] = (),
) -> list[ParsedArchive]:
    """Read every archive in the lake, preferring the immutable stored copy."""

    manifest = lake.read_manifest()
    known: dict[str, dict[str, Any]] = {}
    for sha, record in manifest.get("archives", {}).items():
        known[sha] = dict(record)
    for entry in extra:
        sha = entry.get("sha256")
        if not sha:
            raise LakeError(f"原档登记缺少 sha256：{entry}")
        known.setdefault(sha, dict(entry))

    parsed: list[ParsedArchive] = []
    for sha, record in sorted(known.items()):
        candidates = [lake.archive_path(sha)]
        if record.get("file"):
            candidates.append((Path(data_dir) / record["file"]).resolve())
        path = next((candidate for candidate in candidates if candidate.exists()), None)
        if path is None:
            raise LakeError(f"原档 {record.get('archive') or sha} 既不在仓内也不在数据目录")
        try:
            archive = read_archive(
                path,
                archive=record.get("archive"),
                arrived=record.get("arrived"),
                file=record.get("file"),
            )
        except (ArchiveError, OSError) as exc:
            raise LakeError(f"原档不可读：{exc}") from exc
        if archive.sha256 != sha:
            raise LakeError(f"原档摘要不符：登记 {sha}，读取 {archive.sha256}（{path}）")
        parsed.append(archive)
    return parsed


# ---------------------------------------------------------------------------
# State application (shared by ingest and promotion)
# ---------------------------------------------------------------------------


def _pending_document(outcome: Outcome) -> dict[str, Any]:
    key = outcome.partition_key()
    return {
        "frame_id": outcome.frame_id,
        "device": outcome.frame.device,
        "boot": outcome.frame.boot,
        "seq": outcome.frame.seq,
        "status": outcome.status,
        "plot": key[0] if key else None,
        "day": key[1] if key else None,
        "observed_at": outcome.observed_at.isoformat() if outcome.observed_at else None,
        "firmware": outcome.firmware,
        "offset_cm": _float(outcome.offset_cm),
        "sources": list(outcome.sources),
        "reasons": outcome.reasons,
        "chase": outcome.chase_materials,
        "issues": [issue.as_dict() for issue in outcome.issues],
    }


def _float(value: Any) -> float | None:
    return float(value) if value is not None else None


@dataclass
class AppliedState:
    tables: dict[tuple[str, str], dict[str, Any] | None]
    pending: dict[str, dict[str, Any]]
    blocked: list[dict[str, Any]]
    changes: dict[str, Any]
    review: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def locations(self) -> dict[str, tuple[str, str]]:
        return _locations(self.tables)


def _locations(
    tables: Mapping[tuple[str, str], Mapping[str, Any] | None],
) -> dict[str, tuple[str, str]]:
    result: dict[str, tuple[str, str]] = {}
    for key, document in tables.items():
        if document is None:
            continue
        for frame_id in document["columns"]["frame_id"]:
            if frame_id in result:
                raise LakeError(f"{frame_id} 同时被放进 {result[frame_id]} 与 {key}")
            result[frame_id] = key
    return result


def _completeness_blocks(
    current: Mapping[tuple[str, str], Table],
    pending_by_key: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    pending_ids: Sequence[str] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Find complete plot-days that this state change would hollow out.

    A frame that merely moves to another plot/day keeps its value, so the old
    day can legally lose it.  What must not happen is that a frame which was
    published inside a *complete* plot/day becomes undecidable: that plot/day
    keeps its delivered rows and the frame is not duplicated in the pending
    area.  The check therefore looks at frame identity, not at the frame's new
    placement.
    """

    unresolved: set[str] = set(pending_ids or ())
    for documents in pending_by_key.values():
        unresolved.update(item["frame_id"] for item in documents)

    blocked: dict[tuple[str, str], dict[str, Any]] = {}
    for key, old in current.items():
        if not old.complete:
            continue
        lost = sorted(set(old.columns["frame_id"]) & unresolved)
        if lost:
            blocked[key] = {
                "plot": key[0],
                "day": key[1],
                "reason": "published_frames_became_unresolved",
                "previous_complete": True,
                "frame_ids": lost,
            }
    return blocked


def _close_over_paired_moves(
    current_locations: Mapping[str, tuple[str, str]],
    planned_locations: Mapping[str, tuple[str, str]],
    blocked: dict[tuple[str, str], dict[str, Any]],
) -> None:
    """A move is atomic: if either endpoint is held back, both are."""

    changed = True
    while changed:
        changed = False
        for frame_id, old_key in current_locations.items():
            new_key = planned_locations.get(frame_id)
            endpoints = {old_key}
            if new_key is not None:
                endpoints.add(new_key)
            if endpoints & set(blocked):
                for key in endpoints:
                    if key in blocked:
                        continue
                    blocked[key] = {
                        "plot": key[0],
                        "day": key[1],
                        "reason": "paired_move_endpoint_blocked",
                        "frame_ids": [frame_id],
                    }
                    changed = True


def apply_state(
    outcomes: Mapping[str, Outcome],
    basis: Mapping[str, Any],
    label: str,
    current: Mapping[tuple[str, str], Table],
    current_pending: Mapping[str, Mapping[str, Any]],
    current_review: Mapping[str, Mapping[str, Any]] | None = None,
    guard_complete: bool = True,
    incremental: bool = False,
    keep_labels: bool = False,
) -> AppliedState:
    """Build the next complete state from outcomes.

    * Frames are placed only by their evaluated plot/day; a frame that moved is
      absent from its old partition because every partition is rebuilt from the
      same outcome map.
    * Pending frames never enter a partition; they live in the pending area.
    * A currently published complete plot/day is not replaced by an incomplete
      one.  Moves are closed over: if either endpoint of a move is held back,
      both endpoints keep their delivered content.
    * ``incremental`` is used by ingest: rows already published keep their
      delivered value and basis.  Re-delivery adds observations and resolves
      pending frames, but a new knowledge time only reinterprets old frames
      through an explicit replay, never through a routine delivery.
    """

    partitions = group_observations(outcomes.values())
    pending_documents: dict[str, dict[str, Any]] = {}
    pending_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for outcome in outcomes.values():
        if outcome.status != "pending":
            continue
        document = _pending_document(outcome)
        pending_documents[outcome.frame_id] = document
        key = outcome.partition_key()
        if key is not None:
            pending_by_key.setdefault(key, []).append(document)

    published_before = _locations(
        {key: table.as_dict() for key, table in current.items()}
    )
    # Published frames that the latest signed material cannot place any more
    # must not silently disappear from a delivered plot/day.  A routine
    # delivery keeps the delivered truth until an approved replay can decide.
    demoted = {
        frame_id
        for frame_id, old_key in published_before.items()
        if frame_id in outcomes and not outcomes[frame_id].resolved
    }
    # A published frame whose evaluated placement changed must leave its old
    # partition and appear only in the new one: a reader must never see the same
    # frame under both the old and the new plot/day.
    re_evaluated: set[str] = set()
    relocated: set[str] = set()
    for frame_id, old_key in published_before.items():
        outcome = outcomes.get(frame_id)
        if outcome is None or frame_id in demoted:
            continue
        new_key = outcome.partition_key()
        re_evaluated.add(frame_id)
        if new_key != old_key:
            relocated.add(frame_id)

    added_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for key, items in partitions.items():
        for outcome in items:
            if incremental and outcome.frame_id in published_before:
                # Placement (plot/day/observed_at/deployment_rev) follows the
                # latest published deployment; the delivered value, offset and
                # firmware stay exactly as delivered.
                row = _relocated_row(current, published_before, outcome)
                added_rows.setdefault(key, []).append(row)
                continue
            added_rows.setdefault(key, []).append(row_from_outcome(outcome, label))

    planned: dict[tuple[str, str], dict[str, Any] | None] = {}
    all_keys = set(current) | set(partitions) | set(pending_by_key)
    for key in all_keys:
        old = current.get(key)
        kept = (
            [row for row in old.rows() if row["frame_id"] not in re_evaluated]
            if (incremental and old is not None)
            else []
        )
        rows = kept + added_rows.get(key, [])
        pending_here = sorted(pending_by_key.get(key, []), key=lambda item: item["frame_id"])
        if not rows:
            # A plot/day with only pending frames is represented by the pending
            # area, never by an empty published partition.
            planned[key] = None
            continue
        planned[key] = build_table(key[0], key[1], rows, pending_here, basis, label).as_dict()

    blocked: dict[tuple[str, str], dict[str, Any]] = {}
    # The completeness guard exists because a routine delivery must not replace a
    # complete plot/day with a partial one.  A re-evaluation of the very same
    # frames (``incremental``) has nothing newly unresolved to protect against.
    if guard_complete and not incremental:
        blocked = _completeness_blocks(
            current, pending_by_key, list(pending_documents)
        )
        _close_over_paired_moves(
            _locations({key: table.as_dict() for key, table in current.items()}),
            _locations(planned),
            blocked,
        )

    final: dict[tuple[str, str], Any] = {}
    for key in sorted(set(current) | set(planned)):
        if key in blocked:
            old = current.get(key)
            final[key] = old
        else:
            final[key] = planned.get(key)

    if keep_labels:
        # Publication is a new generation, not a new delivery: every partition
        # carries the label of the delivery that actually created its rows.
        for key, value in list(final.items()):
            if isinstance(value, Table):
                final[key] = current[key]
                continue
            if value is None:
                continue
            previous = current.get(key)
            final[key] = dict(value)
            final[key]["release"] = previous.release if previous else label

    final_documents: dict[tuple[str, str], dict[str, Any] | None] = {
        key: (value.as_dict() if isinstance(value, Table) else value)
        for key, value in final.items()
    }

    # Pending area: carry forward what was pending, replace re-evaluated frames,
    # and drop frames that are published in a protected partition.
    retained_published: set[str] = set()
    for key in blocked:
        table = final.get(key)
        if table is not None:
            retained_published.update(table.columns["frame_id"])

    final_pending: dict[str, dict[str, Any]] = {
        frame_id: dict(document)
        for frame_id, document in current_pending.items()
        if frame_id not in published_before
    }
    review_items: dict[str, dict[str, Any]] = {
        frame_id: dict(document) for frame_id, document in (current_review or {}).items()
    }
    for frame_id, outcome in outcomes.items():
        if outcome.status == "pending":
            if incremental and frame_id in published_before:
                # A routine delivery cannot retract published truth.  Record the
                # new doubt for the duty desk as a review item; the row stays.
                review_item = dict(pending_documents[frame_id])
                review_item["review_only"] = True
                review_item["retained_plot"] = published_before[frame_id][0]
                review_item["retained_day"] = published_before[frame_id][1]
                review_items[frame_id] = review_item
                continue
            if frame_id in retained_published:
                continue
            final_pending[frame_id] = pending_documents[frame_id]
        else:
            final_pending.pop(frame_id, None)
            # Re-decided frames are no longer a review item.
            review_items.pop(frame_id, None)

    changes = _changes_between(current, final_documents)
    return AppliedState(
        tables=final_documents,
        pending=final_pending,
        blocked=[blocked[key] for key in sorted(blocked)],
        changes=changes,
        review=review_items,
    )


def _relocated_row(
    current: Mapping[tuple[str, str], Table],
    published: Mapping[str, tuple[str, str]],
    outcome: Outcome,
) -> dict[str, Any]:
    """Move an already-published row to its new plot/day, keeping its decoding.

    The row's value, missing reason and firmware come from what was delivered;
    only the placement columns change.  This keeps a routine delivery from
    silently reinterpreting history while still satisfying the requirement that
    a moved frame is not left duplicated under its old plot/day.
    """

    old_key = published[outcome.frame_id]
    table = current.get(old_key)
    if table is None:
        return row_from_outcome(outcome)
    index = list(table.columns["frame_id"]).index(outcome.frame_id)
    row = {name: table.columns[name][index] for name in table.columns}
    row["plot"] = outcome.plot
    row["day"] = outcome.day.isoformat() if outcome.day else None
    row["observed_at"] = (
        outcome.observed_at.isoformat() if outcome.observed_at else row["observed_at"]
    )
    row["deployment_rev"] = (
        outcome.deployment.rev if outcome.deployment else row["deployment_rev"]
    )
    row["sources"] = list(outcome.sources)
    return row


def _changes_between(
    before: Mapping[tuple[str, str], Table],
    after: Mapping[tuple[str, str], Mapping[str, Any] | None],
) -> dict[str, Any]:
    added: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    unchanged: list[str] = []
    for key in sorted(set(before) | set(after)):
        old = before.get(key)
        new = after.get(key)
        if old is None and new is None:
            continue
        if old is None:
            added.append(
                {
                    "plot": key[0],
                    "day": key[1],
                    "frame_ids": list(new["columns"]["frame_id"]),
                    "action": "added",
                }
            )
        elif new is None:
            removed.append(
                {
                    "plot": key[0],
                    "day": key[1],
                    "frame_ids": list(old.columns["frame_id"]),
                    "action": "removed",
                }
            )
        else:
            old_ids = list(old.columns["frame_id"])
            new_ids = list(new["columns"]["frame_id"])
            identical = old_ids == new_ids and all(
                list(old.columns[name]) == list(new["columns"].get(name, []))
                for name in COLUMNS
            )
            if identical and old.basis == new.get("basis"):
                unchanged.append(f"{key[0]}/{key[1]}")
                continue
            changed.append(
                {
                    "plot": key[0],
                    "day": key[1],
                    "frame_ids": new_ids,
                    "removed_frame_ids": sorted(set(old_ids) - set(new_ids)),
                    "added_frame_ids": sorted(set(new_ids) - set(old_ids)),
                    "action": "changed",
                }
            )
    return {
        "added": added,
        "changed": changed,
        "removed": removed,
        "unchanged": unchanged,
        "plot_days": sorted(
            {f"{item['plot']}/{item['day']}" for item in added + changed + removed}
        ),
    }


# ---------------------------------------------------------------------------
# Derived views
# ---------------------------------------------------------------------------


def coverage_view(
    tables: Mapping[tuple[str, str], Mapping[str, Any] | None],
    pending: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    coverage: dict[str, Any] = {
        "frames": 0,
        "published": 0,
        "values": 0,
        "missing": 0,
        "pending": 0,
        "plot_days": 0,
        "plots": {},
        "days": {},
    }
    for (plot, day), document in sorted(tables.items()):
        if document is None:
            continue
        coverage["plot_days"] += 1
        statuses = list(document["columns"]["status"])
        value_rows = statuses.count("decoded")
        missing_rows = statuses.count("missing")
        rows = document["row_count"]
        coverage["frames"] += rows
        coverage["published"] += rows
        coverage["values"] += value_rows
        coverage["missing"] += missing_rows
        bucket = coverage["plots"].setdefault(
            plot, {"published": 0, "values": 0, "missing": 0, "pending": 0}
        )
        bucket["published"] += rows
        bucket["values"] += value_rows
        bucket["missing"] += missing_rows
        coverage["days"][f"{plot}/{day}"] = {
            "complete": bool(document.get("complete", False)),
            "rows": rows,
            "values": value_rows,
            "missing": missing_rows,
            "missing_by_reason": dict(document.get("missing_by_reason", {})),
            "pending": len(document.get("pending", [])),
        }
    for frame_id, document in sorted(pending.items()):
        coverage["frames"] += 1
        coverage["pending"] += 1
        plot = document.get("plot") or "(未知)"
        bucket = coverage["plots"].setdefault(
            plot, {"published": 0, "values": 0, "missing": 0, "pending": 0}
        )
        bucket["pending"] += 1
        if document.get("plot") and document.get("day"):
            key = f"{document['plot']}/{document['day']}"
            day_bucket = coverage["days"].setdefault(
                key,
                {
                    "complete": False,
                    "rows": 0,
                    "values": 0,
                    "missing": 0,
                    "pending": 0,
                },
            )
            day_bucket["pending"] += 1
            day_bucket["complete"] = False
    return coverage


def chase_view(pending: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    chase: dict[str, dict[str, Any]] = {}
    for frame_id, document in sorted(pending.items()):
        for issue in document.get("issues", []):
            for material_kind in issue.get("chase", []):
                entry = chase.setdefault(
                    material_kind, {"count": 0, "reasons": {}, "frames": []}
                )
                entry["count"] += 1
                reason = issue.get("reason") or "unknown"
                entry["reasons"][reason] = entry["reasons"].get(reason, 0) + 1
                if frame_id not in entry["frames"]:
                    entry["frames"].append(frame_id)
    return chase


def _history_entry(
    kind: str,
    label: str,
    generation: int,
    basis: Mapping[str, Any],
    changes: Mapping[str, Any],
    blocked: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "kind": kind,
        "label": label,
        "generation": generation,
        "as_of": basis.get("as_of"),
        "ruleset": basis.get("ruleset"),
        "changes": {
            "plot_days": list(changes.get("plot_days", [])),
            "added": [item["plot"] + "/" + item["day"] for item in changes.get("added", [])],
            "changed": [item["plot"] + "/" + item["day"] for item in changes.get("changed", [])],
            "removed": [item["plot"] + "/" + item["day"] for item in changes.get("removed", [])],
        },
        "blocked": [dict(item) for item in blocked],
    }


def _replaced_by_history(history: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    replaced: dict[str, list[dict[str, Any]]] = {}
    for entry in history:
        changes = entry.get("changes", {})
        for action in ("added", "changed", "removed"):
            for label in changes.get(action, []):
                replaced.setdefault(label, []).append(
                    {
                        "generation": entry.get("generation"),
                        "kind": entry.get("kind"),
                        "label": entry.get("label"),
                        "action": action,
                    }
                )
    return {label: entries for label, entries in sorted(replaced.items())}


def duty_document(
    manifest: Mapping[str, Any],
    coverage: Mapping[str, Any],
    pending: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    chase = chase_view(pending)
    pending_by_plot_day: dict[str, list[str]] = {}
    for frame_id, document in sorted(pending.items()):
        if document.get("plot") and document.get("day"):
            pending_by_plot_day.setdefault(
                f"{document['plot']}/{document['day']}", []
            ).append(frame_id)
    releases = manifest.get("releases", {})
    return {
        "generation": manifest.get("generation"),
        "coverage": dict(coverage),
        "chase": chase,
        "chase_order": sorted(chase, key=lambda kind: (-chase[kind]["count"], kind)),
        "pending_by_plot_day": pending_by_plot_day,
        "pending_count": len(pending),
        "replaced_plot_days": _replaced_by_history(manifest.get("history", [])),
        "decode_basis_by_release": {
            release: {
                "cut": entry.get("cut"),
                "as_of": entry.get("as_of"),
                "ruleset": entry.get("decode_basis", {}).get("ruleset"),
                "material": entry.get("decode_basis", {}).get("material"),
                "archives": entry.get("decode_basis", {}).get("archives"),
            }
            for release, entry in sorted(releases.items())
        },
        "replays": {
            replay_id: {
                "as_of": entry.get("as_of"),
                "ruleset": entry.get("ruleset"),
                "against": entry.get("against"),
                "promoted_generation": entry.get("promoted_generation"),
            }
            for replay_id, entry in sorted(manifest.get("replays", {}).items())
        },
        "history": list(manifest.get("history", [])),
    }


# ---------------------------------------------------------------------------
# Evaluation helper
# ---------------------------------------------------------------------------


@dataclass
class Evaluation:
    material: Material
    as_of: date
    ruleset: str
    archives: list[ParsedArchive]
    outcomes: dict[str, Outcome]

    def basis(self) -> dict[str, Any]:
        return decode_basis(
            self.material,
            [
                {"archive": archive.archive, "file": archive.file, "sha256": archive.sha256}
                for archive in self.archives
            ],
            self.as_of,
            self.ruleset,
        )


def _evaluate(
    lake: Lake,
    data_dir: Path,
    material_dir: Path,
    as_of: date,
    ruleset: str,
    extra: Sequence[Mapping[str, Any]] = (),
) -> Evaluation:
    material = load_material(material_dir)
    archives = resolve_archives(lake, data_dir, extra)
    outcomes = evaluate(material, archives, as_of)
    return Evaluation(
        material=material, as_of=as_of, ruleset=ruleset, archives=archives, outcomes=outcomes
    )


def _frame_summary(outcome: Outcome | None) -> dict[str, Any]:
    if outcome is None:
        return {"status": "absent"}
    key = outcome.partition_key()
    return {
        "status": outcome.status,
        "plot": key[0] if key else None,
        "day": key[1] if key else None,
        "observed_at": outcome.observed_at.isoformat() if outcome.observed_at else None,
        "value_cm": (
            float(outcome.payload.value_cm)
            if outcome.payload and outcome.payload.value_cm is not None
            else None
        ),
        "missing": bool(outcome.payload.missing) if outcome.payload else False,
        "missing_reason": outcome.payload.missing_reason if outcome.payload else None,
        "firmware": outcome.firmware,
        "firmware_notices": list(outcome.firmware_notices),
        "offset_cm": _float(outcome.offset_cm),
        "reasons": outcome.reasons,
        "chase": outcome.chase_materials,
        "sources": list(outcome.sources),
    }


def _frame_summaries(outcomes: Mapping[str, Outcome]) -> dict[str, dict[str, Any]]:
    return {frame_id: _frame_summary(outcome) for frame_id, outcome in sorted(outcomes.items())}


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


def ingest(
    lake_dir: Path,
    data_dir: Path,
    material_dir: Path,
    release: str,
    cut: date,
    as_of: date,
    archive_entries: Sequence[Mapping[str, Any]],
    ruleset: str = RULESET,
    issued_entry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Publish one delivered batch.

    Re-delivering the same original bytes, or re-running the identical request,
    is a no-op: it does not advance the generation, does not rewrite coverage or
    the pending ledger, and does not move the processed-position ledger.  A
    different request under an already used release id is refused rather than
    overwriting what was delivered.
    """

    lake = Lake(lake_dir)
    material = load_material(material_dir)
    verified = register_archives(lake, data_dir, archive_entries)
    manifest = lake.read_manifest()
    print_fingerprint = request_fingerprint(material, verified, as_of, ruleset)

    prior = manifest.get("requests", {}).get(release)
    if prior is not None:
        if prior.get("fingerprint") != print_fingerprint:
            raise LakeError(
                f"发布号 {release} 已用不同请求入湖（既有 {prior.get('fingerprint')}，"
                f"本次 {print_fingerprint}）；请换发布号，已交付批次不可覆盖"
            )
        stored = lake.read_release(release)
        if stored is None:
            raise LakeError(f"发布号 {release} 的交付记录缺失，仓内状态不一致")
        result = dict(stored)
        result.update(
            {
                "noop": True,
                "noop_reason": "same_request",
                "generation": manifest.get("generation", 0),
            }
        )
        return result

    duplicate = next(
        (
            other
            for other, entry in sorted(manifest.get("requests", {}).items())
            if other != release and entry.get("fingerprint") == print_fingerprint
        ),
        None,
    )
    if duplicate is not None:
        stored = lake.read_release(duplicate) or {}
        result = dict(stored)
        result.update(
            {
                "release": release,
                "noop": True,
                "noop_reason": "same_request_under_new_release",
                "duplicate_of": duplicate,
                "generation": manifest.get("generation", 0),
                "note": (
                    "相同规则、知悉时点、资料摘要与原档摘要已在 "
                    f"{duplicate} 处理；不新建代次，覆盖统计、待核清单与已处理位置不变"
                ),
            }
        )
        return result

    evaluation = _evaluate(lake, data_dir, material_dir, as_of, ruleset, verified)
    if issued_entry is not None:
        _check_issued(issued_entry, release, evaluation)

    basis = evaluation.basis()
    current = lake.read_all_tables()
    current_pending = _current_pending(lake)
    current_review = lake.read_review().get("review", {})
    applied = apply_state(
        evaluation.outcomes,
        basis,
        release,
        current,
        current_pending,
        current_review=current_review,
        incremental=True,
        keep_labels=True,
    )
    coverage = coverage_view(applied.tables, applied.pending)

    generation = int(manifest.get("generation", 0)) + 1
    history = list(manifest.get("history", []))
    history.append(
        _history_entry("release", release, generation, basis, applied.changes, applied.blocked)
    )
    new_manifest = dict(manifest)
    new_manifest["generation"] = generation
    new_manifest["history"] = history
    new_manifest.setdefault("releases", {})[release] = {
        "release": release,
        "cut": cut.isoformat(),
        "as_of": as_of.isoformat(),
        "fingerprint": print_fingerprint,
        "generation": generation,
        "decode_basis": basis,
        "archives": [
            {"archive": item["archive"], "file": item["file"], "sha256": item["sha256"]}
            for item in verified
        ],
        "published": [
            {
                "plot": key[0],
                "day": key[1],
                "frame_ids": list(document["columns"]["frame_id"]),
            }
            for key, document in sorted(applied.tables.items())
            if document is not None
        ],
        "counts": dict(coverage),
    }
    new_manifest.setdefault("requests", {})[release] = {
        "fingerprint": print_fingerprint,
        "generation": generation,
    }
    for item in verified:
        entry = dict(new_manifest.setdefault("archives", {}).get(item["sha256"], {}))
        entry.update(
            {
                "archive": item["archive"],
                "file": item["file"],
                "arrived": item.get("arrived"),
                "stored": lake.archive_path(item["sha256"]).relative_to(lake.root).as_posix(),
            }
        )
        entry["releases"] = unique_sorted(list(entry.get("releases", [])) + [release])
        new_manifest["archives"][item["sha256"]] = entry

    duties = duty_document(new_manifest, coverage, applied.pending)
    lake.publish_generation(
        new_manifest,
        tables=applied.tables,
        pending=applied.pending,
        coverage=coverage,
        duty=duties,
        review={"review": applied.review},
    )

    request_shas = {item["sha256"] for item in verified}
    request_frames = {
        frame.frame_id
        for archive in evaluation.archives
        if archive.sha256 in request_shas
        for frame in archive.frames
    }
    release_document = {
        "release": release,
        "cut": cut.isoformat(),
        "as_of": as_of.isoformat(),
        "ruleset": ruleset,
        "fingerprint": print_fingerprint,
        "generation": generation,
        "decode_basis": basis,
        "archives": [
            {"archive": item["archive"], "file": item["file"], "sha256": item["sha256"]}
            for item in verified
        ],
        "published": [
            {
                "plot": key[0],
                "day": key[1],
                "row_count": document["row_count"],
                "complete": document["complete"],
                "frame_ids": list(document["columns"]["frame_id"]),
            }
            for key, document in sorted(applied.tables.items())
            if document is not None and set(document["columns"]["frame_id"]) & request_frames
        ],
        "withheld": [
            {
                "frame_id": frame_id,
                "plot": document.get("plot"),
                "day": document.get("day"),
                "reasons": list(document.get("reasons", [])),
                "chase": list(document.get("chase", [])),
                "issues": list(document.get("issues", [])),
            }
            for frame_id, document in sorted(applied.pending.items())
            if frame_id in request_frames
        ],
        "blocked_plot_days": applied.blocked,
        "carried_pending": sorted(
            set(applied.pending) - set(evaluation.outcomes)
        ),
        "changes": applied.changes,
        "replaced": applied.changes["changed"] + applied.changes["removed"],
        "counts": {
            "frames_in_lake": coverage["frames"],
            "frames_in_request": len(request_frames),
            "published": coverage["published"],
            "values": coverage["values"],
            "missing": coverage["missing"],
            "pending": coverage["pending"],
            "plot_days": coverage["plot_days"],
        },
        "frames": {
            frame_id: summary
            for frame_id, summary in _frame_summaries(evaluation.outcomes).items()
            if frame_id in request_frames
        },
        "duty": {
            "chase": duties["chase"],
            "pending_by_plot_day": duties["pending_by_plot_day"],
            "replaced_plot_days": duties["replaced_plot_days"],
            "decode_basis_by_release": duties["decode_basis_by_release"],
        },
        "noop": False,
    }
    write_immutable(lake.release_path(release), release_document)
    result = dict(release_document)
    result["generation"] = generation
    return result


def _check_issued(
    issued: Mapping[str, Any], release: str, evaluation: Evaluation
) -> None:
    """Cross-check a delivery against the issued-batch register when supplied."""

    if issued.get("release") != release:
        raise LakeError(f"签发登记发布号 {issued.get('release')} 与请求 {release} 不符")
    declared = set(issued.get("archives", []))
    present = {archive.archive for archive in evaluation.archives}
    missing = declared - present
    if missing:
        raise LakeError(f"签发登记的原档未入湖：{sorted(missing)}")
    plots = set(issued.get("plots", []))
    days = set(issued.get("days", []))
    resolved = [o for o in evaluation.outcomes.values() if o.resolved]
    published_plots = {o.plot for o in resolved if o.plot}
    published_days = {o.day.isoformat() for o in resolved if o.day}
    if plots and not plots <= published_plots:
        raise LakeError(
            f"已交付地块与解析结果不符：登记 {sorted(plots)}，解析 {sorted(published_plots)}"
        )
    if days and not days <= published_days:
        raise LakeError(
            f"已交付日期与解析结果不符：登记 {sorted(days)}，解析 {sorted(published_days)}"
        )


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------


def replay(
    lake_dir: Path,
    data_dir: Path,
    material_dir: Path,
    replay_id: str,
    as_of: date,
    against: str | None = None,
    ruleset: str = RULESET,
) -> dict[str, Any]:
    """Re-materialise the historical originals at a chosen knowledge time.

    The replay is stored on its own and never touches the current generation or
    a delivered release record.  It can therefore be inspected and compared
    before anyone approves a promotion.
    """

    lake = Lake(lake_dir)
    material = load_material(material_dir)
    manifest = lake.read_manifest()
    archives = resolve_archives(lake, data_dir)
    if not archives:
        raise LakeError("仓内没有可用于重放的原档")
    verified = [
        {"archive": a.archive, "file": a.file, "sha256": a.sha256} for a in archives
    ]
    print_fingerprint = replay_fingerprint(material, verified, as_of, ruleset, against)

    prior = manifest.get("replays", {}).get(replay_id)
    if prior is not None:
        if prior.get("fingerprint") != print_fingerprint:
            raise LakeError(f"重放号 {replay_id} 已用不同请求登记；请换重放号")
        stored = lake.read_replay(replay_id) or {}
        result = dict(stored)
        result.update({"noop": True, "generation": manifest.get("generation", 0)})
        return result

    duplicate = next(
        (
            other
            for other, entry in sorted(manifest.get("replays", {}).items())
            if other != replay_id and entry.get("fingerprint") == print_fingerprint
        ),
        None,
    )
    if duplicate is not None:
        stored = lake.read_replay(duplicate) or {}
        result = dict(stored)
        result.update(
            {
                "replay_id": replay_id,
                "noop": True,
                "duplicate_of": duplicate,
                "generation": manifest.get("generation", 0),
            }
        )
        return result

    target = against or _latest_release_name(manifest)
    old_release = lake.read_release(target) if target else None
    if against is not None and old_release is None:
        raise LakeError(f"找不到对照批次：{target}")

    outcomes = evaluate(material, archives, as_of)
    basis = decode_basis(material, verified, as_of, ruleset)
    current = lake.read_all_tables()
    current_pending = _current_pending(lake)
    current_review = lake.read_review().get("review", {})
    # Replays are materialised *without* the completeness guard so the operator
    # can see exactly which published plot/day a promotion would withhold.
    preview = apply_state(
        outcomes,
        basis,
        replay_id,
        current,
        current_pending,
        current_review=current_review,
        guard_complete=False,
    )
    live_frames, live_basis = _live_state(lake)
    comparison = _compare_replay(live_frames, outcomes, preview.tables)

    document = {
        "replay_id": replay_id,
        "lake": str(lake.root),
        "as_of": as_of.isoformat(),
        "ruleset": ruleset,
        "fingerprint": print_fingerprint,
        "against": target,
        "against_basis": (old_release or {}).get("decode_basis"),
        "live_basis_by_plot_day": live_basis,
        "decode_basis": basis,
        "archives": verified,
        "summary": comparison["summary"],
        "frames": comparison["frames"],
        "plot_days": comparison["plot_days"],
        "unresolved": comparison["unresolved"],
        "tables": {
            f"{key[0]}/{key[1]}": value
            for key, value in sorted(preview.tables.items())
        },
        "pending": preview.pending,
        "promoted": None,
        "noop": False,
    }
    write_immutable(lake.replay_path(replay_id), document)

    manifest = lake.read_manifest()
    next_manifest = dict(manifest)
    next_manifest.setdefault("replays", {})[replay_id] = {
        "replay_id": replay_id,
        "fingerprint": print_fingerprint,
        "as_of": as_of.isoformat(),
        "ruleset": ruleset,
        "against": target,
        "generation": manifest.get("generation", 0),
        "promoted_generation": None,
    }
    _write_manifest(lake, next_manifest)
    result = dict(document)
    result["generation"] = manifest.get("generation", 0)
    return result


def _latest_release_name(manifest: Mapping[str, Any]) -> str | None:
    for entry in reversed(manifest.get("history", [])):
        if entry.get("kind") == "release":
            return entry.get("label")
    releases = manifest.get("releases", {})
    return sorted(releases)[-1] if releases else None


def _live_state(
    lake: Lake, generation: int | None = None
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Snapshot what a reader sees now, per frame, plus per-plot-day basis.

    A replay is compared against the live state (not merely the newest release
    record): frames delivered by an earlier batch are still published and must
    still be classifiable as unchanged, re-decoded or rehomed.
    """

    frames: dict[str, dict[str, Any]] = {}
    basis_by_plot_day: dict[str, dict[str, Any]] = {}
    tables = lake.read_all_tables(generation)
    for (plot, day), table in tables.items():
        label = f"{plot}/{day}"
        basis_by_plot_day[label] = {
            "release": table.release,
            "basis": dict(table.basis),
        }
        for index in range(table.row_count):
            frame_id = table.columns["frame_id"][index]
            value = table.columns["value_cm"][index]
            frames[frame_id] = {
                "status": table.columns["status"][index],
                "plot": plot,
                "day": day,
                "observed_at": table.columns["observed_at"][index],
                "value_cm": float(value) if value is not None else None,
                "missing": table.columns["missing"][index],
                "missing_reason": table.columns["missing_reason"][index],
                "firmware": table.columns["firmware"][index],
                "offset_cm": float(table.columns["offset_cm"][index])
                if table.columns["offset_cm"][index] is not None
                else None,
                "release": table.release,
            }
    for frame_id in lake.pending_index(generation).get("frames", {}):
        document = lake.read_pending(frame_id, generation)
        if document is None:
            continue
        frames[frame_id] = {
            "status": "pending",
            "plot": document.get("plot"),
            "day": document.get("day"),
            "observed_at": document.get("observed_at"),
            "value_cm": None,
            "missing": False,
            "missing_reason": None,
            "firmware": document.get("firmware"),
            "offset_cm": document.get("offset_cm"),
            "reasons": list(document.get("reasons", [])),
            "chase": list(document.get("chase", [])),
            "release": None,
        }
    return frames, basis_by_plot_day
    for entry in reversed(manifest.get("history", [])):
        if entry.get("kind") == "release":
            return entry.get("label")
    releases = manifest.get("releases", {})
    return sorted(releases)[-1] if releases else None


def _classify(
    old: Mapping[str, Any] | None, new: Mapping[str, Any]
) -> tuple[str, str | None]:
    if old is None:
        return "new_frame", "该帧不在对照批次中"
    old_resolved = old.get("status") in ("decoded", "missing")
    new_resolved = new.get("status") in ("decoded", "missing")
    if not old_resolved and new_resolved:
        return "decoded_now", "当时未解码，现在可解码"
    if old_resolved and not new_resolved:
        return "became_unresolved", "当时已发布，现在遇到决定性未解问题"
    if not old_resolved and not new_resolved:
        return "still_unresolved", "两次都未解码"
    moved = (old.get("plot"), old.get("day")) != (new.get("plot"), new.get("day"))
    fields = _changed_fields(old, new)
    if fields:
        return "decode_changed", (
            ("归属同时迁移；" if moved else "") + "变更字段：" + "、".join(fields)
        )
    if moved:
        return (
            "rehomed",
            f"仅归属迁移：{old.get('plot')}/{old.get('day')} -> {new.get('plot')}/{new.get('day')}",
        )
    return "unchanged", None


def _changed_fields(old: Mapping[str, Any], new: Mapping[str, Any]) -> list[str]:
    fields = []
    for name in ("status", "value_cm", "missing", "missing_reason", "firmware", "offset_cm"):
        old_value = old.get(name)
        new_value = new.get(name)
        if isinstance(old_value, (int, float)) and isinstance(new_value, (int, float)):
            if abs(float(old_value) - float(new_value)) < 1e-9:
                continue
        if old_value != new_value:
            fields.append(name)
    return fields


def _compare_replay(
    old_frames: Mapping[str, Mapping[str, Any]],
    outcomes: Mapping[str, Outcome],
    tables: Mapping[tuple[str, str], Mapping[str, Any] | None],
) -> dict[str, Any]:
    frames: dict[str, dict[str, Any]] = {}
    summary = {
        "unchanged": 0,
        "decoded_now": 0,
        "decode_changed": 0,
        "rehomed": 0,
        "still_unresolved": 0,
        "became_unresolved": 0,
        "new_frame": 0,
        "absent_from_replay": 0,
    }
    unresolved: list[dict[str, Any]] = []

    for frame_id in sorted(set(old_frames) | set(outcomes)):
        old = old_frames.get(frame_id)
        outcome = outcomes.get(frame_id)
        if outcome is None:
            summary["absent_from_replay"] += 1
            frames[frame_id] = {
                "classification": "absent_from_replay",
                "old": dict(old) if old else None,
                "new": None,
                "detail": "对照批次中的帧没有出现在本次重放里",
            }
            continue
        new = _frame_summary(outcome)
        classification, detail = _classify(old, new)
        summary[classification] += 1
        frames[frame_id] = {
            "classification": classification,
            "old": None if old is None else dict(old),
            "new": new,
            "detail": detail,
        }
        if outcome.status == "pending":
            unresolved.append(
                {
                    "frame_id": frame_id,
                    "plot": new["plot"],
                    "day": new["day"],
                    "reasons": new["reasons"],
                    "chase": new["chase"],
                }
            )

    plot_days: dict[str, dict[str, Any]] = {}
    for key, document in sorted(tables.items()):
        if document is None:
            continue
        label = f"{key[0]}/{key[1]}"
        classes: dict[str, int] = {}
        for frame_id in document["columns"]["frame_id"]:
            classification = frames.get(frame_id, {}).get("classification", "new_frame")
            classes[classification] = classes.get(classification, 0) + 1
        plot_days[label] = {
            "plot": key[0],
            "day": key[1],
            "row_count": document["row_count"],
            "complete": document["complete"],
            "pending": len(document.get("pending", [])),
            "classifications": classes,
        }
    return {
        "summary": summary,
        "frames": frames,
        "plot_days": plot_days,
        "unresolved": unresolved,
    }


# ---------------------------------------------------------------------------
# Promotion
# ---------------------------------------------------------------------------


def promote_replay(lake_dir: Path, replay_id: str) -> dict[str, Any]:
    """Publish a replay as a new generation, with the completeness guard."""

    lake = Lake(lake_dir)
    manifest = lake.read_manifest()
    record = manifest.get("replays", {}).get(replay_id)
    if record is None:
        raise LakeError(f"没有登记的重放：{replay_id}")
    document = lake.read_replay(replay_id)
    if document is None:
        raise LakeError(f"重放记录文件缺失：{replay_id}")
    if record.get("promoted_generation") is not None:
        result = dict(document)
        result.update(
            {
                "noop": True,
                "generation": manifest.get("generation", 0),
                "promoted_generation": record["promoted_generation"],
            }
        )
        return result

    lake_shas = set(manifest.get("archives", {}))
    replay_shas = {item["sha256"] for item in document.get("archives", [])}
    if lake_shas - replay_shas:
        raise LakeError(
            "重放没有覆盖全部仓内原档，不能晋级：缺少 "
            + ", ".join(sorted(lake_shas - replay_shas))
        )

    tables = {
        tuple(key.split("/", 1)): value
        for key, value in document.get("tables", {}).items()
    }
    pending = dict(document.get("pending", {}))
    basis = dict(document.get("decode_basis", {}))
    current = lake.read_all_tables()
    current_pending = _current_pending(lake)

    applied = _apply_materialised(tables, pending, basis, replay_id, current, current_pending)
    coverage = coverage_view(applied.tables, applied.pending)

    generation = int(manifest.get("generation", 0)) + 1
    history = list(manifest.get("history", []))
    history.append(
        _history_entry("promotion", replay_id, generation, basis, applied.changes, applied.blocked)
    )
    next_manifest = dict(manifest)
    next_manifest["generation"] = generation
    next_manifest["history"] = history
    updated_record = dict(record)
    updated_record["promoted_generation"] = generation
    next_manifest.setdefault("replays", {})[replay_id] = updated_record

    duties = duty_document(next_manifest, coverage, applied.pending)
    lake.publish_generation(
        next_manifest,
        tables=applied.tables,
        pending=applied.pending,
        coverage=coverage,
        duty=duties,
        review={"review": applied.review},
    )

    updated = dict(document)
    updated["promoted"] = generation
    updated["promoted_blocked"] = [dict(item) for item in applied.blocked]
    _write_json(lake.replay_path(replay_id), updated)
    result = dict(updated)
    result.update({"noop": False, "generation": generation})
    return result


def _apply_materialised(
    tables: Mapping[tuple[str, str], Mapping[str, Any]],
    pending: Mapping[str, Mapping[str, Any]],
    basis: Mapping[str, Any],
    label: str,
    current: Mapping[tuple[str, str], Table],
    current_pending: Mapping[str, Mapping[str, Any]],
) -> AppliedState:
    """Apply a replay's materialised tables with the completeness guard."""

    current_documents: dict[tuple[str, str], dict[str, Any] | None] = {
        key: table.as_dict() for key, table in current.items()
    }
    planned: dict[tuple[str, str], dict[str, Any] | None] = dict(tables)
    for key in current:
        planned.setdefault(key, current_documents[key])

    pending_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for frame_id, document in pending.items():
        key = (document.get("plot"), document.get("day"))
        if key[0] is not None and key[1] is not None:
            pending_by_key.setdefault(key, []).append(dict(document))

    blocked = _completeness_blocks(current, pending_by_key, list(pending))
    _close_over_paired_moves(
        _locations(current_documents), _locations(planned), blocked
    )

    final: dict[tuple[str, str], dict[str, Any] | None] = {}
    for key in sorted(set(current_documents) | set(planned)):
        if key in blocked:
            final[key] = current_documents.get(key)
        else:
            final[key] = planned.get(key)

    retained: set[str] = set()
    for key in blocked:
        document = final.get(key)
        if document is not None:
            retained.update(document["columns"]["frame_id"])

    final_pending: dict[str, dict[str, Any]] = {}
    for frame_id, document in pending.items():
        if frame_id in retained:
            continue
        final_pending[frame_id] = dict(document)
    for frame_id, document in current_pending.items():
        if frame_id in final_pending or frame_id in retained:
            continue
        key = (document.get("plot"), document.get("day"))
        if key in blocked:
            final_pending[frame_id] = dict(document)

    # All rows are re-materialised under the replay's basis, so every partition
    # that survives the guard is attributed to the replay.
    for key, document in final.items():
        if document is not None and key not in blocked:
            final[key] = dict(document)
            final[key]["release"] = label

    changes = _changes_between(current, final)
    return AppliedState(
        tables=final,
        pending=final_pending,
        blocked=[blocked[key] for key in sorted(blocked)],
        changes=changes,
    )


# ---------------------------------------------------------------------------
# Patrol, duty and verification
# ---------------------------------------------------------------------------


def patrol(
    lake_dir: Path,
    plot: str,
    day: str,
    columns: Sequence[str] | None = None,
    generation: int | None = None,
) -> dict[str, Any]:
    """Read one plot/day partition, projecting only the requested columns."""

    lake = Lake(lake_dir)
    manifest = lake.read_manifest()
    if generation is None:
        generation = manifest.get("generation", 0)
    table = lake.read_table(plot, day, generation)
    if table is None:
        return {
            "plot": plot,
            "day": day,
            "generation": generation,
            "exists": False,
        }
    selected = list(columns or ("frame_id", "observed_at", "status", "value_cm", "missing", "missing_reason"))
    unknown = [name for name in selected if name not in COLUMNS]
    if unknown:
        raise LakeError(f"未知列 {unknown}；可用列：{list(COLUMNS)}")
    data = table.select(selected)
    return {
        "plot": plot,
        "day": day,
        "generation": generation,
        "exists": True,
        "complete": table.complete,
        "release": table.release,
        "basis": dict(table.basis),
        "missing_by_reason": dict(table.missing_by_reason),
        "row_count": table.row_count,
        "columns": data,
        "pending": [dict(item) for item in table.pending],
    }


def duty(lake_dir: Path, generation: int | None = None) -> dict[str, Any]:
    """Duty-desk view for a generation: what to chase and what changed.

    ``chase`` lists the issued material to follow up for frames that are not
    truth yet.  ``review`` lists already-published frames that newer material
    would now resolve differently; they stay published until an approved replay
    and promotion, but the desk can already see the doubt.
    """

    lake = Lake(lake_dir)
    manifest = lake.read_manifest()
    if generation is None:
        generation = manifest.get("generation", 0)
    document = lake.read_duty(generation)
    if not document:
        raise LakeError(f"代次 {generation} 没有值班视图")
    result = dict(document)
    result["generation"] = generation
    result["latest_generation"] = manifest.get("generation", 0)
    result["review"] = lake.read_review(generation).get("review", {})
    result["review_count"] = len(result["review"])
    return result


def verify(lake_dir: Path) -> dict[str, Any]:
    """Check that coverage, pending ledger, partitions and releases agree."""

    lake = Lake(lake_dir)
    manifest = lake.read_manifest()
    generation = manifest.get("generation", 0)
    errors: list[str] = []
    tables = lake.read_all_tables(generation)
    pending = _current_pending(lake)
    coverage = lake.read_coverage(generation)

    locations: dict[str, tuple[str, str]] = {}
    for key, table in tables.items():
        for frame_id in table.columns["frame_id"]:
            if frame_id in locations:
                errors.append(f"{frame_id} 同时出现在 {locations[frame_id]} 与 {key}")
            locations[frame_id] = key
        if table.complete and table.pending:
            errors.append(f"{key} 标为完整但仍列有待核帧")
    for frame_id in pending:
        if frame_id in locations:
            errors.append(f"{frame_id} 既已发布又留在待归档区")

    if coverage:
        expected_frames = sum(table.row_count for table in tables.values()) + len(pending)
        if coverage.get("frames") != expected_frames:
            errors.append(
                f"覆盖帧数 {coverage.get('frames')} 与实际表行数+待核 {expected_frames} 不符"
            )
        if coverage.get("pending") != len(pending):
            errors.append(
                f"覆盖待核数 {coverage.get('pending')} 与待核清单 {len(pending)} 不符"
            )
        published = sum(table.row_count for table in tables.values())
        if coverage.get("published") != published:
            errors.append(
                f"覆盖已发布数 {coverage.get('published')} 与实际表行数 {published} 不符"
            )

    index = lake.pending_index(generation).get("frames", {})
    if set(index) != set(pending):
        errors.append(
            "待核索引与待核文件不一致："
            f"缺文件 {sorted(set(index) - set(pending))}，缺索引 {sorted(set(pending) - set(index))}"
        )

    for sha in manifest.get("archives", {}):
        stored = lake.archive_path(sha)
        if not stored.exists():
            errors.append(f"原档缺失：{sha}")
            continue
        if hashlib.sha256(stored.read_bytes()).hexdigest() != sha:
            errors.append(f"原档摘要不符：{sha}")

    for release, entry in manifest.get("releases", {}).items():
        document = lake.read_release(release)
        if document is None:
            errors.append(f"交付记录缺失：{release}")
            continue
        if document.get("fingerprint") != entry.get("fingerprint"):
            errors.append(f"交付记录与登记指纹不符：{release}")
        frame_ids = [
            fid for item in document.get("published", []) for fid in item.get("frame_ids", [])
        ]
        if len(frame_ids) != len(set(frame_ids)):
            errors.append(f"交付记录 {release} 中同一帧出现在多个地块日")
        for frame_id in frame_ids:
            if frame_id not in locations and frame_id not in pending:
                errors.append(f"交付记录 {release} 的帧 {frame_id} 不在当前仓内")

    for replay_id in manifest.get("replays", {}):
        if lake.read_replay(replay_id) is None:
            errors.append(f"重放记录缺失：{replay_id}")

    return {
        "lake": str(lake.root),
        "generation": generation,
        "ok": not errors,
        "errors": errors,
        "counts": {
            "plot_days": len(tables),
            "published_frames": sum(table.row_count for table in tables.values()),
            "pending": len(pending),
            "releases": len(manifest.get("releases", {})),
            "replays": len(manifest.get("replays", {})),
        },
    }


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _current_pending(lake: Lake) -> dict[str, dict[str, Any]]:
    documents: dict[str, dict[str, Any]] = {}
    for frame_id in lake.pending_index().get("frames", {}):
        document = lake.read_pending(frame_id)
        if document is not None:
            documents[frame_id] = document
    return documents


def _write_manifest(lake: Lake, manifest: Mapping[str, Any]) -> None:
    _atomic_write(lake.manifest_path, _dump(manifest).encode("utf-8"))


def _write_json(path: Path, document: Any) -> None:
    _atomic_write(path, _dump(document).encode("utf-8"))


def load_issued(data_dir: Path, release: str) -> dict[str, Any] | None:
    path = Path(data_dir) / "issued.json"
    if not path.exists():
        return None
    for entry in json.loads(path.read_text(encoding="utf-8")):
        if entry.get("release") == release:
            return entry
    return None
