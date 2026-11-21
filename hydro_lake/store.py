"""Lake storage with immutable generations.

Layout::

    <lake>/
      manifest.json                  # the single linearisation point
      archives/<sha256>.gzip         # original bytes, written once
      releases/<release>.json        # what a delivered batch looked like
      generations/<n>/
        plots/<plot>/<day>.json      # published columnar partitions
        pending/frames/<id>.json     # not truth yet
        pending/index.json
        coverage.json                # derived from this generation only
        duty.json                    # duty-desk view for this generation
      replays/<replay_id>.json       # independent re-materialisations

A write builds an entire new generation directory and then renames
``manifest.json``.  Readers resolve the manifest generation and then read files
from that generation, so they never observe half of a two-partition move.  Old
generations are retained: that is what makes "what did the delivered batch look
like" and "what changed" answerable later.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

from .columns import Table
from .model import stable_json

MANIFEST = "manifest.json"
SCHEMA_VERSION = 1


def fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        stable_json(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def safe_name(text: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in text)
    return safe or "_"


def empty_manifest() -> dict[str, Any]:
    return {
        "schema": SCHEMA_VERSION,
        "generation": 0,
        "releases": {},
        "archives": {},
        "requests": {},
        "history": [],
    }


class Lake:
    def __init__(self, root: Path | str):
        self.root = Path(root)
        self.manifest_path = self.root / MANIFEST

    # -- manifest ----------------------------------------------------------

    def exists(self) -> bool:
        return self.manifest_path.exists()

    def read_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return empty_manifest()
        return json.loads(self.manifest_path.read_text(encoding="utf-8"))

    def generation_dir(self, generation: int | None = None) -> Path:
        if generation is None:
            generation = self.read_manifest().get("generation", 0)
        return self.root / "generations" / f"{generation:06d}"

    def read_json(self, path: Path) -> Any:
        return json.loads(path.read_text(encoding="utf-8"))

    # -- archives ----------------------------------------------------------

    def archive_path(self, sha256: str) -> Path:
        return self.root / "archives" / f"{sha256}.gzip"

    def store_archive(self, source: Path, sha256: str) -> bool:
        target = self.archive_path(sha256)
        if target.exists():
            if _sha(target) != sha256:
                raise RuntimeError(f"仓内原档摘要不符：{target}")
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = _staged_path(target)
        shutil.copyfile(source, temp)
        if _sha(temp) != sha256:
            temp.unlink(missing_ok=True)
            raise RuntimeError(f"原档复制后摘要不符：{source}")
        os.replace(temp, target)
        return True

    # -- releases ----------------------------------------------------------

    def release_path(self, release: str) -> Path:
        return self.root / "releases" / f"{safe_name(release)}.json"

    def read_release(self, release: str) -> dict[str, Any] | None:
        path = self.release_path(release)
        return self.read_json(path) if path.exists() else None

    # -- generation-scoped read models ------------------------------------

    def generation_plots(self, generation: int | None = None) -> Path:
        return self.generation_dir(generation) / "plots"

    def plot_day_path(self, plot: str, day: str, generation: int | None = None) -> Path:
        return self.generation_plots(generation) / safe_name(plot) / f"{safe_name(day)}.json"

    def read_plot_day(
        self, plot: str, day: str, generation: int | None = None
    ) -> dict[str, Any] | None:
        path = self.plot_day_path(plot, day, generation)
        return self.read_json(path) if path.exists() else None

    def read_table(
        self, plot: str, day: str, generation: int | None = None
    ) -> Table | None:
        data = self.read_plot_day(plot, day, generation)
        return Table.from_dict(data) if data else None

    def read_all_tables(self, generation: int | None = None) -> dict[tuple[str, str], Table]:
        result: dict[tuple[str, str], Table] = {}
        base = self.generation_plots(generation)
        if not base.exists():
            return result
        for plot_dir in sorted(base.iterdir()):
            if not plot_dir.is_dir():
                continue
            for path in sorted(plot_dir.glob("*.json")):
                data = self.read_json(path)
                result[(data["plot"], data["day"])] = Table.from_dict(data)
        return result

    def read_coverage(self, generation: int | None = None) -> dict[str, Any]:
        path = self.generation_dir(generation) / "coverage.json"
        return self.read_json(path) if path.exists() else {}

    def read_review(self, generation: int | None = None) -> dict[str, Any]:
        path = self.generation_dir(generation) / "review.json"
        return self.read_json(path) if path.exists() else {}

    def read_duty(self, generation: int | None = None) -> dict[str, Any]:
        path = self.generation_dir(generation) / "duty.json"
        return self.read_json(path) if path.exists() else {}

    def pending_index(self, generation: int | None = None) -> dict[str, Any]:
        path = self.generation_dir(generation) / "pending" / "index.json"
        return self.read_json(path) if path.exists() else {"frames": {}}

    def read_pending(self, frame_id: str, generation: int | None = None) -> dict[str, Any] | None:
        path = (
            self.generation_dir(generation)
            / "pending"
            / "frames"
            / f"{safe_name(frame_id)}.json"
        )
        return self.read_json(path) if path.exists() else None

    # -- replays -----------------------------------------------------------

    def replay_path(self, replay_id: str) -> Path:
        return self.root / "replays" / f"{safe_name(replay_id)}.json"

    def read_replay(self, replay_id: str) -> dict[str, Any] | None:
        path = self.replay_path(replay_id)
        return self.read_json(path) if path.exists() else None

    def list_replays(self) -> list[str]:
        base = self.root / "replays"
        if not base.exists():
            return []
        return sorted(path.stem for path in base.glob("*.json"))

    # -- commit ------------------------------------------------------------

    def publish_generation(
        self,
        manifest: Mapping[str, Any],
        tables: Mapping[tuple[str, str], Mapping[str, Any] | None],
        pending: Mapping[str, Mapping[str, Any]],
        coverage: Mapping[str, Any],
        duty: Mapping[str, Any],
        review: Mapping[str, Any] | None = None,
    ) -> int:
        """Write a complete generation, then flip the manifest.

        ``tables`` maps plot/day to a table document, or to ``None`` to leave
        that plot/day absent in the new generation.  Nothing outside the new
        generation directory is touched; the manifest rename publishes both
        sides of every move at once.
        """

        self.root.mkdir(parents=True, exist_ok=True)
        generation = int(manifest.get("generation", 0))
        if generation <= 0:
            raise RuntimeError("新代次号必须由调用方推进")
        target_dir = self.root / "generations" / f"{generation:06d}"
        if target_dir.exists():
            raise RuntimeError(f"代次目录已存在，拒绝覆盖：{target_dir}")
        staging = Path(tempfile.mkdtemp(prefix=".gen-", dir=self.root))
        try:
            plots_dir = staging / "plots"
            for (plot, day), document in tables.items():
                if document is None:
                    continue
                path = plots_dir / safe_name(plot) / f"{safe_name(day)}.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(_dump(document), encoding="utf-8")
            frames_dir = staging / "pending" / "frames"
            frames_dir.mkdir(parents=True, exist_ok=True)
            for frame_id, document in pending.items():
                (frames_dir / f"{safe_name(frame_id)}.json").write_text(
                    _dump(document), encoding="utf-8"
                )
            (staging / "pending" / "index.json").write_text(
                _dump(
                    {
                        "frames": {
                            frame_id: _pending_pointer(document)
                            for frame_id, document in sorted(pending.items())
                        }
                    }
                ),
                encoding="utf-8",
            )
            (staging / "coverage.json").write_text(_dump(coverage), encoding="utf-8")
            (staging / "duty.json").write_text(_dump(duty), encoding="utf-8")
            if review is not None:
                (staging / "review.json").write_text(_dump(review), encoding="utf-8")
            target_dir.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staging, target_dir)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise

        updated = dict(manifest)
        updated["generation"] = generation
        _atomic_write(self.manifest_path, _dump(updated).encode("utf-8"))
        return generation


def _pending_pointer(document: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "frame_id": document.get("frame_id"),
        "status": document.get("status"),
        "plot": document.get("plot"),
        "day": document.get("day"),
        "reasons": list(document.get("reasons", ())),
        "chase": list(document.get("chase", ())),
    }


def _dump(value: Any) -> str:
    return json.dumps(stable_json(value), ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _staged_path(target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(prefix=".tmp-", dir=target.parent)
    os.close(handle)
    return Path(name)


def _atomic_write(path: Path, data: bytes) -> None:
    temp = _staged_path(path)
    temp.write_bytes(data)
    os.replace(temp, path)


def write_immutable(path: Path, document: Any) -> bool:
    """Write a document once; return True if it was new.

    Re-delivery must not rewrite what was already delivered, otherwise the
    per-release decode basis would drift under an unchanged release id.
    """

    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _staged_path(path)
    temp.write_bytes(_dump(document).encode("utf-8"))
    os.replace(temp, path)
    return True
