"""Command line entry point for the telemetry lake.

Usage examples::

    python -m hydro_lake ingest --lake var/lake --data data --release daily-17 \
        --cut 2027-01-17 --as-of 2027-01-17
    python -m hydro_lake patrol --lake var/lake --plot 草圩 --day 2027-01-15
    python -m hydro_lake duty --lake var/lake
    python -m hydro_lake replay --lake var/lake --data data --material data \
        --replay r1 --as-of 2027-01-19
    python -m hydro_lake promote --lake var/lake --replay r1
    python -m hydro_lake verify --lake var/lake
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from .pipeline import (
    LakeError,
    duty,
    ingest,
    load_issued,
    patrol,
    promote_replay,
    replay,
    verify,
)


def _date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"不是 ISO 日期：{text}") from exc


def _emit(document) -> None:
    json.dump(document, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
    sys.stdout.write("\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hydro_lake", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    ingest_parser = sub.add_parser("ingest", help="把一个交付批次发布到湖内")
    ingest_parser.add_argument("--lake", required=True, type=Path)
    ingest_parser.add_argument("--data", required=True, type=Path)
    ingest_parser.add_argument("--material", type=Path)
    ingest_parser.add_argument("--release", required=True)
    ingest_parser.add_argument("--cut", required=True, type=_date)
    ingest_parser.add_argument("--as-of", required=True, type=_date, dest="as_of")
    ingest_parser.add_argument(
        "--archive",
        action="append",
        default=None,
        metavar="NAME",
        help="只处理指定原档名（默认使用 data/archives.json 全部）",
    )

    patrol_parser = sub.add_parser("patrol", help="按地块与日读取列式数据")
    patrol_parser.add_argument("--lake", required=True, type=Path)
    patrol_parser.add_argument("--plot", required=True)
    patrol_parser.add_argument("--day", required=True)
    patrol_parser.add_argument("--column", action="append", dest="columns")
    patrol_parser.add_argument("--generation", type=int)

    duty_parser = sub.add_parser("duty", help="值班视图：待催材料、替换地块日、解码依据")
    duty_parser.add_argument("--lake", required=True, type=Path)
    duty_parser.add_argument("--generation", type=int)

    replay_parser = sub.add_parser("replay", help="按新规则/新资料重放历史原档")
    replay_parser.add_argument("--lake", required=True, type=Path)
    replay_parser.add_argument("--data", type=Path)
    replay_parser.add_argument("--material", type=Path)
    replay_parser.add_argument("--replay", required=True)
    replay_parser.add_argument("--as-of", required=True, type=_date, dest="as_of")
    replay_parser.add_argument("--against")

    promote_parser = sub.add_parser("promote", help="把已登记的重放晋级为新一代次")
    promote_parser.add_argument("--lake", required=True, type=Path)
    promote_parser.add_argument("--replay", required=True)

    verify_parser = sub.add_parser("verify", help="核对仓内一致性")
    verify_parser.add_argument("--lake", required=True, type=Path)

    return parser


def _archive_entries(data_dir: Path, names: list[str] | None) -> list[dict]:
    path = data_dir / "archives.json"
    entries = json.loads(path.read_text(encoding="utf-8"))
    if names:
        wanted = set(names)
        entries = [entry for entry in entries if entry.get("archive") in wanted]
        missing = wanted - {entry.get("archive") for entry in entries}
        if missing:
            raise LakeError(f"archives.json 中没有这些原档：{sorted(missing)}")
    return entries


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "ingest":
            material_dir = args.material or args.data
            entries = _archive_entries(args.data, args.archive)
            issued = load_issued(args.data, args.release)
            _emit(
                ingest(
                    lake_dir=args.lake,
                    data_dir=args.data,
                    material_dir=material_dir,
                    release=args.release,
                    cut=args.cut,
                    as_of=args.as_of,
                    archive_entries=entries,
                    issued_entry=issued,
                )
            )
        elif args.command == "patrol":
            _emit(patrol(args.lake, args.plot, args.day, args.columns, args.generation))
        elif args.command == "duty":
            _emit(duty(args.lake, args.generation))
        elif args.command == "replay":
            material_dir = args.material or args.data
            _emit(
                replay(
                    lake_dir=args.lake,
                    data_dir=args.data or args.lake,
                    material_dir=material_dir,
                    replay_id=args.replay,
                    as_of=args.as_of,
                    against=args.against,
                )
            )
        elif args.command == "promote":
            _emit(promote_replay(args.lake, args.replay))
        elif args.command == "verify":
            _emit(verify(args.lake))
        else:  # pragma: no cover - argparse enforces choices
            parser.error(f"未知命令：{args.command}")
    except (LakeError, ValueError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
