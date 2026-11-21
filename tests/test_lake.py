"""End-to-end behaviour of the telemetry lake.

These tests build synthetic archives from the documented frame layout and drive
the public pipeline functions.  They are intentionally about behaviour that the
source contract alone cannot check: de-duplication, signed-version gating,
pending isolation, replay comparison and guarded promotion.
"""

from __future__ import annotations

import gzip
import json
import struct
import tempfile
import unittest
from pathlib import Path

from hydro_lake.pipeline import (
    LakeError,
    duty,
    ingest,
    patrol,
    promote_replay,
    replay,
    verify,
)

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data"

HEADER = struct.Struct(">2scHB")


def frame(device: str, boot: str, seq: int, payload: bytes) -> bytes:
    return HEADER.pack(device.encode(), boot.encode(), seq, len(payload)) + payload


def gateway(*frames: bytes) -> bytes:
    return b"HL01" + b"".join(frames)


def write_archive(directory: Path, name: str, raw: bytes) -> Path:
    path = directory / name
    path.write_bytes(gzip.compress(raw, 9))
    return path


def fw1(value_mm: int, status: int = 0) -> bytes:
    return struct.pack("<HB", value_mm, status)


def fw2(value_centi_cm: int, status: int = 0) -> bytes:
    return value_centi_cm.to_bytes(3, "little", signed=True) + bytes([status])


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    lines = [",".join(header)] + [",".join(row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def material(directory: Path, *, firmware=None, offsets=None, deployments=None) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    firmware = firmware or [
        ["U1", "d1", "A", "1", "5", "1", "2027-01-16", ""],
    ]
    offsets = offsets or [
        ["d1", "A", "1", "5", "0", "2027-01-16"],
    ]
    deployments = deployments or [
        ["d1", "A", "2027-01-15T00:00:00+08:00", "草圩", "1", "2027-01-16"],
    ]
    _write_csv(
        directory / "signed-firmware.csv",
        ["notice", "device", "boot", "begin", "end", "firmware", "issued", "replaces"],
        firmware,
    )
    _write_csv(
        directory / "offsets.csv",
        ["device", "boot", "begin", "end", "offset_cm", "issued"],
        offsets,
    )
    _write_csv(
        directory / "deployment.csv",
        ["device", "boot", "starts", "plot", "rev", "published"],
        deployments,
    )
    return directory


def describe(path: Path, archive: str = "g1") -> dict:
    import hashlib

    return {
        "archive": archive,
        "file": path.name,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


class LakeCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.data = self.tmp / "data"
        self.data.mkdir()
        self.material_dir = material(self.tmp / "material")
        self.lake = self.tmp / "lake"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def ingest(self, entry: dict, *, release="daily-1", as_of="2027-01-17", backfill=True):
        entries = [entry]
        if backfill:
            # The second archive must be registered in the same request for the
            # first ingest; tests that need a second batch pass backfill=False.
            second = self.data / "second.gzip"
            if second.exists():
                entries.append(describe(second, "g2"))
        result = ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=self.material_dir,
            release=release,
            cut=_date(as_of),
            as_of=_date(as_of),
            archive_entries=entries,
        )
        return result


def _date(text: str):
    from datetime import date

    return date.fromisoformat(text)


class FrameIdentityAndDecoding(LakeCase):
    def test_swapped_endian_or_units_come_from_signed_notice(self):
        # fw1: 2-byte LE millimetres + status. 25000 mm = 2500 cm.
        path = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(25000))))
        result = self.ingest(describe(path))
        table = patrol(self.lake, "草圩", "2027-01-15", ["value_cm", "firmware"])
        self.assertTrue(table["exists"])
        self.assertEqual(table["columns"]["value_cm"], ["2500"])
        self.assertEqual(table["columns"]["firmware"], [1])
        self.assertEqual(result["counts"]["pending"], 0)

    def test_missing_sentinel_is_missing_but_sibling_still_publishes(self):
        raw = gateway(
            frame("d1", "A", 1, fw1(25000)),
            frame("d1", "A", 2, fw1(0x7FFF, 0)),
        )
        path = write_archive(self.data, "g1.gzip", raw)
        self.ingest(describe(path))
        table = patrol(self.lake, "草圩", "2027-01-15", ["value_cm", "missing", "missing_reason"])
        self.assertEqual(table["columns"]["value_cm"], ["2500", None])
        self.assertEqual(table["columns"]["missing_reason"], [None, "sensor_no_reading"])
        self.assertEqual(table["missing_by_reason"], {"sensor_no_reading": 1})
        page = duty(self.lake)
        self.assertEqual(page["pending_count"], 0)
        self.assertIn("草圩/2027-01-15", page["replaced_plot_days"])

    def test_undecodable_frame_does_not_erase_siblings(self):
        # Signed fw2 only; a fw1-shaped 3-byte payload under it is undecodable.
        # Its sibling still publishes; the bad frame is pending with a reason.
        notices = material(
            self.tmp / "fw2",
            firmware=[["U2", "d1", "A", "1", "5", "2", "2027-01-16", ""]],
        )
        path = write_archive(
            self.data,
            "g1.gzip",
            gateway(
                frame("d1", "A", 1, fw2(250000)),
                frame("d1", "A", 2, fw1(1234)),
            ),
        )
        ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=notices,
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(path)],
        )
        table = patrol(self.lake, "草圩", "2027-01-15", ["frame_id", "status"])
        # Only the decodable frame is truth; the other is pending, not dropped
        # and not guessed into the delivered partition.
        self.assertEqual(table["columns"]["frame_id"], ["d1/A/1"])
        self.assertFalse(table["complete"])
        self.assertEqual([p["frame_id"] for p in table["pending"]], ["d1/A/2"])
        self.assertEqual(
            table["pending"][0]["reasons"], ["undecodable_length"]
        )

    def test_signed_interval_gates_format_not_payload_shape(self):
        # Signed firmware is fw1, but the producer sent a fw2-shaped 4-byte
        # payload.  Shape must not override the signature: pending, undecoded.
        path = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw2(250000))))
        self.ingest(describe(path))
        table = patrol(self.lake, "草圩", "2027-01-15", ["frame_id"])
        self.assertFalse(table["exists"])
        index = duty(self.lake)
        self.assertEqual(index["pending_count"], 1)
        reasons = index["pending_by_plot_day"].get("草圩/2027-01-15")
        self.assertEqual(reasons, ["d1/A/1"])

    def test_sentinel_under_wrong_version_is_not_version_guessed(self):
        # A fw2 3-byte payload whose bytes equal the fw1 sentinel: still fw1 by
        # signature, so it is a missing reading, not a fw2 value.
        path = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(0x7FFF, 1))))
        self.ingest(describe(path))
        table = patrol(self.lake, "草圩", "2027-01-15", ["missing", "missing_reason", "value_cm"])
        self.assertEqual(table["columns"]["missing"], [True])
        self.assertEqual(table["columns"]["value_cm"], [None])

    def test_firmware_conflict_stays_pending_with_reason(self):
        conflict = material(
            self.tmp / "conflict",
            firmware=[
                ["U1", "d1", "A", "1", "5", "1", "2027-01-16", ""],
                ["U2", "d1", "A", "1", "3", "2", "2027-01-16", ""],
            ],
        )
        path = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(25000))))
        result = ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=conflict,
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(path)],
        )
        self.assertEqual(result["counts"]["published"], 0)
        self.assertEqual(result["withheld"][0]["reasons"], ["firmware_notice_conflict"])
        self.assertEqual(result["withheld"][0]["chase"], ["signed-firmware"])

    def test_replaces_only_within_overlap_and_same_firmware_is_not_conflict(self):
        # U2 replaces U1 only on 1..3; seq 3-5 keep fw1 from U1.  Later U3 for
        # the same firmware overlaps U1's 2..5 -- not a conflict.
        notices = material(
            self.tmp / "replace",
            firmware=[
                ["U1", "d1", "A", "1", "5", "1", "2027-01-16", ""],
                ["U2", "d1", "A", "1", "3", "2", "2027-01-16", "U1"],
                ["U3", "d1", "A", "2", "5", "1", "2027-01-17", ""],
            ],
        )
        path = write_archive(
            self.data,
            "g1.gzip",
            gateway(
                frame("d1", "A", 1, fw2(250000)),  # U2 wins: fw2
                frame("d1", "A", 3, fw1(25000)),  # U1 survives: fw1
            ),
        )
        result = ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=notices,
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(path)],
        )
        self.assertEqual(result["counts"]["pending"], 0)
        frames = result["frames"]
        self.assertEqual(frames["d1/A/1"]["firmware"], 2)
        self.assertEqual(frames["d1/A/1"]["firmware_notices"], ["U2"])
        self.assertEqual(frames["d1/A/3"]["firmware"], 1)
        self.assertEqual(frames["d1/A/3"]["firmware_notices"], ["U1", "U3"])

    def test_offset_only_applies_to_valid_values(self):
        notices = material(
            self.tmp / "offset",
            offsets=[
                ["d1", "A", "1", "5", "2.5", "2027-01-16"],
            ],
        )
        path = write_archive(
            self.data,
            "g1.gzip",
            gateway(
                frame("d1", "A", 1, fw1(25000)),
                frame("d1", "A", 2, fw1(0x7FFF, 1)),
            ),
        )
        ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=notices,
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(path)],
        )
        table = patrol(self.lake, "草圩", "2027-01-15", ["value_cm", "missing"])
        self.assertEqual(table["columns"]["value_cm"], ["2502.5", None])


class Deduplication(LakeCase):
    def test_same_frame_redelivered_is_one_row(self):
        raw = gateway(frame("d1", "A", 1, fw1(25000)))
        first = write_archive(self.data, "g1.gzip", raw)
        second = write_archive(self.data, "second.gzip", raw)
        result = ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=self.material_dir,
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(first, "g1"), describe(second, "g2")],
        )
        self.assertEqual(result["counts"]["frames_in_lake"], 1)
        table = patrol(self.lake, "草圩", "2027-01-15", ["frame_id"])
        self.assertEqual(table["columns"]["frame_id"], ["d1/A/1"])

    def test_frame_counter_is_unique_only_within_boot(self):
        path = write_archive(
            self.data,
            "g1.gzip",
            gateway(
                frame("d1", "A", 1, fw1(25000)),
                frame("d1", "B", 1, fw1(26000)),
            ),
        )
        result = ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=material(
                self.tmp / "boots",
                firmware=[
                    ["U1", "d1", "A", "1", "5", "1", "2027-01-16", ""],
                    ["U2", "d1", "B", "1", "5", "1", "2027-01-16", ""],
                ],
                deployments=[
                    ["d1", "A", "2027-01-15T00:00:00+08:00", "草圩", "1", "2027-01-16"],
                    ["d1", "B", "2027-01-16T00:00:00+08:00", "草圩", "1", "2027-01-16"],
                ],
            ),
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(path)],
        )
        self.assertEqual(result["counts"]["frames_in_lake"], 2)
        self.assertEqual(result["counts"]["plot_days"], 2)

    def test_identical_request_is_noop(self):
        path = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(25000))))
        entry = describe(path)
        first = self.ingest(entry)
        second = self.ingest(entry)
        self.assertFalse(first["noop"])
        self.assertTrue(second["noop"])
        self.assertEqual(second["generation"], first["generation"])
        check = verify(self.lake)
        self.assertTrue(check["ok"], check["errors"])

    def test_same_request_under_new_release_is_noop(self):
        path = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(25000))))
        entry = describe(path)
        first = self.ingest(entry)
        other = self.ingest(entry, release="daily-1b")
        self.assertTrue(other["noop"])
        self.assertEqual(other["duplicate_of"], "daily-1")
        self.assertEqual(other["generation"], first["generation"])

    def test_different_request_under_used_release_is_refused(self):
        path = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(25000))))
        self.ingest(describe(path))
        other = write_archive(self.data, "g9.gzip", gateway(frame("d1", "A", 1, fw1(26000))))
        with self.assertRaises(LakeError):
            ingest(
                lake_dir=self.lake,
                data_dir=self.data,
                material_dir=self.material_dir,
                release="daily-1",
                cut=_date("2027-01-17"),
                as_of=_date("2027-01-17"),
                archive_entries=[describe(other, "g9")],
            )


class ReplayAndPromotion(LakeCase):
    def _initial(self, raw: bytes) -> dict:
        path = write_archive(self.data, "g1.gzip", raw)
        entry = describe(path)
        self.entry = entry
        return self.ingest(entry)

    def test_replay_distinguishes_never_decoded_rederived_and_rehomed(self):
        raw = gateway(
            frame("d1", "A", 1, fw1(25000)),
            frame("d1", "A", 2, fw1(0x7FFF, 1)),
        )
        self._initial(raw)
        # New material at a later knowledge time: fw2 replaces fw1 for seq 2-4,
        # a deployment revision moves the boot to another plot, and calibration
        # changes.
        later = material(
            self.tmp / "later",
            firmware=[
                ["U1", "d1", "A", "1", "5", "1", "2027-01-16", ""],
                ["U2", "d1", "A", "2", "4", "2", "2027-01-19", "U1"],
            ],
            offsets=[
                ["d1", "A", "1", "5", "0", "2027-01-16"],
                ["d1", "A", "1", "5", "3", "2027-01-19"],
            ],
            deployments=[
                ["d1", "A", "2027-01-15T00:00:00+08:00", "草圩", "1", "2027-01-16"],
                ["d1", "A", "2027-01-15T00:00:00+08:00", "滩西", "2", "2027-01-19"],
            ],
        )
        result = replay(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=later,
            replay_id="r1",
            as_of=_date("2027-01-19"),
        )
        self.assertEqual(result["against"], "daily-1")
        self.assertEqual(result["decode_basis"]["as_of"], "2027-01-19")
        frames = result["frames"]
        # d1/A/1 moves plot (rev 2) *and* gets the newer calibration: both a
        # relocation and a decode change, reported as the latter with a note.
        self.assertEqual(frames["d1/A/1"]["classification"], "decode_changed")
        self.assertIn("归属同时迁移", frames["d1/A/1"]["detail"])
        self.assertIn("value_cm", frames["d1/A/1"]["detail"])
        # d1/A/2 was a fw1 missing sentinel; under fw2 the 3-byte payload is a
        # decisive unresolved problem, not a value to guess.
        self.assertEqual(frames["d1/A/2"]["classification"], "became_unresolved")
        self.assertEqual(result["summary"]["still_unresolved"], 0)
        # The replay preview places the moved frames by their new plot.
        self.assertIn("滩西/2027-01-15", result["plot_days"])
        self.assertNotIn("草圩/2027-01-15", result["plot_days"])

    def test_promotion_will_not_hollow_out_a_complete_plot_day(self):
        raw = gateway(
            frame("d1", "A", 1, fw1(25000)),
            frame("d1", "A", 2, fw1(25000)),
        )
        self._initial(raw)
        # The replay makes seq 2 undecodable but keeps seq 1 decodable.  The
        # delivered complete plot/day must not be replaced by a partial one.
        later = material(
            self.tmp / "later",
            firmware=[
                ["U1", "d1", "A", "1", "2", "1", "2027-01-16", ""],
                ["U2", "d1", "A", "2", "4", "2", "2027-01-19", "U1"],
            ],
        )
        replay(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=later,
            replay_id="r1",
            as_of=_date("2027-01-19"),
        )
        promoted = promote_replay(self.lake, "r1")
        self.assertEqual(promoted["promoted"], 2)
        blocked = {item["reason"] for item in promoted["promoted_blocked"]}
        self.assertIn("published_frames_became_unresolved", blocked)
        table = patrol(self.lake, "草圩", "2027-01-15", ["frame_id", "value_cm"])
        self.assertTrue(table["complete"])
        self.assertEqual(table["row_count"], 2)
        self.assertEqual(table["columns"]["value_cm"], ["2500", "2500"])
        self.assertTrue(verify(self.lake)["ok"])

    def test_promotion_of_a_partial_replay_publishes_what_is_safe(self):
        raw = gateway(
            frame("d1", "A", 1, fw1(25000)),
        )
        self._initial(raw)
        later = material(
            self.tmp / "later",
            firmware=[
                ["U1", "d1", "A", "1", "5", "1", "2027-01-16", ""],
            ],
            offsets=[
                ["d1", "A", "1", "5", "4", "2027-01-19"],
            ],
        )
        result = replay(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=later,
            replay_id="r1",
            as_of=_date("2027-01-19"),
        )
        self.assertEqual(result["frames"]["d1/A/1"]["classification"], "decode_changed")
        promote_replay(self.lake, "r1")
        table = patrol(self.lake, "草圩", "2027-01-15", ["value_cm", "offset_cm"])
        self.assertEqual(table["columns"]["value_cm"], ["2504"])
        self.assertEqual(table["release"], "r1")
        self.assertEqual(table["basis"]["as_of"], "2027-01-19")

    def test_replay_repeat_is_stable_and_does_not_touch_delivery(self):
        raw = gateway(frame("d1", "A", 1, fw1(25000)))
        self._initial(raw)
        first = replay(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=self.material_dir,
            replay_id="r1",
            as_of=_date("2027-01-17"),
        )
        second = replay(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=self.material_dir,
            replay_id="r1",
            as_of=_date("2027-01-17"),
        )
        self.assertFalse(first["noop"])
        self.assertTrue(second["noop"])
        # The delivered release still carries its own basis and generation.
        delivered = json.loads((self.lake / "releases" / "daily-1.json").read_text())
        self.assertEqual(delivered["decode_basis"]["as_of"], "2027-01-17")
        self.assertEqual(delivered["generation"], 1)
        self.assertTrue(verify(self.lake)["ok"])

    def test_redelivery_after_promotion_stays_noop(self):
        raw = gateway(frame("d1", "A", 1, fw1(25000)))
        self._initial(raw)
        replay(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=self.material_dir,
            replay_id="r1",
            as_of=_date("2027-01-17"),
        )
        promote_replay(self.lake, "r1")
        before = verify(self.lake)
        again = self.ingest(self.entry)
        self.assertTrue(again["noop"])
        after = verify(self.lake)
        self.assertEqual(before["counts"], after["counts"])
        self.assertEqual(before["generation"], after["generation"])

    def test_history_keeps_each_release_basis_for_the_duty_desk(self):
        raw = gateway(frame("d1", "A", 1, fw1(25000)))
        self._initial(raw)
        page = duty(self.lake)
        basis = page["decode_basis_by_release"]["daily-1"]
        self.assertEqual(basis["as_of"], "2027-01-17")
        self.assertEqual(basis["ruleset"], "hydro-lake/1")
        self.assertEqual(
            basis["material"],
            {
                "deployment.csv": _digest(self.material_dir / "deployment.csv"),
                "offsets.csv": _digest(self.material_dir / "offsets.csv"),
                "signed-firmware.csv": _digest(self.material_dir / "signed-firmware.csv"),
            },
        )


def _digest(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


class DeliveryVersusReplay(LakeCase):
    def test_new_delivery_does_not_reinterpret_published_frames(self):
        # Batch 1 at 2027-01-17: 100 mm with a 0 cm calibration.
        day1 = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(100))))
        ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=self.material_dir,
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(day1, "g1")],
        )
        self.assertEqual(patrol(self.lake, "草圩", "2027-01-15", ["value_cm"])["columns"]["value_cm"], ["10"])

        # Batch 2 arrives later with a new calibration that would change the
        # old frame's value.  Delivery must not silently rewrite history.
        revised = material(
            self.tmp / "revised",
            offsets=[
                ["d1", "A", "1", "5", "0", "2027-01-16"],
                ["d1", "A", "1", "5", "5", "2027-01-19"],
            ],
        )
        day2 = write_archive(self.data, "g2.gzip", gateway(frame("d1", "A", 2, fw1(200))))
        ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=revised,
            release="daily-2",
            cut=_date("2027-01-19"),
            as_of=_date("2027-01-19"),
            archive_entries=[describe(day2, "g2")],
        )
        table = patrol(self.lake, "草圩", "2027-01-15", ["frame_id", "value_cm", "offset_cm"])
        self.assertEqual(table["columns"]["frame_id"], ["d1/A/1", "d1/A/2"])
        self.assertEqual(table["columns"]["value_cm"], ["10", "25"])

        # Only an explicit, approved replay reinterprets the delivered frame.
        result = replay(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=revised,
            replay_id="rev",
            as_of=_date("2027-01-19"),
        )
        self.assertEqual(result["frames"]["d1/A/1"]["old"]["value_cm"], 10.0)
        self.assertEqual(result["frames"]["d1/A/1"]["new"]["value_cm"], 15.0)
        self.assertEqual(result["frames"]["d1/A/1"]["classification"], "decode_changed")
        # Until promoted, the live partition is unchanged.
        self.assertEqual(
            patrol(self.lake, "草圩", "2027-01-15", ["value_cm"])["columns"]["value_cm"],
            ["10", "25"],
        )
        promote_replay(self.lake, "rev")
        # Only the already-delivered frame is re-decoded; the frame first
        # delivered under daily-2 keeps its 5 cm offset in both states.
        self.assertEqual(
            patrol(self.lake, "草圩", "2027-01-15", ["value_cm"])["columns"]["value_cm"],
            ["15", "25"],
        )

    def test_later_delivery_relocates_without_duplicating_or_reinterpreting(self):
        # Batch 1 at 2027-01-17: the boot belongs to 草圩, 100 mm, 0 cm offset.
        first = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(100))))
        ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=self.material_dir,
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(first, "g1")],
        )
        # Batch 2 publishes a higher deployment revision that moves the boot to
        # 滩西 *and* a new calibration.  The frame must move, exactly once, and
        # its already-delivered value must not be silently re-derived.
        moved = material(
            self.tmp / "moved",
            offsets=[
                ["d1", "A", "1", "5", "0", "2027-01-16"],
                ["d1", "A", "1", "5", "5", "2027-01-19"],
            ],
            deployments=[
                ["d1", "A", "2027-01-15T00:00:00+08:00", "草圩", "1", "2027-01-16"],
                ["d1", "A", "2027-01-15T00:00:00+08:00", "滩西", "2", "2027-01-19"],
            ],
        )
        second = write_archive(self.data, "g2.gzip", gateway(frame("d1", "A", 2, fw1(200))))
        result = ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=moved,
            release="daily-2",
            cut=_date("2027-01-19"),
            as_of=_date("2027-01-19"),
            archive_entries=[describe(second, "g2")],
        )
        # Both ends of the move are reported: the new plot/day gains the frame,
        # the emptied old plot/day is removed.
        self.assertEqual(
            result["changes"]["plot_days"],
            ["滩西/2027-01-15", "草圩/2027-01-15"],
        )
        self.assertEqual(
            [item["action"] for item in result["changes"]["removed"]], ["removed"]
        )
        old_side = patrol(self.lake, "草圩", "2027-01-15", ["frame_id"])
        self.assertFalse(old_side["exists"])
        new_side = patrol(self.lake, "滩西", "2027-01-15", ["frame_id", "value_cm"])
        self.assertEqual(new_side["columns"]["frame_id"], ["d1/A/1", "d1/A/2"])
        # d1/A/1 keeps the value delivered under daily-1; d1/A/2 is new under
        # daily-2 and uses the 5 cm calibration of the same request.
        self.assertEqual(new_side["columns"]["value_cm"], ["10", "25"])
        # No reader can find the same frame under both plots.
        everywhere = [
            plot
            for plot in ("草圩", "滩西")
            if (page := patrol(self.lake, plot, "2027-01-15", ["frame_id"]))["exists"]
            and "d1/A/1" in page["columns"]["frame_id"]
        ]
        self.assertEqual(everywhere, ["滩西"])
        self.assertTrue(verify(self.lake)["ok"])

    def test_delivery_never_silently_retracts_published_truth(self):
        first = write_archive(self.data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(100))))
        ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=self.material_dir,
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(first, "g1")],
        )
        # The next delivery's material has no signed firmware for d1/A/1.
        gap = material(
            self.tmp / "gap",
            firmware=[["U9", "d2", "F", "1", "2", "1", "2027-01-16", ""]],
        )
        second = write_archive(self.data, "g2.gzip", gateway(frame("d1", "A", 2, fw1(200))))
        ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=gap,
            release="daily-2",
            cut=_date("2027-01-19"),
            as_of=_date("2027-01-19"),
            archive_entries=[describe(second, "g2")],
        )
        table = patrol(self.lake, "草圩", "2027-01-15", ["frame_id", "value_cm"])
        # The delivered row survives; the new doubt is a duty-desk review item,
        # not a silent retraction and not a duplicate pending copy.
        self.assertEqual(table["columns"]["frame_id"], ["d1/A/1"])
        self.assertEqual(table["columns"]["value_cm"], ["10"])
        page = duty(self.lake)
        self.assertIn("d1/A/1", page["review"])
        self.assertEqual(page["review"]["d1/A/1"]["chase"], ["signed-firmware"])
        self.assertNotIn("d1/A/1", page["pending_by_plot_day"].get("草圩/2027-01-15", []))
        self.assertTrue(verify(self.lake)["ok"])

    def test_later_delivery_resolves_a_pending_frame(self):
        gap = material(
            self.tmp / "gap",
            firmware=[
                ["U1", "d1", "A", "2", "5", "1", "2027-01-16", ""],
            ],
        )
        raw = gateway(frame("d1", "A", 1, fw1(100)), frame("d1", "A", 2, fw1(200)))
        first = write_archive(self.data, "g1.gzip", raw)
        ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=gap,
            release="daily-1",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=[describe(first, "g1")],
        )
        page = duty(self.lake)
        self.assertEqual(page["pending_by_plot_day"]["草圩/2027-01-15"], ["d1/A/1"])
        self.assertEqual(page["chase"]["signed-firmware"]["count"], 1)

        fixed = material(
            self.tmp / "fixed",
            firmware=[
                ["U1", "d1", "A", "1", "5", "1", "2027-01-16", ""],
            ],
        )
        ingest(
            lake_dir=self.lake,
            data_dir=self.data,
            material_dir=fixed,
            release="daily-2",
            cut=_date("2027-01-19"),
            as_of=_date("2027-01-19"),
            archive_entries=[describe(first, "g1")],
        )
        table = patrol(self.lake, "草圩", "2027-01-15", ["frame_id", "value_cm"])
        self.assertEqual(table["columns"]["frame_id"], ["d1/A/1", "d1/A/2"])
        self.assertTrue(table["complete"])
        page = duty(self.lake)
        self.assertEqual(page["pending_count"], 0)
        self.assertTrue(verify(self.lake)["ok"])


class GenerationAtomicity(unittest.TestCase):
    def test_reader_never_sees_half_a_move(self):
        # A revision moves the whole boot from 草圩 to 滩西 on the same day.
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            data = tmp / "data"
            data.mkdir()
            lake = tmp / "lake"
            before = material(tmp / "before")
            archive = write_archive(
                data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(100)))
            )
            entry = describe(archive)
            ingest(
                lake_dir=lake,
                data_dir=data,
                material_dir=before,
                release="daily-1",
                cut=_date("2027-01-17"),
                as_of=_date("2027-01-17"),
                archive_entries=[entry],
            )
            after = material(
                tmp / "after",
                deployments=[
                    ["d1", "A", "2027-01-15T00:00:00+08:00", "草圩", "1", "2027-01-16"],
                    ["d1", "A", "2027-01-15T00:00:00+08:00", "滩西", "2", "2027-01-19"],
                ],
            )
            result = replay(
                lake_dir=lake,
                data_dir=data,
                material_dir=after,
                replay_id="move",
                as_of=_date("2027-01-19"),
            )
            self.assertEqual(result["frames"]["d1/A/1"]["classification"], "rehomed")
            promote_replay(lake, "move")

            here = patrol(lake, "草圩", "2027-01-15", ["frame_id"])
            there = patrol(lake, "滩西", "2027-01-15", ["frame_id"])
            self.assertFalse(here["exists"])
            self.assertEqual(there["columns"]["frame_id"], ["d1/A/1"])
            # The superseded generation still has the old side, so a reader
            # pinned to a generation never sees a half-moved pair in one view.
            old = patrol(lake, "草圩", "2027-01-15", ["frame_id"], generation=1)
            self.assertEqual(old["columns"]["frame_id"], ["d1/A/1"])
            self.assertTrue(verify(lake)["ok"])

    def test_delivery_without_a_plot_day_leaves_it_alone(self):
        with tempfile.TemporaryDirectory() as tmp_name:
            tmp = Path(tmp_name)
            data = tmp / "data"
            data.mkdir()
            lake = tmp / "lake"
            mat = material(tmp / "mat")
            one = write_archive(data, "g1.gzip", gateway(frame("d1", "A", 1, fw1(100))))
            ingest(
                lake_dir=lake,
                data_dir=data,
                material_dir=mat,
                release="daily-1",
                cut=_date("2027-01-17"),
                as_of=_date("2027-01-17"),
                archive_entries=[describe(one, "g1")],
            )
            two = write_archive(data, "g2.gzip", gateway(frame("d1", "A", 3, fw1(300))))
            result = ingest(
                lake_dir=lake,
                data_dir=data,
                material_dir=mat,
                release="daily-2",
                cut=_date("2027-01-19"),
                as_of=_date("2027-01-19"),
                archive_entries=[describe(two, "g2")],
            )
            self.assertEqual(
                result["published"][0]["frame_ids"], ["d1/A/1", "d1/A/3"]
            )
            table = patrol(lake, "草圩", "2027-01-15", ["frame_id"])
            self.assertEqual(table["columns"]["frame_id"], ["d1/A/1", "d1/A/3"])
            # The partition keeps the label of the delivery that created it.
            self.assertEqual(table["release"], "daily-1")


class Verification(LakeCase):
    def _seed(self) -> None:
        path = write_archive(
            self.data,
            "g1.gzip",
            gateway(
                frame("d1", "A", 1, fw1(100)),
                frame("d1", "A", 2, fw1(200)),
            ),
        )
        self.entry = describe(path)
        self.ingest(self.entry)

    def test_clean_lake_verifies(self):
        self._seed()
        result = verify(self.lake)
        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(result["counts"]["published_frames"], 2)
        self.assertEqual(result["counts"]["pending"], 0)

    def test_verify_detects_a_duplicated_frame(self):
        self._seed()
        generation = verify(self.lake)["generation"]
        plots = self.lake / "generations" / f"{generation:06d}" / "plots"
        # Copy the row into a second plot/day by hand: a frame must not survive
        # under two locations.
        source = json.loads((plots / "草圩" / "2027-01-15.json").read_text())
        duplicate = json.loads(json.dumps(source))
        duplicate["plot"] = "滩西"
        (plots / "滩西").mkdir(exist_ok=True)
        (plots / "滩西" / "2027-01-15.json").write_text(json.dumps(duplicate))
        result = verify(self.lake)
        self.assertFalse(result["ok"])
        self.assertTrue(any("同时出现在" in error for error in result["errors"]))

    def test_verify_detects_coverage_drift(self):
        self._seed()
        generation = verify(self.lake)["generation"]
        path = self.lake / "generations" / f"{generation:06d}" / "coverage.json"
        document = json.loads(path.read_text())
        document["pending"] += 1
        path.write_text(json.dumps(document))
        result = verify(self.lake)
        self.assertFalse(result["ok"])
        self.assertTrue(any("待核" in error for error in result["errors"]))

    def test_repeated_request_keeps_ledgers_aligned(self):
        self._seed()
        before = verify(self.lake)
        for _ in range(3):
            self.ingest(self.entry)
        after = verify(self.lake)
        self.assertTrue(after["ok"])
        self.assertEqual(before["generation"], after["generation"])
        self.assertEqual(before["counts"], after["counts"])


class SourceScenario(unittest.TestCase):
    """The shipped data must run through the whole chain unchanged."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = Path(self._tmp.name)
        self.lake = self.tmp / "lake"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_shipped_archive(self):
        result = ingest(
            lake_dir=self.lake,
            data_dir=DATA,
            material_dir=DATA,
            release="daily-17",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=json.loads((DATA / "archives.json").read_text(encoding="utf-8")),
            issued_entry=json.loads((DATA / "issued.json").read_text(encoding="utf-8"))[0],
        )
        self.assertEqual(result["counts"]["frames_in_lake"], 7)
        self.assertEqual(result["counts"]["pending"], 2)
        self.assertEqual(result["counts"]["published"], 5)
        # d2 has two overlapping signed notices for different firmware at
        # 2027-01-17; both frames wait rather than being guessed.
        withheld = {item["frame_id"]: item for item in result["withheld"]}
        self.assertEqual(set(withheld), {"d2/F/1", "d2/F/2"})
        self.assertEqual(withheld["d2/F/1"]["chase"], ["signed-firmware"])
        # d1/A/2's 0x7fff is a missing reading, not a version hint.
        gu = patrol(self.lake, "草圩", "2027-01-15", ["value_cm", "missing_reason"])
        self.assertEqual(gu["columns"]["value_cm"][1], None)
        self.assertTrue(verify(self.lake)["ok"])

    def test_later_knowledge_resolves_conflict_and_relocates(self):
        ingest(
            lake_dir=self.lake,
            data_dir=DATA,
            material_dir=DATA,
            release="daily-17",
            cut=_date("2027-01-17"),
            as_of=_date("2027-01-17"),
            archive_entries=json.loads((DATA / "archives.json").read_text(encoding="utf-8")),
        )
        result = replay(
            lake_dir=self.lake,
            data_dir=DATA,
            material_dir=DATA,
            replay_id="r19",
            as_of=_date("2027-01-19"),
        )
        frames = result["frames"]
        self.assertEqual(frames["d2/F/1"]["classification"], "decoded_now")
        self.assertEqual(frames["d2/F/2"]["classification"], "still_unresolved")
        self.assertEqual(frames["d2/F/2"]["new"]["reasons"], ["undecodable_length"])
        # d1/A/1 belongs to the rev-2 deployment published on 2027-01-19.
        self.assertEqual(frames["d1/A/1"]["classification"], "rehomed")
        self.assertEqual(frames["d1/A/1"]["new"]["plot"], "滩西")


if __name__ == "__main__":
    unittest.main()
