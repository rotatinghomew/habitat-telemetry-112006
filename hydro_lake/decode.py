"""Payload decoding.

The firmware number comes from the signed notice in force for the frame; the
payload is only interpreted once that is settled.  A payload whose length,
status byte or version is contradictory is *undecodable* and must never be
published as truth, even if some bytes resemble a missing sentinel.
"""

from __future__ import annotations

from decimal import Decimal

from .model import MISSING_SENTINEL_FW1, MISSING_SENTINEL_FW2, PayloadValue


class DecodeError(ValueError):
    def __init__(self, reason: str, detail: str):
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def decode_payload(firmware: int, payload: bytes) -> PayloadValue:
    if firmware == 1:
        return _decode_firmware1(payload)
    if firmware == 2:
        return _decode_firmware2(payload)
    raise DecodeError("undecodable_version", f"未签署的固件号：{firmware}")


def _status(payload: bytes, position: int) -> int:
    status = payload[position]
    if status not in (0, 1):
        raise DecodeError(
            "undecodable_status",
            f"状态字节 {status} 不是自检缺测(1)或正常(0)",
        )
    return status


def _missing(raw: int, status: int, sentinel: int, firmware: int) -> PayloadValue | None:
    # The sentinel is a missing reading under the signed version; it is not a
    # licence to guess a different version, and it is not a contradiction.
    if raw == sentinel:
        return PayloadValue(
            missing=True,
            value_cm=None,
            raw_value=raw,
            status=status,
            missing_reason="sensor_no_reading",
            detail=f"固件{firmware}缺测哨兵 0x{sentinel:x}",
        )
    if status == 1:
        return PayloadValue(
            missing=True,
            value_cm=None,
            raw_value=raw,
            status=status,
            missing_reason="device_self_test_missing",
            detail="状态=自检缺测，该计数不产生水位",
        )
    return None


def _decode_firmware1(payload: bytes) -> PayloadValue:
    if len(payload) != 3:
        raise DecodeError(
            "undecodable_length",
            f"固件1应为 3 字节载荷，实际 {len(payload)} 字节（疑似混入另一代帧）",
        )
    raw = int.from_bytes(payload[0:2], "little", signed=False)
    status = _status(payload, 2)
    missing = _missing(raw, status, MISSING_SENTINEL_FW1, 1)
    if missing is not None:
        return missing
    # Signed firmware 1 payload is unsigned millimetres.
    return PayloadValue(
        missing=False,
        value_cm=Decimal(raw) / Decimal(10),
        raw_value=raw,
        status=status,
    )


def _decode_firmware2(payload: bytes) -> PayloadValue:
    if len(payload) != 4:
        raise DecodeError(
            "undecodable_length",
            f"固件2应为 4 字节载荷，实际 {len(payload)} 字节（疑似混入另一代帧）",
        )
    raw = int.from_bytes(payload[0:3], "little", signed=True)
    status = _status(payload, 3)
    missing = _missing(raw, status, MISSING_SENTINEL_FW2, 2)
    if missing is not None:
        return missing
    # Signed firmware 2 payload is signed hundredths of a centimetre.
    return PayloadValue(
        missing=False,
        value_cm=Decimal(raw) / Decimal(100),
        raw_value=raw,
        status=status,
    )


def apply_offset(value: PayloadValue, offset_cm: Decimal | None) -> PayloadValue:
    """The dictionary says the cm bias is only added to valid readings."""

    if value.missing or value.value_cm is None or offset_cm is None:
        return value
    return PayloadValue(
        missing=False,
        value_cm=value.value_cm + offset_cm,
        raw_value=value.raw_value,
        status=value.status,
        missing_reason=value.missing_reason,
        detail=value.detail,
    )
