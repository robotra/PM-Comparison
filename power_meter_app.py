"""
Multi Power Meter App
=====================
Connect to an arbitrary number of cycling power meters simultaneously over
Bluetooth Low Energy (BLE) and/or ANT+. Displays live power, logs to CSV,
shows a side-by-side comparison, and exposes ERG-mode controls for any
connected meter that also implements the BLE Fitness Machine Service
(typical of smart trainers like KICKR, Neo, Saris H3, Tacx, JetBlack, etc.).

Architecture overview
---------------------
- One asyncio event loop runs in a background thread (for BLE via `bleak`).
- ANT+ runs in its own thread (the `openant` library is not asyncio-native).
- The Tkinter GUI runs on the main thread and polls a thread-safe queue
  for new readings ~10x/sec. This keeps the UI responsive and avoids the
  classic "don't touch Tk widgets from another thread" trap.
- Each "slot" (1, 2, 3) is a self-contained meter connection. Slots are
  independent so you can mix BLE and ANT+ in any combination.

Data flow:
    Meter -> protocol handler (BLE or ANT+) -> thread-safe Queue -> GUI

Dependencies (install once):
    pip install bleak openant

ANT+ also requires:
    - A USB ANT+ stick (Garmin, Suunto, CycPlus, etc.) plugged in.
    - The Zadig driver replacement on Windows (see README.md).
"""

import asyncio
import csv
import json
import queue
import re
import threading
import time
import tkinter as tk
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from tkinter import ttk, filedialog, messagebox, simpledialog
from typing import Optional

# Third-party libraries. We import them inside try/except so the app can
# still launch and show a friendly error if the user hasn't installed them.
try:
    from bleak import BleakClient, BleakScanner
    BLEAK_AVAILABLE = True
except ImportError:
    BLEAK_AVAILABLE = False

def _ensure_libusb_dll_path():
    """Register the bundled libusb-1.0 DLL on Windows so pyusb (and therefore
    openant) can find it.

    Background: openant talks to the ANT+ stick via pyusb, which uses ctypes
    to dlopen `libusb-1.0`. On Linux/macOS the system loader handles this. On
    Windows there's no system libusb - users typically `pip install libusb`,
    which ships precompiled per-arch DLLs inside the package, but does NOT
    add them to the DLL search path. The result: pyusb's `get_backend()`
    silently returns None and openant raises `NoBackendError` with an empty
    message, which from the user's seat reads "ANT+ driver not found."

    Fix: locate the bundled DLL and call `os.add_dll_directory` on it before
    pyusb imports. No-op on non-Windows or if the `libusb` package isn't
    installed (in which case the user is on their own to provide the DLL,
    e.g. via Zadig's libusb runtime).
    """
    import sys
    if sys.platform != "win32":
        return
    try:
        import libusb  # provided by `pip install libusb`
    except ImportError:
        return
    import os
    import platform
    machine = platform.machine().lower()
    if machine in ("arm64", "aarch64"):
        arch = "arm64"
    elif sys.maxsize > 2 ** 32:
        arch = "x86_64"
    else:
        arch = "x86"
    dll_dir = os.path.join(os.path.dirname(libusb.__file__),
                           "_platform", "windows", arch)
    if not os.path.isdir(dll_dir):
        return
    try:
        os.add_dll_directory(dll_dir)
    except (FileNotFoundError, OSError):
        pass
    # Also prepend to PATH so any code that searches the env (rather than
    # going through the dll-directory list) still finds it.
    os.environ["PATH"] = dll_dir + os.pathsep + os.environ.get("PATH", "")


# Run before importing openant so its lazy-loaded pyusb backend resolves.
_ensure_libusb_dll_path()

try:
    from openant.easy.node import Node
    from openant.devices import ANTPLUS_NETWORK_KEY
    from openant.devices.power_meter import PowerMeter, PowerData
    OPENANT_AVAILABLE = True
except ImportError:
    OPENANT_AVAILABLE = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Standard BLE Cycling Power Service & Measurement characteristic UUIDs.
# These are defined by the Bluetooth SIG and are the same on every meter
# that follows the spec (Stages, 4iiii, Quarq, Assioma, Favero, etc.).
CYCLING_POWER_SERVICE_UUID = "00001818-0000-1000-8000-00805f9b34fb"
CYCLING_POWER_MEASUREMENT_UUID = "00002a63-0000-1000-8000-00805f9b34fb"
CYCLING_POWER_FEATURE_UUID = "00002a65-0000-1000-8000-00805f9b34fb"
CYCLING_POWER_CONTROL_POINT_UUID = "00002a66-0000-1000-8000-00805f9b34fb"

# Cycling Power Control Point opcodes (BLE GATT spec).
CP_OP_SET_CRANK_LENGTH = 0x04
CP_OP_REQUEST_CRANK_LENGTH = 0x05
CP_OP_START_OFFSET_COMPENSATION = 0x0B
CP_OP_RESPONSE_CODE = 0x20

# Result codes (third byte of a control-point response indication).
CP_RESULT_SUCCESS = 0x01
CP_RESULT_OP_NOT_SUPPORTED = 0x02
CP_RESULT_INVALID_PARAMETER = 0x03
CP_RESULT_OP_FAILED = 0x04
CP_RESULT_NOT_PERMITTED = 0x05

CP_RESULT_NAMES = {
    CP_RESULT_SUCCESS: "Success",
    CP_RESULT_OP_NOT_SUPPORTED: "Operation not supported",
    CP_RESULT_INVALID_PARAMETER: "Invalid parameter",
    CP_RESULT_OP_FAILED: "Operation failed",
    CP_RESULT_NOT_PERMITTED: "Operation not permitted",
}

# Feature flag bits in the Cycling Power Feature characteristic, per the BLE
# Cycling Power Service spec v1.1 section 3.4. The previous values here were
# off (bits 3 and 4 are Crank Revolution Data Supported and Extreme Magnitudes
# Supported, not the calibration features), which manifested as some meters
# either falsely passing the gate or - on Garmin pedals etc. - getting the
# button enabled and then refused at the control point with "Operation not
# supported". The correct bits:
#   bit 9  - Offset Compensation Supported
#   bit 12 - Crank Length Adjustment Supported
CP_FEATURE_OFFSET_COMPENSATION = 1 << 9
CP_FEATURE_CRANK_LENGTH_ADJUSTMENT = 1 << 12

# --- Fitness Machine Service (FTMS) - smart trainer control ---------------
# Standard BLE service every modern smart trainer exposes. Lets us drive ERG
# mode (target power), simulation (slope/wind/Crr), or raw resistance level.
FITNESS_MACHINE_SERVICE_UUID = "00001826-0000-1000-8000-00805f9b34fb"
FITNESS_MACHINE_FEATURE_UUID = "00002acc-0000-1000-8000-00805f9b34fb"
FITNESS_MACHINE_CONTROL_POINT_UUID = "00002ad9-0000-1000-8000-00805f9b34fb"
FITNESS_MACHINE_STATUS_UUID = "00002ada-0000-1000-8000-00805f9b34fb"
SUPPORTED_POWER_RANGE_UUID = "00002ad8-0000-1000-8000-00805f9b34fb"
SUPPORTED_RESISTANCE_RANGE_UUID = "00002ad6-0000-1000-8000-00805f9b34fb"

# FTMS Control Point opcodes.
FTMS_OP_REQUEST_CONTROL = 0x00
FTMS_OP_RESET = 0x01
FTMS_OP_SET_TARGET_RESISTANCE = 0x04
FTMS_OP_SET_TARGET_POWER = 0x05
FTMS_OP_START_RESUME = 0x07
FTMS_OP_STOP_PAUSE = 0x08
FTMS_OP_SET_SIM_PARAMS = 0x11
FTMS_OP_RESPONSE_CODE = 0x80

FTMS_RESULT_SUCCESS = 0x01
FTMS_RESULT_OP_NOT_SUPPORTED = 0x02
FTMS_RESULT_INVALID_PARAMETER = 0x03
FTMS_RESULT_OP_FAILED = 0x04
FTMS_RESULT_CONTROL_NOT_PERMITTED = 0x05

FTMS_RESULT_NAMES = {
    FTMS_RESULT_SUCCESS: "Success",
    FTMS_RESULT_OP_NOT_SUPPORTED: "Operation not supported",
    FTMS_RESULT_INVALID_PARAMETER: "Invalid parameter",
    FTMS_RESULT_OP_FAILED: "Operation failed",
    FTMS_RESULT_CONTROL_NOT_PERMITTED: "Control not permitted (request control first)",
}

# Bits in the Fitness Machine Features lower 32-bit field that matter to us.
FTMS_FEATURE_POWER_TARGET_SUPPORTED = 1 << 14   # actually in target-features
# Target-features (second uint32 of FTMS Feature characteristic).
FTMS_TARGET_FEATURE_RESISTANCE = 1 << 0
FTMS_TARGET_FEATURE_POWER = 1 << 1
FTMS_TARGET_FEATURE_SIM = 1 << 13

# Persisted state (last-used crank length per meter, etc.) lives here.
CONFIG_PATH = Path.home() / ".power_meter_app.json"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PowerReading:
    """A single power reading from a meter, tagged with which slot it came from."""
    slot: int           # slot_id of the meter that produced this reading
    timestamp: float    # Unix time when we received the reading
    power_watts: int    # Instantaneous power in watts
    cadence_rpm: Optional[float] = None  # Optional, not all meters provide it


@dataclass
class MeterSlot:
    """Holds the runtime state of a single meter slot.

    Slot IDs are assigned monotonically by the app and never reused, so a
    queued reading from a removed slot is silently dropped rather than
    reappearing in some new slot that happens to share its number.
    """
    slot_id: int
    protocol: str = "BLE"           # "BLE" or "ANT+"
    address_or_id: str = ""         # BLE MAC address or ANT+ device number
    name: str = ""                  # Display name shown in the UI
    connected: bool = False
    latest_power: int = 0           # Most recent power reading (watts)
    latest_cadence: Optional[float] = None
    last_update: float = 0.0        # Unix time of last reading
    # The thread/task that owns this connection, so we can cancel it cleanly.
    worker: Optional[object] = field(default=None, repr=False)
    # An event used to ask the worker to stop.
    stop_event: Optional[threading.Event] = field(default=None, repr=False)

    # --- Calibration / control surfaces ---
    # Populated by the worker once a connection comes up; cleared on disconnect.
    features: int = 0                                       # CP feature bitfield
    crank_length_mm: Optional[float] = None                 # Last known/applied
    ble_client: Optional[object] = field(default=None, repr=False)
    ble_cp_responses: dict = field(default_factory=dict, repr=False)
    ant_device: Optional[object] = field(default=None, repr=False)
    ant_command_queue: Optional[queue.Queue] = field(default=None, repr=False)
    ant_pending: dict = field(default_factory=dict, repr=False)

    # --- FTMS (smart trainer) state ---
    # `ftms_available` is set by the BLE worker after it confirms the
    # Fitness Machine Service exists on the connected device. The slot's
    # trainer panel stays hidden until this flag flips True.
    ftms_available: bool = False
    ftms_target_features: int = 0                           # Target-features bitfield
    ftms_power_min: Optional[int] = None
    ftms_power_max: Optional[int] = None
    ftms_resistance_min: Optional[float] = None
    ftms_resistance_max: Optional[float] = None
    ftms_target_power_w: Optional[int] = None               # Last commanded target
    ftms_responses: dict = field(default_factory=dict, repr=False)


# ---------------------------------------------------------------------------
# BLE Cycling Power Measurement parsing
# ---------------------------------------------------------------------------

def parse_cycling_power_measurement(data: bytearray) -> tuple[int, Optional[float]]:
    """
    Parse a Cycling Power Measurement notification per BLE GATT spec.

    Layout (little-endian):
        Bytes 0-1: Flags (16-bit bitfield)
        Bytes 2-3: Instantaneous Power, signed 16-bit, in watts
        ...optional fields follow depending on the flags.

    For our purposes we only need power. We return cadence as None; deriving
    real-time cadence from the spec requires tracking crank revolutions
    between notifications, which is more complexity than this hobbyist app
    needs. (Cadence over ANT+, by contrast, comes pre-calculated.)

    Returns (power_watts, cadence_rpm_or_None).
    """
    if len(data) < 4:
        return 0, None

    # Bytes 2-3 are signed instantaneous power. Negative values are valid
    # (some meters report small negatives during coasting), but for display
    # we clamp negatives to 0 so the UI doesn't look broken.
    power = int.from_bytes(data[2:4], byteorder="little", signed=True)
    power = max(0, power)
    return power, None


# ---------------------------------------------------------------------------
# Plain-text workout parser
# ---------------------------------------------------------------------------
#
# The trainer panel exposes a single text box. We accept terse, ad-hoc
# phrases instead of a structured DSL so it's quick to type during a ride:
#
#   200w                                       -> hold 200 W
#   200                                        -> hold 200 W (units optional)
#   +25 / -50                                  -> nudge target by N watts
#   stop                                       -> drop ERG
#   alternate between 150 and 200w every 1min  -> 1min @ 150, 1min @ 200, repeat
#   ramp from 150 to 250w over 5min            -> linear interpolation in 5s steps
#   200w for 2min, 150w for 30sec, repeat      -> step program (optional ", repeat")
#
# The parser is deliberately forgiving: case-insensitive, "w"/"watts" both OK,
# "min"/"m"/"minutes" all map to 60s, etc. On failure we raise WorkoutParseError
# with a message we can drop straight into the slot's status line.

class WorkoutParseError(ValueError):
    """Raised by `parse_trainer_command` when the text doesn't match any
    supported pattern. The message is shown directly to the user."""


_TIME_UNITS = {
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
}


def _parse_duration(s: str) -> float:
    """Parse '90sec', '2 min', '1.5 min', '1h30m', or bare '90' (seconds).

    Composite forms like '1h30m' work because we sum *every* number+unit
    pair found in the string. A bare number without a unit is interpreted
    as seconds.
    """
    text = s.strip().lower()
    if not text:
        raise WorkoutParseError("missing duration")
    pairs = re.findall(r"(\d+(?:\.\d+)?)\s*([a-z]*)", text)
    if not pairs:
        raise WorkoutParseError(f"can't read duration '{s}'")
    total = 0.0
    for value, unit in pairs:
        if unit == "":
            total += float(value)        # bare seconds
        else:
            mult = _TIME_UNITS.get(unit)
            if mult is None:
                raise WorkoutParseError(f"unknown time unit '{unit}'")
            total += float(value) * mult
    if total <= 0:
        raise WorkoutParseError("duration must be positive")
    return total


def _format_duration(seconds: float) -> str:
    """Inverse of `_parse_duration` for status messages. Picks the unit that
    keeps the number short: seconds for under a minute, minutes otherwise."""
    if seconds < 60:
        return f"{int(round(seconds))}s"
    minutes = seconds / 60
    if abs(minutes - round(minutes)) < 1 / 60:
        return f"{int(round(minutes))}min"
    return f"{minutes:.1f}min"


def _ramp_steps(start_w: int, end_w: int, total_s: float,
                step_s: float = 5.0) -> list[tuple[int, float]]:
    """Approximate a linear power ramp as a list of (watts, duration) steps.

    FTMS doesn't have a native ramp opcode - the trainer just holds whatever
    target we last set. So we slice the ramp into ~5-second increments and
    fire Set Target Power at each tick. Five seconds is a sweet spot: short
    enough that the watts feel continuous, long enough to avoid hammering
    the BLE control point.
    """
    n = max(2, int(round(total_s / step_s)))
    out = []
    for i in range(n):
        frac = i / (n - 1) if n > 1 else 1.0
        watts = int(round(start_w + (end_w - start_w) * frac))
        # Last slice absorbs the rounding remainder so the ramp lands
        # exactly on `total_s`.
        if i < n - 1:
            out.append((watts, step_s))
        else:
            out.append((watts, max(0.1, total_s - step_s * (n - 1))))
    return out


def parse_trainer_command(text: str) -> dict:
    """Turn user text into a `program` dict the scheduler can execute.

    Return shapes:
        {"type": "stop", "summary": str}
        {"type": "set", "watts": int, "summary": str}
        {"type": "nudge", "delta": int, "summary": str}
        {"type": "program", "repeat": bool,
         "steps": [(watts, duration_s), ...], "summary": str}
    """
    t = text.strip().lower()

    if t in {"stop", "off", "pause", "halt", "end"}:
        return {"type": "stop", "summary": "stop"}

    # Relative nudge: "+25", "-50", optionally suffixed with w.
    m = re.fullmatch(r"([+-]\d+)\s*w?", t)
    if m:
        delta = int(m.group(1))
        return {"type": "nudge", "delta": delta,
                "summary": f"adjust {delta:+d} W"}

    # Alternate: "alternate between A and B [w] every <duration>"
    m = re.fullmatch(
        r"alternate\s+between\s+(\d+)\s*w?\s+(?:and|/)\s+(\d+)\s*w?\s+every\s+(.+)",
        t,
    )
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        d = _parse_duration(m.group(3))
        return {
            "type": "program", "repeat": True,
            "steps": [(a, d), (b, d)],
            "summary": f"alternate {a} W / {b} W every {_format_duration(d)} (looping)",
        }

    # Ramp: "ramp [from] A to B [w] (over|in) <duration>"
    m = re.fullmatch(
        r"ramp(?:\s+from)?\s+(\d+)\s*w?\s+to\s+(\d+)\s*w?\s+(?:over|in)\s+(.+)",
        t,
    )
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        total = _parse_duration(m.group(3))
        steps = _ramp_steps(a, b, total)
        return {
            "type": "program", "repeat": False, "steps": steps,
            "summary": f"ramp {a} -> {b} W over {_format_duration(total)}",
        }

    # Step sequence: "200w for 2min, 150w for 30sec[, repeat]" or with "then".
    if " for " in t:
        repeat = False
        body = t
        # `, repeat` or trailing `repeat` toggles looping.
        repeat_pattern = re.compile(r"(?:,\s*|\s+(?:then\s+)?)repeat\s*$")
        if repeat_pattern.search(body):
            repeat = True
            body = repeat_pattern.sub("", body).strip().rstrip(",").strip()
        steps = []
        for piece in re.split(r"\s*,\s*|\s+then\s+", body):
            piece = piece.strip()
            if not piece:
                continue
            mm = re.fullmatch(r"(\d+)\s*w?\s+for\s+(.+)", piece)
            if not mm:
                raise WorkoutParseError(
                    f"can't read step '{piece}' (expected '<watts>w for <duration>')"
                )
            steps.append((int(mm.group(1)), _parse_duration(mm.group(2))))
        if not steps:
            raise WorkoutParseError("no steps found")
        summary = " then ".join(
            f"{w} W for {_format_duration(d)}" for w, d in steps
        )
        if repeat:
            summary += " (looping)"
        return {"type": "program", "repeat": repeat, "steps": steps,
                "summary": summary}

    # Bare set: "200" / "200w" / "200 watts"
    m = re.fullmatch(r"(\d+)\s*(?:w|watt|watts)?", t)
    if m:
        watts = int(m.group(1))
        return {"type": "set", "watts": watts, "summary": f"{watts} W"}

    raise WorkoutParseError(
        "Not understood. Try '200w', '+25', "
        "'alternate between 150 and 200w every 1min', "
        "'ramp from 150 to 250w over 5min', "
        "'200w for 2min, 150w for 30sec, repeat', or 'stop'."
    )


# ---------------------------------------------------------------------------
# BLE worker - runs inside the asyncio loop in the background thread
# ---------------------------------------------------------------------------

async def ble_meter_task(slot: MeterSlot, address: str, reading_queue: queue.Queue,
                         stop_event: threading.Event):
    """
    Connect to a BLE power meter and stream readings into reading_queue
    until stop_event is set.

    `bleak` handles all the OS-specific BLE plumbing on Windows (WinRT),
    macOS (CoreBluetooth) and Linux (BlueZ). We just describe what we want.
    """
    # Wrap the BleakClient context manager so the connect attempt itself is
    # cancellable. Without this, an unreachable meter pins the worker for up
    # to 20s and the user's Disconnect click does nothing in the meantime.
    client = BleakClient(address, timeout=20.0)

    async def _cancel_on_stop():
        # Fires the moment the GUI flips stop_event, so the connect (or any
        # later await) can be torn down promptly via task cancellation.
        while not stop_event.is_set():
            await asyncio.sleep(0.1)

    main_task = asyncio.current_task()
    canceller = asyncio.create_task(_cancel_on_stop())

    def _cancel_main(_):
        if not main_task.done():
            main_task.cancel()
    canceller.add_done_callback(_cancel_main)

    try:
        async with client:
            slot.connected = True
            slot.ble_client = client
            slot.ble_cp_responses = {}

            # Read the feature bitfield up front so the UI can gate
            # calibration controls on what the meter actually supports.
            try:
                feat = await client.read_gatt_char(CYCLING_POWER_FEATURE_UUID)
                if len(feat) >= 4:
                    slot.features = int.from_bytes(feat[:4], "little")
            except Exception:
                slot.features = 0

            # Callback fires every time the meter sends a power notification
            # (typically once per second, sometimes faster).
            def on_notify(_char, data: bytearray):
                power, cadence = parse_cycling_power_measurement(data)
                reading_queue.put(PowerReading(
                    slot=slot.slot_id,
                    timestamp=time.time(),
                    power_watts=power,
                    cadence_rpm=cadence,
                ))

            # Control point responses arrive as indications. We dispatch each
            # response back to whichever coroutine sent the matching request,
            # via a per-opcode Future stashed on the slot.
            def on_cp_indicate(_char, data: bytearray):
                if len(data) < 3 or data[0] != CP_OP_RESPONSE_CODE:
                    return
                request_opcode = data[1]
                result_code = data[2]
                payload = bytes(data[3:])
                fut = slot.ble_cp_responses.pop(request_opcode, None)
                if fut is not None and not fut.done():
                    fut.set_result((result_code, payload))

            # Subscribe to the Cycling Power Measurement characteristic.
            await client.start_notify(CYCLING_POWER_MEASUREMENT_UUID, on_notify)

            cp_available = False
            try:
                await client.start_notify(CYCLING_POWER_CONTROL_POINT_UUID, on_cp_indicate)
                cp_available = True
            except Exception:
                pass  # Some meters don't expose the control point.

            # Pre-fetch crank length so the calibration dialog can pre-fill it.
            if cp_available and (slot.features & CP_FEATURE_CRANK_LENGTH_ADJUSTMENT):
                try:
                    rc, payload = await ble_control_point_request(
                        slot, CP_OP_REQUEST_CRANK_LENGTH, b"", timeout=2.0
                    )
                    if rc == CP_RESULT_SUCCESS and len(payload) >= 2:
                        slot.crank_length_mm = int.from_bytes(payload[:2], "little") / 2.0
                except Exception:
                    pass  # Non-fatal; user can still set it from the dialog.

            # --- Smart trainer detection (FTMS) ---
            # Many smart trainers expose both CPS *and* FTMS on the same BLE
            # connection. If we find FTMS, claim control and prep the panel.
            ftms_available = False
            try:
                services = client.services  # cached after async with __aenter__
                has_ftms = any(
                    s.uuid.lower() == FITNESS_MACHINE_SERVICE_UUID.lower()
                    for s in services
                )
            except Exception:
                has_ftms = False

            if has_ftms:
                try:
                    await client.start_notify(
                        FITNESS_MACHINE_CONTROL_POINT_UUID, _make_ftms_indicate(slot)
                    )
                    ftms_available = True
                except Exception:
                    ftms_available = False

            if ftms_available:
                # Read target-features (second 32-bit word of the FTMS Feature
                # characteristic) so the UI can hide controls the trainer
                # doesn't actually support.
                try:
                    feat = await client.read_gatt_char(FITNESS_MACHINE_FEATURE_UUID)
                    if len(feat) >= 8:
                        slot.ftms_target_features = int.from_bytes(feat[4:8], "little")
                except Exception:
                    slot.ftms_target_features = 0

                # Optional: supported power range (min, max, step).
                try:
                    pr = await client.read_gatt_char(SUPPORTED_POWER_RANGE_UUID)
                    if len(pr) >= 4:
                        slot.ftms_power_min = int.from_bytes(pr[0:2], "little", signed=True)
                        slot.ftms_power_max = int.from_bytes(pr[2:4], "little", signed=True)
                except Exception:
                    pass

                try:
                    rr = await client.read_gatt_char(SUPPORTED_RESISTANCE_RANGE_UUID)
                    if len(rr) >= 4:
                        slot.ftms_resistance_min = int.from_bytes(rr[0:2], "little", signed=True) / 10.0
                        slot.ftms_resistance_max = int.from_bytes(rr[2:4], "little", signed=True) / 10.0
                except Exception:
                    pass

                # Request control. Most trainers need this once per session
                # before they'll honour Set Target Power et al. Failure here
                # isn't fatal - we still show the UI and surface errors when
                # the user tries to use it.
                try:
                    await ftms_control_point_request(
                        slot, FTMS_OP_REQUEST_CONTROL, b"", timeout=3.0
                    )
                except Exception:
                    pass

                slot.ftms_available = True
                # Tell the GUI to reveal the trainer panel for this slot.
                reading_queue.put(("FTMS_READY", slot.slot_id))

            # Idle here until we're asked to stop. The canceller task above
            # will cancel us as soon as stop_event flips, so a long sleep
            # here is fine and slightly cheaper than the old 200ms poll.
            while not stop_event.is_set():
                await asyncio.sleep(1.0)

            try:
                await client.stop_notify(CYCLING_POWER_MEASUREMENT_UUID)
            except Exception:
                pass
            if cp_available:
                try:
                    await client.stop_notify(CYCLING_POWER_CONTROL_POINT_UUID)
                except Exception:
                    pass
            if ftms_available:
                # Release control / stop ERG so the trainer doesn't keep
                # holding the last commanded target after we walk away.
                try:
                    await ftms_control_point_request(
                        slot, FTMS_OP_RESET, b"", timeout=2.0
                    )
                except Exception:
                    pass
                try:
                    await client.stop_notify(FITNESS_MACHINE_CONTROL_POINT_UUID)
                except Exception:
                    pass
    except asyncio.CancelledError:
        # User-initiated cancel via stop_event. Treat as a clean disconnect,
        # not an error - no popup needed.
        pass
    except Exception as e:
        # Push an error sentinel into the queue so the UI can show it.
        reading_queue.put(("ERROR", slot.slot_id, f"BLE error: {e}"))
    finally:
        canceller.cancel()
        slot.connected = False
        slot.ble_client = None
        slot.ble_cp_responses = {}
        slot.ftms_available = False
        slot.ftms_responses = {}
        slot.ftms_target_features = 0
        slot.ftms_power_min = slot.ftms_power_max = None
        slot.ftms_resistance_min = slot.ftms_resistance_max = None
        slot.ftms_target_power_w = None
        reading_queue.put(("DISCONNECTED", slot.slot_id))


# ---------------------------------------------------------------------------
# BLE control-point helpers (calibration etc.)
# ---------------------------------------------------------------------------

async def ble_control_point_request(slot: MeterSlot, opcode: int,
                                    payload: bytes = b"", timeout: float = 5.0):
    """
    Send a request to the Cycling Power Control Point and await the matching
    response indication. Returns (result_code, response_payload).

    Must be called on the same asyncio loop that owns `slot.ble_client` -
    i.e. submitted via AsyncLoopThread.submit().
    """
    client = slot.ble_client
    if client is None:
        raise RuntimeError("BLE client not connected.")
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    slot.ble_cp_responses[opcode] = fut
    frame = bytes([opcode]) + payload

    async def _round_trip():
        # Wrapped together so a hung write counts toward the same budget as
        # a missing response - otherwise a stalled write would block forever.
        await client.write_gatt_char(CYCLING_POWER_CONTROL_POINT_UUID, frame, response=True)
        return await fut

    try:
        return await asyncio.wait_for(_round_trip(), timeout=timeout)
    finally:
        # In case of timeout/exception, drop the dangling future so a later
        # request can re-use the opcode key cleanly.
        slot.ble_cp_responses.pop(opcode, None)


async def ble_set_crank_length(slot: MeterSlot, length_mm: float):
    """Set crank length in millimetres. Returns (result_code, b'')."""
    encoded = int(round(length_mm * 2))  # Spec: units of 0.5 mm, uint16 LE.
    if not 0 <= encoded <= 0xFFFF:
        return CP_RESULT_INVALID_PARAMETER, b""
    payload = encoded.to_bytes(2, "little")
    rc, _ = await ble_control_point_request(slot, CP_OP_SET_CRANK_LENGTH, payload)
    if rc == CP_RESULT_SUCCESS:
        slot.crank_length_mm = length_mm
    return rc, b""


async def ble_read_crank_length(slot: MeterSlot):
    """Read the meter's currently-stored crank length. Returns (rc, length_or_None)."""
    rc, payload = await ble_control_point_request(slot, CP_OP_REQUEST_CRANK_LENGTH, b"")
    length = None
    if rc == CP_RESULT_SUCCESS and len(payload) >= 2:
        length = int.from_bytes(payload[:2], "little") / 2.0
        slot.crank_length_mm = length
    return rc, length


def _make_ftms_indicate(slot: MeterSlot):
    """Build the FTMS Control Point indication handler for this slot.

    Each response indication starts with 0x80 (response code), followed by the
    request opcode, the result code, and op-specific payload bytes. We resolve
    the per-opcode Future stashed on the slot so the awaiting coroutine wakes
    up with the parsed result.
    """
    def on_indicate(_char, data: bytearray):
        if len(data) < 3 or data[0] != FTMS_OP_RESPONSE_CODE:
            return
        request_opcode = data[1]
        result_code = data[2]
        payload = bytes(data[3:])
        fut = slot.ftms_responses.pop(request_opcode, None)
        if fut is not None and not fut.done():
            fut.set_result((result_code, payload))
    return on_indicate


async def ftms_control_point_request(slot: MeterSlot, opcode: int,
                                     payload: bytes = b"", timeout: float = 5.0):
    """Send an FTMS Control Point request and await the response indication.

    Mirrors `ble_control_point_request` but uses the FTMS response map and
    UUID. Must run on the same asyncio loop that owns `slot.ble_client`.
    """
    client = slot.ble_client
    if client is None:
        raise RuntimeError("BLE client not connected.")
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    slot.ftms_responses[opcode] = fut
    frame = bytes([opcode]) + payload

    async def _round_trip():
        await client.write_gatt_char(FITNESS_MACHINE_CONTROL_POINT_UUID, frame, response=True)
        return await fut

    try:
        return await asyncio.wait_for(_round_trip(), timeout=timeout)
    finally:
        slot.ftms_responses.pop(opcode, None)


async def ftms_set_target_power(slot: MeterSlot, watts: int):
    """ERG mode: hold the rider at `watts` regardless of cadence/gear.

    Watts is encoded as a signed int16 little-endian. Negative values are
    legal in spec but most trainers reject them - we clamp to >= 0.
    """
    watts = max(-32768, min(32767, int(watts)))
    payload = watts.to_bytes(2, "little", signed=True)
    rc, _ = await ftms_control_point_request(slot, FTMS_OP_SET_TARGET_POWER, payload)
    if rc == FTMS_RESULT_SUCCESS:
        slot.ftms_target_power_w = watts
    return rc


async def ftms_set_target_resistance(slot: MeterSlot, level: float):
    """Set raw resistance level (units of 0.1, signed 16-bit)."""
    encoded = int(round(level * 10))
    encoded = max(-32768, min(32767, encoded))
    payload = encoded.to_bytes(2, "little", signed=True)
    rc, _ = await ftms_control_point_request(slot, FTMS_OP_SET_TARGET_RESISTANCE, payload)
    return rc


async def ftms_stop(slot: MeterSlot):
    """Stop/Pause: tells the trainer to drop ERG and let the rider freewheel."""
    payload = bytes([0x01])  # 0x01 = Stop, 0x02 = Pause; we want a hard stop.
    rc, _ = await ftms_control_point_request(slot, FTMS_OP_STOP_PAUSE, payload)
    if rc == FTMS_RESULT_SUCCESS:
        slot.ftms_target_power_w = None
    return rc


async def ftms_request_control(slot: MeterSlot):
    rc, _ = await ftms_control_point_request(slot, FTMS_OP_REQUEST_CONTROL, b"")
    return rc


async def ble_zero_offset(slot: MeterSlot, timeout: float = 15.0):
    """
    Trigger an offset compensation. The meter typically takes a few seconds
    to settle (the user must not pedal during this). Response payload, if
    present, holds the raw offset value as a signed int16 - useful for
    spotting drift over time.
    """
    rc, payload = await ble_control_point_request(
        slot, CP_OP_START_OFFSET_COMPENSATION, b"", timeout=timeout
    )
    offset = None
    if rc == CP_RESULT_SUCCESS and len(payload) >= 2:
        offset = int.from_bytes(payload[:2], "little", signed=True)
    return rc, offset


# ---------------------------------------------------------------------------
# Asyncio loop manager - keeps a single background event loop alive for BLE
# ---------------------------------------------------------------------------

class AsyncLoopThread:
    """
    Runs an asyncio event loop in a dedicated background thread.

    Why: Tkinter's mainloop and asyncio's event loop both want to "own" the
    main thread, so they don't mix well. We park asyncio on its own thread
    and submit coroutines to it via run_coroutine_threadsafe.
    """
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def submit(self, coro):
        """Schedule a coroutine on the background loop. Returns a Future."""
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)


# ---------------------------------------------------------------------------
# ANT+ worker - runs in its own thread
# ---------------------------------------------------------------------------

def _antplus_error_hint(kind: str, msg: str) -> str:
    """Translate an openant/libusb exception into a one-liner the user can
    actually act on. Empty string when we don't recognise the failure."""
    k = (kind or "").lower()
    m = (msg or "").lower()

    # libusb backend missing - by far the most common Windows symptom and the
    # one that typically lands here with an empty exception message.
    if "nobackend" in k or "no backend" in m or ("libusb" in m and "not found" in m):
        return ("libusb backend not found. On Windows, install a libusb-1.0 "
                "DLL (e.g. via `pip install libusb` or the Zadig setup) and "
                "make sure the ANT+ stick is bound to the WinUSB driver in "
                "Zadig. On Linux, install libusb-1.0-0.")
    # No ANT+ stick plugged in / OS doesn't see it.
    if "no such device" in m or "ant" in m and "not found" in m:
        return ("No ANT+ USB stick detected. Plug it in (or try a different "
                "port) and retry.")
    # Stick is present but the driver isn't WinUSB - openant can't talk to it.
    if "access" in m and ("denied" in m or "permission" in m):
        return ("USB access denied. On Windows, run Zadig and replace the "
                "stick's driver with WinUSB. On Linux, add a udev rule or "
                "run as a user in the `plugdev` group.")
    if "resource busy" in m or "busy" in m:
        return ("ANT+ stick is busy. Close Garmin Express / ANT Agent / "
                "any other app that grabs the stick, then retry.")
    if "timeout" in m:
        return ("ANT+ pairing timed out. Pedal once to wake the meter, "
                "double-check the device ID, and retry.")
    return ""


def antplus_meter_task(slot: MeterSlot, device_id: int, reading_queue: queue.Queue,
                       stop_event: threading.Event):
    """
    Connect to an ANT+ power meter and stream readings.

    Note on ANT+ multi-device limitations: A single ANT+ USB stick has a
    finite number of channels (typically 8 on a Garmin stick), but each
    channel needs its own Node... actually openant supports multiple
    PowerMeter devices on one Node, which is what we do here when more
    than one slot is ANT+. For simplicity *this function* opens its own
    Node per slot - which works fine if you have multiple ANT+ sticks, or
    only one ANT+ meter total. For multiple ANT+ meters on one stick, see
    the README's "Advanced" section.
    """
    node = None
    device = None
    cmd_queue: queue.Queue = queue.Queue()
    slot.ant_command_queue = cmd_queue
    slot.ant_pending = {}
    try:
        node = Node()
        node.set_network_key(0x00, ANTPLUS_NETWORK_KEY)
        device = PowerMeter(node=node, device_id=device_id)
        slot.ant_device = device
        slot.connected = True

        def on_power_data(page: int, page_name: str, data: PowerData):
            # openant fires this callback whenever a power data page arrives.
            # `data.instantaneous_power` is in watts; `data.cadence` is rpm.
            try:
                power = int(data.instantaneous_power) if data.instantaneous_power is not None else 0
                cadence = float(data.cadence) if data.cadence is not None else None
            except (TypeError, ValueError):
                power, cadence = 0, None

            reading_queue.put(PowerReading(
                slot=slot.slot_id,
                timestamp=time.time(),
                power_watts=max(0, power),
                cadence_rpm=cadence,
            ))

        device.on_power_data = on_power_data

        # Generic page hook - we use it to catch calibration responses
        # (page 0x01) and parameter reads (page 0x02 sub 0x01). openant
        # routes "interesting" pages through typed callbacks but exposes
        # everything else via these "other"/"unknown" hooks. We attach to
        # whichever hook the installed version supports.
        def on_raw_page(data, *_):
            try:
                payload = bytes(data) if not isinstance(data, (bytes, bytearray)) else data
            except Exception:
                return
            if len(payload) < 2:
                return
            page = payload[0]
            sub = payload[1] if len(payload) > 1 else 0

            if page == 0x01:
                # Manual zero / general calibration response.
                cb = slot.ant_pending.pop("zero_offset", None)
                if cb is not None:
                    if sub == 0xAC and len(payload) >= 8:
                        offset = int.from_bytes(payload[6:8], "little", signed=True)
                        cb({"ok": True, "offset": offset})
                    elif sub == 0xAF:
                        cb({"ok": False, "msg": "Manual zero failed"})
                    else:
                        cb({"ok": True, "offset": None})
            elif page == 0x02 and sub == 0x01 and len(payload) >= 5:
                encoded = payload[4]
                if encoded <= 0xFD:
                    slot.crank_length_mm = 110.0 + encoded / 2.0
                read_cb = slot.ant_pending.pop("read_crank_length", None)
                if read_cb is not None:
                    read_cb({"ok": True, "length_mm": slot.crank_length_mm})
                set_cb = slot.ant_pending.pop("set_crank_length", None)
                if set_cb is not None:
                    set_cb({"ok": True, "length_mm": slot.crank_length_mm})

        for hook in ("on_data", "on_other_data", "on_unknown_data"):
            if hasattr(device, hook):
                try:
                    setattr(device, hook, on_raw_page)
                except Exception:
                    pass

        # node.start() is blocking - it runs the ANT+ event loop. We run it
        # in a sub-thread so this thread can service the command queue and
        # poll stop_event.
        ant_thread = threading.Thread(target=node.start, daemon=True)
        ant_thread.start()

        while not stop_event.is_set():
            try:
                cmd = cmd_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                cmd()
            except Exception as e:
                text = str(e).strip()
                kind = type(e).__name__
                full = f"{kind}: {text}" if text else kind
                reading_queue.put(
                    ("ERROR", slot.slot_id, f"ANT+ command error: {full}")
                )

    except Exception as e:
        # Many openant / libusb errors have no useful str() (NoBackendError
        # is the worst offender - it raises with an empty message). Always
        # include the exception type so "ANT+ error: " can never reach the
        # user, and stitch on a hint for the cases users actually hit.
        text = str(e).strip()
        kind = type(e).__name__
        full = f"{kind}: {text}" if text else kind
        hint = _antplus_error_hint(kind, text)
        msg = f"ANT+ error: {full}"
        if hint:
            msg += f"\n\n{hint}"
        reading_queue.put(("ERROR", slot.slot_id, msg))
    finally:
        # Clean shutdown - openant is finicky, so wrap each step in try.
        try:
            if device is not None:
                device.close_channel()
        except Exception:
            pass
        try:
            if node is not None:
                node.stop()
        except Exception:
            pass
        slot.connected = False
        slot.ant_device = None
        slot.ant_command_queue = None
        # Fail any pending callbacks so the dialog doesn't hang waiting.
        for cb in list(slot.ant_pending.values()):
            try:
                cb({"ok": False, "msg": "Disconnected"})
            except Exception:
                pass
        slot.ant_pending = {}
        reading_queue.put(("DISCONNECTED", slot.slot_id))


# ---------------------------------------------------------------------------
# ANT+ calibration helpers (called from inside the ANT+ worker thread)
# ---------------------------------------------------------------------------

def _antplus_channel(slot: MeterSlot):
    """Best-effort channel lookup that tolerates openant version differences."""
    dev = slot.ant_device
    if dev is None:
        return None
    return (getattr(dev, "channel", None)
            or getattr(dev, "_channel", None)
            or getattr(getattr(dev, "_device", None), "channel", None))


def _antplus_send_ack(slot: MeterSlot, page_bytes: bytes) -> bool:
    """Send an 8-byte acknowledged data page on the ANT+ channel."""
    channel = _antplus_channel(slot)
    if channel is None:
        return False
    try:
        channel.send_acknowledged_data(bytes(page_bytes))
        return True
    except Exception:
        return False


def antplus_set_crank_length(slot: MeterSlot, length_mm: float, on_result):
    """Page 0x02 sub-page 0x01 - Set Crank Parameters."""
    encoded = int(round((length_mm - 110.0) * 2))
    if not 0 <= encoded <= 0xFD:
        on_result({"ok": False, "msg": "Length out of ANT+ range (110-236.5 mm)"})
        return
    page = bytes([0x02, 0x01, 0xFF, 0xFF, encoded, 0xFF, 0xFF, 0xFF])
    slot.ant_pending["set_crank_length"] = on_result
    if not _antplus_send_ack(slot, page):
        slot.ant_pending.pop("set_crank_length", None)
        on_result({"ok": False, "msg": "ANT+ send failed"})


def antplus_read_crank_length(slot: MeterSlot, on_result):
    """Common Page 0x46 - Request Data Page (page 0x02, sub-page 0x01)."""
    page = bytes([0x46, 0xFF, 0xFF, 0x01, 0x02, 0x04, 0x02, 0x01])
    slot.ant_pending["read_crank_length"] = on_result
    if not _antplus_send_ack(slot, page):
        slot.ant_pending.pop("read_crank_length", None)
        on_result({"ok": False, "msg": "ANT+ send failed"})


def antplus_zero_offset(slot: MeterSlot, on_result):
    """Page 0x01 sub-page 0xAA - Manual Zero (general calibration)."""
    page = bytes([0x01, 0xAA, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF])
    slot.ant_pending["zero_offset"] = on_result
    if not _antplus_send_ack(slot, page):
        slot.ant_pending.pop("zero_offset", None)
        on_result({"ok": False, "msg": "ANT+ send failed"})


# ---------------------------------------------------------------------------
# BLE scanner - returns a list of nearby cycling power meters
# ---------------------------------------------------------------------------

async def scan_ble_power_meters(duration: float = 6.0) -> list:
    """
    Scan for BLE devices advertising the Cycling Power Service.

    Returns a list of (name, address) tuples. We filter on the service
    UUID so we don't show every random Bluetooth gadget in the room.
    """
    discovered = []
    seen = set()

    def detection_callback(device, advertisement_data):
        # Some meters advertise the service UUID; some only expose it after
        # connection. We accept both cases - if the UUID is in the advert,
        # great; otherwise we include any device with a recognizable name.
        uuids = [u.lower() for u in (advertisement_data.service_uuids or [])]
        is_power = CYCLING_POWER_SERVICE_UUID.lower() in uuids
        # Common name patterns. Conservative - better to show a meter with
        # an unusual name than to hide it.
        name = device.name or ""
        looks_like_meter = any(kw in name.lower() for kw in
            ["power", "stages", "quarq", "4iiii", "assioma", "favero",
             "vector", "kickr", "neo", "p2m", "p2max", "rotor", "infocrank"])

        if (is_power or looks_like_meter) and device.address not in seen:
            seen.add(device.address)
            discovered.append((name or "Unknown", device.address))

    scanner = BleakScanner(detection_callback=detection_callback)
    await scanner.start()
    await asyncio.sleep(duration)
    await scanner.stop()
    return discovered


# ---------------------------------------------------------------------------
# Recording setup dialog
# ---------------------------------------------------------------------------

class RecordingSetupDialog(tk.Toplevel):
    """Modal: collect a session name, free-form notes, and a CSV path before
    starting a recording. The result is exposed via `self.result` (a dict)
    or None if the user cancelled."""

    def __init__(self, app):
        super().__init__(app.root)
        self.app = app
        self.title("New recording session")
        self.geometry("540x460")
        self.transient(app.root)
        self.grab_set()  # Modal: block the main window until the user picks.
        self.result = None
        self._build_ui()
        self.name_entry.focus_set()
        self.name_entry.select_range(0, "end")

    def _build_ui(self):
        pad = {"padx": 12, "pady": (8, 2)}

        ttk.Label(self, text="Session name:").pack(anchor="w", **pad)
        default_name = f"Session {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        self.name_var = tk.StringVar(value=default_name)
        self.name_entry = ttk.Entry(self, textvariable=self.name_var)
        self.name_entry.pack(fill="x", padx=12)

        ttk.Label(self, text="Notes (optional, multi-line):").pack(anchor="w", **pad)
        self.notes_text = tk.Text(self, height=10, wrap="word")
        self.notes_text.pack(fill="both", expand=True, padx=12, pady=(0, 6))

        ttk.Label(self, text="File:").pack(anchor="w", **pad)
        path_row = ttk.Frame(self)
        path_row.pack(fill="x", padx=12)
        default_filename = f"power_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.path_var = tk.StringVar(value=str(Path.cwd() / default_filename))
        ttk.Entry(path_row, textvariable=self.path_var).pack(side="left", fill="x", expand=True)
        ttk.Button(path_row, text="Browse...", command=self._browse).pack(side="left", padx=4)

        btn_row = ttk.Frame(self)
        btn_row.pack(fill="x", padx=12, pady=12)
        ttk.Button(btn_row, text="Start", command=self._start).pack(side="right")
        ttk.Button(btn_row, text="Cancel", command=self.destroy).pack(side="right", padx=6)

        # Enter on the name field jumps to notes; Esc cancels anywhere.
        self.bind("<Escape>", lambda _: self.destroy())

    def _browse(self):
        cur = self.path_var.get()
        initial_dir = str(Path(cur).parent) if cur else str(Path.cwd())
        initial_file = (Path(cur).name if cur
                        else f"power_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        path = filedialog.asksaveasfilename(
            parent=self,
            defaultextension=".csv",
            initialdir=initial_dir,
            initialfile=initial_file,
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if path:
            self.path_var.set(path)

    def _start(self):
        path = self.path_var.get().strip()
        if not path:
            messagebox.showwarning("No file", "Pick a file path first.", parent=self)
            return
        self.result = {
            "name": self.name_var.get().strip() or "Untitled",
            "notes": self.notes_text.get("1.0", "end").strip(),
            "path": path,
        }
        self.destroy()


# ---------------------------------------------------------------------------
# Calibration dialog
# ---------------------------------------------------------------------------

class CalibrationDialog(tk.Toplevel):
    """Per-slot calibration window: read/set crank length, run zero offset."""

    COMMON_LENGTHS = (165.0, 170.0, 172.5, 175.0)

    def __init__(self, app, slot_id: int):
        super().__init__(app.root)
        self.app = app
        self.slot_id = slot_id
        self.slot = app._slot(slot_id)
        if self.slot is None:
            self.destroy()
            return
        self.title(f"Calibrate Meter {slot_id}")
        self.geometry("440x460")
        # Non-modal: live readings stay visible behind the dialog.
        self.transient(app.root)
        self._build_ui()
        self._refresh_meter_info()
        self._refresh_crank_length()

    def _build_ui(self):
        pad = {"padx": 12, "pady": 6}

        info = ttk.LabelFrame(self, text="Meter", padding=8)
        info.pack(fill="x", **pad)
        self.info_var = tk.StringVar(value="...")
        ttk.Label(info, textvariable=self.info_var, justify="left").pack(anchor="w")

        crank = ttk.LabelFrame(self, text="Crank length", padding=8)
        crank.pack(fill="x", **pad)
        self.current_crank_var = tk.StringVar(value="Current: -")
        ttk.Label(crank, textvariable=self.current_crank_var).pack(anchor="w")

        row = ttk.Frame(crank)
        row.pack(fill="x", pady=(6, 0))
        ttk.Label(row, text="New (mm):").pack(side="left")
        self.length_var = tk.StringVar(value="")
        ttk.Entry(row, textvariable=self.length_var, width=8).pack(side="left", padx=4)
        for L in self.COMMON_LENGTHS:
            ttk.Button(row, text=f"{L:g}", width=5,
                       command=lambda v=L: self.length_var.set(str(v))
                       ).pack(side="left", padx=1)

        btn_row = ttk.Frame(crank)
        btn_row.pack(fill="x", pady=(6, 0))
        self.read_btn = ttk.Button(btn_row, text="Read from meter",
                                   command=self._refresh_crank_length)
        self.read_btn.pack(side="left")
        self.apply_btn = ttk.Button(btn_row, text="Apply",
                                    command=self._apply_crank_length)
        self.apply_btn.pack(side="left", padx=6)

        zero = ttk.LabelFrame(self, text="Zero offset", padding=8)
        zero.pack(fill="x", **pad)
        ttk.Label(zero,
                  text="Stop pedaling. Lift the rear wheel. Then click below.",
                  foreground="gray").pack(anchor="w")
        zrow = ttk.Frame(zero)
        zrow.pack(fill="x", pady=(6, 0))
        self.zero_btn = ttk.Button(zrow, text="Calibrate (Zero offset)",
                                   command=self._zero_offset)
        self.zero_btn.pack(side="left")
        self.offset_var = tk.StringVar(value="")
        ttk.Label(zrow, textvariable=self.offset_var).pack(side="left", padx=10)

        self.status_var = tk.StringVar(value="")
        self.status_label = ttk.Label(self, textvariable=self.status_var,
                                      foreground="gray")
        self.status_label.pack(anchor="w", padx=12, pady=(4, 8))

        ttk.Button(self, text="Close", command=self.destroy).pack(
            side="bottom", pady=(0, 8)
        )

    def _refresh_meter_info(self):
        s = self.slot
        if s.protocol == "BLE":
            feats = []
            if s.features & CP_FEATURE_CRANK_LENGTH_ADJUSTMENT:
                feats.append("crank length adjustment")
            if s.features & CP_FEATURE_OFFSET_COMPENSATION:
                feats.append("offset compensation")
            feats_str = ", ".join(feats) if feats else "(none advertised)"
        else:
            feats_str = "(ANT+: feature flags not introspected)"
        self.info_var.set(
            f"Protocol: {s.protocol}\n"
            f"ID/Address: {s.address_or_id or '-'}\n"
            f"Name: {s.name or '-'}\n"
            f"Features: {feats_str}"
        )
        # Gate buttons on advertised BLE features. ANT+ has no such flags
        # exposed cheaply; we let the user try and surface any failures.
        if s.protocol == "BLE":
            if not (s.features & CP_FEATURE_CRANK_LENGTH_ADJUSTMENT):
                self.apply_btn.config(state="disabled")
                self.read_btn.config(state="disabled")
            if not (s.features & CP_FEATURE_OFFSET_COMPENSATION):
                self.zero_btn.config(state="disabled")

    def _refresh_crank_length(self):
        cur = self.slot.crank_length_mm
        if cur is not None:
            self.current_crank_var.set(f"Current: {cur:g} mm")
            if not self.length_var.get():
                self.length_var.set(f"{cur:g}")
        if not self.slot.connected:
            return
        self._set_status("Reading crank length...", "orange")
        self.app._calibrate_read_crank_length(self.slot_id, self._on_read_done)

    def _on_read_done(self, result):
        if result.get("ok"):
            length = result.get("length_mm") or self.slot.crank_length_mm
            if length is not None:
                self.current_crank_var.set(f"Current: {length:g} mm")
                if not self.length_var.get():
                    self.length_var.set(f"{length:g}")
                self._set_status("Crank length read.", "green")
            else:
                self._set_status("Read OK but no value returned.", "gray")
        else:
            self._set_status(f"Read failed: {result.get('msg', '?')}", "red")

    def _apply_crank_length(self):
        try:
            length = float(self.length_var.get())
        except ValueError:
            self._set_status("Enter a valid number.", "red")
            return
        if not 70.0 <= length <= 240.0:
            self._set_status("Length must be between 70 and 240 mm.", "red")
            return
        self._set_status(f"Setting crank length to {length:g} mm...", "orange")
        self.apply_btn.config(state="disabled")
        self.app._calibrate_set_crank_length(self.slot_id, length, self._on_apply_done)

    def _on_apply_done(self, result):
        s = self.slot
        if s.protocol != "BLE" or (s.features & CP_FEATURE_CRANK_LENGTH_ADJUSTMENT):
            self.apply_btn.config(state="normal")
        if result.get("ok"):
            length = result.get("length_mm")
            if length is not None:
                self.current_crank_var.set(f"Current: {length:g} mm")
            self._set_status(f"OK: {result.get('msg', 'Set')}", "green")
        else:
            hint = self._failure_hint(result.get("msg", ""))
            self._set_status(f"Failed: {result.get('msg', '?')}{hint}", "red")

    def _zero_offset(self):
        if not self.slot.connected:
            self._set_status("Not connected.", "red")
            return
        self._set_status("Running zero-offset calibration (don't pedal)...", "orange")
        self.zero_btn.config(state="disabled")
        self.offset_var.set("")
        self.app._calibrate_zero_offset(self.slot_id, self._on_zero_done)

    def _on_zero_done(self, result):
        s = self.slot
        if s.protocol != "BLE" or (s.features & CP_FEATURE_OFFSET_COMPENSATION):
            self.zero_btn.config(state="normal")
        if result.get("ok"):
            offset = result.get("offset")
            if offset is not None:
                if abs(offset) <= 50:
                    color = "green"
                elif abs(offset) <= 100:
                    color = "orange"
                else:
                    color = "red"
                self.offset_var.set(f"Offset: {offset}")
                self.status_var.set(f"OK: zero-offset complete (raw {offset}).")
                self.status_label.config(foreground=color)
            else:
                self._set_status("OK: zero-offset complete.", "green")
        else:
            hint = self._failure_hint(result.get("msg", ""))
            self._set_status(f"Failed: {result.get('msg', '?')}{hint}", "red")

    @staticmethod
    def _failure_hint(msg: str) -> str:
        m = (msg or "").lower()
        if "not permitted" in m:
            return " - try with the meter awake (pedal once, then retry)."
        if "invalid parameter" in m:
            return " - value out of range."
        if "operation failed" in m:
            return " - keep pedals/crank stationary, then retry."
        if "not supported" in m:
            # Common on Garmin Vector/Rally and a handful of older meters:
            # they implement calibration only over ANT+, even when the BLE
            # CP control point is exposed. Steer the user that way rather
            # than leaving them stuck thinking the pedals are broken.
            return (" - meter rejects this over BLE. Many Garmin pedals "
                    "(Vector / Rally) and similar dual-protocol meters "
                    "expose calibration only on ANT+. Re-pair this slot as "
                    "ANT+ and retry.")
        return ""

    def _set_status(self, text: str, color: str = "gray"):
        self.status_var.set(text)
        self.status_label.config(foreground=color)


# ---------------------------------------------------------------------------
# Main GUI application
# ---------------------------------------------------------------------------

class PowerMeterApp:
    """The Tkinter GUI. Owns the slots, the recording state, and the queue."""

    INITIAL_SLOTS = 2        # How many empty slots to start with on launch
    POLL_INTERVAL_MS = 100   # How often we drain the queue and refresh UI
    STALE_AFTER_SEC = 3.0    # If no reading for this long, show power as 0
    SLOT_PANEL_WIDTH = 280   # Fixed slot-panel width so horizontal scroll works

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Multi Power Meter")
        self.root.geometry("980x640")

        # Thread-safe queue: workers push readings, GUI pops them.
        self.reading_queue: queue.Queue = queue.Queue()

        # Background asyncio loop for BLE.
        self.async_thread = AsyncLoopThread() if BLEAK_AVAILABLE else None

        # Slots are stored in display order; widgets are keyed by slot_id so
        # adding/removing a slot in the middle doesn't shift the lookups.
        # Slot IDs are monotonic and never reused - this means a stray queue
        # item from a removed slot is silently dropped instead of polluting
        # whatever happens to be in that ordinal position now.
        self.slots: list[MeterSlot] = []
        self.slot_widgets: dict[int, dict] = {}
        self._next_slot_id = 1

        # Active text-driven workout schedules, keyed by slot_id. Each value
        # is the `after()` job id we'd cancel if the user replaces or stops it.
        self._schedules: dict[int, dict] = {}

        # Recording state.
        self.recording = False
        self.recording_file = None
        self.csv_writer = None
        self.recording_start_time: Optional[float] = None
        self.session_name: Optional[str] = None

        # Persisted state (last-used crank length per meter, etc.)
        self.config = self._load_config()

        self._build_ui()

        # Spin up a couple of empty slots so the UI isn't blank on first run.
        for _ in range(self.INITIAL_SLOTS):
            self._add_meter()

        # Kick off the periodic queue-draining loop.
        self.root.after(self.POLL_INTERVAL_MS, self._poll_queue)

        # Make sure we shut down workers cleanly on window close.
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # -- Slot lookup helpers -------------------------------------------------

    def _slot(self, slot_id: int) -> Optional[MeterSlot]:
        return next((s for s in self.slots if s.slot_id == slot_id), None)

    def _widgets(self, slot_id: int) -> Optional[dict]:
        return self.slot_widgets.get(slot_id)

    # -- UI construction -----------------------------------------------------

    def _build_ui(self):
        # Top status bar showing what's installed.
        status = []
        status.append("BLE: " + ("ready" if BLEAK_AVAILABLE else "MISSING (pip install bleak)"))
        status.append("ANT+: " + ("ready" if OPENANT_AVAILABLE else "MISSING (pip install openant)"))
        ttk.Label(self.root, text="  |  ".join(status), foreground="gray").pack(pady=(8, 0))

        # Toolbar: add/remove meters.
        toolbar = ttk.Frame(self.root)
        toolbar.pack(fill="x", padx=10, pady=(8, 0))
        ttk.Button(toolbar, text="+ Add meter", command=self._add_meter).pack(side="left")
        ttk.Label(
            toolbar,
            text="Tip: a smart trainer's controls appear inline once it connects.",
            foreground="gray",
        ).pack(side="left", padx=10)

        # Scrollable container: a Canvas hosts an inner frame that holds the
        # slot panels. Horizontal scrolling means we can fit an unbounded
        # number of slots without forcing the user's window to grow.
        outer = ttk.Frame(self.root)
        outer.pack(fill="both", expand=True, padx=10, pady=10)
        self.slots_canvas = tk.Canvas(outer, highlightthickness=0)
        hbar = ttk.Scrollbar(outer, orient="horizontal",
                             command=self.slots_canvas.xview)
        self.slots_canvas.configure(xscrollcommand=hbar.set)
        self.slots_canvas.pack(side="top", fill="both", expand=True)
        hbar.pack(side="bottom", fill="x")

        self.slots_inner = ttk.Frame(self.slots_canvas)
        self._slots_window = self.slots_canvas.create_window(
            (0, 0), window=self.slots_inner, anchor="nw"
        )
        # Update scrollregion whenever the inner frame resizes (e.g. a slot
        # got added or a trainer panel expanded).
        def _on_inner_configure(_e):
            self.slots_canvas.configure(scrollregion=self.slots_canvas.bbox("all"))
        self.slots_inner.bind("<Configure>", _on_inner_configure)
        # Match the inner frame's height to the canvas so panels can fill
        # vertically without leaving dead space below.
        def _on_canvas_configure(e):
            self.slots_canvas.itemconfigure(self._slots_window, height=e.height)
        self.slots_canvas.bind("<Configure>", _on_canvas_configure)
        # Mouse wheel = horizontal scroll while hovered over the slots area.
        def _on_wheel(e):
            self.slots_canvas.xview_scroll(int(-e.delta / 120), "units")
        self.slots_canvas.bind("<Enter>",
            lambda _e: self.slots_canvas.bind_all("<MouseWheel>", _on_wheel))
        self.slots_canvas.bind("<Leave>",
            lambda _e: self.slots_canvas.unbind_all("<MouseWheel>"))

        # Bottom: comparison + recording controls.
        bottom = ttk.Frame(self.root)
        bottom.pack(fill="x", padx=10, pady=10)

        self.diff_label = ttk.Label(bottom, text="Difference: -",
                                    font=("Segoe UI", 11))
        self.diff_label.pack(side="left")

        self.record_btn = ttk.Button(bottom, text="Start Recording",
                                     command=self._toggle_recording)
        self.record_btn.pack(side="right")

        # Mid-session note - enabled only while a recording is active.
        self.add_note_btn = ttk.Button(bottom, text="Add note...", state="disabled",
                                       command=self._add_note)
        self.add_note_btn.pack(side="right", padx=4)

        self.record_status = ttk.Label(bottom, text="Not recording",
                                       foreground="gray")
        self.record_status.pack(side="right", padx=10)

    def _build_slot_panel(self, parent, slot_id: int) -> dict:
        """Build the per-slot UI: protocol picker, scan/connect, big power
        readout, plus an inline trainer-control panel that's hidden until
        the BLE worker reports the device exposes the Fitness Machine Service.
        """
        frame = ttk.LabelFrame(parent, text=f"Meter {slot_id}", padding=8)
        # Fixed width so panels lay out predictably under horizontal scroll.
        frame.configure(width=self.SLOT_PANEL_WIDTH)
        frame.pack_propagate(False)

        # Header: remove button on the right.
        header = ttk.Frame(frame)
        header.pack(fill="x")
        remove_btn = ttk.Button(header, text="Remove", width=8,
                                command=lambda s=slot_id: self._remove_meter(s))
        remove_btn.pack(side="right")

        # Protocol selector (BLE vs ANT+).
        proto_var = tk.StringVar(value="BLE")
        proto_frame = ttk.Frame(frame)
        proto_frame.pack(fill="x", pady=(4, 0))
        ttk.Label(proto_frame, text="Protocol:").pack(side="left")
        ttk.Radiobutton(proto_frame, text="BLE", variable=proto_var,
                        value="BLE",
                        command=lambda s=slot_id: self._on_protocol_changed(s)
                        ).pack(side="left")
        ttk.Radiobutton(proto_frame, text="ANT+", variable=proto_var,
                        value="ANT+",
                        command=lambda s=slot_id: self._on_protocol_changed(s)
                        ).pack(side="left")

        # Device picker. For BLE this is a dropdown of scanned MACs;
        # for ANT+ this is an entry where the user types the device ID.
        device_label_var = tk.StringVar(value="Device (BLE MAC, or pick from Scan):")
        ttk.Label(frame, textvariable=device_label_var).pack(anchor="w", pady=(8, 0))
        device_var = tk.StringVar()
        device_combo = ttk.Combobox(frame, textvariable=device_var, width=24)
        device_combo.pack(fill="x")

        # Buttons: Scan (BLE) / Connect / Disconnect.
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", pady=(8, 0))
        scan_btn = ttk.Button(btn_frame, text="Scan",
                              command=lambda s=slot_id: self._scan_ble(s))
        scan_btn.pack(side="left")
        connect_btn = ttk.Button(btn_frame, text="Connect",
                                 command=lambda s=slot_id: self._connect(s))
        connect_btn.pack(side="left", padx=4)
        disconnect_btn = ttk.Button(btn_frame, text="Disconnect", state="disabled",
                                    command=lambda s=slot_id: self._disconnect(s))
        disconnect_btn.pack(side="left")

        # Second row: calibration. Disabled until we receive a reading.
        btn_frame2 = ttk.Frame(frame)
        btn_frame2.pack(fill="x", pady=(4, 0))
        calibrate_btn = ttk.Button(btn_frame2, text="Calibrate...", state="disabled",
                                   command=lambda s=slot_id: self._open_calibration(s))
        calibrate_btn.pack(side="left")

        # Big power number.
        power_var = tk.StringVar(value="—")
        power_label = ttk.Label(frame, textvariable=power_var,
                                font=("Segoe UI", 36, "bold"),
                                foreground="#1a73e8")
        power_label.pack(pady=(15, 0))
        ttk.Label(frame, text="watts", foreground="gray").pack()

        # Cadence.
        cadence_var = tk.StringVar(value="cadence: —")
        ttk.Label(frame, textvariable=cadence_var, foreground="gray").pack(pady=(8, 0))

        # Connection status.
        status_var = tk.StringVar(value="● disconnected")
        status_label = ttk.Label(frame, textvariable=status_var, foreground="gray")
        status_label.pack(pady=(8, 0))

        # ---- Trainer control panel (hidden until FTMS detected) ----
        # Built up-front so the worker thread doesn't have to construct widgets
        # from a non-Tk thread; we just `pack` it when FTMS becomes available.
        trainer_frame = ttk.LabelFrame(frame, text="Trainer (ERG)", padding=6)
        trainer_target_var = tk.StringVar(value="")
        trainer_actual_var = tk.StringVar(value="target: —")
        ttk.Label(trainer_frame, textvariable=trainer_actual_var,
                  foreground="gray").pack(anchor="w")

        # Quick-set row: numeric entry + +/- nudges + Set/Stop.
        quick = ttk.Frame(trainer_frame)
        quick.pack(fill="x", pady=(4, 0))
        ttk.Label(quick, text="W:").pack(side="left")
        target_entry = ttk.Entry(quick, textvariable=trainer_target_var, width=6)
        target_entry.pack(side="left", padx=(2, 4))
        target_entry.bind(
            "<Return>",
            lambda _e, s=slot_id: self._trainer_set_from_entry(s),
        )
        ttk.Button(quick, text="Set", width=4,
                   command=lambda s=slot_id: self._trainer_set_from_entry(s)
                   ).pack(side="left")
        ttk.Button(quick, text="-25", width=4,
                   command=lambda s=slot_id: self._trainer_nudge(s, -25)
                   ).pack(side="left", padx=(4, 0))
        ttk.Button(quick, text="+25", width=4,
                   command=lambda s=slot_id: self._trainer_nudge(s, +25)
                   ).pack(side="left", padx=(2, 0))
        ttk.Button(quick, text="Stop", width=5,
                   command=lambda s=slot_id: self._trainer_stop(s)
                   ).pack(side="left", padx=(6, 0))

        # Plain-text workout entry. Examples (placeholder text):
        #   200w
        #   alternate between 150 and 200w every 1min
        #   ramp 150 to 250w over 10min
        #   200w 2min, 150w 30sec, repeat
        ttk.Label(trainer_frame, text="Workout (plain text):").pack(
            anchor="w", pady=(8, 0)
        )
        cmd_var = tk.StringVar(value="")
        cmd_entry = ttk.Entry(trainer_frame, textvariable=cmd_var)
        cmd_entry.pack(fill="x")
        cmd_entry.bind(
            "<Return>",
            lambda _e, s=slot_id: self._trainer_run_command(s),
        )

        cmd_btn_row = ttk.Frame(trainer_frame)
        cmd_btn_row.pack(fill="x", pady=(4, 0))
        ttk.Button(cmd_btn_row, text="Run",
                   command=lambda s=slot_id: self._trainer_run_command(s)
                   ).pack(side="left")
        ttk.Button(cmd_btn_row, text="Cancel workout",
                   command=lambda s=slot_id: self._trainer_cancel_schedule(s)
                   ).pack(side="left", padx=4)

        cmd_status_var = tk.StringVar(value="")
        cmd_status_label = ttk.Label(
            trainer_frame, textvariable=cmd_status_var,
            foreground="gray", wraplength=self.SLOT_PANEL_WIDTH - 30,
            justify="left",
        )
        cmd_status_label.pack(anchor="w", pady=(4, 0))

        return {
            "frame": frame,
            "remove_btn": remove_btn,
            "proto_var": proto_var,
            "device_var": device_var,
            "device_combo": device_combo,
            "device_label_var": device_label_var,
            "scan_btn": scan_btn,
            "connect_btn": connect_btn,
            "disconnect_btn": disconnect_btn,
            "calibrate_btn": calibrate_btn,
            "power_var": power_var,
            "cadence_var": cadence_var,
            "status_var": status_var,
            # Set when an ERROR queue item arrives, so the trailing
            # DISCONNECTED doesn't clobber the red error status.
            "had_error": False,
            "status_label": status_label,
            # Cache of (name, address) tuples from the most recent scan -
            # the combobox shows names, but we need addresses to connect.
            "scan_results": [],
            # Trainer panel widgets and state (panel itself stays unpacked
            # until the BLE worker confirms FTMS is supported).
            "trainer_frame": trainer_frame,
            "trainer_target_var": trainer_target_var,
            "trainer_actual_var": trainer_actual_var,
            "trainer_visible": False,
            "cmd_var": cmd_var,
            "cmd_status_var": cmd_status_var,
            "cmd_status_label": cmd_status_label,
        }

    # -- Protocol toggle -----------------------------------------------------

    def _on_protocol_changed(self, slot_id: int):
        """Clear the device picker when the protocol switches.

        Without this, a `Garmin Vector  [AA:BB:CC:DD:EE:FF]` label left over
        from a BLE scan stays in the entry after the user flips to ANT+, and
        Connect throws "ANT+ device ID must be a number" because that label
        clearly isn't an integer. Wiping the picker on toggle removes the
        most common path into that error.
        """
        sw = self._widgets(slot_id)
        if sw is None:
            return
        proto = sw["proto_var"].get()
        sw["device_var"].set("")
        sw["device_combo"]["values"] = []
        sw["scan_results"] = []
        if proto == "BLE":
            sw["device_label_var"].set("Device (BLE MAC, or pick from Scan):")
        else:
            sw["device_label_var"].set(
                "Device (ANT+ device ID, e.g. 12345; 0 = pair with first found):"
            )

    # -- Scanning ------------------------------------------------------------

    def _scan_ble(self, slot_id: int):
        """User clicked Scan. Run a BLE scan in the async thread."""
        if not BLEAK_AVAILABLE:
            messagebox.showerror("BLE not available",
                                 "Install bleak: pip install bleak")
            return
        sw = self._widgets(slot_id)
        if sw is None:
            return
        sw["scan_btn"].config(state="disabled", text="Scanning...")

        # Submit the coroutine to the background loop. When it completes,
        # marshal the result back to the UI thread via root.after.
        future = self.async_thread.submit(scan_ble_power_meters(duration=6.0))

        def on_done(fut):
            try:
                results = fut.result()
            except Exception as e:
                results = []
                self.root.after(0, lambda: messagebox.showerror("Scan failed", str(e)))
            self.root.after(0, lambda: self._scan_finished(slot_id, results))

        future.add_done_callback(on_done)

    def _scan_finished(self, slot_id: int, results: list):
        """Populate the dropdown with scan results."""
        sw = self._widgets(slot_id)
        if sw is None:
            return  # Slot was removed before the scan came back.
        sw["scan_btn"].config(state="normal", text="Scan")
        sw["scan_results"] = results
        # Combobox values are display strings; we look up the address by
        # matching back against scan_results when the user hits Connect.
        labels = [f"{name}  [{addr}]" for name, addr in results]
        sw["device_combo"]["values"] = labels
        if labels:
            sw["device_combo"].current(0)
        else:
            messagebox.showinfo("Scan complete", "No power meters found nearby.")

    # -- Connect / disconnect ------------------------------------------------

    def _connect(self, slot_id: int):
        sw = self._widgets(slot_id)
        slot = self._slot(slot_id)
        if sw is None or slot is None:
            return

        if slot.connected:
            return

        protocol = sw["proto_var"].get()
        device_text = sw["device_var"].get().strip()
        if not device_text:
            messagebox.showwarning("No device", "Pick or enter a device first.")
            return

        # Each worker gets its own stop_event so we can cancel one without
        # affecting the others.
        stop_event = threading.Event()
        slot.stop_event = stop_event
        slot.protocol = protocol
        # Reset calibration-related state for the new connection.
        slot.features = 0
        slot.crank_length_mm = None
        # Clear any sticky error status from the previous attempt.
        sw["had_error"] = False

        if protocol == "BLE":
            if not BLEAK_AVAILABLE:
                messagebox.showerror("BLE missing", "pip install bleak")
                return
            # Pull the address out of the combobox label, or accept a raw MAC.
            address = self._extract_ble_address(sw, device_text)
            if not address:
                messagebox.showerror("Bad address",
                                     "Couldn't parse a BLE address. Try scanning first.")
                return
            slot.address_or_id = address
            slot.name = device_text
            self.async_thread.submit(
                ble_meter_task(slot, address, self.reading_queue, stop_event)
            )
        else:  # ANT+
            if not OPENANT_AVAILABLE:
                messagebox.showerror("ANT+ missing", "pip install openant (and plug in a USB ANT+ stick)")
                return
            raw_id = self._parse_ant_device_id(device_text)
            if raw_id is None or raw_id < 0:
                messagebox.showerror(
                    "Bad ANT+ ID",
                    f"Couldn't read an ANT+ device ID from {device_text!r}.\n\n"
                    "Enter a plain integer (e.g. 12345). Use 0 to pair with the "
                    "first power meter found.\n\n"
                    "Tip: if you just switched from BLE, the field may still "
                    "hold a scan label like 'Garmin  [AA:BB:..]'. Clear it and "
                    "type the device ID printed on/in your meter's app.",
                )
                return
            # ANT+ channel device numbers are 16-bit. A handful of meters
            # (Garmin pedals in particular) print the full 32-bit hardware
            # serial - that number won't fit in the channel-ID slot, but the
            # low 16 bits are what actually pair on the air. Mask quietly
            # rather than rejecting; surface the resolved ID in the name so
            # the user can sanity-check against their head unit.
            device_id = raw_id & 0xFFFF
            slot.address_or_id = str(device_id)
            if device_id == raw_id:
                slot.name = f"ANT+ {device_id}"
            else:
                slot.name = f"ANT+ {device_id} (from {raw_id})"
                print(
                    f"[Slot {slot_id}] ANT+ ID {raw_id} > 0xFFFF; "
                    f"using low 16 bits ({device_id}) for channel match."
                )
            t = threading.Thread(
                target=antplus_meter_task,
                args=(slot, device_id, self.reading_queue, stop_event),
                daemon=True,
            )
            t.start()
            slot.worker = t

        # Pre-populate the crank length from the persisted config so the
        # dialog has a sensible default if the meter doesn't reply with one.
        remembered = self.config.get("crank_lengths", {}).get(self._meter_config_key(slot))
        if isinstance(remembered, (int, float)):
            slot.crank_length_mm = float(remembered)

        # Optimistically update UI; the worker will push a confirmation
        # (or an error) into the queue shortly.
        sw["status_var"].set("● connecting...")
        sw["status_label"].config(foreground="orange")
        sw["connect_btn"].config(state="disabled")
        sw["disconnect_btn"].config(state="normal")

    @staticmethod
    def _parse_ant_device_id(text: str) -> Optional[int]:
        """Read an ANT+ device ID out of the picker. Lenient on whitespace,
        accepts decimal or `0x..` hex. Returns None if no integer is found."""
        s = text.strip()
        if not s:
            return None
        # Accept hex form (rare but harmless: ANT+ stick logs sometimes show
        # the ID this way).
        try:
            if s.lower().startswith("0x"):
                return int(s, 16)
            return int(s)
        except ValueError:
            pass
        # Last resort: pull the first contiguous run of digits out. Catches
        # cases like "12345 (Garmin Vector)" if the user pasted a label.
        m = re.search(r"\d+", s)
        if m:
            try:
                return int(m.group(0))
            except ValueError:
                return None
        return None

    def _extract_ble_address(self, sw, text: str) -> str:
        """Find the MAC address either inside the [brackets] or matching by name."""
        # Did the user pick from the dropdown? Format is "Name  [AA:BB:...]".
        if "[" in text and "]" in text:
            return text[text.index("[") + 1 : text.index("]")]
        # Fall back to matching against the cached scan results by name.
        for name, addr in sw["scan_results"]:
            if name == text or addr == text:
                return addr
        # Last resort - assume the user typed a MAC directly.
        if ":" in text or "-" in text:
            return text
        return ""

    def _disconnect(self, slot_id: int):
        slot = self._slot(slot_id)
        if slot is None:
            return
        # Cancel any active text-driven workout schedule alongside the
        # underlying connection so the trainer doesn't keep getting commands.
        self._trainer_cancel_schedule(slot_id, silent=True)
        if slot.stop_event is not None:
            slot.stop_event.set()
        # The worker will push a DISCONNECTED message to the queue when done,
        # which is where we'll actually update the UI.

    # -- Queue draining and UI refresh --------------------------------------

    def _poll_queue(self):
        """Drain the reading queue, update slot state, refresh the UI."""
        try:
            while True:
                item = self.reading_queue.get_nowait()
                self._handle_queue_item(item)
        except queue.Empty:
            pass

        self._refresh_displays()
        self.root.after(self.POLL_INTERVAL_MS, self._poll_queue)

    def _handle_queue_item(self, item):
        # Workers push either a PowerReading or a (tag, ...) tuple for events.
        if isinstance(item, PowerReading):
            slot = self._slot(item.slot)
            sw = self._widgets(item.slot)
            if slot is None or sw is None:
                return  # Reading from a removed slot - drop silently.
            slot.latest_power = item.power_watts
            slot.latest_cadence = item.cadence_rpm
            slot.last_update = item.timestamp
            slot.connected = True
            # First reading? Allow the user to open the calibration dialog.
            if str(sw["calibrate_btn"]["state"]) == "disabled":
                sw["calibrate_btn"].config(state="normal")
            if self.recording and self.csv_writer:
                self._write_recording_row(item)
            return

        if isinstance(item, tuple):
            tag = item[0]
            if tag == "DISCONNECTED":
                slot_id = item[1]
                slot = self._slot(slot_id)
                sw = self._widgets(slot_id)
                if slot is None or sw is None:
                    return
                slot.connected = False
                sw["connect_btn"].config(state="normal")
                sw["disconnect_btn"].config(state="disabled")
                sw["calibrate_btn"].config(state="disabled")
                # Hide trainer panel and cancel any running workout: the
                # connection is gone so commands would just error out.
                self._trainer_cancel_schedule(slot_id, silent=True)
                self._hide_trainer_panel(slot_id)
                # If an ERROR preceded this DISCONNECTED, preserve the red
                # error indicator so the user can still see what went wrong
                # after dismissing the messagebox. Cleared on next connect.
                if not sw.get("had_error"):
                    sw["status_var"].set("● disconnected")
                    sw["status_label"].config(foreground="gray")
            elif tag == "ERROR":
                slot_id, msg = item[1], item[2]
                sw = self._widgets(slot_id)
                if sw is None:
                    return
                sw["had_error"] = True
                sw["status_var"].set(f"● error")
                sw["status_label"].config(foreground="red")
                # Show the error in a non-blocking way - print to console
                # and pop a dialog. (Only one dialog at a time is fine for
                # a hobbyist app; busy users can comment out the messagebox.)
                print(f"[Slot {slot_id}] {msg}")
                messagebox.showerror(f"Meter {slot_id} error", msg)
            elif tag == "FTMS_READY":
                # Worker found a Fitness Machine Service on this connection -
                # reveal the trainer-control panel inside its frame.
                slot_id = item[1]
                self._show_trainer_panel(slot_id)

    def _refresh_displays(self):
        """Update each slot's power/cadence labels and the comparison footer."""
        now = time.time()
        live_powers = []

        for slot in self.slots:
            sw = self.slot_widgets.get(slot.slot_id)
            if sw is None:
                continue
            stale = (now - slot.last_update) > self.STALE_AFTER_SEC

            if slot.connected and not stale:
                sw["power_var"].set(str(slot.latest_power))
                live_powers.append(slot.latest_power)
                cad = slot.latest_cadence
                sw["cadence_var"].set(
                    f"cadence: {cad:.0f} rpm" if cad is not None else "cadence: —"
                )
                sw["status_var"].set(f"● connected ({slot.protocol})")
                sw["status_label"].config(foreground="green")
            elif slot.connected and stale:
                # Still connected but no data recently - show 0 in a muted color.
                sw["power_var"].set("0")
                sw["status_var"].set(f"● connected, no data ({slot.protocol})")
                sw["status_label"].config(foreground="orange")
            else:
                sw["power_var"].set("—")
                sw["cadence_var"].set("cadence: —")

        # Comparison: show min, max, spread between live meters.
        if len(live_powers) >= 2:
            lo, hi = min(live_powers), max(live_powers)
            spread = hi - lo
            pct = (spread / hi * 100) if hi > 0 else 0
            self.diff_label.config(
                text=f"Spread: {spread} W  ({pct:.1f}%)   |   min {lo} / max {hi}"
            )
        else:
            self.diff_label.config(text="Spread: connect at least 2 meters to compare")

    # -- Calibration plumbing -----------------------------------------------

    def _open_calibration(self, slot_id: int):
        slot = self._slot(slot_id)
        if slot is None:
            return
        if not slot.connected:
            messagebox.showinfo("Not connected",
                                f"Connect Meter {slot_id} before calibrating.")
            return
        CalibrationDialog(self, slot_id)

    def _calibrate_set_crank_length(self, slot_id, length_mm, on_done):
        self._calibrate_dispatch(slot_id, on_done,
                                 op="set_crank_length", length_mm=length_mm)

    def _calibrate_read_crank_length(self, slot_id, on_done):
        self._calibrate_dispatch(slot_id, on_done, op="read_crank_length")

    def _calibrate_zero_offset(self, slot_id, on_done):
        self._calibrate_dispatch(slot_id, on_done, op="zero_offset")

    def _calibrate_dispatch(self, slot_id, on_done, op, length_mm=None):
        """
        Route a calibration command to the appropriate worker (BLE async loop
        or ANT+ thread) and marshal the result back to `on_done` on the Tk
        thread. Includes a guard timer so the dialog can't hang forever if
        the worker dies mid-request.
        """
        slot = self._slot(slot_id)
        if slot is None:
            on_done({"ok": False, "msg": "Slot no longer exists"})
            return
        state = {"done": False}
        timeout_ms = 20000 if op == "zero_offset" else 8000

        def complete(result):
            if state["done"]:
                return
            state["done"] = True
            # Persist a successful crank-length set so it survives restarts.
            if op == "set_crank_length" and result.get("ok") and length_mm is not None:
                key = self._meter_config_key(slot)
                self.config.setdefault("crank_lengths", {})[key] = length_mm
                self._save_config()
            on_done(result)

        def gui_complete(result):
            self.root.after(0, lambda: complete(result))

        def fire_timeout():
            complete({"ok": False, "msg": "Timed out waiting for response"})

        self.root.after(timeout_ms, fire_timeout)

        if slot.protocol == "BLE":
            if self.async_thread is None or slot.ble_client is None:
                complete({"ok": False, "msg": "BLE not connected"})
                return
            if op == "set_crank_length":
                coro = ble_set_crank_length(slot, length_mm)
            elif op == "read_crank_length":
                coro = ble_read_crank_length(slot)
            else:
                coro = ble_zero_offset(slot)
            future = self.async_thread.submit(coro)

            def on_ble_done(fut):
                try:
                    rv = fut.result()
                except Exception as e:
                    gui_complete({"ok": False, "msg": str(e)})
                    return
                if op == "read_crank_length":
                    rc, length = rv
                    gui_complete({
                        "ok": rc == CP_RESULT_SUCCESS and length is not None,
                        "msg": CP_RESULT_NAMES.get(rc, f"Code {rc:#x}"),
                        "length_mm": length,
                    })
                elif op == "zero_offset":
                    rc, offset = rv
                    gui_complete({
                        "ok": rc == CP_RESULT_SUCCESS,
                        "msg": CP_RESULT_NAMES.get(rc, f"Code {rc:#x}"),
                        "offset": offset,
                    })
                else:
                    rc, _ = rv
                    gui_complete({
                        "ok": rc == CP_RESULT_SUCCESS,
                        "msg": CP_RESULT_NAMES.get(rc, f"Code {rc:#x}"),
                        "length_mm": length_mm if rc == CP_RESULT_SUCCESS else None,
                    })

            future.add_done_callback(on_ble_done)
        else:  # ANT+
            if slot.ant_command_queue is None or slot.ant_device is None:
                complete({"ok": False, "msg": "ANT+ worker not running"})
                return
            if op == "set_crank_length":
                slot.ant_command_queue.put(
                    lambda: antplus_set_crank_length(slot, length_mm, gui_complete)
                )
            elif op == "read_crank_length":
                slot.ant_command_queue.put(
                    lambda: antplus_read_crank_length(slot, gui_complete)
                )
            else:
                slot.ant_command_queue.put(
                    lambda: antplus_zero_offset(slot, gui_complete)
                )

    # -- Adding / removing meters -------------------------------------------

    def _add_meter(self):
        """Append a new empty slot to the right end of the panel row."""
        slot_id = self._next_slot_id
        self._next_slot_id += 1
        slot = MeterSlot(slot_id=slot_id)
        self.slots.append(slot)
        sw = self._build_slot_panel(self.slots_inner, slot_id)
        sw["frame"].pack(side="left", fill="y", padx=5, pady=2)
        self.slot_widgets[slot_id] = sw
        # Force the canvas to recalc scrollregion for the new panel.
        self.slots_inner.update_idletasks()
        self.slots_canvas.configure(scrollregion=self.slots_canvas.bbox("all"))
        # Scroll to the right edge so the new slot is visible.
        self.slots_canvas.xview_moveto(1.0)

    def _remove_meter(self, slot_id: int):
        """Remove a slot. Refuses while a connection is live - the user
        should disconnect first so the worker can shut down cleanly."""
        slot = self._slot(slot_id)
        sw = self._widgets(slot_id)
        if slot is None or sw is None:
            return
        if slot.connected or slot.stop_event is not None:
            messagebox.showinfo(
                "Disconnect first",
                f"Meter {slot_id} is still connected. Hit Disconnect first.",
            )
            return
        self._trainer_cancel_schedule(slot_id, silent=True)
        sw["frame"].destroy()
        self.slots = [s for s in self.slots if s.slot_id != slot_id]
        self.slot_widgets.pop(slot_id, None)
        self.slots_inner.update_idletasks()
        self.slots_canvas.configure(scrollregion=self.slots_canvas.bbox("all"))

    # -- Trainer panel show/hide --------------------------------------------

    def _show_trainer_panel(self, slot_id: int):
        sw = self._widgets(slot_id)
        slot = self._slot(slot_id)
        if sw is None or slot is None:
            return
        if not sw["trainer_visible"]:
            sw["trainer_frame"].pack(fill="x", pady=(8, 0))
            sw["trainer_visible"] = True
        # Show the supported power range as the panel's subtitle.
        if slot.ftms_power_min is not None and slot.ftms_power_max is not None:
            sw["trainer_actual_var"].set(
                f"target: — (range {slot.ftms_power_min}-{slot.ftms_power_max} W)"
            )
        else:
            sw["trainer_actual_var"].set("target: —")
        sw["cmd_status_var"].set("")

    def _hide_trainer_panel(self, slot_id: int):
        sw = self._widgets(slot_id)
        if sw is None:
            return
        if sw["trainer_visible"]:
            sw["trainer_frame"].forget()
            sw["trainer_visible"] = False
        sw["trainer_target_var"].set("")
        sw["trainer_actual_var"].set("target: —")
        sw["cmd_status_var"].set("")

    # -- Trainer single-shot commands ---------------------------------------

    def _trainer_set_from_entry(self, slot_id: int):
        sw = self._widgets(slot_id)
        if sw is None:
            return
        try:
            watts = int(float(sw["trainer_target_var"].get()))
        except ValueError:
            sw["cmd_status_var"].set("Enter a number for watts.")
            return
        # Setting from the quick-set entry stops any active workout - it's
        # what the user just chose, so it's the new ground truth.
        self._trainer_cancel_schedule(slot_id, silent=True)
        self._trainer_set_target(slot_id, watts, source="manual")

    def _trainer_nudge(self, slot_id: int, delta: int):
        slot = self._slot(slot_id)
        sw = self._widgets(slot_id)
        if slot is None or sw is None:
            return
        # Base the nudge on the last commanded target if we have one,
        # otherwise off the current entry value, otherwise off live power.
        base = slot.ftms_target_power_w
        if base is None:
            try:
                base = int(float(sw["trainer_target_var"].get()))
            except (ValueError, TypeError):
                base = slot.latest_power
        new_w = max(0, base + delta)
        self._trainer_cancel_schedule(slot_id, silent=True)
        self._trainer_set_target(slot_id, new_w, source="manual")

    def _trainer_stop(self, slot_id: int):
        self._trainer_cancel_schedule(slot_id, silent=True)
        slot = self._slot(slot_id)
        if slot is None or not slot.ftms_available or self.async_thread is None:
            return
        future = self.async_thread.submit(ftms_stop(slot))

        def on_done(fut):
            try:
                rc = fut.result()
            except Exception as e:
                self.root.after(0,
                    lambda: self._trainer_status(slot_id, f"Stop failed: {e}", "red"))
                return
            self.root.after(0, lambda: self._on_trainer_response(slot_id, rc, "Stopped"))

        future.add_done_callback(on_done)

    def _trainer_set_target(self, slot_id: int, watts: int, source: str = "manual"):
        """Send Set Target Power. `source` controls the status line wording
        (so the user can tell apart manual entries from workout steps)."""
        slot = self._slot(slot_id)
        sw = self._widgets(slot_id)
        if slot is None or sw is None:
            return
        if not slot.ftms_available or slot.ble_client is None or self.async_thread is None:
            self._trainer_status(slot_id, "Trainer not connected.", "red")
            return
        if slot.ftms_power_min is not None and watts < slot.ftms_power_min and watts > 0:
            watts = max(watts, slot.ftms_power_min)
        if slot.ftms_power_max is not None and watts > slot.ftms_power_max:
            watts = slot.ftms_power_max
        sw["trainer_target_var"].set(str(watts))
        sw["trainer_actual_var"].set(f"target: {watts} W")
        future = self.async_thread.submit(ftms_set_target_power(slot, watts))

        def on_done(fut):
            try:
                rc = fut.result()
            except Exception as e:
                self.root.after(0,
                    lambda: self._trainer_status(slot_id, f"Send failed: {e}", "red"))
                return
            label = ("Set" if source == "manual" else "Workout") + f" -> {watts} W"
            self.root.after(0, lambda: self._on_trainer_response(slot_id, rc, label))

        future.add_done_callback(on_done)

    def _on_trainer_response(self, slot_id: int, rc: int, label: str):
        if rc == FTMS_RESULT_SUCCESS:
            self._trainer_status(slot_id, f"OK: {label}", "green")
            return
        # If the trainer says control isn't permitted, try to grab control
        # once and replay nothing - the user can hit Set again. Some trainers
        # silently lose control after a long idle.
        if rc == FTMS_RESULT_CONTROL_NOT_PERMITTED:
            slot = self._slot(slot_id)
            if slot is not None and self.async_thread is not None:
                self.async_thread.submit(ftms_request_control(slot))
            self._trainer_status(
                slot_id,
                f"{label}: control was not permitted - re-requested. Try again.",
                "orange",
            )
            return
        msg = FTMS_RESULT_NAMES.get(rc, f"code {rc:#x}")
        self._trainer_status(slot_id, f"{label}: {msg}", "red")

    def _trainer_status(self, slot_id: int, text: str, color: str = "gray"):
        sw = self._widgets(slot_id)
        if sw is None:
            return
        sw["cmd_status_var"].set(text)
        sw["cmd_status_label"].configure(foreground=color)

    # -- Plain-text workout parser + scheduler ------------------------------

    def _trainer_run_command(self, slot_id: int):
        """Parse and execute the text in the slot's command entry."""
        sw = self._widgets(slot_id)
        slot = self._slot(slot_id)
        if sw is None or slot is None:
            return
        text = sw["cmd_var"].get().strip()
        if not text:
            return
        if not slot.ftms_available:
            self._trainer_status(slot_id, "Trainer not connected.", "red")
            return
        try:
            program = parse_trainer_command(text)
        except WorkoutParseError as e:
            self._trainer_status(slot_id, f"Parse error: {e}", "red")
            return
        # Cancel anything previously running, then either set once or run a
        # step program. Always show what we understood so the user can sanity-
        # check that "alternate between 150 and 200w every 1min" parsed right.
        self._trainer_cancel_schedule(slot_id, silent=True)
        if program["type"] == "stop":
            self._trainer_stop(slot_id)
            self._trainer_status(slot_id, "Stopped (user).", "gray")
            return
        if program["type"] == "set":
            self._trainer_set_target(slot_id, program["watts"], source="manual")
            return
        if program["type"] == "nudge":
            self._trainer_nudge(slot_id, program["delta"])
            return
        # Multi-step: store the program and start at step 0.
        self._schedules[slot_id] = {
            "program": program,
            "step_idx": 0,
            "after_id": None,
            "summary": program["summary"],
            "step_started": time.time(),
        }
        self._trainer_status(
            slot_id, f"Workout: {program['summary']}", "blue",
        )
        self._trainer_run_step(slot_id)

    def _trainer_run_step(self, slot_id: int):
        """Execute the current step of the slot's workout, then schedule
        the next one with `root.after`. Re-entrant via after()."""
        sched = self._schedules.get(slot_id)
        slot = self._slot(slot_id)
        if sched is None or slot is None:
            return
        if not slot.ftms_available:
            # Trainer disappeared between steps. Bail and tell the user.
            self._trainer_cancel_schedule(slot_id, silent=False)
            return
        steps = sched["program"]["steps"]
        idx = sched["step_idx"]
        # Programs marked `repeat=True` loop back to step 0 forever; otherwise
        # we stop after running through the list once.
        if idx >= len(steps):
            if sched["program"].get("repeat"):
                idx = 0
                sched["step_idx"] = 0
            else:
                self._trainer_status(slot_id, "Workout complete.", "green")
                self._schedules.pop(slot_id, None)
                return
        watts, duration_s = steps[idx]
        sched["step_started"] = time.time()
        self._trainer_set_target(slot_id, watts, source="workout")
        sw = self._widgets(slot_id)
        if sw is not None:
            sw["cmd_status_var"].set(
                f"Workout step {idx + 1}/{len(steps)}: {watts} W "
                f"for {_format_duration(duration_s)}"
            )
        # Schedule the next step. `after_id` lets us cancel mid-step.
        sched["step_idx"] = idx + 1
        sched["after_id"] = self.root.after(
            int(duration_s * 1000),
            lambda: self._trainer_run_step(slot_id),
        )

    def _trainer_cancel_schedule(self, slot_id: int, silent: bool = False):
        """Cancel any active text-driven workout for this slot. Doesn't stop
        ERG mode itself - the trainer keeps holding the last commanded target
        until the user presses Stop or sets a new one."""
        sched = self._schedules.pop(slot_id, None)
        if sched is None:
            return
        after_id = sched.get("after_id")
        if after_id is not None:
            try:
                self.root.after_cancel(after_id)
            except Exception:
                pass
        if not silent:
            self._trainer_status(slot_id, "Workout cancelled.", "gray")

    @staticmethod
    def _meter_config_key(slot: MeterSlot) -> str:
        return f"{slot.protocol}:{slot.address_or_id}"

    def _load_config(self) -> dict:
        try:
            if CONFIG_PATH.exists():
                return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
        return {}

    def _save_config(self):
        try:
            CONFIG_PATH.write_text(json.dumps(self.config, indent=2),
                                   encoding="utf-8")
        except Exception:
            pass

    # -- Recording -----------------------------------------------------------

    def _toggle_recording(self):
        if not self.recording:
            self._start_recording()
        else:
            self._stop_recording()

    def _start_recording(self):
        # Modal setup dialog: collect name, notes, and the file path together
        # so the metadata travels with the CSV.
        dialog = RecordingSetupDialog(self)
        self.root.wait_window(dialog)
        info = dialog.result
        if info is None:
            return  # User cancelled.

        try:
            self.recording_file = open(info["path"], "w", newline="", encoding="utf-8")
        except OSError as e:
            messagebox.showerror("Can't open file", str(e))
            return

        # Metadata header as comment lines. Tools like pandas can skip these
        # via `comment='#'`; eyeballing the file at the top stays readable.
        started = datetime.now().isoformat(timespec="seconds")
        self.recording_file.write(f"# session_name: {info['name']}\n")
        self.recording_file.write(f"# started: {started}\n")
        if info["notes"]:
            self.recording_file.write("# notes:\n")
            for line in info["notes"].splitlines():
                self.recording_file.write(f"#   {line}\n")
        self.recording_file.flush()

        self.csv_writer = csv.writer(self.recording_file)
        # Header row. elapsed_s is seconds since recording started, which is
        # the column that's most useful for plotting later.
        self.csv_writer.writerow([
            "timestamp_iso", "elapsed_s", "slot", "protocol",
            "name", "power_w", "cadence_rpm",
        ])
        self.recording_start_time = time.time()
        self.recording = True
        self.session_name = info["name"]
        self.record_btn.config(text="Stop Recording")
        self.add_note_btn.config(state="normal")
        self.record_status.config(
            text=f"Recording '{info['name']}' -> {info['path']}",
            foreground="red",
        )

    def _add_note(self):
        """Append a timestamped note to the active CSV mid-session."""
        if not self.recording or self.recording_file is None:
            return
        note = simpledialog.askstring(
            "Add note",
            "Note (saved with current elapsed time):",
            parent=self.root,
        )
        if not note:
            return
        elapsed = time.time() - (self.recording_start_time or time.time())
        # Written as a CSV-comment line so the data columns stay clean.
        # Newlines in the note get folded so each note remains one line.
        flat = note.replace("\r", " ").replace("\n", " | ")
        self.recording_file.write(f"# note @ {elapsed:.1f}s: {flat}\n")
        self.recording_file.flush()

    def _write_recording_row(self, reading: PowerReading):
        slot = self._slot(reading.slot)
        if slot is None:
            return
        elapsed = reading.timestamp - (self.recording_start_time or reading.timestamp)
        self.csv_writer.writerow([
            datetime.fromtimestamp(reading.timestamp).isoformat(timespec="milliseconds"),
            f"{elapsed:.3f}",
            reading.slot,
            slot.protocol,
            slot.name,
            reading.power_watts,
            f"{reading.cadence_rpm:.1f}" if reading.cadence_rpm is not None else "",
        ])
        # Flush every row so a crash doesn't lose your ride.
        self.recording_file.flush()

    def _stop_recording(self):
        self.recording = False
        if self.recording_file:
            ended = datetime.now().isoformat(timespec="seconds")
            self.recording_file.write(f"# ended: {ended}\n")
            self.recording_file.close()
            self.recording_file = None
        self.csv_writer = None
        self.session_name = None
        self.record_btn.config(text="Start Recording")
        self.add_note_btn.config(state="disabled")
        self.record_status.config(text="Not recording", foreground="gray")

    # -- Shutdown ------------------------------------------------------------

    def _on_close(self):
        # Tell every worker to stop, give them a beat, then exit.
        for slot in self.slots:
            if slot.stop_event is not None:
                slot.stop_event.set()
        if self.recording:
            self._stop_recording()
        # Don't wait too long - daemon threads will die with the process anyway.
        time.sleep(0.3)
        if self.async_thread is not None:
            self.async_thread.stop()
        self.root.destroy()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    root = tk.Tk()
    # ttk's "vista" theme on Windows is the cleanest-looking default.
    try:
        ttk.Style().theme_use("vista")
    except tk.TclError:
        pass  # Not on Windows or theme unavailable - default is fine.
    PowerMeterApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
