"""
Multi Power Meter App
=====================
Connect to up to 3 cycling power meters simultaneously over Bluetooth Low
Energy (BLE) and/or ANT+. Displays live power, logs to CSV, and shows a
side-by-side comparison.

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

# Feature flag bits in the Cycling Power Feature characteristic.
CP_FEATURE_OFFSET_COMPENSATION = 1 << 3
CP_FEATURE_CRANK_LENGTH_ADJUSTMENT = 1 << 4

# Persisted state (last-used crank length per meter, etc.) lives here.
CONFIG_PATH = Path.home() / ".power_meter_app.json"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PowerReading:
    """A single power reading from a meter, tagged with which slot it came from."""
    slot: int           # 1, 2, or 3 - which UI slot/meter this belongs to
    timestamp: float    # Unix time when we received the reading
    power_watts: int    # Instantaneous power in watts
    cadence_rpm: Optional[float] = None  # Optional, not all meters provide it


@dataclass
class MeterSlot:
    """Holds the runtime state of one of the three meter slots."""
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
                reading_queue.put(
                    ("ERROR", slot.slot_id, f"ANT+ command error: {e}")
                )

    except Exception as e:
        reading_queue.put(("ERROR", slot.slot_id, f"ANT+ error: {e}"))
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
        self.slot = app.slots[slot_id - 1]
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
            return " - meter does not advertise this operation."
        return ""

    def _set_status(self, text: str, color: str = "gray"):
        self.status_var.set(text)
        self.status_label.config(foreground=color)


# ---------------------------------------------------------------------------
# Main GUI application
# ---------------------------------------------------------------------------

class PowerMeterApp:
    """The Tkinter GUI. Owns the slots, the recording state, and the queue."""

    NUM_SLOTS = 3
    POLL_INTERVAL_MS = 100   # How often we drain the queue and refresh UI
    STALE_AFTER_SEC = 3.0    # If no reading for this long, show power as 0

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Multi Power Meter")
        self.root.geometry("780x520")

        # Thread-safe queue: workers push readings, GUI pops them.
        self.reading_queue: queue.Queue = queue.Queue()

        # Background asyncio loop for BLE.
        self.async_thread = AsyncLoopThread() if BLEAK_AVAILABLE else None

        # State for each of the three slots.
        self.slots = [MeterSlot(slot_id=i + 1) for i in range(self.NUM_SLOTS)]

        # Recording state.
        self.recording = False
        self.recording_file = None
        self.csv_writer = None
        self.recording_start_time: Optional[float] = None
        self.session_name: Optional[str] = None

        # Persisted state (last-used crank length per meter, etc.)
        self.config = self._load_config()

        self._build_ui()

        # Kick off the periodic queue-draining loop.
        self.root.after(self.POLL_INTERVAL_MS, self._poll_queue)

        # Make sure we shut down workers cleanly on window close.
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # -- UI construction -----------------------------------------------------

    def _build_ui(self):
        # Top status bar showing what's installed.
        status = []
        status.append("BLE: " + ("ready" if BLEAK_AVAILABLE else "MISSING (pip install bleak)"))
        status.append("ANT+: " + ("ready" if OPENANT_AVAILABLE else "MISSING (pip install openant)"))
        ttk.Label(self.root, text="  |  ".join(status), foreground="gray").pack(pady=(8, 0))

        # Container for the three slot panels.
        slots_frame = ttk.Frame(self.root)
        slots_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.slot_widgets = []
        for i in range(self.NUM_SLOTS):
            sw = self._build_slot_panel(slots_frame, i + 1)
            sw["frame"].pack(side="left", fill="both", expand=True, padx=5)
            self.slot_widgets.append(sw)

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
        """Build the per-slot UI: protocol picker, scan/connect, big power readout."""
        frame = ttk.LabelFrame(parent, text=f"Meter {slot_id}", padding=8)

        # Protocol selector (BLE vs ANT+).
        proto_var = tk.StringVar(value="BLE")
        proto_frame = ttk.Frame(frame)
        proto_frame.pack(fill="x")
        ttk.Label(proto_frame, text="Protocol:").pack(side="left")
        ttk.Radiobutton(proto_frame, text="BLE", variable=proto_var,
                        value="BLE").pack(side="left")
        ttk.Radiobutton(proto_frame, text="ANT+", variable=proto_var,
                        value="ANT+").pack(side="left")

        # Device picker. For BLE this is a dropdown of scanned MACs;
        # for ANT+ this is an entry where the user types the device ID.
        ttk.Label(frame, text="Device:").pack(anchor="w", pady=(8, 0))
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

        return {
            "frame": frame,
            "proto_var": proto_var,
            "device_var": device_var,
            "device_combo": device_combo,
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
        }

    # -- Scanning ------------------------------------------------------------

    def _scan_ble(self, slot_id: int):
        """User clicked Scan. Run a BLE scan in the async thread."""
        if not BLEAK_AVAILABLE:
            messagebox.showerror("BLE not available",
                                 "Install bleak: pip install bleak")
            return
        sw = self.slot_widgets[slot_id - 1]
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
        sw = self.slot_widgets[slot_id - 1]
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
        sw = self.slot_widgets[slot_id - 1]
        slot = self.slots[slot_id - 1]

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
            try:
                device_id = int(device_text)
            except ValueError:
                messagebox.showerror("Bad ANT+ ID",
                                     "ANT+ device ID must be a number (0 to pair with first found).")
                return
            slot.address_or_id = str(device_id)
            slot.name = f"ANT+ {device_id}"
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
        slot = self.slots[slot_id - 1]
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
            slot = self.slots[item.slot - 1]
            slot.latest_power = item.power_watts
            slot.latest_cadence = item.cadence_rpm
            slot.last_update = item.timestamp
            slot.connected = True
            sw = self.slot_widgets[item.slot - 1]
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
                self.slots[slot_id - 1].connected = False
                sw = self.slot_widgets[slot_id - 1]
                sw["connect_btn"].config(state="normal")
                sw["disconnect_btn"].config(state="disabled")
                sw["calibrate_btn"].config(state="disabled")
                # If an ERROR preceded this DISCONNECTED, preserve the red
                # error indicator so the user can still see what went wrong
                # after dismissing the messagebox. Cleared on next connect.
                if not sw.get("had_error"):
                    sw["status_var"].set("● disconnected")
                    sw["status_label"].config(foreground="gray")
            elif tag == "ERROR":
                slot_id, msg = item[1], item[2]
                sw = self.slot_widgets[slot_id - 1]
                sw["had_error"] = True
                sw["status_var"].set(f"● error")
                sw["status_label"].config(foreground="red")
                # Show the error in a non-blocking way - print to console
                # and pop a dialog. (Only one dialog at a time is fine for
                # a hobbyist app; busy users can comment out the messagebox.)
                print(f"[Slot {slot_id}] {msg}")
                messagebox.showerror(f"Meter {slot_id} error", msg)

    def _refresh_displays(self):
        """Update each slot's power/cadence labels and the comparison footer."""
        now = time.time()
        live_powers = []

        for i, slot in enumerate(self.slots):
            sw = self.slot_widgets[i]
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
        slot = self.slots[slot_id - 1]
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
        slot = self.slots[slot_id - 1]
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
        slot = self.slots[reading.slot - 1]
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
