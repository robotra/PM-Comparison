# Multi Power Meter

A desktop app to connect to an **arbitrary number of cycling power meters at once** over Bluetooth Low Energy (BLE) and/or ANT+, see live power side-by-side, log to CSV, run calibration (zero offset and crank length), and **drive a smart trainer in ERG mode** — all without a head unit.

Built for hobbyist comparison — A/B-ing two meters, validating drift, sanity-checking a freshly-installed crankset, or running a structured workout against a smart trainer.

## Features

- **Arbitrary number of meters** in any mix of BLE and ANT+, scrollable horizontally; add and remove slots on the fly
- **Live readouts** of power (W) and cadence (rpm) at ~10 Hz UI refresh
- **Side-by-side spread** between connected meters (`max - min`, percentage)
- **CSV recording** with millisecond timestamps and elapsed-seconds column for easy plotting
- **Smart trainer control (FTMS)** — when a connected device exposes the Fitness Machine Service, an inline panel appears in its slot with quick `Set` / `±25 W` / `Stop` buttons and a plain-text command box (see [Trainer commands](#trainer-commands))
- **Calibration dialog**:
  - Read / set crank length (BLE Cycling Power Control Point, ANT+ Page 0x02 sub 0x01)
  - Run zero-offset compensation with raw-offset readback (color-coded: green ≤50, orange ≤100, red >100)
  - Persists last-used crank length per meter to `~/.power_meter_app.json`
- **Cancellable BLE connect** — Disconnect button works during a slow connect attempt
- **Graceful failure** — meters that don't expose calibration features have those buttons greyed out

## Requirements

- Python 3.10+
- `bleak` (BLE)
- `openant` (ANT+, optional)
- For ANT+: a USB ANT+ stick (Garmin, Suunto, CycPlus) and on Windows the Zadig driver swap
- For ANT+ on Windows: the `libusb` pip package (ships the DLL openant needs)

```bash
pip install bleak openant libusb
python power_meter_app.py
```

The app auto-registers the libusb DLL bundled in the `libusb` pip package on Windows (so you won't see *"ANT+ driver not found"* / `NoBackendError`), but it can't bypass the Zadig driver swap — the OS-level driver still has to be WinUSB before openant can talk to the stick.

The app launches even if one of the libraries is missing — the status bar at the top tells you which protocols are available.

## Platform support

| Platform | BLE | ANT+ | Notes |
|----------|-----|------|-------|
| Windows 10/11 (x64) | ✓ | ✓ | Needs Zadig for ANT+ stick |
| Windows on ARM | ✓ | ⚠ | libusb arm64 is finicky |
| macOS (Intel & Apple Silicon) | ✓ | ✓ | `brew install libusb` for ANT+ |
| Linux (incl. Raspberry Pi 64-bit) | ✓ | ✓ | `apt install bluez libusb-1.0-0 python3-tk` |

## Usage

1. **Pick a protocol** for each slot (BLE or ANT+).
2. **BLE**: click *Scan*, pick the meter from the dropdown, click *Connect*.
   **ANT+**: type the device ID (or `0` to pair with the first found), click *Connect*.
3. Live power appears within a second or two. The *Calibrate...* button enables once the first reading arrives.
4. Click *Start Recording* to log to a CSV file (one row per reading per meter).

### Calibration

Open *Calibrate...* on a connected slot. Two operations:

- **Crank length** — read the meter's current value, type a new one (or pick from 165 / 170 / 172.5 / 175 mm), click *Apply*. The new value is saved and pre-filled the next time you connect that meter.
- **Zero offset** — stop pedaling, lift the rear wheel, click *Calibrate (Zero offset)*. The displayed raw offset gives a sense of strain-gauge drift over time.

For BLE, calibration uses the standard Cycling Power Control Point characteristic. For ANT+, it uses the standard Bicycle Power profile pages. ANT+ behavior varies by pedal; if calibration silently times out, your `openant` version may not expose the generic page hook this app uses.

> **Garmin Vector / Rally pedals (and similar dual-protocol meters):** these often expose calibration *only* over ANT+, and respond to BLE calibration requests with `Operation not supported`. If a BLE calibration is rejected, switch the slot's protocol to ANT+ and try again.

## Trainer commands

When a slot connects to a device that supports the BLE Fitness Machine Service (KICKR, Neo, Saris H3, Tacx, JetBlack, Wattbike Atom, etc.), a *Trainer (ERG)* panel appears inside its frame. The plain-text command box accepts ad-hoc workout phrases — type and hit Enter (or *Run*).

### Three example workouts

```
ramp from 100 to 200w over 10min
```
A **10-minute warmup ramp**: target power slides linearly from 100 W to 200 W. Internally the ramp is sliced into ~5-second steps and each step is sent as a Set Target Power command.

```
alternate between 220 and 130w every 5min
```
**Sweet-spot intervals**: 5 minutes at 220 W, 5 minutes at 130 W, repeating until you hit *Stop* or *Cancel workout*. Useful for unbounded interval sets where you decide afterwards how long the session lasted.

```
100w for 5min, 200w for 20min, 100w for 5min
```
**Stepped workout** (warmup + 20-min work + cooldown). A finite step program — runs once and reports "Workout complete." when finished. Append `, repeat` to make any sequence loop.

### Parsing guidelines

The parser is case-insensitive, whitespace-tolerant, and supports the following forms:

| You type | What it does |
|----------|--------------|
| `200w`, `200`, `200 watts` | Hold target at 200 W |
| `+25`, `-50` | Adjust the current target by ±N W |
| `stop`, `off`, `pause` | Drop ERG mode (rider can freewheel) |
| `alternate between A and B[w] every <duration>` | Two-step loop, fixed duration each side |
| `ramp [from] A to B[w] (over\|in) <duration>` | Linear power ramp in 5-second steps |
| `Aw for <dur>, Bw for <dur>[, ...]` | Step program; separators: `,` or ` then ` |
| `... , repeat` (suffix) | Make a step program loop forever |

**Watts** — the trailing `w` / `watts` is optional everywhere. Any bare integer in a watts position is read as watts.

**Durations** — use `s`/`sec`/`seconds`, `m`/`min`/`minutes`, or `h`/`hr`/`hours`. Composite forms work: `1h30m`, `2m30s`. A bare number with no unit is read as seconds.

**Bounds** — the trainer's advertised power range (when present) is used to clamp targets, so `9999w` on a trainer that maxes at 2000 W will be sent as 2000 W. The active step is shown in the trainer panel's status line so you can see what's running.

**Stopping** — *Cancel workout* stops the schedule but leaves the trainer holding the last commanded target. *Stop* drops ERG entirely. Disconnecting cancels both automatically.

**What's not supported (yet)** — repeat counts (e.g. `4x...`), simulation/grade commands, FTP-relative targets (`80%FTP`). They're parser features, not trainer-protocol limits, so additions are mostly local to `parse_trainer_command` in [power_meter_app.py](power_meter_app.py).

## CSV format

Wide format — one row per tick (1 Hz by default), one power+cadence column pair per slot. Slot identity is captured in a comment header so the bare `s1`/`s2`/... column names map back to physical meters:

```
# session_name: Sat AM ride
# started: 2026-04-27T10:15:31
# tick_hz: 1
# columns:
#   s1 = BLE Stages LR (AA:BB:CC:DD:EE:FF)
#   s2 = ANT+ 12345 (12345)
timestamp_iso,elapsed_s,s1_power_w,s1_cadence_rpm,s2_power_w,s2_cadence_rpm
2026-04-27T10:15:32.412,1.000,212,84.0,215,84.5
2026-04-27T10:15:33.412,2.000,213,84.0,216,84.5
...
```

`elapsed_s` is seconds since you clicked *Start Recording*. The file is flushed every tick, so a crash loses at most one second. Cells go blank when a slot's data is stale (no reading in the last 3 s) instead of forward-filling stale values, so gaps in your ride are visible. Slots added mid-recording don't get a column — column layout is fixed at *Start Recording* time.

## Architecture

```
Meter -> protocol handler (BLE/ANT+) -> thread-safe Queue -> Tkinter GUI
```

- One asyncio event loop runs in a background thread, owned by `AsyncLoopThread`. All BLE work (scan, connect, calibration) is submitted there.
- ANT+ runs in its own thread per slot; the main worker loop services a per-slot command queue (calibration commands push callables into it).
- The GUI polls the reading queue every 100 ms — workers never touch Tk widgets directly.

See the docstring at the top of [power_meter_app.py](power_meter_app.py) for the longer version.

## Building a standalone executable

```bash
pip install pyinstaller
pyinstaller --onefile --windowed --name "MultiPowerMeter" power_meter_app.py
```

If `openant` doesn't get bundled correctly, add `--collect-all openant`.

`--onefile` produces a slow-to-start single executable; drop it for a faster-launching folder build under `dist/MultiPowerMeter/`.

## Known limitations

- **No power balance** — left/right balance from dual-sided meters is not parsed or shown.
- **Cadence on BLE** — the spec carries cumulative crank revolutions and an event timestamp rather than rpm directly, so the app derives rpm by diffing successive notifications. The first notification after connect therefore lands without cadence (no previous sample to diff against), and very low cadences (~< 30 rpm) update slowly. Meters that don't set the crank-revolution-data flag give no cadence at all over BLE.
- **Multiple ANT+ meters on one stick** — the worker opens its own Node per slot, so multiple ANT+ slots want multiple sticks. Mixing one ANT+ slot with several BLE slots on a single stick works fine.
- **ANT+ calibration is best-effort** — page formats follow the spec, but pedal implementations vary. Verify with your head unit before trusting an in-app result.

## Troubleshooting

- **BLE scan finds nothing** — Bluetooth is off, or the meter isn't advertising (some meters only advertise after a wake-up pedal stroke).
- **ANT+ "Access denied (insufficient permissions)" / `USBError [Errno 13]`** — the stick is enumerated but the Windows driver bound to it isn't WinUSB, so libusb can't claim the interface. See the [Zadig walkthrough](#zadig-walkthrough-windows-ant-driver-swap) below.
- **ANT+ "no device found"** — same root cause as above, or the stick isn't plugged in. Run the Zadig walkthrough.
- **Calibration says "Operation not permitted"** — the meter is asleep. Pedal once and retry.
- **Calibration says "Operation not supported"** — the meter doesn't expose that command on this protocol. Garmin Vector/Rally and similar dual-protocol pedals usually expose calibration only on ANT+; switch the slot to ANT+ and retry. Otherwise fall back to Garmin Connect / your head unit.
- **Trainer panel never appears** — the device you connected doesn't expose the BLE Fitness Machine Service (`0x1826`). Some smart trainers expose FTMS only when not paired to another app — quit Zwift/TrainerRoad and reconnect.

## Zadig walkthrough (Windows ANT+ driver swap)

`USBError [Errno 13] Access denied` means Windows enumerated the ANT+ stick but bound a non-libusb-compatible driver to it (typically the Dynastream/Garmin one). libusb can claim the interface only if the driver is **WinUSB**.

1. Unplug the ANT+ stick. Close anything that grabs it: **Garmin Express**, **ANT Agent**, Zwift, TrainerRoad, etc. Without this, Zadig sees the stick as "in use" and Replace Driver fails halfway through.
2. Download Zadig from <https://zadig.akeo.ie/> and right-click → **Run as administrator**. This matters — without admin, the replace step exits with an access-denied error of its own.
3. Plug the stick back in.
4. In Zadig: **Options → List All Devices** (otherwise the stick won't be in the dropdown).
5. From the dropdown, select the ANT+ device by USB ID:
   - `0FCF 1008` — Garmin USB ANT Stick (most common)
   - `0FCF 1009` — ANTUSB-m
   - `0FCF 1004` — older ANTUSB
   Don't pick anything with a different vendor ID (`0FCF` is Dynastream/Garmin).
6. The line under the dropdown shows `Driver: <current> → <target>`. Use the up/down arrows next to the *target* to choose **WinUSB** (NOT libusbK or libusb-win32 — openant's pyusb backend looks for WinUSB specifically).
7. Click **Replace Driver**. The button label may say *Install Driver* if no current driver is bound. Either is fine. Wait ~30s.
8. Close Zadig. Restart this app and connect the ANT+ slot.

To undo (e.g. you want to use the stick with Garmin Express again): in Device Manager, right-click the ANT USB-m → *Uninstall device*, then unplug & replug — Windows will reinstall the original Dynastream driver.
- **Buttons frozen during connect** — fixed. If you still see this, you're running an older revision; pull the latest [power_meter_app.py](power_meter_app.py).
