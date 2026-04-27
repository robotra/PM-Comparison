# Multi Power Meter

A desktop app to connect up to **3 cycling power meters at once** over Bluetooth Low Energy (BLE) and/or ANT+, see live power side-by-side, log to CSV, and run calibration (zero offset and crank length) without needing a head unit.

Built for hobbyist comparison — A/B-ing two meters, validating drift, or sanity-checking a freshly-installed crankset.

## Features

- **3 simultaneous meters** in any mix of BLE and ANT+
- **Live readouts** of power (W) and cadence (rpm) at ~10 Hz UI refresh
- **Side-by-side spread** between connected meters (`max - min`, percentage)
- **CSV recording** with millisecond timestamps and elapsed-seconds column for easy plotting
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

```bash
pip install bleak openant
python power_meter_app.py
```

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

## CSV format

```
timestamp_iso,elapsed_s,slot,protocol,name,power_w,cadence_rpm
2026-04-27T10:15:32.412,0.000,1,BLE,Stages LR,212,84.0
2026-04-27T10:15:32.512,0.100,2,ANT+ 12345,ANT+ 12345,215,84.5
...
```

`elapsed_s` is seconds since you clicked *Start Recording*. The file is flushed on every row, so a crash won't lose your ride.

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
- **No cadence over BLE** — the spec carries it but extracting it requires tracking crank revolutions across notifications. ANT+ provides cadence directly and that one *is* shown.
- **Multiple ANT+ meters on one stick** — the worker opens its own Node per slot, so multiple ANT+ slots want multiple sticks. Mixing one ANT+ + two BLE on a single stick works fine.
- **ANT+ calibration is best-effort** — page formats follow the spec, but pedal implementations vary. Verify with your head unit before trusting an in-app result.

## Troubleshooting

- **BLE scan finds nothing** — Bluetooth is off, or the meter isn't advertising (some meters only advertise after a wake-up pedal stroke).
- **ANT+ "no device found"** — wrong driver. On Windows, run Zadig and replace the Garmin/Dynastream USB driver with WinUSB.
- **Calibration says "Operation not permitted"** — the meter is asleep. Pedal once and retry.
- **Calibration says "Operation not supported"** — the meter genuinely doesn't expose this command. Use Garmin Connect or your head unit instead.
- **Buttons frozen during connect** — fixed. If you still see this, you're running an older revision; pull the latest [power_meter_app.py](power_meter_app.py).
