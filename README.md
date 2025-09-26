# LYWSD03MMC Passive Receiver

This repository contains a single Python script, `lywsd03mmc_passive_receiver.py`,
which listens for BLE advertisements emitted by Xiaomi LYWSD03MMC sensors
running the ATC firmware. The script stores every received measurement in a
local SQLite database and exposes a small HTTP API to query the collected data.

## Requirements

- Linux with BlueZ and a Bluetooth adapter that supports BLE scanning.
- Python 3.8 or newer.
- The [`bluepy`](https://github.com/IanHarvey/bluepy) package (only `bluetooth._bluetooth` is required).

Running the script usually requires elevated privileges. To grant the Python
interpreter the required capabilities without using `sudo`, execute:

```bash
sudo setcap cap_net_raw,cap_net_admin+eip $(command -v python3)
```

## Usage

```bash
python3 lywsd03mmc_passive_receiver.py --interface 0 --port 8000
```

Arguments:

- `--interface`: HCI adapter index (default: `0`).
- `--database`: Path to the SQLite database file (default: `lywsd03mmc.db`).
- `--host`: Address for the HTTP server (default: `0.0.0.0`).
- `--port`: Port for the HTTP server (default: `8000`).
- `--log-level`: Logging verbosity (default: `INFO`).

## HTTP API

All responses are JSON encoded.

- `GET /health` – simple health probe returning `{ "status": "ok" }`.
- `GET /devices` – latest measurement for every discovered device.
- `GET /devices/<MAC>?limit=50&since=<timestamp>` – measurement history for the
  given device. `limit` defaults to 50 (maximum 1000) and `since` can be used to
  filter results newer than the supplied Unix timestamp.
- `GET /devices/<MAC>/range?range=24h` – measurement history restricted to the
  provided relative time range (e.g. `24h`, `48h`, `1w`, `1m`). The `range`
  parameter also accepts the alias `period`, and the optional `limit` parameter
  defaults to 1000 results with the same maximum cap.

## Data Storage

Measurements are persisted in the configured SQLite database with the following
columns:

- `mac`
- `temperature`
- `humidity`
- `voltage`
- `battery`
- `rssi`
- `timestamp`

Each new advertisement is stored as a separate record.
