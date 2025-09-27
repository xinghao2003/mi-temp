#!/usr/bin/env python3
"""Passive receiver and local API for LYWSD03MMC sensors running ATC firmware.

The script listens for BLE advertisements in ATC format, stores every
measurement in a local SQLite database and exposes a small HTTP API to query
those readings.
"""
from __future__ import annotations

import argparse
import array
import errno
import fcntl
import json
import logging
import select
import signal
import socket
import sqlite3
import struct
import threading
import time
from dataclasses import dataclass
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import bluetooth._bluetooth as bluez  # type: ignore

# ---------------------------------------------------------------------------
# BLE / HCI helpers (adapted from bluetooth_utils.py)
# ---------------------------------------------------------------------------

LE_META_EVENT = 0x3E
EVT_LE_ADVERTISING_REPORT = 0x02
OGF_LE_CTL = 0x08
OCF_LE_SET_SCAN_PARAMETERS = 0x000B
OCF_LE_SET_SCAN_ENABLE = 0x000C
SCAN_TYPE_PASSIVE = 0x00
SCAN_ENABLE = 0x01
SCAN_DISABLE = 0x00
FILTER_POLICY_NO_WHITELIST = 0x00


def raw_packet_to_str(pkt: Iterable[int]) -> str:
    """Return the hexadecimal representation of a raw HCI packet."""
    return "".join(f"{b:02x}" for b in pkt)


def toggle_device(dev_id: int, enable: bool) -> None:
    """Enable or disable the given bluetooth adapter."""
    hci_sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_RAW, socket.BTPROTO_HCI)
    try:
        request = array.array("b", struct.pack("H", dev_id))
        ioctl_cmd = bluez.HCIDEVUP if enable else bluez.HCIDEVDOWN
        try:
            fcntl.ioctl(hci_sock.fileno(), ioctl_cmd, request[0])
        except OSError as err:
            if err.errno != errno.EALREADY:
                raise
    finally:
        hci_sock.close()


def enable_le_scan(
    sock: socket.socket,
    interval: int = 0x0800,
    window: int = 0x0800,
    filter_policy: int = FILTER_POLICY_NO_WHITELIST,
    filter_duplicates: bool = True,
) -> None:
    """Enable passive BLE scanning on the provided socket."""
    cmd_pkt = struct.pack("<BHHBBB", SCAN_TYPE_PASSIVE, interval, window, filter_policy, 0x00, 0x00)
    bluez.hci_send_cmd(sock, OGF_LE_CTL, OCF_LE_SET_SCAN_PARAMETERS, cmd_pkt)
    cmd_pkt = struct.pack("<BB", SCAN_ENABLE, 0x01 if filter_duplicates else 0x00)
    bluez.hci_send_cmd(sock, OGF_LE_CTL, OCF_LE_SET_SCAN_ENABLE, cmd_pkt)


def disable_le_scan(sock: socket.socket) -> None:
    """Disable BLE scanning."""
    cmd_pkt = struct.pack("<BB", SCAN_DISABLE, 0x00)
    bluez.hci_send_cmd(sock, OGF_LE_CTL, OCF_LE_SET_SCAN_ENABLE, cmd_pkt)


def parse_le_advertising_events(
    sock: socket.socket,
    handler: Callable[[str, int, bytes, int], None],
    stop_event: Optional[threading.Event] = None,
    debug: bool = False,
) -> None:
    """Continuously parse LE advertising events and invoke *handler* for each."""
    if handler is None:
        raise ValueError("handler must be provided")

    old_filter = sock.getsockopt(bluez.SOL_HCI, bluez.HCI_FILTER, 14)
    hci_filter = bluez.hci_filter_new()
    bluez.hci_filter_set_ptype(hci_filter, bluez.HCI_EVENT_PKT)
    bluez.hci_filter_set_event(hci_filter, LE_META_EVENT)
    sock.setsockopt(bluez.SOL_HCI, bluez.HCI_FILTER, hci_filter)

    try:
        while True:
            if stop_event and stop_event.is_set():
                break
            try:
                rlist, _, _ = select.select([sock], [], [], 1.0)
            except (OSError, ValueError):
                break
            if not rlist:
                continue
            try:
                full_pkt = sock.recv(255)
            except OSError:
                break
            if not full_pkt:
                continue
            ptype, event, plen = struct.unpack("BBB", full_pkt[:3])
            if event != LE_META_EVENT:
                if debug:
                    logging.debug("Unexpected HCI event %s", event)
                continue
            sub_event, = struct.unpack("B", full_pkt[3:4])
            if sub_event != EVT_LE_ADVERTISING_REPORT:
                if debug:
                    logging.debug("Unexpected LE subevent %s", sub_event)
                continue
            pkt = full_pkt[4:]
            adv_type = struct.unpack("b", pkt[1:2])[0]
            mac = bluez.ba2str(pkt[3:9])
            data = pkt[9:-1]
            rssi = struct.unpack("b", full_pkt[-1:])[0]
            if debug:
                logging.debug(
                    "BLE advertisement: mac=%s adv_type=%02x data=%s rssi=%d",
                    mac,
                    adv_type,
                    raw_packet_to_str(data),
                    rssi,
                )
            handler(mac, adv_type, data, rssi)
    finally:
        sock.setsockopt(bluez.SOL_HCI, bluez.HCI_FILTER, old_filter)


# ---------------------------------------------------------------------------
# Domain model & persistence
# ---------------------------------------------------------------------------


@dataclass
class Measurement:
    mac: str
    temperature: float
    humidity: float
    voltage: Optional[float]
    battery: Optional[int]
    rssi: int
    timestamp: int


class DataStore:
    """Thread-safe wrapper around a SQLite database."""

    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        self._create_schema()

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS measurements (
                    mac TEXT NOT NULL,
                    temperature REAL NOT NULL,
                    humidity REAL NOT NULL,
                    voltage REAL,
                    battery INTEGER,
                    rssi INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_measurements_mac_ts ON measurements(mac, timestamp DESC)"
            )
            self._conn.commit()

    def save(self, measurement: Measurement) -> None:
        logging.debug("Persisting measurement: %s", measurement)
        with self._lock:
            self._conn.execute(
                "INSERT INTO measurements (mac, temperature, humidity, voltage, battery, rssi, timestamp)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    measurement.mac,
                    measurement.temperature,
                    measurement.humidity,
                    measurement.voltage,
                    measurement.battery,
                    measurement.rssi,
                    measurement.timestamp,
                ),
            )
            self._conn.commit()

    def get_latest(self) -> List[Dict[str, object]]:
        query = (
            "SELECT m.mac, m.temperature, m.humidity, m.voltage, m.battery, m.rssi, m.timestamp "
            "FROM measurements m "
            "JOIN (SELECT mac, MAX(timestamp) AS ts FROM measurements GROUP BY mac) latest "
            "  ON latest.mac = m.mac AND latest.ts = m.timestamp "
            "ORDER BY m.mac"
        )
        with self._lock:
            rows = list(self._conn.execute(query))
        return [self._row_to_dict(row) for row in rows]

    def get_history(self, mac: str, limit: Optional[int], since: Optional[int]) -> List[Dict[str, object]]:
        params: List[object] = [mac]
        query = (
            "SELECT mac, temperature, humidity, voltage, battery, rssi, timestamp "
            "FROM measurements WHERE mac = ?"
        )
        if since is not None:
            query += " AND timestamp >= ?"
            params.append(since)
        query += " ORDER BY timestamp DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with self._lock:
            rows = list(self._conn.execute(query, params))
        return [self._row_to_dict(row) for row in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @staticmethod
    def _row_to_dict(row: Tuple[object, ...]) -> Dict[str, object]:
        keys = ("mac", "temperature", "humidity", "voltage", "battery", "rssi", "timestamp")
        return {key: row[idx] for idx, key in enumerate(keys)}


# ---------------------------------------------------------------------------
# BLE advertisement decoder
# ---------------------------------------------------------------------------


class ATCDecoder:
    """Decode BLE advertisements emitted by ATC firmware."""

    _PREAMBLE = "161a18"

    def __init__(self) -> None:
        self._last_adv: Dict[str, str] = {}

    def decode(self, mac: str, payload: str, rssi: int) -> Optional[Measurement]:
        mac = mac.upper()
        idx = payload.find(self._PREAMBLE)
        if idx == -1:
            return None
        offset = idx + len(self._PREAMBLE)
        data = payload[offset:]
        if len(data) not in (26, 30):
            logging.debug("Unsupported ATC payload length for %s: %d", mac, len(data))
            return None

        adv_number = data[-4:-2] if len(data) == 30 else data[-2:]
        if self._last_adv.get(mac) == adv_number:
            return None
        self._last_adv[mac] = adv_number

        if len(data) == 26:  # ATC1441 format
            temperature = int.from_bytes(bytearray.fromhex(data[12:16]), byteorder="big", signed=True) / 10.0
            humidity = float(int(data[16:18], 16))
            battery = int(data[18:20], 16)
            voltage = int(data[20:24], 16) / 1000.0
        else:  # len(data) == 30, custom format
            temperature = int.from_bytes(bytearray.fromhex(data[12:16]), byteorder="little", signed=True) / 100.0
            humidity = int.from_bytes(bytearray.fromhex(data[16:20]), byteorder="little", signed=False) / 100.0
            voltage = int.from_bytes(bytearray.fromhex(data[20:24]), byteorder="little", signed=False) / 1000.0
            battery = int.from_bytes(bytearray.fromhex(data[24:26]), byteorder="little", signed=False)

        measurement = Measurement(
            mac=mac,
            temperature=round(temperature, 2),
            humidity=round(humidity, 2),
            voltage=round(voltage, 3),
            battery=battery,
            rssi=rssi,
            timestamp=int(time.time()),
        )
        logging.debug("Decoded measurement: %s", measurement)
        return measurement


# ---------------------------------------------------------------------------
# BLE scanner thread
# ---------------------------------------------------------------------------


class PassiveScanner(threading.Thread):
    """Background thread that continuously listens for ATC broadcasts."""

    def __init__(self, interface: int, store: DataStore, decoder: ATCDecoder) -> None:
        super().__init__(daemon=True)
        self._interface = interface
        self._store = store
        self._decoder = decoder
        self._stop_event = threading.Event()
        self._sock: Optional[socket.socket] = None

    def run(self) -> None:
        logging.info("Starting BLE passive scanner on hci%s", self._interface)
        toggle_device(self._interface, True)
        self._sock = bluez.hci_open_dev(self._interface)
        enable_le_scan(self._sock, filter_duplicates=False)

        def handler(mac: str, adv_type: int, data: bytes, rssi: int) -> None:
            packet = raw_packet_to_str(data)
            measurement = self._decoder.decode(mac, packet, rssi)
            if measurement:
                self._store.save(measurement)

        try:
            parse_le_advertising_events(self._sock, handler=handler, stop_event=self._stop_event)
        except Exception:
            logging.exception("BLE scanning stopped due to an unexpected error")
        finally:
            logging.info("Stopping BLE passive scanner")
            try:
                disable_le_scan(self._sock)
            except Exception:
                pass
            if self._sock is not None:
                try:
                    self._sock.close()
                except Exception:
                    pass

    def stop(self) -> None:
        self._stop_event.set()
        if self._sock is not None:
            try:
                disable_le_scan(self._sock)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------


class SensorAPIHandler(BaseHTTPRequestHandler):
    """HTTP API that exposes stored measurements."""

    server_version = "ATCReceiver/1.0"

    def __init__(self, store: DataStore, *args, **kwargs) -> None:
        self._store = store
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:  # noqa: N802 (HTTP verb name)
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._json_response({"status": "ok"})
            return
        if parsed.path == "/devices":
            self._json_response(self._store.get_latest())
            return
        if parsed.path.startswith("/devices/"):
            parts = [p for p in parsed.path.split("/") if p]
            if not parts or parts[0].lower() != "devices":
                self._json_response({"error": "Not found"}, status=404)
                return

            if len(parts) == 2:
                mac = parts[1].upper()
                query = parse_qs(parsed.query)
                limit_param = query.get("limit", ["50"])[0]
                limit_value = self._parse_int(limit_param, default=50)
                limit = max(1, min(limit_value, 1000))
                since_value = query.get("since", [None])[0]
                since = self._parse_int(since_value, default=None) if since_value else None
                history = self._store.get_history(mac, limit, since)
                if history:
                    self._json_response(history)
                else:
                    self._json_response({"error": "No data for device"}, status=404)
                return

            if len(parts) == 3 and parts[2].lower() == "range":
                mac = parts[1].upper()
                query = parse_qs(parsed.query)
                range_value = query.get("range", [None])[0] or query.get("period", [None])[0]
                seconds = self._parse_range(range_value) if range_value else None
                if seconds is None:
                    self._json_response({"error": "Invalid range value"}, status=400)
                    return
                limit_param = query.get("limit", [None])[0]
                limit: Optional[int]
                if limit_param is None:
                    limit = 1000
                else:
                    normalized_limit = limit_param.strip().lower()
                    if normalized_limit == "all":
                        limit = None
                    else:
                        parsed_limit = self._parse_int(limit_param, default=None)
                        if parsed_limit is None:
                            limit = 1000
                        else:
                            if parsed_limit <= 0:
                                limit = 1
                            elif parsed_limit > 1000:
                                limit = None
                            else:
                                limit = parsed_limit
                since = int(time.time()) - seconds
                history = self._store.get_history(mac, limit, since)
                if history:
                    self._json_response(history)
                else:
                    self._json_response({"error": "No data for device"}, status=404)
                return

            self._json_response({"error": "Not found"}, status=404)
            return
        self._json_response({"error": "Not found"}, status=404)

    def log_message(self, fmt: str, *args) -> None:  # noqa: D401 - match BaseHTTPRequestHandler
        logging.info("HTTP %s - %s", self.client_address[0], fmt % args)

    def _json_response(self, payload: object, status: int = 200) -> None:
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    @staticmethod
    def _parse_int(value: str, default: Optional[int]) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_range(value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        normalized = value.strip().lower()
        aliases = {
            "24h": 24 * 3600,
            "48h": 48 * 3600,
            "1d": 24 * 3600,
            "2d": 2 * 24 * 3600,
            "3d": 3 * 24 * 3600,
            "7d": 7 * 24 * 3600,
            "1w": 7 * 24 * 3600,
            "1week": 7 * 24 * 3600,
            "2w": 14 * 24 * 3600,
            "1m": 30 * 24 * 3600,
            "1month": 30 * 24 * 3600,
        }
        if normalized in aliases:
            return aliases[normalized]
        try:
            number = int(normalized[:-1])
            suffix = normalized[-1]
        except (ValueError, IndexError):
            return None
        seconds_per_unit = {
            "h": 3600,
            "d": 24 * 3600,
            "w": 7 * 24 * 3600,
            "m": 30 * 24 * 3600,
        }
        unit_seconds = seconds_per_unit.get(suffix)
        if unit_seconds is None or number <= 0:
            return None
        return number * unit_seconds


# ---------------------------------------------------------------------------
# Application entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interface", type=int, default=0, help="HCI interface number (default: 0)")
    parser.add_argument("--database", default="lywsd03mmc.db", help="Path to the SQLite database file")
    parser.add_argument("--host", default="0.0.0.0", help="Host/IP address for the HTTP API")
    parser.add_argument("--port", type=int, default=8000, help="Port for the HTTP API (default: 8000)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")

    store = DataStore(args.database)
    decoder = ATCDecoder()
    scanner = PassiveScanner(args.interface, store, decoder)
    scanner.start()

    handler_factory = partial(SensorAPIHandler, store)
    ThreadingHTTPServer.allow_reuse_address = True
    server = ThreadingHTTPServer((args.host, args.port), handler_factory)
    server.daemon_threads = True
    logging.info("HTTP API listening on http://%s:%d", args.host, args.port)

    shutdown_initiated = threading.Event()

    def shutdown(signum: int, _frame) -> None:
        if shutdown_initiated.is_set():
            return
        shutdown_initiated.set()
        logging.info("Received signal %s, shutting down", signum)
        threading.Thread(target=server.shutdown, name="HTTPServerShutdown", daemon=True).start()
        scanner.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        server.serve_forever()
    finally:
        logging.info("Stopping services")
        scanner.stop()
        scanner.join(timeout=5)
        store.close()
        server.server_close()


if __name__ == "__main__":
    main()
