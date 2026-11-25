"""
Network-connected IoT Device Simulator
Software-only demonstration of MQTT publishing, reconnect handling,
and persistent offline buffering. No physical sensor is used.
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

BROKER = "test.mosquitto.org"
PORT = 1883
QUEUE_FILE = Path(__file__).with_name("offline_queue.jsonl")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("iot-device")


class OfflineQueue:
    """A small disk-backed queue so readings survive disconnection/restarts."""

    def __init__(self, path: Path):
        self.path = path
        self.path.touch(exist_ok=True)

    def add(self, reading: dict) -> None:
        with self.path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(reading) + "\n")
        log.warning("OFFLINE BUFFER: saved reading locally (queue: %d)",
                    self.size())

    def peek_all(self) -> list[dict]:
        items = []
        with self.path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line:
                    items.append(json.loads(line))
        return items

    def remove_oldest(self) -> None:
        items = self.peek_all()
        if items:
            remaining = items[1:]
            temp_path = self.path.with_suffix(".tmp")
            with temp_path.open("w", encoding="utf-8") as file:
                for item in remaining:
                    file.write(json.dumps(item) + "\n")
            temp_path.replace(self.path)

    def size(self) -> int:
        return len(self.peek_all())


def make_reading() -> dict:
    """Generate realistic-looking simulated sensor data."""
    return {
        "device_id": "python-iot-simulator",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_c": round(random.uniform(22.0, 32.0), 2),
        "humidity_percent": round(random.uniform(35.0, 75.0), 2),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Simulate an MQTT sensor with reconnect and offline buffering."
    )
    parser.add_argument("--count", type=int, default=20,
                        help="Number of readings to generate (default: 20).")
    parser.add_argument("--interval", type=float, default=2.0,
                        help="Seconds between readings (default: 2).")
    parser.add_argument("--offline-demo", action="store_true",
                        help="Simulate a 12-second publishing outage at startup.")
    parser.add_argument("--offline-seconds", type=float, default=12.0,
                        help="Duration of simulated outage (default: 12 seconds).")
    parser.add_argument("--broker", default=BROKER,
                        help=f"MQTT broker hostname (default: {BROKER}).")
    parser.add_argument("--port", type=int, default=PORT,
                        help=f"MQTT broker port (default: {PORT}).")
    args = parser.parse_args()

    if args.count < 1 or args.interval <= 0:
        parser.error("--count must be at least 1 and --interval must be positive")

    queue = OfflineQueue(QUEUE_FILE)
    device_tag = uuid.uuid4().hex[:8]
    topic = f"upstartinterns/iot-demo/{device_tag}/readings"
    client_id = f"iot-simulator-{device_tag}"
    connected = False

    def on_connect(client, userdata, flags, reason_code, properties):
        nonlocal connected
        if reason_code == 0:
            connected = True
            log.info("CONNECTED to MQTT broker %s:%s", args.broker, args.port)
        else:
            connected = False
            log.error("MQTT connection rejected: %s", reason_code)

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
        nonlocal connected
        connected = False
        log.warning("DISCONNECTED from MQTT broker (reason: %s). Reconnect will be attempted.",
                    reason_code)

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    log.info("Starting software sensor simulator (no physical hardware).")
    log.info("MQTT topic: %s", topic)
    log.info("Offline queue file: %s", QUEUE_FILE.name)

    # Initial connection attempts. Once loop_start() is running, Paho
    # automatically retries if the connection later drops.
    delay = 1
    while True:
        try:
            client.connect(args.broker, args.port, keepalive=60)
            break
        except OSError as exc:
            log.warning("Initial connection failed: %s. Retrying in %s second(s).",
                        exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)

    client.loop_start()
    start = time.monotonic()
    outage_ends = start + args.offline_seconds if args.offline_demo else start
    if args.offline_demo:
        log.warning("DEMO OUTAGE: readings will be buffered for %.1f seconds.",
                    args.offline_seconds)

    def publish_pending():
        """Send queued readings in order; remove each only after QoS 1 acknowledgement."""
        while connected and queue.size() > 0:
            reading = queue.peek_all()[0]
            try:
                info = client.publish(topic, json.dumps(reading), qos=1)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    log.warning("Publish could not be queued (code %s).", info.rc)
                    return
                info.wait_for_publish(timeout=5)
                if not info.is_published():
                    log.warning("No publish acknowledgement yet; keeping reading buffered.")
                    return
                queue.remove_oldest()
                log.info("PUBLISHED (QoS 1): %s | remaining buffered: %d",
                         json.dumps(reading), queue.size())
            except (RuntimeError, ValueError) as exc:
                log.warning("Publish failed; keeping reading buffered: %s", exc)
                return

    try:
        for number in range(1, args.count + 1):
            reading = make_reading()
            log.info("READING %d/%d: temperature=%.2f C, humidity=%.2f%%",
                     number, args.count,
                     reading["temperature_c"], reading["humidity_percent"])

            # Always persist first. This protects readings if the connection
            # drops between generating a reading and publishing it.
            queue.add(reading)

            demo_is_offline = args.offline_demo and time.monotonic() < outage_ends
            if demo_is_offline:
                log.warning("SIMULATED OFFLINE: waiting for connectivity to return.")
            elif connected:
                if args.offline_demo and time.monotonic() >= outage_ends:
                    log.info("SIMULATED CONNECTION RESTORED: flushing buffered readings.")
                publish_pending()
            else:
                log.warning("MQTT unavailable: reading remains safely buffered.")

            if number < args.count:
                time.sleep(args.interval)

        # Give the client a final chance to send any remaining buffered data.
        if connected and not (args.offline_demo and time.monotonic() < outage_ends):
            publish_pending()

        log.info("Finished. Readings still buffered on disk: %d", queue.size())
        log.info("Use the MQTT topic printed above to subscribe and view messages.")
    except KeyboardInterrupt:
        log.info("Stopped by user. Any unsent readings remain in the offline queue.")
    finally:
        client.disconnect()
        client.loop_stop()


if __name__ == "__main__":
    main()
