import json
import os
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv(
    "MQTT_TOPIC",
    "application/9e793c11-67d5-428c-a21d-2949e55a0646/device/+/event/up",
)

PG_HOST = os.getenv("PG_HOST", "host.docker.internal")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB = os.getenv("PG_DB", "sirma")
PG_USER = os.getenv("PG_USER", "sirma")
PG_PASS = os.getenv("PG_PASS", "")

# Si quieres SOLO GPS del dispositivo, pon STRICT_DEVICE_GPS=true
STRICT_DEVICE_GPS = os.getenv("STRICT_DEVICE_GPS", "false").lower() == "true"


def parse_ts(ts_str: str) -> datetime:
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def extract_device_gps(payload: dict):
    obj = payload.get("object") or {}
    gps = obj.get("gpsLocation")
    if isinstance(gps, dict) and len(gps) > 0:
        k = next(iter(gps.keys()))
        loc = gps.get(k) or {}
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        alt = loc.get("altitude")
        if lat is not None and lon is not None:
            return float(lat), float(lon), (float(alt) if alt is not None else None)
    return None, None, None


def extract_gateway_gps(payload: dict):
    rx = payload.get("rxInfo") or []
    if isinstance(rx, list) and len(rx) > 0:
        loc = (rx[0] or {}).get("location") or {}
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        if lat is not None and lon is not None:
            return float(lat), float(lon), None
    return None, None, None


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


UPSERT_LAST = """
INSERT INTO ultima_telemetria (
  dispositivo_id, soldado_id, dev_eui,
  ts, latitud, longitud, altitud
) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dispositivo_id) DO UPDATE SET
  soldado_id = EXCLUDED.soldado_id,
  dev_eui = EXCLUDED.dev_eui,
  ts = EXCLUDED.ts,
  latitud = EXCLUDED.latitud,
  longitud = EXCLUDED.longitud,
  altitud = EXCLUDED.altitud
"""

INSERT_HIST = """
INSERT INTO telemetria (
  dispositivo_id, soldado_id, ts,
  latitud, longitud, altitud,
  frames
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Connected rc={rc}. Subscribing: {MQTT_TOPIC}")
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        print(f"[WARN] Not JSON on {msg.topic}: {e}")
        return

    dev_eui = ((payload.get("deviceInfo") or {}).get("devEui")) or payload.get("devEui")
    if not dev_eui:
        print("[WARN] Missing devEui")
        return

    ts_str = payload.get("time")
    if not ts_str:
        print("[WARN] Missing time")
        return
    ts = parse_ts(ts_str)

    # 1) GPS del dispositivo
    lat, lon, alt = extract_device_gps(payload)
    source = "device"

    # 2) fallback al gateway si no hay GPS del dispositivo
    if lat is None or lon is None:
        if STRICT_DEVICE_GPS:
            print(f"[SKIP] No device GPS devEui={dev_eui} fCnt={payload.get('fCnt')}")
            return
        lat, lon, alt = extract_gateway_gps(payload)
        source = "gateway"

    if lat is None or lon is None:
        print(f"[SKIP] No location devEui={dev_eui} fCnt={payload.get('fCnt')}")
        return

    frames = payload.get("fCnt")

    conn = None
    try:
        conn = get_conn()
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT id, soldado_id FROM nodos WHERE dev_eui=%s", (dev_eui,))
            row = cur.fetchone()
            if not row:
                print(f"[WARN] devEui={dev_eui} no existe en nodos. (Créalo primero)")
                conn.rollback()
                return

            dispositivo_id = int(row["id"])
            soldado_id = row["soldado_id"]

            cur.execute(INSERT_HIST, (dispositivo_id, soldado_id, ts, lat, lon, alt, frames))
            cur.execute(UPSERT_LAST, (dispositivo_id, soldado_id, dev_eui, ts, lat, lon, alt))

        conn.commit()
        print(f"[OK] devEui={dev_eui} id={dispositivo_id} lat={lat} lon={lon} src={source}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[ERROR] DB: {e}")
    finally:
        if conn:
            conn.close()


def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()
