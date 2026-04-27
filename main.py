import os
import json
import gzip
import shutil
import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, Request, HTTPException
from pythonjsonlogger import jsonlogger
import logging

# -------------------------
# Config
# -------------------------

DATA_DIR = os.getenv("DATA_DIR", "/home/eswar/Traverse_model_data/")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "10000"))

QUEUE_KEY = "weather_queue"

os.makedirs(DATA_DIR, exist_ok=True)

app = FastAPI()
r = redis.from_url(REDIS_URL, decode_responses=True)

# -------------------------
# Logging (JSON)
# -------------------------

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# -------------------------
# Validation
# -------------------------

REQUIRED_FIELDS = {
    "device_uuid",
    "platform",
    "vehicle_type",
    "started_at",
    "ended_at",
    "accelerometer",
    "gyroscope",
    "gps",
}


def validate_payload(payload: Dict[str, Any]):
    missing = REQUIRED_FIELDS - payload.keys()
    if missing:
        raise HTTPException(400, f"Missing fields: {missing}")

    if not payload["gps"]:
        raise HTTPException(400, "gps empty")


def make_trip_id(payload):
    safe_time = payload["started_at"].replace(":", "_")
    return f"{payload['device_uuid']}_{safe_time}"


def atomic_write(path: str, data: Dict[str, Any]):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


# -------------------------
# Weather batching
# -------------------------

async def fetch_weather_batch(keys):
    results = {}
    async with httpx.AsyncClient(timeout=10.0) as client:
        for lat, lng, hour in keys:
            try:
                response = await client.get(
                    'https://archive-api.open-meteo.com/v1/archive',
                    params=[
                        ('latitude', lat),
                        ('longitude', lng),
                        ('start_date', hour[:10]),
                        ('end_date', hour[:10]),
                        ('hourly', 'temperature_2m'),
                        ('hourly', 'rain'),
                        ('hourly', 'windspeed_10m'),
                        ('hourly', 'weathercode'),
                        ('timezone', 'auto')
                    ]
                )
                data = response.json()
                hour_index = int(hour[11:13])  # extract hour from "2026-04-27T11:00"

                results[(lat, lng, hour)] = {
                    'temp_celsius': data['hourly']['temperature_2m'][hour_index],
                    'rain_mm': data['hourly']['rain'][hour_index],
                    'wind_kmh': data['hourly']['windspeed_10m'][hour_index],
                    'weathercode': data['hourly']['weathercode'][hour_index],
                }

            except Exception as e:
                results[(lat, lng, hour)] = {'error': 'weather_failed'}

    return results


# -------------------------
# Worker
# -------------------------

async def weather_worker():
    while True:
        batch = []

        # block until at least one item
        item = await r.brpop(QUEUE_KEY)
        batch.append(item[1])

        # drain more (non-blocking)
        for _ in range(49):  # batch size = 50
            extra = await r.rpop(QUEUE_KEY)
            if not extra:
                break
            batch.append(extra)

        jobs = []
        weather_keys = set()

        for path in batch:
            if not os.path.exists(path):
                continue

            with open(path) as f:
                payload = json.load(f)

            first = payload["gps"][0]
            dt = datetime.fromisoformat(payload["started_at"])
            hour = dt.strftime("%Y-%m-%dT%H:00")

            key = (first["lat"], first["lng"], hour)

            jobs.append((path, payload, key))
            weather_keys.add(key)

        weather_data = await fetch_weather_batch(list(weather_keys))

        for path, payload, key in jobs:
            try:
                payload["weather"] = weather_data.get(key, {})

                done_path = path.replace(".json", ".done.json")
                atomic_write(done_path, payload)

                os.remove(path)

                logger.info({
                    "event": "weather_enriched",
                    "file": done_path
                })

            except Exception as e:
                logger.error({
                    "event": "worker_error",
                    "error": str(e)
                })


# -------------------------
# Rotation
# -------------------------

def rotate_and_compress():
    now = datetime.utcnow()

    for fname in os.listdir(DATA_DIR):
        if not fname.endswith(".done.json"):
            continue

        path = os.path.join(DATA_DIR, fname)
        mtime = datetime.utcfromtimestamp(os.path.getmtime(path))

        if now - mtime > timedelta(hours=1):
            gz_path = path + ".gz"

            if os.path.exists(gz_path):
                continue

            with open(path, "rb") as f_in:
                with gzip.open(gz_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            os.remove(path)

            logger.info({
                "event": "file_compressed",
                "file": gz_path
            })


async def rotation_worker():
    while True:
        rotate_and_compress()
        await asyncio.sleep(300)


# -------------------------
# API
# -------------------------

@app.on_event("startup")
async def startup():
    asyncio.create_task(weather_worker())
    asyncio.create_task(rotation_worker())


@app.get("/ping")
async def ping():
    return {"status": "ok"}


@app.post("/sync")
async def sync(request: Request):
    try:
        raw = await request.body()

        try:
            decompressed = gzip.decompress(raw)
            payload = json.loads(decompressed)
        except Exception:
            raise HTTPException(400, "Invalid gzip/json")

        validate_payload(payload)

        trip_id = make_trip_id(payload)
        base = os.path.join(DATA_DIR, trip_id)

        raw_path = base + ".json"
        done_path = base + ".done.json"
        gz_path = done_path + ".gz"

        # Dedup
        if os.path.exists(done_path) or os.path.exists(gz_path):
            return {"status": "duplicate_ignored"}

        if os.path.exists(raw_path):
            return {"status": "already_received"}

        # Queue protection
        qsize = await r.llen(QUEUE_KEY)
        if qsize > MAX_QUEUE_SIZE:
            logger.warning({"event": "queue_full", "size": qsize})
            raise HTTPException(503, "Queue overloaded")

        # Save
        atomic_write(raw_path, payload)

        # Enqueue
        await r.lpush(QUEUE_KEY, raw_path)

        logger.info({
            "event": "trip_received",
            "trip_id": trip_id,
            "queue_size": qsize
        })

        return {"status": "accepted"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error({"event": "sync_error", "error": str(e)})
        raise HTTPException(400, str(e))