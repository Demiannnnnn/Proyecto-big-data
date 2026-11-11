"""Recolector: lee trades en vivo desde un WebSocket público (Binance) y los guarda en MongoDB.

Flujo:  WebSocket -> parse_trade() -> buffer en memoria -> insert_many() por lotes -> colección time-series.
Se configura con variables de entorno (ver docker-compose.yml).
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import websockets
from pymongo import MongoClient
from pymongo.errors import PyMongoError

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DB_NAME = os.getenv("DB_NAME", "crypto")
COLLECTION = os.getenv("COLLECTION", "trades")
SYMBOLS = [s.strip().lower() for s in os.getenv("SYMBOLS", "btcusdt,ethusdt,bnbusdt,solusdt,xrpusdt").split(",")]
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))        # inserta cuando el buffer llega a este tamaño...
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "1"))   # ...o cuando pasan estos segundos
WS_URL = os.getenv(
    "WS_URL",
    "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{s}@trade" for s in SYMBOLS),
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")


def parse_trade(raw):
    """Convierte un mensaje del WebSocket en un documento para MongoDB (o None si no es un trade)."""
    msg = json.loads(raw)
    d = msg.get("data", msg)          # el endpoint /stream envuelve el evento en {"stream":..., "data":...}
    if d.get("e") != "trade":
        return None
    return { # lo pasa a este formato el js ojito
        "ts": datetime.fromtimestamp(d["T"] / 1000, tz=timezone.utc),   # campo de tiempo de la colección time-series
        "symbol": d["s"],                                                # metaField: identifica cada serie
        "price": float(d["p"]),
        "qty": float(d["q"]),
        "trade_id": d["t"],
        "buyer_maker": bool(d["m"]),
    }


def get_collection():
    """Conecta a MongoDB y crea la colección time-series si no existe."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
    db = client[DB_NAME]
    if COLLECTION not in db.list_collection_names():
        db.create_collection(
            COLLECTION,
            timeseries={"timeField": "ts", "metaField": "symbol", "granularity": "seconds"},
        )
        log.info("Colección time-series '%s.%s' creada", DB_NAME, COLLECTION)
    db[COLLECTION].create_index([("symbol", 1), ("ts", 1)])
    return db[COLLECTION]


async def flush(coll, buf):
    """Inserta el lote en MongoDB. Devuelve cuántos documentos se insertaron."""
    try:
        await asyncio.to_thread(coll.insert_many, buf, ordered=False)
        return len(buf)
    except PyMongoError as e:
        log.error("Falló insert_many (%d docs descartados): %s", len(buf), e)
        return 0


async def consume(coll):
    buf, total = [], 0
    last_flush = last_log = time.monotonic()
    backoff = 1
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, open_timeout=15) as ws:
                log.info("Conectado a %s", WS_URL)
                backoff = 1
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=FLUSH_SECONDS)
                        doc = parse_trade(raw)
                        if doc:
                            buf.append(doc)
                    except asyncio.TimeoutError:
                        pass
                    now = time.monotonic()
                    if buf and (len(buf) >= BATCH_SIZE or now - last_flush >= FLUSH_SECONDS):
                        total += await flush(coll, buf)
                        buf, last_flush = [], now
                    if now - last_log >= 10:
                        log.info("Insertados en total: %d", total)
                        last_log = now
        except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as e:
            log.warning("Conexión perdida (%s). Reintentando en %d s", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    asyncio.run(consume(get_collection()))