"""Cargador masivo: llena crypto.trades con trades históricos de Binance hasta TARGET_DOCS documentos.

Fuente: archivos diarios públicos de https://data.binance.vision (spot/daily/trades), los mismos trades
que entrega el WebSocket, en CSV comprimido. Se recorren los días hacia atrás desde ayer (UTC) y,
por cada día, los símbolos de SYMBOLS, hasta llegar al objetivo.

Es re-ejecutable: cada archivo cargado queda registrado en la colección `load_progress`. Si una carga se
corta a mitad de un archivo, en la siguiente ejecución se borran esos trades parciales y se vuelve a cargar.
"""
import csv
import hashlib
import io
import logging
import os
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

from collector import COLLECTION, DB_NAME, SYMBOLS, get_collection

TARGET_DOCS = int(os.getenv("TARGET_DOCS", "10000000"))
BATCH_SIZE = int(os.getenv("LOAD_BATCH_SIZE", "10000"))
MAX_DAYS_BACK = int(os.getenv("MAX_DAYS_BACK", "60"))
BASE_URL = "https://data.binance.vision/data/spot/daily/trades"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("loader")


def download(symbol, day):
    """Descarga el zip del día y verifica su SHA-256. Devuelve la ruta local o None si no existe."""
    name = f"{symbol}-trades-{day}.zip"
    url = f"{BASE_URL}/{symbol}/{name}"
    try:
        with urllib.request.urlopen(url + ".CHECKSUM", timeout=30) as r:
            expected = r.read().decode().split()[0]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    fd, path = tempfile.mkstemp(suffix=".zip")
    h = hashlib.sha256()
    with os.fdopen(fd, "wb") as f, urllib.request.urlopen(url, timeout=120) as r:
        while chunk := r.read(1 << 20):
            f.write(chunk)
            h.update(chunk)
    if h.hexdigest() != expected:
        os.remove(path)
        raise ValueError(f"Checksum inválido para {name}")
    return path


def rows(path, symbol):
    """Lee el CSV del zip y genera documentos con el mismo formato que el recolector en vivo.

    Columnas: trade_id, price, qty, quote_qty, time, is_buyer_maker, is_best_match.
    Desde 2025 el tiempo viene en microsegundos (antes en milisegundos).
    """
    with zipfile.ZipFile(path) as z, z.open(z.namelist()[0]) as raw:
        for r in csv.reader(io.TextIOWrapper(raw, encoding="utf-8")):
            if not r or not r[0].isdigit():      # salta un posible encabezado
                continue
            t = int(r[4])
            ms = t // 1000 if t > 10**14 else t
            yield {
                "ts": datetime.fromtimestamp(ms / 1000, tz=timezone.utc),
                "symbol": symbol,
                "price": float(r[1]),
                "qty": float(r[2]),
                "trade_id": int(r[0]),
                "buyer_maker": r[5] == "True",
            }


def day_range(day):
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    return {"$gte": start, "$lt": start + timedelta(days=1)}


def main():
    coll = get_collection()
    progress = coll.database["load_progress"]

    # Limpia cargas interrumpidas a mitad de archivo.
    for p in progress.find({"status": "in_progress"}):
        n = coll.delete_many({"symbol": p["symbol"], "ts": day_range(p["day"].date())}).deleted_count
        log.warning("Carga interrumpida de %s: %d trades parciales borrados", p["_id"], n)
        progress.delete_one({"_id": p["_id"]})

    total = coll.count_documents({})
    log.info("Documentos en %s.%s: %d. Objetivo: %d", DB_NAME, COLLECTION, total, TARGET_DOCS)
    started = time.monotonic()
    today = datetime.now(timezone.utc).date()

    for back in range(1, MAX_DAYS_BACK + 1):
        day = today - timedelta(days=back)
        for sym in (s.upper() for s in SYMBOLS):
            if total >= TARGET_DOCS:
                break
            key = f"{sym}-{day}"
            if progress.find_one({"_id": key, "status": "done"}):
                continue
            path = download(sym, day)
            if path is None:
                log.info("%s no está publicado todavía, se salta", key)
                continue
            progress.replace_one(
                {"_id": key},
                {"_id": key, "symbol": sym, "day": datetime(day.year, day.month, day.day), "status": "in_progress"},
                upsert=True,
            )
            loaded, buf = 0, []
            try:
                for doc in rows(path, sym):
                    buf.append(doc)
                    if len(buf) >= BATCH_SIZE or total + loaded + len(buf) >= TARGET_DOCS:
                        coll.insert_many(buf, ordered=False)
                        loaded += len(buf)
                        buf = []
                        if total + loaded >= TARGET_DOCS:
                            break
                if buf:
                    coll.insert_many(buf, ordered=False)
                    loaded += len(buf)
            finally:
                os.remove(path)
            total += loaded
            progress.update_one({"_id": key}, {"$set": {"status": "done", "docs": loaded}})
            rate = total / max(time.monotonic() - started, 1e-9)
            log.info("%s: %d trades cargados. Total: %d / %d (%.0f docs/s)", key, loaded, total, TARGET_DOCS, rate)
        if total >= TARGET_DOCS:
            break

    log.info("Listo. Documentos en %s.%s: %d", DB_NAME, COLLECTION, coll.count_documents({}))


if __name__ == "__main__":
    main()
