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

TARGET_DOCS = int(os.getenv("TARGET_DOCS", "10000000"))   # mínimo: se cargan días completos hasta superarlo
BATCH_SIZE = int(os.getenv("LOAD_BATCH_SIZE", "10000"))
MAX_DAYS_BACK = int(os.getenv("MAX_DAYS_BACK", "60"))
PROGRESS_SECONDS = float(os.getenv("PROGRESS_SECONDS", "2"))   # cada cuánto se muestra el avance
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
        size = int(r.headers.get("Content-Length", 0))
        log.info("Descargando %s (%.1f MB)...", name, size / 1e6)
        done, started = 0, time.monotonic()
        last = started
        while chunk := r.read(1 << 20):
            f.write(chunk)
            h.update(chunk)
            done += len(chunk)
            now = time.monotonic()
            if now - last >= PROGRESS_SECONDS:
                last = now
                speed = done / (now - started)
                left = fmt_secs((size - done) / speed) if size and speed else "?"
                log.info("  descarga %s: %.1f / %.1f MB (%.0f%%) | %.1f MB/s | restante ~%s",
                         name, done / 1e6, size / 1e6, 100 * done / size if size else 0, speed / 1e6, left)
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


def fmt_secs(s):
    s = int(s)
    return f"{s // 3600}h {s % 3600 // 60:02d}m {s % 60:02d}s" if s >= 3600 else f"{s // 60}m {s % 60:02d}s"


class Progress:
    """Muestra el avance total cada PROGRESS_SECONDS: documentos, porcentaje, velocidad y tiempo restante."""

    def __init__(self, start_total):
        self.start_total = start_total
        self.started = self.last = time.monotonic()
        self.excluded = 0.0      # segundos de descarga: no cuentan para la velocidad de inserción

    def exclude(self, seconds):
        self.excluded += seconds

    def show(self, total, label, force=False):
        now = time.monotonic()
        if not force and now - self.last < PROGRESS_SECONDS:
            return
        self.last = now
        elapsed = now - self.started
        inserting = elapsed - self.excluded
        rate = (total - self.start_total) / inserting if inserting > 0 else 0
        eta = fmt_secs(max(TARGET_DOCS - total, 0) / rate) if rate > 0 else "?"
        pct = 100 * total / TARGET_DOCS
        log.info("Avance: %s / %s (%5.1f%%) | %s docs/s | transcurrido %s | restante ~%s | %s",
                 f"{total:,}".replace(",", "."), f"{TARGET_DOCS:,}".replace(",", "."), pct,
                 f"{rate:,.0f}".replace(",", "."), fmt_secs(elapsed), eta, label)


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
    prog = Progress(total)
    today = datetime.now(timezone.utc).date()

    # Se cargan días completos (todos los símbolos), del más reciente hacia atrás. El objetivo solo se
    # revisa al terminar cada día, así que el total final puede pasar de TARGET_DOCS.
    for back in range(1, MAX_DAYS_BACK + 1):
        if total >= TARGET_DOCS:
            break
        day = today - timedelta(days=back)
        log.info("== Día %s ==", day)
        for sym in (s.upper() for s in SYMBOLS):
            key = f"{sym}-{day}"
            if progress.find_one({"_id": key, "status": "done"}):
                continue
            t0 = time.monotonic()
            path = download(sym, day)
            prog.exclude(time.monotonic() - t0)
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
                    if len(buf) >= BATCH_SIZE:
                        coll.insert_many(buf, ordered=False)
                        loaded += len(buf)
                        buf = []
                        prog.show(total + loaded, key)
                if buf:
                    coll.insert_many(buf, ordered=False)
                    loaded += len(buf)
            finally:
                os.remove(path)
            total += loaded
            progress.update_one({"_id": key}, {"$set": {"status": "done", "docs": loaded}})
            log.info("%s terminado: %d trades cargados", key, loaded)
            prog.show(total, key, force=True)

    # Resumen: días cargados (del más reciente al más antiguo) y documentos por día.
    days = list(progress.aggregate([
        {"$match": {"status": "done"}},
        {"$group": {"_id": "$day", "docs": {"$sum": "$docs"}, "symbols": {"$push": "$symbol"}}},
        {"$sort": {"_id": -1}},
    ]))
    fmt = lambda n: f"{n:,}".replace(",", ".")
    log.info("========== RESUMEN ==========")
    log.info("Días obtenidos: %d", len(days))
    for d in days:
        log.info("  %s: %s trades (%s)", d["_id"].date(), fmt(d["docs"]), ", ".join(sorted(d["symbols"])))
    log.info("Documentos en %s.%s: %s (objetivo mínimo: %s)",
             DB_NAME, COLLECTION, fmt(coll.count_documents({})), fmt(TARGET_DOCS))
    if total < TARGET_DOCS:
        log.warning("No se alcanzó el objetivo en %d días hacia atrás (MAX_DAYS_BACK)", MAX_DAYS_BACK)


if __name__ == "__main__":
    main()
