"""Recolector con límite: consume trades EN VIVO de Binance hasta sumar SUMAR trades nuevos.

Complementa a loader.py: la carga histórica aporta el volumen y este script muestra la velocidad, es decir,
cómo MongoDB recibe y guarda los trades en tiempo real mientras ocurren.

Usa el mismo flujo y formato que collector.py (WebSocket -> lotes -> insert_many en la colección
time-series), pero va mostrando el avance y se detiene al llegar a SUMAR o con Ctrl+C. En ambos casos
termina con un resumen de lo consumido.
"""
import asyncio
import os
import time
from collections import Counter

import websockets

from collector import BATCH_SIZE, COLLECTION, DB_NAME, FLUSH_SECONDS, WS_URL, flush, get_collection, log, parse_trade

SUMAR = int(os.getenv("SUMAR", "10000000000"))                 # límite de trades nuevos: lo fija scripts/recolectar.sh
PROGRESS_SECONDS = float(os.getenv("PROGRESS_SECONDS", "2"))   # cada cuánto se muestra el avance


def miles(n):
    return f"{n:,.0f}".replace(",", ".")


def duracion(s):
    s = int(s)
    return f"{s // 3600}h {s % 3600 // 60:02d}m {s % 60:02d}s" if s >= 3600 else f"{s // 60}m {s % 60:02d}s"


async def recolectar(coll):
    destino = f"{DB_NAME}.{COLLECTION}"
    base = await asyncio.to_thread(coll.count_documents, {})
    log.info("Documentos en %s: %s. Límite: %s trades nuevos en vivo (Ctrl+C para detener antes).",
             destino, miles(base), miles(SUMAR))

    buf, nuevos, por_simbolo = [], 0, Counter()
    started = last_flush = last_log = time.monotonic()
    backoff = 1
    try:
        while nuevos < SUMAR:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, open_timeout=15, close_timeout=1) as ws:
                    log.info("Conectado a %s", WS_URL)
                    backoff = 1
                    while nuevos < SUMAR:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=FLUSH_SECONDS)
                            doc = parse_trade(raw)
                            if doc:
                                buf.append(doc)
                        except asyncio.TimeoutError:
                            pass
                        now = time.monotonic()
                        falta = SUMAR - nuevos
                        if buf and (len(buf) >= min(BATCH_SIZE, falta) or now - last_flush >= FLUSH_SECONDS):
                            lote = buf[:falta]                 # no pasarse del límite
                            n = await flush(coll, lote)
                            if n:
                                nuevos += n
                                por_simbolo.update(d["symbol"] for d in lote)
                            buf, last_flush = [], now
                        if now - last_log >= PROGRESS_SECONDS or nuevos >= SUMAR:
                            last_log = now
                            elapsed = now - started
                            rate = nuevos / elapsed if elapsed > 0 else 0
                            log.info("En vivo: %s nuevos de %s | %s trades/s | total en %s: %s | transcurrido %s",
                                     miles(nuevos), miles(SUMAR), miles(rate), destino, miles(base + nuevos),
                                     duracion(elapsed))
            except (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError) as e:
                log.warning("Conexión perdida (%s). Reintentando en %d s", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
    finally:   # también corre con Ctrl+C: los trades del último segundo aún sin guardar se descartan
        motivo = "Límite alcanzado" if nuevos >= SUMAR else "Detenido"
        log.info("%s: %s trades nuevos en %s. Total en %s: %s",
                 motivo, miles(nuevos), duracion(time.monotonic() - started), destino, miles(base + nuevos))
        for sym, n in por_simbolo.most_common():
            log.info("  %s: %s", sym, miles(n))


if __name__ == "__main__":
    try:
        asyncio.run(recolectar(get_collection()))
    except KeyboardInterrupt:
        pass   # Ctrl+C es la forma normal de detenerlo; el resumen ya se mostró
