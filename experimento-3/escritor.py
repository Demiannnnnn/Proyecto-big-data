"""Experimento 3 - escritor: inserta trades reales sin parar en el replica set y mide la caída del primario.

Uso (lo llama exp3_failover.sh):  python escritor.py <1|majority> <repetición>

1. Lee una muestra de trades reales de crypto.trades (MongoDB principal).
2. Durante DURACION segundos los inserta en lotes de LOTE, a TASA trades/s, en exp3.trades, con write
   concern w:1 o w:"majority". Cada trade lleva un número `n` único para saber después cuáles se
   confirmaron. La tasa es fija para que ambos modos escriban al mismo ritmo y no se agote la memoria.
3. Mientras tanto el script de afuera mata el primario. Se mide el hueco más largo entre dos escrituras
   confirmadas = segundos sin escribir.
4. Espera a que los 3 nodos vuelvan (el primario caído hace rollback de lo que no alcanzó a replicar) y
   cuenta los trades perdidos = confirmados que ya no están en la base.
"""
import os
import sys
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

MODO, REP = sys.argv[1], sys.argv[2]
W = 1 if MODO == "1" else "majority"
DURACION = float(os.getenv("DURACION", "40"))
LOTE = int(os.getenv("LOTE", "100"))
TASA = float(os.getenv("TASA", "2000"))          # trades por segundo
MUESTRA = int(os.getenv("MUESTRA", "20000"))
FUENTE_URI = os.getenv("FUENTE_URI", "mongodb://host.docker.internal:27017")
RS_URI = os.getenv("RS_URI", "mongodb://rs1:27017,rs2:27017,rs3:27017/?replicaSet=rs0")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# 1) Muestra de trades reales
muestra = list(MongoClient(FUENTE_URI, serverSelectionTimeoutMS=5000).crypto.trades
               .find({}, {"_id": 0}).limit(MUESTRA))
if not muestra:
    sys.exit("crypto.trades está vacía: primero ejecute ./setup.sh en la raíz del repo")

# Tiempos cortos para que el cliente detecte rápido al primario nuevo (mide al servidor, no al driver).
client = MongoClient(RS_URI, serverSelectionTimeoutMS=500, connectTimeoutMS=1000,
                     socketTimeoutMS=2000, heartbeatFrequencyMS=500)
db = client.exp3
db.drop_collection("trades")
db.create_collection("trades", timeseries={"timeField": "ts", "metaField": "symbol", "granularity": "seconds"})
col = db.get_collection("trades", write_concern=WriteConcern(w=W))

# 2) Escritura continua
log(f"Escribiendo con w:{MODO} durante {DURACION:.0f} s a {TASA:.0f} trades/s (repetición {REP})")
confirmados, n, max_hueco, fallando = [], 0, 0.0, False
inicio = ultimo_ok = siguiente = time.monotonic()
while time.monotonic() - inicio < DURACION:
    siguiente += LOTE / TASA                       # ritmo fijo; tras una caída no se "recupera" lo atrasado
    espera = siguiente - time.monotonic()
    if espera > 0:
        time.sleep(espera)
    else:
        siguiente = time.monotonic()
    docs = []
    for _ in range(LOTE):
        d = dict(muestra[n % len(muestra)])
        d["n"] = n
        docs.append(d)
        n += 1
    try:
        col.insert_many(docs, ordered=True)
        ahora = time.monotonic()
        if fallando:
            log(f"Escribe de nuevo tras {ahora - ultimo_ok:.2f} s sin escribir")
            fallando = False
        max_hueco = max(max_hueco, ahora - ultimo_ok)
        ultimo_ok = ahora
        confirmados.append((docs[0]["n"], docs[-1]["n"]))
    except PyMongoError as e:
        if not fallando:
            log(f"No puede escribir: {type(e).__name__}")
            fallando = True
        time.sleep(0.05)

# 3) Esperar a que los 3 nodos estén sanos (PRIMARY o SECONDARY); aquí ocurre el rollback del caído
log("Esperando que los 3 nodos vuelvan...")
limite = time.monotonic() + 180
while time.monotonic() < limite:
    try:
        miembros = client.admin.command("replSetGetStatus")["members"]
        if len(miembros) == 3 and all(m["state"] in (1, 2) for m in miembros):
            break
    except PyMongoError:
        pass
    time.sleep(1)
else:
    log("ATENCIÓN: el cluster no volvió a 3 nodos sanos; el conteo puede no ser final")

# 4) Perdidos = confirmados que ya no están
presentes = {d["n"] for d in col.with_options(read_concern=ReadConcern("majority")).find({}, {"_id": 0, "n": 1})}
total = sum(b - a + 1 for a, b in confirmados)
perdidos = sum(1 for a, b in confirmados for k in range(a, b + 1) if k not in presentes)
log(f"Confirmados: {total} | perdidos: {perdidos} | máximo sin escribir: {max_hueco:.2f} s")
print(f"RESULTADO|w:{MODO}|{REP}|{max_hueco:.2f}|{total}|{perdidos}", flush=True)
