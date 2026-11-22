# Proyecto-big-data

Implementación y uso de una herramienta de Big Data: **MongoDB** (colecciones time-series).

El dataset es un flujo en vivo de trades de criptomonedas (Binance, WebSocket público) que un
recolector en Python guarda en MongoDB. Crece solo mientras los contenedores estén corriendo.

## Requisitos

- Docker y Docker Compose
- Acceso a internet (para conectarse a `stream.binance.com`)

## Orden de uso

| # | Paso                        | Comando                                   | Cuándo                                              |
|---|-----------------------------|-------------------------------------------|-----------------------------------------------------|
| 1 | Cargar los 10 M de trades   | `./setup.sh`                              | Una vez, con tiempo: descarga el histórico de Binance |
| 2 | Demostración en tiempo real | `./scripts/recolectar.sh`                 | En clases; se detiene con Ctrl+C                    |
| 3 | Consultas                   | `docker compose exec mongo mongosh crypto` | Después; ver [Pruebas básicas](#pruebas-básicas)  |

## Instalación y carga de datos (10 millones de trades)

```bash
./setup.sh
```

Construye las imágenes, levanta MongoDB y lo llena con trades históricos de Binance (archivos públicos de
[data.binance.vision](https://data.binance.vision), mismos campos que el WebSocket). Carga **días completos**
(BTC, ETH, BNB, SOL y XRP), desde ayer hacia atrás, hasta la mínima cantidad de días que sume al menos
**10.000.000 de trades** (el total puede pasar de 10 M). Al final muestra cuántos días se cargaron y cuáles.
Se puede volver a ejecutar sin duplicar: si se cortó, continúa desde donde quedó.

```bash
TARGET_DOCS=1000000 ./setup.sh   # otro objetivo
./setup.sh --live                # además deja corriendo el recolector en vivo (seguirá sumando trades)
```

## Consumo en vivo con límite

```bash
./scripts/recolectar.sh               # consume trades en vivo hasta el límite SUMAR o hasta Ctrl+C
SUMAR=50000 ./scripts/recolectar.sh   # otro límite
```

Muestra en tiempo real cómo entran los trades a MongoDB: cantidad, trades/s y total de la colección.
El límite `SUMAR` es la primera variable de `scripts/recolectar.sh`. Por defecto es 10.000.000.000, es
decir, en la práctica corre hasta que se corta con **Ctrl+C**. Al detenerse, por el límite o por
Ctrl+C, muestra un resumen de cuántos trades llegaron por símbolo. Si el recolector permanente
(`collector`) está corriendo, el script lo detiene antes, para no guardar dos veces los mismos trades.

## Recolector en vivo

```bash
docker compose up -d --build
```

Levanta dos servicios:

| Servicio    | Qué hace                                                                       |
|-------------|--------------------------------------------------------------------------------|
| `mongo`     | MongoDB 8.0 (puerto 27017, datos persistidos en el volumen `mongo_data`)       |
| `collector` | Lee trades de BTC, ETH, BNB, SOL y XRP y los inserta por lotes en `crypto.trades` |

Verificar que está insertando:

```bash
docker compose logs -f collector     # debe decir "Conectado a wss://..." e "Insertados en total: N"
```

Detener (conserva los datos): `docker compose down`
Detener y borrar los datos: `docker compose down -v`

## Pruebas básicas

Abrir la consola de MongoDB:

```bash
docker compose exec mongo mongosh crypto
```

**1. Escritura:** la hace el recolector. Comprobar cuántos documentos hay:

```js
db.trades.countDocuments()
```

**2. Consulta:** últimos 5 trades de BTC:

```js
db.trades.find({ symbol: "BTCUSDT" }, { _id: 0 }).sort({ ts: -1 }).limit(5)
```

**3. Agregación:** precio promedio y cantidad de trades por minuto y símbolo:

```js
db.trades.aggregate([
  { $group: {
      _id: { sym: "$symbol", min: { $dateTrunc: { date: "$ts", unit: "minute" } } },
      avg: { $avg: "$price" },
      n: { $sum: 1 } } },
  { $sort: { "_id.min": -1 } },
  { $limit: 5 }
])
```

## Estructura

```
docker-compose.yml       # mongo + collector (+ loader y recolector, que se ejecutan a pedido)
setup.sh                 # despliegue + carga histórica hasta TARGET_DOCS
scripts/recolectar.sh    # consumo en vivo hasta SUMAR trades nuevos, con avance en tiempo real
collector/collector.py   # WebSocket -> lotes -> insert_many en colección time-series
collector/loader.py      # carga histórica desde data.binance.vision (servicio `loader`, perfil "load")
collector/recolectar.py  # recolector con límite SUMAR (servicio `recolector`, perfil "live")
collector/Dockerfile
```

Documento guardado (campo `ts` = timeField, `symbol` = metaField):

```json
{ "ts": "2026-11-11T02:15:13.833Z", "symbol": "BTCUSDT", "price": 73432.55,
  "qty": 0.01017, "trade_id": 6838057223, "buyer_maker": false }
```
