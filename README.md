# Proyecto-big-data

Implementación y uso de una herramienta de Big Data: **MongoDB** (colecciones time-series).

El dataset es un flujo en vivo de trades de criptomonedas (Binance, WebSocket público) que un
recolector en Python guarda en MongoDB. Crece solo mientras los contenedores estén corriendo.

## Requisitos

- Docker y Docker Compose
- Acceso a internet (para conectarse a `stream.binance.com`)

## Instalación y carga de datos (10 millones de trades)

```bash
./setup.sh
```

Construye las imágenes, levanta MongoDB y lo llena con **10.000.000 de trades** históricos de Binance
(archivos públicos de [data.binance.vision](https://data.binance.vision), mismos campos que el WebSocket).
Recorre los días hacia atrás desde ayer, para BTC, ETH, BNB, SOL y XRP, hasta llegar al objetivo.
Se puede volver a ejecutar sin duplicar: si se cortó, continúa desde donde quedó.

```bash
TARGET_DOCS=1000000 ./setup.sh   # otro objetivo
./setup.sh --live                # además deja corriendo el recolector en vivo (seguirá sumando trades)
```

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
docker-compose.yml     # mongo + collector
setup.sh               # despliegue + carga histórica hasta TARGET_DOCS
collector/collector.py # WebSocket -> lotes -> insert_many en colección time-series
collector/loader.py    # carga histórica desde data.binance.vision (servicio `loader`, perfil "load")
collector/Dockerfile
```

Documento guardado (campo `ts` = timeField, `symbol` = metaField):

```json
{ "ts": "2026-11-11T02:15:13.833Z", "symbol": "BTCUSDT", "price": 73432.55,
  "qty": 0.01017, "trade_id": 6838057223, "buyer_maker": false }
```
