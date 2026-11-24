// ============================================================================
// Experimento 2 (Hernán Muñoz) - Modelado: colección normal, con índice y time-series
// ============================================================================
// Pregunta: ¿cuánto espacio en disco ocupa, y cuánto tarda en responder una
// consulta por símbolo y rango de tiempo, una colección time-series comparada
// con una colección normal sin índice y con una normal con índice (symbol, ts)?
//
// Se modifica : cómo se guardan los mismos trades:
//                 A = normal sin índice, B = normal con índice, C = time-series.
// Se mide     : espacio en disco (storageSize + totalIndexSize) y tiempo de la
//               consulta (executionTimeMillis de explain), con documentos examinados.
// Se mantiene : los mismos N trades (los primeros de crypto.trades en orden
//               ts, symbol, trade_id; copiados de A a B y C), la misma consulta,
//               la base "bench" nueva y el mismo equipo.
//
// Reproducibilidad: el script NO depende de fechas ni símbolos escritos a mano.
// Todo se calcula con reglas fijas a partir de los datos que haya:
//   - Muestra : los N primeros trades ordenados por (ts, symbol, trade_id),
//               un orden sin empates, así la muestra es siempre la misma.
//   - Símbolo : el que más trades tiene en la muestra.
//   - Ventana : el tramo continuo de tiempo de ese símbolo que contiene el
//               FRACCION (2 %) de la muestra, centrado en sus trades.
//   Así la consulta devuelve siempre ~2 % de N, con cualquier dataset, y las
//   aceleraciones se pueden comparar entre equipos. Con los mismos datos
//   (p. ej. el mismo día de data.binance.vision) se obtiene la misma muestra
//   exacta; la "huella" impresa permite comprobarlo.
//
// Lo normal es correrlo con ./experimento-2/exp2_modelado.sh, que hace todo.
// También se puede ejecutar a mano con mongosh, sobre los trades que ya están
// en crypto.trades (cargados con ./setup.sh, cualquier día):
//
//   docker compose cp experimento-2/exp2_modelado.js mongo:/tmp/exp2_modelado.js
//   docker compose exec -T mongo mongosh crypto --quiet --file /tmp/exp2_modelado.js
//
// Variables de entorno opcionales (se pasan con -e):
//   N=100000          trades a usar (por defecto 1.000.000)
//   DIA=2026-11-20    usar solo los trades de ese día UTC (por defecto, todos)
//   FRACCION=0.02     parte de la muestra que devuelve la consulta
//   docker compose exec -T -e N=100000 mongo mongosh crypto --quiet --file /tmp/exp2_modelado.js
//
// El recolector puede seguir activo: solo agrega trades NUEVOS a crypto.trades y
// el experimento usa los N primeros. Para medir sin ruido de CPU, deténgalo:
//   docker compose stop collector   (y después: docker compose start collector)
// crypto.trades nunca se modifica; la base "bench" se borra y se recrea.
// ============================================================================

var bench = db.getSiblingDB("bench");   // base aparte para no tocar crypto.trades
var NOMBRES = ["normal_sin_indice", "normal_con_indice", "timeseries"];
var MIB = 1024 * 1024;

function env(nombre) { return (typeof process !== "undefined" && process.env[nombre]) || ""; }
var DIA = env("DIA");                                   // "" = todos los trades
var FRACCION = parseFloat(env("FRACCION") || "0.02");

// Consulta del experimento: un símbolo y una ventana de tiempo.
// Se calculan en calcularConsulta() a partir de los datos (ver encabezado).
var SIMBOLO = null;
var DESDE = null;
var HASTA = null;
function filtro() { return { symbol: SIMBOLO, ts: { $gte: DESDE, $lt: HASTA } }; }

// Rellena con espacios a la izquierda para alinear las tablas.
function pad(x, ancho) { var s = String(x); while (s.length < ancho) { s = " " + s; } return s; }
function padR(x, ancho) { var s = String(x); while (s.length < ancho) { s = s + " "; } return s; }
function mib(bytes) { return (bytes / MIB).toFixed(2); }

// Filtro del día pedido en DIA (o ninguno).
function filtroDia() {
  if (!DIA) { return {}; }
  var d = new Date(DIA + "T00:00:00Z");
  return { ts: { $gte: d, $lt: new Date(d.getTime() + 86400000) } };
}

// Crea las tres colecciones en "bench" con los mismos n trades.
// Se borra la base completa para no arrastrar restos de corridas anteriores.
function crear(n) {
  bench.dropDatabase();

  // A: normal sin índice (no lleva la opción timeseries; $out la crea como normal).
  // Orden (ts, symbol, trade_id): sin empates, así la muestra es siempre la misma.
  db.getSiblingDB("crypto").trades.aggregate([
    { $match: filtroDia() },
    { $sort: { ts: 1, symbol: 1, trade_id: 1 } },
    { $limit: n },
    { $out: { db: "bench", coll: "normal_sin_indice" } }
  ], { allowDiskUse: true }).toArray();

  // B: copia de A + índice (symbol, ts).
  bench.normal_sin_indice.aggregate([{ $out: "normal_con_indice" }]).toArray();
  bench.normal_con_indice.createIndex({ symbol: 1, ts: 1 });

  // C: copia de A guardada como time-series (sin _id, que no necesita).
  bench.normal_sin_indice.aggregate([
    { $project: { _id: 0 } },
    { $out: { db: "bench", coll: "timeseries",
              timeseries: { timeField: "ts", metaField: "symbol", granularity: "seconds" } } }
  ]).toArray();

  NOMBRES.forEach(function (c) {
    print(c + ": " + bench.getCollection(c).countDocuments({}) + " trades");
  });
  print("Buckets de la time-series: " + bench.system.buckets.timeseries.countDocuments({}));
}

// Fechas mínima y máxima de los trades copiados.
function rango() {
  var r = bench.normal_sin_indice.aggregate([
    { $group: { _id: null, min: { $min: "$ts" }, max: { $max: "$ts" } } }
  ]).toArray()[0];
  print("Desde " + r.min.toISOString() + " hasta " + r.max.toISOString());
}

// Huella de la muestra: si dos equipos obtienen lo mismo, usaron los mismos trades.
function huella() {
  print("Huella de la muestra (trades por símbolo | suma de trade_id):");
  bench.normal_sin_indice.aggregate([
    { $group: { _id: "$symbol", n: { $sum: 1 }, ids: { $sum: { $toDecimal: "$trade_id" } } } },
    { $sort: { _id: 1 } }
  ]).forEach(function (h) { print("  " + padR(h._id, 9) + " " + pad(h.n, 9) + " | " + h.ids); });
}

// Calcula la consulta con reglas fijas (ver encabezado):
// símbolo con más trades y el tramo de tiempo, centrado, con FRACCION de la muestra.
function calcularConsulta() {
  var n = bench.normal_sin_indice.countDocuments({});
  var top = bench.normal_sin_indice.aggregate([
    { $group: { _id: "$symbol", n: { $sum: 1 } } },
    { $sort: { n: -1, _id: 1 } }, { $limit: 1 }
  ]).toArray()[0];
  SIMBOLO = top._id;
  var m = Math.max(1, Math.round(FRACCION * n));       // trades que debe devolver
  if (m >= top.n) { m = top.n - 1; }
  var inicio = Math.floor((top.n - m) / 2);
  // Se usa el índice (symbol, ts) de B para ubicar los trades en las posiciones
  // inicio e inicio+m del símbolo, ordenados por tiempo.
  function tsEn(pos) {
    return bench.normal_con_indice.find({ symbol: SIMBOLO }, { _id: 0, ts: 1 })
      .sort({ ts: 1 }).skip(pos).limit(1).toArray()[0].ts;
  }
  DESDE = tsEn(inicio);
  HASTA = tsEn(inicio + m);
  print("Consulta calculada: " + SIMBOLO + " (" + top.n + " trades en la muestra), " +
        "objetivo ~" + m + " trades (" + (FRACCION * 100) + " % de " + n + ")");
  print("  ventana " + DESDE.toISOString() + " -> " + HASTA.toISOString() +
        " (" + ((HASTA - DESDE) / 1000).toFixed(1) + " s)");
}

// Lee el espacio de cada colección: datos ya comprimidos + índices, en bytes.
function medirEspacio() {
  return NOMBRES.map(function (c) {
    var s = bench.getCollection(c).stats();
    return { c: c, datos: s.storageSize, indices: s.totalIndexSize };
  });
}

// storageSize se actualiza con retraso (checkpoint de WiredTiger). Se fuerza la
// escritura y se espera hasta que los tamaños no cambien entre dos lecturas.
function esperarEstable(maxSeg) {
  db.adminCommand({ fsync: 1 });
  var antes = JSON.stringify(medirEspacio());
  for (var t = 0; t < maxSeg; t += 5) {
    sleep(5000);
    var ahora = JSON.stringify(medirEspacio());
    if (ahora === antes) { return; }
    antes = ahora;
  }
  print("Aviso: los tamaños seguían cambiando tras " + maxSeg + " s.");
}

// Espacio en disco: datos ya comprimidos (storageSize) + índices (totalIndexSize).
function espacio() {
  print("Midiendo espacio (se espera a que MongoDB escriba a disco)...");
  esperarEstable(90);
  print("Colección           | Datos (MiB) | Índices (MiB) | Total (MiB)");
  medirEspacio().forEach(function (e) {
    print(padR(e.c, 19) + " | " +
          pad(mib(e.datos), 11) + " | " + pad(mib(e.indices), 13) + " | " +
          pad(mib(e.datos + e.indices), 11));
  });
}

// La misma consulta debe devolver los mismos trades en las tres colecciones.
function verificar() {
  NOMBRES.forEach(function (c) {
    print(c + ": " + bench.getCollection(c).countDocuments(filtro()) + " trades");
  });
}

// Una corrida con explain: milisegundos, devueltos, examinados y claves de índice.
// En la time-series, "examinados" cuenta buckets y el resultado viene en otro lugar.
function plan(coleccion) {
  var x = bench.getCollection(coleccion).find(filtro()).explain("executionStats");
  var e = x.executionStats || x.stages[0].$cursor.executionStats;
  return { ms: e.executionTimeMillis, devueltos: e.nReturned,
           examinados: e.totalDocsExamined, claves: e.totalKeysExamined };
}

function mediana(v) {
  var s = v.slice().sort(function (a, b) { return a - b; });
  return s[Math.floor(s.length / 2)];
}

// 6 corridas por colección: la primera es calentamiento y no se usa.
function tiempos() {
  var med = [];
  var info = [];
  NOMBRES.forEach(function (c) {
    var ms = [];
    var ultimo = null;
    for (var i = 0; i < 6; i++) { ultimo = plan(c); ms.push(ultimo.ms); }
    print(c + ": " + ms.join(", ") + "   (la primera es calentamiento)");
    med.push(mediana(ms.slice(1)));
    info.push(ultimo);
  });

  print("\nRESULTADOS (consulta " + SIMBOLO + " entre " + DESDE.toISOString() + " y " + HASTA.toISOString() + ")");
  print("Colección           | Examinados | Claves | Mediana (ms) | Aceleración");
  NOMBRES.forEach(function (c, k) {
    print(padR(c, 19) + " | " +
          pad(info[k].examinados, 10) + " | " + pad(info[k].claves, 6) + " | " +
          pad(med[k], 12) + " | " + (med[k] === 0 ? "n/d (<1 ms)" : (med[0] / med[k]).toFixed(1) + "x"));
  });
}

// Todo seguido: crea las colecciones, calcula la consulta y mide espacio y tiempos.
function experimento(n) {
  crear(n);
  rango();
  huella();
  calcularConsulta();
  espacio();
  verificar();
  tiempos();
}

// Ejecución directa: toma los trades que ya están en crypto.trades.
var N = parseInt(env("N") || "1000000", 10);
var HAY = db.getSiblingDB("crypto").trades.countDocuments(filtroDia());
print("Trades en crypto.trades" + (DIA ? " del día " + DIA : "") + ": " + HAY + " (se usarán " + N + ")");
if (HAY === 0) { throw new Error("No hay trades. Cárguelos primero con ./setup.sh (en la raíz del repo)."); }
if (HAY < N) {
  print("Hay menos trades que los pedidos; se usan los " + HAY + " disponibles.");
  N = HAY;
}
experimento(N);
