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
// Se mantiene : los mismos N trades (los primeros por fecha de crypto.trades,
//               copiados de A a B y C), la misma consulta, la base "bench" nueva,
//               el mismo equipo y el recolector desactivado.
//
// Este archivo SOLO define funciones; no ejecuta nada al cargarse.
// Uso, dentro de mongosh (docker compose exec mongo mongosh crypto):
//
//   load("/tmp/exp2_modelado.js")   carga las funciones
//   crear(1000000)                  crea A, B y C en la base "bench" con N trades
//   rango()                         fechas mínima y máxima de los trades copiados
//   espacio()                       tabla de espacio en disco de A, B y C
//   verificar()                     la consulta debe devolver lo mismo en las tres
//   tiempos()                       6 corridas por colección + mediana y aceleración
//   experimento(1000000)            todo lo anterior seguido
//
// La base "bench" es solo de pruebas: crypto.trades nunca se modifica.
// ============================================================================

var bench = db.getSiblingDB("bench");   // base aparte para no tocar crypto.trades
var NOMBRES = ["normal_sin_indice", "normal_con_indice", "timeseries"];
var MIB = 1024 * 1024;

// Consulta fija del experimento: un símbolo y una ventana de 10 minutos.
// Si cambia el dataset, mire rango() y ajuste estas fechas.
var SIMBOLO = "BTCUSDT";
var DESDE = ISODate("2026-11-20T01:00:00Z");
var HASTA = ISODate("2026-11-20T01:10:00Z");
function filtro() { return { symbol: SIMBOLO, ts: { $gte: DESDE, $lt: HASTA } }; }

// Rellena con espacios a la izquierda para alinear las tablas.
function pad(x, ancho) { var s = String(x); while (s.length < ancho) { s = " " + s; } return s; }
function padR(x, ancho) { var s = String(x); while (s.length < ancho) { s = s + " "; } return s; }
function mib(bytes) { return (bytes / MIB).toFixed(2); }

// Crea las tres colecciones en "bench" con los mismos n trades.
// Se borra la base completa para no arrastrar restos de corridas anteriores.
function crear(n) {
  bench.dropDatabase();

  // A: normal sin índice (no lleva la opción timeseries; $out la crea como normal).
  db.getSiblingDB("crypto").trades.aggregate([
    { $sort: { ts: 1 } },
    { $limit: n },
    { $out: { db: "bench", coll: "normal_sin_indice" } }
  ]).toArray();

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

// Fechas mínima y máxima de los trades copiados (sirve para elegir la ventana).
function rango() {
  var r = bench.normal_sin_indice.aggregate([
    { $group: { _id: null, min: { $min: "$ts" }, max: { $max: "$ts" } } }
  ]).toArray()[0];
  print("Desde " + r.min.toISOString() + " hasta " + r.max.toISOString());
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

// Todo seguido: crea las colecciones, mide espacio y tiempos.
function experimento(n) {
  crear(n);
  rango();
  espacio();
  verificar();
  tiempos();
}

print("Funciones cargadas: crear(n), rango(), espacio(), verificar(), tiempos(), experimento(n)");