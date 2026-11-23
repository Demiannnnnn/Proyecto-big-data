// ============================================================================
// Experimento 1 (Bruno Ganora) - Rendimiento de escritura según el tamaño del lote
// ============================================================================
// Pregunta: ¿cómo cambia el throughput (documentos por segundo) de MongoDB, en
// una colección time-series, al aumentar el tamaño del lote de insertMany
// de 1 a 100, 1.000 y 10.000 documentos?
//
// Se modifica : el tamaño del lote.
// Se mide     : documentos insertados por segundo = N / tiempo de inserción.
// Se mantiene : los mismos N trades reales, una colección time-series nueva y
//               vacía en cada corrida, write concern w:1, el mismo equipo y el
//               recolector desactivado.
//
// Este archivo SOLO define funciones; no ejecuta nada al cargarse.
// Uso, dentro de mongosh (docker compose exec mongo mongosh crypto):
//
//   load("/tmp/exp1_lotes.js")   carga las funciones
//   ultimoTrade()                 instante del trade más reciente (¿llegan trades nuevos?)
//   cargarMuestra(100000)         deja N trades reales en memoria
//   medir(100)                    UNA corrida con un tamaño de lote
//   experimento()                 los 4 lotes seguidos + tabla de resultados
//
// La base "bench" es solo de pruebas: crypto.trades nunca se modifica.
// ============================================================================

var muestra = [];                       // trades de prueba en memoria
var bench = db.getSiblingDB("bench");   // base aparte para no tocar crypto.trades

// Reloj en milisegundos con decimales (más preciso que Date.now()).
function ahora() { return (typeof performance !== "undefined") ? performance.now() : Date.now(); }

// Instante del trade más reciente de crypto.trades.
// Si dos llamadas separadas por unos segundos dan lo mismo, no llegan trades
// nuevos: el recolector está desactivado.
function ultimoTrade() {
  var ultimo = db.getSiblingDB("crypto").trades
    .find({}, { _id: 0, ts: 1 }).sort({ ts: -1 }).limit(1).toArray()[0];
  print("Último trade en crypto.trades: " + (ultimo ? ultimo.ts.toISOString() : "(vacía)"));
}

// Lee los primeros n trades (por tiempo) y los deja en memoria, para que el
// cronómetro mida solo la escritura y no la lectura.
function cargarMuestra(n) {
  muestra = db.getSiblingDB("crypto").trades
    .find({}, { _id: 0 }).sort({ ts: 1 }).limit(n).toArray();
  if (muestra.length < n) {
    print("ATENCIÓN: solo hay " + muestra.length + " trades; se pidieron " + n + ".");
  }
  print("Trades en memoria: " + muestra.length);
  print("Desde " + muestra[0].ts.toISOString() + " hasta " + muestra[muestra.length - 1].ts.toISOString());
}

// Inserta la muestra completa en una colección time-series nueva, 'lote'
// documentos por llamada, y devuelve los segundos que tardó.
function insertar(lote) {
  if (muestra.length === 0) { throw new Error("Primero ejecute cargarMuestra(100000)"); }
  var n = muestra.length;

  // 1) Base "bench" vacía y colección time-series nueva con la misma definición
  //    que crypto.trades. Se borra la base completa (y no solo la colección)
  //    para no arrastrar restos de corridas anteriores.
  bench.dropDatabase();
  bench.createCollection("trades", {
    timeseries: { timeField: "ts", metaField: "symbol", granularity: "seconds" }
  });
  var col = bench.getCollection("trades");

  // 2) Lo único que se cronometra: insertar los n trades, 'lote' por llamada.
  var inicio = ahora();
  for (var i = 0; i < n; i += lote) {
    col.insertMany(muestra.slice(i, i + lote), { ordered: true, writeConcern: { w: 1 } });
  }
  var segundos = (ahora() - inicio) / 1000;

  // 3) Validación: deben haber entrado exactamente n trades.
  var insertados = col.countDocuments({});
  if (insertados !== n) { throw new Error("Se esperaban " + n + " y hay " + insertados); }
  return segundos;
}

// Una medición con un tamaño de lote, mostrada en pantalla.
function medir(lote) {
  var n = muestra.length;
  var segundos = insertar(lote);
  print("Lote " + lote + ": " + Math.ceil(n / lote) + " llamadas a insertMany, " +
        segundos.toFixed(2) + " s, " + Math.round(n / segundos) + " docs/s");
}

// Experimento: un calentamiento que no se registra y una medición por lote.
// Sin argumentos usa los 4 lotes del informe. También acepta otra lista:
//   experimento([1000, 100000])
function experimento(lotes) {
  lotes = lotes || [1, 100, 1000, 10000];
  var n = muestra.length;
  lotes.forEach(function (lote) {
    if (lote > n) { print("ATENCIÓN: el lote " + lote + " es mayor que la muestra (" + n + "): sería una sola llamada."); }
  });

  print("Calentamiento (no se registra)...");
  insertar(10000);

  var tasas = [];
  lotes.forEach(function (lote) {
    print("Midiendo lote " + lote + "...");
    var segundos = insertar(lote);
    tasas.push(n / segundos);
    print("Lote " + lote + " listo: " + segundos.toFixed(2) + " s");
  });
  bench.dropDatabase();

  print("\nRESULTADOS (N = " + n + ")");
  print("    Lote | Llamadas |   Docs/s | Aceleración");
  lotes.forEach(function (lote, k) {
    print(pad(lote, 8) + " | " + pad(Math.ceil(n / lote), 8) + " | " +
          pad(Math.round(tasas[k]), 8) + " | " + (tasas[k] / tasas[0]).toFixed(1) + "x");
  });
}

// Versión extendida (solo para la presentación): agrega lotes de 100.000,
// 1.000.000 y 10.000.000, pero solo los que caben en la muestra cargada.
// Con cargarMuestra(1000000) llega hasta el lote de 1.000.000.
function experimentoExtendido() {
  var todos = [1, 100, 1000, 10000, 100000, 1000000, 10000000];
  experimento(todos.filter(function (lote) { return lote <= muestra.length; }));
}

// Rellena con espacios a la izquierda para alinear la tabla.
function pad(x, ancho) { var s = String(x); while (s.length < ancho) { s = " " + s; } return s; }

print("Funciones cargadas: ultimoTrade(), cargarMuestra(n), medir(lote), experimento()");
