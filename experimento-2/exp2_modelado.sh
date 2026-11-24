#!/usr/bin/env bash
# ============================================================================
# Experimento 2 (Hernán Muñoz) - Modelado: normal, normal con índice y time-series
# ============================================================================
# Hace todo el experimento con un solo comando:
#   1. desactiva el recolector (si estaba activo) para medir sin ruido de CPU,
#   2. copia exp2_modelado.js al contenedor mongo,
#   3. lo ejecuta con mongosh: crea A, B y C en la base "bench", calcula la
#      consulta a partir de los datos y muestra espacio, conteos y tiempos,
#   4. reactiva el recolector al terminar (o si algo falla).
#
# Funciona con los trades que haya en crypto.trades, de cualquier día: la
# muestra, el símbolo y la ventana de la consulta se calculan con reglas fijas
# (ver el encabezado de exp2_modelado.js), no con fechas escritas a mano.
#
# Uso (desde cualquier carpeta; en Windows, Git Bash o WSL):
#   ./experimento-2/exp2_modelado.sh                         # 1.000.000 de trades (unos minutos)
#   ./experimento-2/exp2_modelado.sh 100000                  # prueba rápida con 100.000
#   ./experimento-2/exp2_modelado.sh 1000000 2026-11-20      # solo trades de ese día UTC
#   FRACCION=0.05 ./experimento-2/exp2_modelado.sh           # la consulta devuelve el 5 %
#
# Requiere el MongoDB de la Etapa 1 levantado y con trades (./setup.sh).
# crypto.trades no se modifica; la base "bench" se borra y se recrea.
# ============================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1                      # evita que Git Bash cambie las rutas /tmp

cd "$(dirname "$0")/.."                        # raíz del repo (donde está docker-compose.yml)
N="${1:-1000000}"
DIA="${2:-}"
FRACCION="${FRACCION:-0.02}"
JS="experimento-2/exp2_modelado.js"

hora() { date +%H:%M:%S; }

[ -f "$JS" ] || { echo "No se encuentra $JS"; exit 1; }

# MongoDB debe estar corriendo.
if ! docker compose ps --status running --services | grep -qx mongo; then
  echo "[$(hora)] MongoDB no está corriendo. Levántelo con: docker compose up -d mongo"
  exit 1
fi

# Paso 1: desactivar el recolector y reactivarlo al salir, solo si estaba activo.
RECOLECTOR_ACTIVO=0
if docker compose ps --status running --services | grep -qx collector; then
  RECOLECTOR_ACTIVO=1
  echo "[$(hora)] Desactivando el recolector"
  docker compose stop collector
fi
reactivar() {
  if [ "$RECOLECTOR_ACTIVO" = "1" ]; then
    echo "[$(hora)] Reactivando el recolector"
    docker compose start collector
  fi
}
trap reactivar EXIT

# Paso 2: copiar el script al contenedor.
docker compose cp "$JS" mongo:/tmp/exp2_modelado.js

# Pasos 3 en adelante: el script cuenta los trades, arma la muestra, calcula la
# consulta y mide todo (si hay menos de N trades, usa los que haya).
echo "[$(hora)] Ejecutando el experimento con N=$N${DIA:+ DIA=$DIA} FRACCION=$FRACCION"
docker compose exec -T -e N="$N" -e DIA="$DIA" -e FRACCION="$FRACCION" mongo \
  mongosh crypto --quiet --file /tmp/exp2_modelado.js

echo "[$(hora)] Listo. La base bench queda con las 3 colecciones para revisarlas."
