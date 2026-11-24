#!/usr/bin/env bash
# ============================================================================
# Experimento 2 (Hernán Muñoz) - Modelado: normal, normal con índice y time-series
# ============================================================================
# Hace todo el experimento con un solo comando:
#   1. desactiva el recolector (si estaba activo) para que no entren trades nuevos,
#   2. copia exp2_modelado.js al contenedor mongo,
#   3. crea A, B y C en la base "bench" y muestra espacio, conteos y tiempos,
#   4. reactiva el recolector al terminar (o si algo falla).
#
# Uso (desde cualquier carpeta; en Windows, Git Bash o WSL):
#   ./experimento-2/exp2_modelado.sh              # 1.000.000 de trades (unos minutos)
#   ./experimento-2/exp2_modelado.sh 100000       # prueba rápida con 100.000
#
# Requiere el MongoDB de la Etapa 1 levantado y con al menos N trades en
# crypto.trades. crypto.trades no se modifica; la base "bench" se borra y se recrea.
# ============================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1                      # evita que Git Bash cambie las rutas /tmp

cd "$(dirname "$0")/.."                        # raíz del repo (donde está docker-compose.yml)
N="${1:-1000000}"
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

# Comprobar que hay suficientes trades.
HAY=$(docker compose exec -T mongo mongosh crypto --quiet --eval 'db.trades.countDocuments({})' | tr -d '\r' | tail -n 1)
echo "[$(hora)] Trades en crypto.trades: $HAY (se usarán $N)"
if [ "$HAY" -lt "$N" ]; then
  echo "Hay menos trades que los pedidos. Use un N menor, por ejemplo: $0 $HAY"
  exit 1
fi

# Paso 2: copiar el script al contenedor.
docker compose cp "$JS" mongo:/tmp/exp2_modelado.js

# Pasos 3 a 8: ejecutar el experimento completo dentro de mongosh.
echo "[$(hora)] Ejecutando el experimento con N=$N"
docker compose exec -T mongo mongosh crypto --quiet \
  --eval "load('/tmp/exp2_modelado.js'); experimento($N)"

echo "[$(hora)] Listo. La base bench queda con las 3 colecciones para revisarlas."