#!/usr/bin/env bash
# Consume trades EN VIVO de Binance y muestra en tiempo real cómo entran a MongoDB.
# Se detiene al sumar SUMAR trades nuevos o con Ctrl+C; en ambos casos muestra un resumen.
#
#   ./scripts/recolectar.sh               # usa el SUMAR de abajo
#   SUMAR=50000 ./scripts/recolectar.sh   # otro límite
#
# Pensado para después de ./setup.sh: la carga histórica da el volumen y este script muestra la velocidad.
# Al terminar, MongoDB sigue arriba con los datos.

# ── LÍMITE: cuántos trades nuevos consumir en vivo ───────────────────────────────────────────────
# Con un número enorme corre hasta que se corta con Ctrl+C.
SUMAR=${SUMAR:-10000000000}
# ─────────────────────────────────────────────────────────────────────────────────────────────────

set -euo pipefail
export SUMAR
cd "$(dirname "$0")/.."

command -v docker >/dev/null || { echo "Error: Docker no está instalado." >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Error: Docker no está corriendo." >&2; exit 1; }

echo "==> Construyendo imagen"
docker compose --profile live build recolector

echo "==> Levantando MongoDB"
docker compose up -d --wait mongo

# Si el recolector permanente también estuviera corriendo, los mismos trades se guardarían dos veces.
if [[ -n "$(docker compose ps -q collector)" ]]; then
  echo "==> Deteniendo el recolector permanente (collector) para no duplicar trades"
  docker compose stop collector
fi

echo "==> Consumiendo trades en vivo (límite: ${SUMAR}, Ctrl+C para detener)"
docker compose --profile live run --rm recolector

echo "==> Listo. Consola: docker compose exec mongo mongosh crypto"
