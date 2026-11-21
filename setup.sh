#!/usr/bin/env bash
# Despliega MongoDB y lo deja listo con TARGET_DOCS trades de Binance (por defecto 10.000.000).
#
#   ./setup.sh                      # MongoDB + 10 millones de trades históricos
#   TARGET_DOCS=500000 ./setup.sh   # otro objetivo
#   ./setup.sh --live               # además deja corriendo el recolector en vivo (seguirá sumando trades)
#
# Es re-ejecutable: si ya hay datos, continúa desde donde quedó.
set -euo pipefail
cd "$(dirname "$0")"

export TARGET_DOCS="${TARGET_DOCS:-10000000}"

command -v docker >/dev/null || { echo "Error: Docker no está instalado." >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Error: Docker no está corriendo." >&2; exit 1; }

echo "==> Construyendo imágenes"
docker compose --profile load build

echo "==> Levantando MongoDB"
docker compose up -d --wait mongo

echo "==> Cargando trades históricos de Binance hasta ${TARGET_DOCS} documentos"
docker compose --profile load run --rm loader

COUNT=$(docker compose exec -T mongo mongosh crypto --quiet --eval 'db.trades.countDocuments()')
echo "==> crypto.trades tiene ${COUNT} documentos"

if [[ "${1:-}" == "--live" ]]; then
  echo "==> Iniciando recolector en vivo"
  docker compose up -d collector
fi

echo "==> Listo. Consola: docker compose exec mongo mongosh crypto"
