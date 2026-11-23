#!/usr/bin/env bash
# ============================================================================
# Experimento 3 (Demian) - Tolerancia a fallos
# ============================================================================
# Pregunta: ¿cuánto tarda MongoDB en recuperarse si cae el primario?
#
# Se modifica : el write concern, w:1 frente a w:"majority" (3 repeticiones c/u).
# Se mide     : segundos sin poder escribir y trades perdidos (confirmados al
#               cliente pero borrados después por el rollback del nodo caído).
# Se mantiene : replica set de 3 nodos nuevo, los mismos trades reales, lotes de
#               100 a 2.000 trades/s, el primario se mata con `docker kill` a
#               los 5 s de escribir y se vuelve a levantar 3 s después de la
#               elección del nuevo.
#
# Requisito: el MongoDB principal arriba y con datos (./setup.sh en la raíz).
# Uso, desde la raíz del repo:
#
#   ./experimento-3/exp3_failover.sh                  # w:1 y w:"majority", 3 veces c/u (~6 min)
#   REPETICIONES=1 ./experimento-3/exp3_failover.sh   # una vez cada uno
#   TASA=5000 DURACION=60 ./experimento-3/exp3_failover.sh
#
# Levanta su propio replica set (rs1, rs2, rs3) y lo borra al terminar:
# crypto.trades nunca se modifica.
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")"

REPETICIONES=${REPETICIONES:-3}
MODOS=${MODOS:-"1 majority"}
TASA=${TASA:-2000}          # trades por segundo que escribe el escritor
DURACION=${DURACION:-40}    # segundos que escribe en cada corrida
DC="docker compose"

# Servicio que es primario ahora (rs1, rs2 o rs3), o vacío si no hay.
primario() {
  local s p
  for s in rs1 rs2 rs3; do
    p=$($DC exec -T "$s" mongosh --quiet --eval 'db.hello().primary || ""' 2>/dev/null) || continue
    if [[ -n "$p" ]]; then echo "${p%%:*}"; return; fi
  done
}

# Espera a que haya un primario distinto de $1 (si se pasa).
esperar_primario() {
  local p
  while :; do p=$(primario); [[ -n "$p" && "$p" != "${1:-}" ]] && return; sleep 0.5; done
}

command -v docker >/dev/null || { echo "Error: Docker no está instalado." >&2; exit 1; }

# Al terminar (bien, con error o con Ctrl+C) se borra el replica set del experimento.
trap 'echo "==> Borrando el replica set del experimento"; $DC --profile exp down -v --remove-orphans >/dev/null 2>&1' EXIT

echo "==> Levantando replica set de 3 nodos (rs1, rs2, rs3)"
$DC --profile exp down -v --remove-orphans >/dev/null 2>&1 || true
$DC --profile exp build -q escritor
$DC up -d --wait rs1 rs2 rs3
$DC exec -T rs1 mongosh --quiet --eval '
  rs.initiate({_id: "rs0", members: [
    {_id: 0, host: "rs1:27017"}, {_id: 1, host: "rs2:27017"}, {_id: 2, host: "rs3:27017"}]})' >/dev/null
esperar_primario
echo "==> Primario inicial: $(primario)"

RESULTADOS=()
for modo in $MODOS; do
  for rep in $(seq 1 "$REPETICIONES"); do
    echo
    echo "======== w:${modo} - repetición ${rep}/${REPETICIONES} ========"
    LOG=$(mktemp)
    $DC --profile exp run --rm -T -e TASA="$TASA" -e DURACION="$DURACION" \
      escritor python -u escritor.py "$modo" "$rep" 2>&1 | tee "$LOG" &
    PID=$!
    until grep -q "Escribiendo" "$LOG"; do
      kill -0 "$PID" 2>/dev/null || { echo "Error: el escritor terminó antes de empezar" >&2; exit 1; }
      sleep 0.5
    done
    sleep 5
    CAIDO=$(primario)
    echo "==> [$(date -u +%T)] Se mata el primario: ${CAIDO}"
    $DC kill "$CAIDO" >/dev/null 2>&1
    esperar_primario "$CAIDO"
    echo "==> [$(date -u +%T)] Nuevo primario: $(primario)"
    sleep 3
    echo "==> [$(date -u +%T)] Se vuelve a levantar ${CAIDO} (hará rollback de lo que no replicó)"
    $DC start "$CAIDO" >/dev/null 2>&1
    wait "$PID"
    RESULTADOS+=("$(grep '^RESULTADO' "$LOG")")
    rm -f "$LOG"
  done
done

echo
echo "RESULTADOS"
printf '%-12s | %3s | %17s | %11s | %8s\n' "Write concern" "Rep" "Seg. sin escribir" "Confirmados" "Perdidos"
for r in "${RESULTADOS[@]}"; do
  IFS='|' read -r _ modo rep seg total perd <<<"$r"
  printf '%-12s | %3s | %17s | %11s | %8s\n' "$modo" "$rep" "$seg" "$total" "$perd"
done
echo
echo "PROMEDIOS"
printf '%s\n' "${RESULTADOS[@]}" | awk -F'|' '
  { s[$2] += $4; p[$2] += $6; c[$2]++; if (!($2 in o)) { o[$2] = ++k; m[k] = $2 } }
  END { for (i = 1; i <= k; i++) printf "%-12s | %.2f s sin escribir | %.1f trades perdidos\n", m[i], s[m[i]]/c[m[i]], p[m[i]]/c[m[i]] }'
echo
