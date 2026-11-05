#!/usr/bin/env bash
set -e

usage() {
  echo "Uso: scripts/notebook.sh              abre Jupyter para ejecutar el notebook a mano"
  echo "     scripts/notebook.sh --cliente N  ejecuta el notebook para un cliente (0, 1 o 2) sin navegador"
  echo "     scripts/notebook.sh --todos      ejecuta el notebook para los tres clientes"
  echo "Variables: INPUT_ROOT (carpeta con client_0, client_1, client_2; por defecto input/),"
  echo "           CLIENT_N_DIR (carpeta de un cliente en particular), PORT (8888), NOTEBOOK_MEMORY (ej. 12g)"
}

root="$(cd "$(dirname "$0")/.." && pwd)"
image="${NOTEBOOK_IMAGE:-wiki-notebook}"
input_root="${INPUT_ROOT:-$root/input}"
port="${PORT:-8888}"

mode="jupyter"
clients=(0 1 2)
case "${1:-}" in
  "") ;;
  --todos) mode="batch" ;;
  --cliente)
    [[ "${2:-}" =~ ^[0-2]$ ]] || { usage; exit 1; }
    mode="batch"
    clients=("$2")
    ;;
  *) usage; exit 1 ;;
esac

if ! docker image inspect "$image" >/dev/null 2>&1; then
  echo "Construyendo la imagen $image (solo la primera vez)..."
  docker build -t "$image" -f "$root/scripts/Dockerfile" "$root/scripts"
fi

mkdir -p "$root/output_notebook"

mounts=()
for n in 0 1 2; do
  variable="CLIENT_${n}_DIR"
  dir="${!variable:-$input_root/client_$n}"
  if [[ -d "$dir" ]]; then
    mounts+=(-v "$(realpath "$dir"):/data/client_$n:ro")
  else
    echo "Aviso: no existe $dir, el cliente $n no se va a poder ejecutar" >&2
  fi
done

memory=()
if [[ -n "${NOTEBOOK_MEMORY:-}" ]]; then
  memory=(-m "$NOTEBOOK_MEMORY" --memory-swap "$NOTEBOOK_MEMORY")
fi

terminal=()
if [[ -t 0 ]]; then
  terminal=(-it)
fi

run=(docker run --rm "${terminal[@]}" -u "$(id -u):$(id -g)" -e HOME=/tmp "${memory[@]}"
  -v "$root/scripts:/repo/scripts:ro" -v "$root/output_notebook:/repo/output_notebook" "${mounts[@]}")

if [[ "$mode" == "jupyter" ]]; then
  echo "Abri la URL con el token que aparece abajo, abri analisis_cliente.ipynb y cambia CLIENT en la primera celda."
  echo "Los resultados quedan en output_notebook/client_N/. Para terminar: Ctrl+C."
  "${run[@]}" -p "127.0.0.1:$port:8888" "$image" bash -c \
    'mkdir -p /tmp/work && cp /repo/scripts/analisis_cliente.ipynb /tmp/work/ && exec jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --notebook-dir /tmp/work'
  exit 0
fi

failed=0
for n in "${clients[@]}"; do
  echo "== client_$n"
  mkdir -p "$root/output_notebook/client_$n"
  rm -f "$root"/output_notebook/client_"$n"/q*.csv "$root"/output_notebook/client_"$n"/q5_*.txt
  "${run[@]}" -e CLIENT="$n" "$image" jupyter nbconvert --to notebook --execute --allow-errors \
    /repo/scripts/analisis_cliente.ipynb --output executed.ipynb --output-dir "/repo/output_notebook/client_$n" 2>&1 \
    | grep -v -E "Warning|warn|NbConvertApp\] (Converting|Writing)|Kernel is running" || true
  for file in q1.csv q4.csv; do
    if [[ ! -f "$root/output_notebook/client_$n/$file" ]]; then
      echo "ERROR: no se genero $file para el cliente $n; revisar output_notebook/client_$n/executed.ipynb" >&2
      failed=1
    fi
  done
done
exit $failed
