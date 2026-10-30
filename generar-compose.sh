#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Uso: $0 <archivo-salida> <cantidad-clientes>" >&2
  exit 1
fi

output_file="$1"
client_count="$2"

if ! [[ "$client_count" =~ ^[0-9]+$ ]]; then
  echo "Error: la cantidad de clientes debe ser un entero no negativo" >&2
  exit 1
fi

root_dir="$(cd "$(dirname "$0")" && pwd)"

cliente() {
  local id=$1
  if [[ ! -d "$root_dir/input/client_${id}" ]]; then
    echo "Aviso: no existe input/client_${id}, el cliente no tendra datos que enviar" >&2
  fi
  cat <<YAML
  client_${id}:
    build:
      context: ./src/
      dockerfile: client/Dockerfile
    container_name: client_${id}
    depends_on:
    - load_balancer
    environment:
    - INPUT_DIR=/datasets
    - OUTPUT_DIR=/output
    - SERVER_HOST=load_balancer
    - SERVER_PORT=5000
    volumes:
    - ../input/client_${id}:/datasets
    - ../output/client_${id}:/output

YAML
}

load_balancer() {
  cat <<'YAML'
  load_balancer:
    build:
      context: ./src/
      dockerfile: loadbalancer/Dockerfile
    container_name: load_balancer
    depends_on:
    - gateway_0
    environment:
    - SERVER_HOST=load_balancer
    - SERVER_PORT=5000
    - GATEWAYS=gateway_0:5678

YAML
}

gateway() {
  cat <<'YAML'
  gateway_0:
    build:
      context: ./src/
      dockerfile: gateway/Dockerfile
    container_name: gateway_0
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
    - ID=0
    - SERVER_HOST=gateway_0
    - SERVER_PORT=5678
    - MOM_HOST=rabbitmq
    - MOM_PORT=5672
    - EXPECTED_QUERIES=4 # Temporal, actualizar cuando se implementen mas queries

YAML
}

date_filter() {
  cat <<'YAML'
  date_filter_0:
    build:
      context: ./src/
      dockerfile: filters/Dockerfile
    container_name: date_filter_0
    depends_on:
    - rabbitmq
    environment:
    - MOM_HOST=rabbitmq
    - MOM_PORT=5672

YAML
}

worker() {
  local name=$1
  local directory=$2
  shift 2
  cat <<YAML
  ${name}:
    build:
      context: ./src/
      dockerfile: ${directory}/Dockerfile
    container_name: ${name}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
    - MOM_HOST=rabbitmq
    - MOM_PORT=5672
YAML
  for variable in "$@"; do
    echo "    - ${variable}"
  done
  echo "    - LOG_LEVEL=info"
  echo
}

rabbitmq() {
  cat <<'YAML'
  rabbitmq:
    build:
      context: ./src/rabbitmq
      dockerfile: Dockerfile
    container_name: rabbitmq
    environment:
    - RABBITMQ_LOG_LEVELS=error
    healthcheck:
      interval: 5s
      retries: 10
      start_period: 50s
      test: rabbitmq-diagnostics check_port_connectivity
      timeout: 3s
    ports:
    - 15672:15672
YAML
}

{
  echo "# GENERADO por generar-compose.sh. No editar a mano: los cambios se hacen en el generador."
  echo "services:"

  for id in $(seq 0 $((client_count - 1))); do
    cliente "$id"
  done

  load_balancer
  gateway
  date_filter
  worker q2_filter_0 q2filter
  worker image_fetcher_0 imagefetcher CHUNK_SIZE=65536
  worker tokenizer_0 tokenizer
  worker q5_avg_partial_0 q5avgpartial
  worker q5_filter_0 q5filter Q5_FILTER_ID=0
  worker q5_avg_aggregator_0 q5avgaggregator Q5_AVG_PARTIAL_AMOUNT=1 Q5_FILTER_AMOUNT=1
  worker q4_word_counter_0 q4wordcounter TOKENIZER_AMOUNT=1
  worker q4_top20_merger_0 q4top20merger Q4_WORD_COUNTER_AMOUNT=1
  rabbitmq
} > "$output_file"

echo "Docker Compose generado en $output_file con $client_count cliente(s)."
