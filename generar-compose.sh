#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 5 ]]; then
  echo "Uso: $0 <archivo-salida> <cantidad-clientes> [cantidad-gateways] [cantidad-tokenizers] [cantidad-q4-wordcounters]" >&2
  exit 1
fi

output_file="$1"
client_count="$2"
gateway_count="${3:-1}"
tokenizer_count="${4:-1}"
q4_counter_count="${5:-1}"

for val in "$client_count" "$gateway_count" "$tokenizer_count" "$q4_counter_count"; do
  if ! [[ "$val" =~ ^[0-9]+$ ]] || [[ "$val" -lt 1 ]]; then
    echo "Error: los conteos deben ser enteros positivos" >&2
    exit 1
  fi
done

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
    user: "$(id -u):$(id -g)"
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
  local count=$1
  local gateways_list=""
  for id in $(seq 0 $((count - 1))); do
    if [[ -n "$gateways_list" ]]; then
      gateways_list="${gateways_list},gateway_${id}:5678"
    else
      gateways_list="gateway_${id}:5678"
    fi
  done

  cat <<YAML
  load_balancer:
    build:
      context: ./src/
      dockerfile: loadbalancer/Dockerfile
    container_name: load_balancer
    depends_on:
YAML
  for id in $(seq 0 $((count - 1))); do
    echo "    - gateway_${id}"
  done
  cat <<YAML
    environment:
    - SERVER_HOST=load_balancer
    - SERVER_PORT=5000
    - GATEWAYS=${gateways_list}

YAML
}

gateway() {
  local id=$1
  cat <<YAML
  gateway_${id}:
    build:
      context: ./src/
      dockerfile: gateway/Dockerfile
    container_name: gateway_${id}
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
    - ID=${id}
    - SERVER_HOST=gateway_${id}
    - SERVER_PORT=5678
    - MOM_HOST=rabbitmq
    - MOM_PORT=5672
    - EXPECTED_QUERIES=5

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

  load_balancer "$gateway_count"

  for id in $(seq 0 $((gateway_count - 1))); do
    gateway "$id"
  done

  worker q1_filter_0 q1filter
  worker q2_filter_0 q2filter
  worker q2_image_fetcher_0 q2imagefetcher CHUNK_SIZE=65536

  for id in $(seq 0 $((tokenizer_count - 1))); do
    worker "tokenizer_${id}" tokenizer "TOKENIZER_ID=${id}" "TOKENIZER_AMOUNT=${tokenizer_count}" "Q4_WORD_COUNTER_AMOUNT=${q4_counter_count}"
  done

  worker q3_vocabulary_join_0 q3vocabularyjoin "TOKENIZER_AMOUNT=${tokenizer_count}"
  worker q5_avg_partial_0 q5avgpartial "TOKENIZER_AMOUNT=${tokenizer_count}"
  worker q5_filter_0 q5filter ID=0 "TOKENIZER_AMOUNT=${tokenizer_count}"
  worker q5_avg_aggregator_0 q5avgaggregator Q5_AVG_PARTIAL_AMOUNT=1 Q5_FILTER_AMOUNT=1

  for id in $(seq 0 $((q4_counter_count - 1))); do
    worker "q4_word_counter_${id}" q4wordcounter "Q4_WORD_COUNTER_ID=${id}" "TOKENIZER_AMOUNT=${tokenizer_count}"
  done

  worker q4_top20_merger_0 q4top20merger "Q4_WORD_COUNTER_AMOUNT=${q4_counter_count}"
  rabbitmq
} > "$output_file"

echo "Docker Compose generado en $output_file con $client_count cliente(s), $gateway_count gateway(s), $tokenizer_count tokenizer(s) y $q4_counter_count q4-counter(s)."
