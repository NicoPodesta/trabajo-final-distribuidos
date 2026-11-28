#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 13 ]]; then
  echo "Uso: $0 <archivo-salida> [cantidad-clientes] [cantidad-gateways] [cantidad-tokenizers] [cantidad-q4-wordcounters] [cantidad-q5-avgpartials] [cantidad-q5-filters] [cantidad-q1-filters] [cantidad-q2-filters] [cantidad-q2-fetchers] [cantidad-q3-joins] [cantidad-load-balancers] [cantidad-monitores]" >&2
  exit 1
fi

output_file="$1"
client_count="${2:-3}"
gateway_count="${3:-3}"
tokenizer_count="${4:-3}"
q4_counter_count="${5:-3}"
q5_avg_partial_count="${6:-1}"
q5_filter_count="${7:-1}"
q1_filter_count="${8:-1}"
q2_filter_count="${9:-1}"
q2_fetcher_count="${10:-1}"
q3_join_count="${11:-1}"
lb_count="${12:-1}"
monitor_count="${13:-1}"

for val in "$client_count" "$gateway_count" "$tokenizer_count" "$q4_counter_count" "$q5_avg_partial_count" "$q5_filter_count" "$q1_filter_count" "$q2_filter_count" "$q2_fetcher_count" "$q3_join_count" "$lb_count" "$monitor_count"; do
  if ! [[ "$val" =~ ^[0-9]+$ ]] || [[ "$val" -lt 1 ]]; then
    echo "Error: los conteos deben ser enteros positivos" >&2
    exit 1
  fi
done

root_dir="$(cd "$(dirname "$0")" && pwd)"
data_volumes=()

lb_service_name() {
  local id=$1
  local total=$2
  if [[ "$total" -eq 1 ]]; then
    echo "load_balancer"
  else
    echo "load_balancer_${id}"
  fi
}

cliente() {
  local id=$1
  local total_lbs=$2
  if [[ ! -d "$root_dir/input/client_${id}" ]]; then
    echo "Aviso: no existe input/client_${id}, el cliente no tendra datos que enviar" >&2
  fi

  local lbs_list=""
  for lb_id in $(seq 0 $((total_lbs - 1))); do
    local svc
    svc=$(lb_service_name "$lb_id" "$total_lbs")
    if [[ -n "$lbs_list" ]]; then
      lbs_list="${lbs_list},${svc}:5000"
    else
      lbs_list="${svc}:5000"
    fi
  done

  cat <<YAML
  client_${id}:
    build:
      context: ./src/
      dockerfile: client/Dockerfile
    container_name: client_${id}
    user: "$(id -u):$(id -g)"
    depends_on:
YAML
  for lb_id in $(seq 0 $((total_lbs - 1))); do
    echo "    - $(lb_service_name "$lb_id" "$total_lbs")"
  done
  cat <<YAML
    environment:
    - INPUT_DIR=/datasets
    - OUTPUT_DIR=/output
YAML
  if [[ "$total_lbs" -eq 1 ]]; then
    cat <<YAML
    - SERVER_HOST=load_balancer
    - SERVER_PORT=5000
YAML
  else
    echo "    - LOAD_BALANCERS=${lbs_list}"
  fi
  cat <<YAML
    volumes:
    - ../input/client_${id}:/datasets
    - ../output/client_${id}:/output

YAML
}

load_balancer() {
  local id=$1
  local gw_count=$2
  local total_lbs=$3
  local svc
  svc=$(lb_service_name "$id" "$total_lbs")

  local gateways_list=""
  for gw_id in $(seq 0 $((gw_count - 1))); do
    if [[ -n "$gateways_list" ]]; then
      gateways_list="${gateways_list},gateway_${gw_id}:5678"
    else
      gateways_list="gateway_${gw_id}:5678"
    fi
  done

  cat <<YAML
  ${svc}:
    build:
      context: ./src/
      dockerfile: loadbalancer/Dockerfile
    container_name: ${svc}
    depends_on:
YAML
  for gw_id in $(seq 0 $((gw_count - 1))); do
    echo "    - gateway_${gw_id}"
  done
  cat <<YAML
    environment:
    - LB_ID=${id}
    - SERVER_HOST=${svc}
    - SERVER_PORT=5000
    - GATEWAYS=${gateways_list}
    - HEALTH_PORT=9090

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
    - Q1_FILTER_AMOUNT=${q1_filter_count}
    - Q2_FETCHER_AMOUNT=${q2_fetcher_count}
    - Q3_JOIN_AMOUNT=${q3_join_count}
    - Q5_FILTER_AMOUNT=${q5_filter_count}
    - HEALTH_PORT=9090

YAML
}

monitor() {
  local id=$1
  local total_lbs=$2
  local gw_count=$3
  local q4_counter_count=$4
  local tokenizer_count=$5
  local q5_avg_partial_count=$6
  local q5_filter_count=$7
  local q1_filter_count=$8
  local total_monitors=$9

  local peers=()
  for mon_id in $(seq 0 $((total_monitors - 1))); do
    if [[ "$mon_id" -eq "$id" ]]; then
      continue
    fi
    peers+=("${mon_id}=monitor_${mon_id}")
  done
  local peers_list=""
  if [[ ${#peers[@]} -gt 0 ]]; then
    peers_list=$(IFS=,; echo "${peers[*]}")
  fi

  local targets=()
  for lb_id in $(seq 0 $((total_lbs - 1))); do
    targets+=("$(lb_service_name "$lb_id" "$total_lbs")")
  done
  for gw_id in $(seq 0 $((gw_count - 1))); do
    targets+=("gateway_${gw_id}")
  done
  for filter_id in $(seq 0 $((q1_filter_count - 1))); do
    targets+=("q1_filter_${filter_id}")
  done
  for counter_id in $(seq 0 $((q4_counter_count - 1))); do
    targets+=("q4_word_counter_${counter_id}")
  done
  targets+=("q4_top20_merger")
  for tokenizer_id in $(seq 0 $((tokenizer_count - 1))); do
    targets+=("tokenizer_${tokenizer_id}")
  done
  for partial_id in $(seq 0 $((q5_avg_partial_count - 1))); do
    targets+=("q5_avg_partial_${partial_id}")
  done
  targets+=("q5_avg_aggregator_0")
  for filter_id in $(seq 0 $((q5_filter_count - 1))); do
    targets+=("q5_filter_${filter_id}")
  done
  local targets_list
  targets_list=$(IFS=,; echo "${targets[*]}")

  cat <<YAML
  monitor_${id}:
    build:
      context: ./src/
      dockerfile: monitor/Dockerfile
    container_name: monitor_${id}
    depends_on:
YAML
  for target in "${targets[@]}"; do
    echo "    - ${target}"
  done
  cat <<YAML
    environment:
    - MONITOR_ID=${id}
    - MONITOR_PORT=9091
    - HEALTH_PORT=9090
YAML
  if [[ -n "$peers_list" ]]; then
    echo "    - PEERS=${peers_list}"
  fi
  cat <<YAML
    - TARGETS=${targets_list}
    - LOG_LEVEL=info
    volumes:
    - /var/run/docker.sock:/var/run/docker.sock

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
  if [[ -n "${data_volume:-}" ]]; then
    echo "    volumes:"
    echo "    - ${data_volume}:/data"
  fi
  echo
}

stateful_worker() {
  local data_volume="$1_data"
  data_volumes+=("$data_volume")
  worker "$@" "DATA_DIR=/data"
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
    cliente "$id" "$lb_count"
  done

  for id in $(seq 0 $((lb_count - 1))); do
    load_balancer "$id" "$gateway_count" "$lb_count"
  done

  for id in $(seq 0 $((gateway_count - 1))); do
    gateway "$id"
  done

  for id in $(seq 0 $((monitor_count - 1))); do
    monitor "$id" "$lb_count" "$gateway_count" "$q4_counter_count" "$tokenizer_count" "$q5_avg_partial_count" "$q5_filter_count" "$q1_filter_count" "$monitor_count"
  done

  for id in $(seq 0 $((q1_filter_count - 1))); do
    worker "q1_filter_${id}" q1filter "Q1_FILTER_ID=${id}" "Q1_FILTER_AMOUNT=${q1_filter_count}" 'Q1_PREFETCH=${Q1_PREFETCH:-4}' "HEALTH_PORT=9090"
  done
  for id in $(seq 0 $((q2_filter_count - 1))); do
    worker "q2_filter_${id}" q2filter "Q2_FILTER_ID=${id}" "Q2_FILTER_AMOUNT=${q2_filter_count}" 'Q2_PREFETCH=${Q2_PREFETCH:-4}' "Q2_FETCHER_AMOUNT=${q2_fetcher_count}"
  done
  for id in $(seq 0 $((q2_fetcher_count - 1))); do
    worker "q2_image_fetcher_${id}" q2imagefetcher CHUNK_SIZE=65536 'Q2_MAX_PENDING_REQUESTS=${Q2_MAX_PENDING_REQUESTS:-4}' 'Q2_DOWNLOAD_WORKERS=${Q2_DOWNLOAD_WORKERS:-3}' "Q2_FILTER_AMOUNT=${q2_filter_count}" "Q2_FETCHER_ID=${id}" "Q2_FETCHER_AMOUNT=${q2_fetcher_count}"
  done

  for id in $(seq 0 $((tokenizer_count - 1))); do
    worker "tokenizer_${id}" tokenizer "TOKENIZER_ID=${id}" "TOKENIZER_AMOUNT=${tokenizer_count}" "Q4_WORD_COUNTER_AMOUNT=${q4_counter_count}" "Q3_JOIN_AMOUNT=${q3_join_count}" "Q5_AVG_PARTIAL_AMOUNT=${q5_avg_partial_count}" "TOKENIZER_PREFETCH=20" "HEALTH_PORT=9090"
  done

  for id in $(seq 0 $((q3_join_count - 1))); do
    worker "q3_vocabulary_join_${id}" q3vocabularyjoin "Q3_JOIN_ID=${id}" "TOKENIZER_AMOUNT=${tokenizer_count}" "Q3_JOIN_PREFETCH=20"
  done
  for id in $(seq 0 $((q5_avg_partial_count - 1))); do
    stateful_worker "q5_avg_partial_${id}" q5avgpartial "Q5_AVG_PARTIAL_ID=${id}" "Q5_AVG_PARTIAL_AMOUNT=${q5_avg_partial_count}" "Q5_AVG_PARTIAL_PREFETCH=20" "HEALTH_PORT=9090"
  done
  for id in $(seq 0 $((q5_filter_count - 1))); do
    worker "q5_filter_${id}" q5filter "Q5_FILTER_ID=${id}" "Q5_FILTER_AMOUNT=${q5_filter_count}" "Q5_PENDING_DIR=/tmp/q5filter" "Q5_FILTER_PREFETCH=20" "HEALTH_PORT=9090"
  done
  stateful_worker q5_avg_aggregator_0 q5avgaggregator "Q5_AVG_PARTIAL_AMOUNT=${q5_avg_partial_count}" "Q5_FILTER_AMOUNT=${q5_filter_count}" "HEALTH_PORT=9090"

  for id in $(seq 0 $((q4_counter_count - 1))); do
    worker "q4_word_counter_${id}" q4wordcounter "Q4_WORD_COUNTER_ID=${id}" "Q4_WORD_COUNTER_PREFETCH=20" "HEALTH_PORT=9090"
  done

  worker q4_top20_merger q4top20merger "Q4_WORD_COUNTER_AMOUNT=${q4_counter_count}" "HEALTH_PORT=9090"
  rabbitmq
  if [[ ${#data_volumes[@]} -gt 0 ]]; then
    echo "volumes:"
    for data_volume in "${data_volumes[@]}"; do
      echo "  ${data_volume}:"
    done
  fi
} > "$output_file"

echo "Docker Compose generado en $output_file con $client_count cliente(s), $lb_count load-balancer(s), $gateway_count gateway(s), $monitor_count monitor(es), $tokenizer_count tokenizer(s), $q4_counter_count q4-counter(s), $q5_avg_partial_count q5-avgpartial(s), $q5_filter_count q5-filter(s), $q1_filter_count q1-filter(s), $q2_filter_count q2-filter(s), $q2_fetcher_count q2-fetcher(s) y $q3_join_count q3-join(s)."
