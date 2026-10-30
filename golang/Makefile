SHELL := /bin/bash

up:
	docker compose -f docker-compose.yaml config --services | grep '^client_' | sed 's#^#../output/#' | xargs mkdir -p
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

switch:
	@echo Escenarios de prueba:
	@echo "1) 1 cliente, 1 gateway, 1 tokenizer, 1 q4_word_counter"
	@echo "2) 2 clientes, 1 gateway, 1 tokenizer, 1 q4_word_counter"
	@echo "3) 2 clientes, 2 gateways, 1 tokenizer, 1 q4_word_counter"
	@echo "4) 2 clientes, 2 gateways, 3 tokenizers, 2 q4_word_counters"
	@read -p "Selecciona uno [1-4]: " option; \
	case $$option in \
		1) ../generar-compose.sh docker-compose.yaml 1 1 1 1 ;; \
		2) ../generar-compose.sh docker-compose.yaml 2 1 1 1 ;; \
		3) ../generar-compose.sh docker-compose.yaml 2 2 1 1 ;; \
		4) ../generar-compose.sh docker-compose.yaml 2 2 3 2 ;; \
		*) echo "Opcion invalida"; exit 1 ;; \
	esac
.PHONY: switch
