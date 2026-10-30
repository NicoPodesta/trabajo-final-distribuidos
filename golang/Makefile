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
	@echo "1) Un cliente"
	@echo "2) Dos clientes"
	@echo "3) Un cliente, tres tokenizers"
	@echo "4) Dos clientes, tres tokenizers"
	@read -p "Selecciona uno [1-4]: " option; \
	case $$option in \
		1) ../generar-compose.sh docker-compose.yaml 1 ;; \
		2) ../generar-compose.sh docker-compose.yaml 2 ;; \
		3) ../generar-compose.sh docker-compose.yaml 1 3 ;; \
		4) ../generar-compose.sh docker-compose.yaml 2 3 ;; \
		*) echo "Opcion invalida"; exit 1 ;; \
	esac
.PHONY: switch
