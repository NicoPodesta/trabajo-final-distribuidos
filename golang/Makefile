SHELL := /bin/bash

up:
	docker compose -f docker-compose.yaml up --build --remove-orphans --detach --wait
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs
