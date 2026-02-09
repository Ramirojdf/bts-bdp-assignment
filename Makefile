.PHONY: run test build_docker run_docker stop_docker

.DEFAULT_GOAL:=help

SHELL=bash

run:
	uvicorn bdi_api.app:app --proxy-headers --host 0.0.0.0 --port 8080

test:
	pytest --cov=bdi_api --cov-report=html

build_docker:
	docker build -t bdi-api:latest -f docker/Dockerfile .

run_docker: build_docker
	docker run -p 8080:8080 bdi-api:latest

stop_docker:
	docker stop $$(docker ps -q --filter ancestor=bdi-api:latest)
