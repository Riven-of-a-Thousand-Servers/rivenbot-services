POSTGRES_USERNAME:=root
POSTGRES_PASSWORD:=password
GIT_SHA:=$(shell git rev-parse --short HEAD)
IMAGE_NAME:=rivenbot/rivenbot-cli

export

.PHONY: docker-build
docker-build:
	docker buildx build --platform linux/arm64,linux/amd64 -t $(IMAGE_NAME):latest -t $(IMAGE_NAME):$(GIT_SHA) .

.PHONY: docker-push
docker-push: docker-build
	docker push $(IMAGE_NAME):latest 
	docker push $(IMAGE_NAME):$(GIT_SHA)

.PHONY: run-migrate
run-migrate: build-migrate
	docker compose up -d
	bin/migrate

.PHONY: build
build:
	go build -o bin/rivenbot ./main.go

.PHONY: up
up:
	docker compose up --detach --build

.PHONY: watch
watch:
	docker compose watch rivenbot
