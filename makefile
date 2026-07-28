POSTGRES_USERNAME:=root
POSTGRES_PASSWORD:=password
GIT_SHA:=$(shell git rev-parse --short HEAD)
CRAWLER_IMAGE_NAME:=rivenbot/crawler
PROXY_IMAGE_NAME:=rivenbot/proxy

export

.PHONY: build-crawler
build-crawler:
	docker buildx build --platform linux/arm64,linux/amd64 -t $(CRAWLER_IMAGE_NAME):latest -t $(CRAWLER_IMAGE_NAME):$(GIT_SHA) --build-arg SERVICE=crawler .

.PHONY: push-crawler
push-crawler: build-crawler
	docker push $(CRAWLER_IMAGE_NAME):latest 
	docker push $(CRAWLER_IMAGE_NAME):$(GIT_SHA)

.PHONY: build-proxy
build-proxy:
	docker buildx build --platform linux/arm64,linux/amd64 -t $(PROXY_IMAGE_NAME):latest -t $(PROXY_IMAGE_NAME):$(GIT_SHA) --build-arg SERVICE=proxy .

.PHONY: push-proxy
push-proxy: build-proxy
	docker push $(PROXY_IMAGE_NAME):latest
	docker push $(PROXY_IMAGE_NAME):$(GIT_SHA)

.PHONY: migrate
build-migrate:
	go build -o bin/migrate cmd/migrate/main.go

.PHONY: run-migrate
run-migrate: build-migrate
	docker compose up -d
	bin/migrate

.PHONY: up
up:
	docker compose up --detach --build

.PHONY: watch
watch:
	docker compose watch processing-service
