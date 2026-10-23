.PHONY: test integration check build up down test-db
TEST_DATABASE_URL ?= postgres://rockets:rockets@127.0.0.1:55432/rockets_test?sslmode=disable

test:
	go test -race ./...

integration:
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -tags=integration ./... -count=1

check:
	go vet ./...
	go test -race ./...

build:
	go build -o bin/rockets ./cmd/rockets

up:
	docker compose up -d --build --wait

down:
	docker compose down

test-db:
	docker compose -f compose.test.yaml -p rockets-test up -d --wait
