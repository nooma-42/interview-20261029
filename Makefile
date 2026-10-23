.PHONY: test integration check build up down test-db

test:
	go test -race ./...

integration:
	@set -a; . ./.env; set +a; \
	PGUSER="$$POSTGRES_USER" PGPASSWORD="$$POSTGRES_PASSWORD" PGDATABASE="$${POSTGRES_DB}_test" \
	TEST_DATABASE_URL="$${TEST_DATABASE_URL:-postgres://127.0.0.1:55432/?sslmode=disable}" \
	go test -race -tags=integration ./... -count=1

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
