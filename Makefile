export DATABASE_URL ?= postgres://postgres@localhost:5544/triggerinbox?sslmode=disable

.PHONY: db-up db-down psql test run loadtest

db-up:
	./scripts/db.sh up

db-down:
	./scripts/db.sh down

psql:
	./scripts/db.sh psql

test:
	go test -count=1 ./...

run:
	go run ./cmd/inbox

loadtest:
	go run ./cmd/loadtest
