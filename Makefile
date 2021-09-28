.PHONY: test lint

lint:
	golangci-lint run -E bodyclose -E gocritic -E gosec -E makezero -E nilerr -E noctx -E prealloc -E unconvert -E unparam -E wastedassign

test:
	go test

update-deps:
	go get -d -u ./...
	go mod tidy
