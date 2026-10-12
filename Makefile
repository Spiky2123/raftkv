.PHONY: test race stress bench

test:
	go test ./...

race:
	go test -race ./...

stress:
    go test -race -count=100 -timeout 20m -v $(ARGS)

bench:
	go test -bench=. -benchmem ./...
