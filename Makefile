.PHONY: proto test coverage

GOCACHE ?= /private/tmp/taskq-go-cache
COVERPROFILE ?= /private/tmp/taskq-cover.out
COVERFILTER ?= /private/tmp/taskq-cover-filtered.out
COVERAGE_MIN ?= 80

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		matching/matching.proto

test:
	GOCACHE=$(GOCACHE) go test ./...

coverage:
	GOCACHE=$(GOCACHE) go test ./... -coverpkg=./... -coverprofile=$(COVERPROFILE)
	grep -v 'matching/matching.pb.go' $(COVERPROFILE) | \
		grep -v 'matching/matching_grpc.pb.go' | \
		grep -v 'internal/example/main.go' | \
		grep -v 'membership/example/example_usage.go' > $(COVERFILTER)
	@pct=$$(go tool cover -func=$(COVERFILTER) | awk '/^total:/ {gsub("%","",$$3); print $$3}'); \
	awk -v pct="$$pct" -v min="$(COVERAGE_MIN)" 'BEGIN { if (pct + 0 < min + 0) exit 1 }'; \
	echo "coverage: $$pct% >= $(COVERAGE_MIN)%"
