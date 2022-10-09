## help: print this help message
help:
	@echo "Usage:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ":" | sed -e 's/^/  /'

## lint: runs golangci lint based on .golangci.yml configuration
## TODO we could add auto install for golangci-lint. assuming already installed is wrong. May be we could add make init and install all related tools.
.PHONY: lint
lint:
	golangci-lint run -c .golangci.yml  --fix -v

## test: runs tests
.PHONY: test
test:
	go test -v ./... -coverprofile=unit_coverage.out -short

## unit-coverage-html: extract unit tests coverage to html format
.PHONY: unit-coverage-html
unit-coverage-html:
	make test
	go tool cover -html=unit_coverage.out -o unit_coverage.html

exceptionTopicName = 'exception'

## produce: produce test message
## TODO jq and kafka console producer could not be installed. above TODO is valid for this one too.
.PHONY: produce
produce:
	jq -rc . internal/exception/testdata/message.json | kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic ${exceptionTopicName}

## produce: produce test message with retry header
.PHONY: produce-with-header
produce-with-header:
	jq -rc . internal/exception/testdata/message.json | kcat -b 127.0.0.1:9092 -t ${exceptionTopicName} -P -H x-retry-count=1