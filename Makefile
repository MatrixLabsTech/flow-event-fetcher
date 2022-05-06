.PHONY: build-service
build-service:
	go build -o flow-event-fetcher-service

.PHONY: proto-gen
proto-gen:
	protoc --go_out=plugins=grpc:. proto/v1/*.proto --go_opt=paths=source_relative