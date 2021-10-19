FROM golang:latest AS buildContainer
WORKDIR /go/src/app

COPY . .

LABEL maintainer "Lucklyric<asun@whitematrix.io>"

RUN CGO_ENABLED=0 GOOS=linux go build -v -mod mod -ldflags "-s -w" -o restapi .

FROM alpine:latest
WORKDIR /app
COPY --from=buildContainer /go/src/app/restapi .

ENV GIN_MODE release
ENV SPORK_JSON_URL https://raw.githubusercontent.com/Lucklyric/flow-spork-info/main/spork.json

ENV PORT 8989
EXPOSE 8989

CMD ["./restapi"]
