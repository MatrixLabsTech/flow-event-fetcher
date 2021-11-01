FROM golang:latest AS buildContainer
WORKDIR /go/src/app

COPY . .

LABEL maintainer "Lucklyric<asun@whitematrix.io>"

RUN CGO_ENABLED=0 GOOS=linux go build -v -mod mod -ldflags "-s -w" -o restapi .

FROM alpine:latest
WORKDIR /app
COPY --from=buildContainer /go/src/app/restapi .

ENV GIN_MODE release

ENV PORT 8989
EXPOSE 8989

CMD ["sh","-c", "./restapi -port ${PORT} -sporkUrl ${SPORK_URL}"]
