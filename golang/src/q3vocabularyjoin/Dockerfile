FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY q3vocabularyjoin ./q3vocabularyjoin
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -o bin/q3vocabularyjoin ./q3vocabularyjoin

FROM busybox:latest
COPY --from=builder /build/bin/q3vocabularyjoin /q3vocabularyjoin
ENTRYPOINT ["./q3vocabularyjoin"]
