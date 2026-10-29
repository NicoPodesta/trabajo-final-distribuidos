FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY q2filter ./q2filter
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -o bin/q2filter ./q2filter

FROM busybox:latest
COPY --from=builder /build/bin/q2filter /q2filter
ENTRYPOINT ["./q2filter"]
