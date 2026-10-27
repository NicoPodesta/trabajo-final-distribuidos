FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY q4top20merger ./q4top20merger
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/q4top20merger ./q4top20merger

FROM busybox:latest
COPY --from=builder /build/bin/q4top20merger /q4top20merger
ENTRYPOINT ["./q4top20merger"]
