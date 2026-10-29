FROM golang:1.24-alpine3.23 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY imagefetcher ./imagefetcher
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -o bin/imagefetcher ./imagefetcher

FROM busybox:latest
COPY --from=builder /build/bin/imagefetcher /imagefetcher
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN mkdir -p /tmp
ENTRYPOINT ["./imagefetcher"]
