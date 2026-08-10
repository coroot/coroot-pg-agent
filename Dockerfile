FROM golang:1.26.5-alpine AS builder
RUN apk add --no-cache ca-certificates
COPY go.mod /tmp/src/
COPY go.sum /tmp/src/
WORKDIR /tmp/src/
RUN go mod download
COPY . /tmp/src/
RUN go test ./...
ARG VERSION=unknown
RUN CGO_ENABLED=0 go install -mod=readonly -ldflags "-X main.version=$VERSION" /tmp/src

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /go/bin/coroot-pg-agent /usr/bin/coroot-pg-agent
ENTRYPOINT ["coroot-pg-agent"]
