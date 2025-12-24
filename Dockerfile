FROM golang:1.25.5-alpine AS builder

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY consumer consumer

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o consumer ./consumer

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /app/consumer .
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

USER nonroot:nonroot

ENTRYPOINT ["/app/consumer"]
