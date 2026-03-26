FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go .
RUN CGO_ENABLED=0 go build -o workload .

FROM alpine:3.19
COPY --from=builder /app/workload /usr/local/bin/
ENTRYPOINT ["workload"]
