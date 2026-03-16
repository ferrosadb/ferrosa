FROM golang:1.22-alpine

WORKDIR /tests
COPY go.mod go.sum ./
RUN go mod download

COPY main_test.go ./

CMD ["go", "test", "-v", "-count=1", "./..."]
