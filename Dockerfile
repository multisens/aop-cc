# Common Core component of the platform. Build context: this repository.

FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/aop-cc ./cmd/aop-cc

FROM alpine:3.22
COPY --from=builder /out/aop-cc /usr/local/bin/aop-cc
USER nobody
CMD ["aop-cc"]
