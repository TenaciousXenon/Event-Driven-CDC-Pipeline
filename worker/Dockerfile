# syntax=docker/dockerfile:1
FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/worker . && \
    CGO_ENABLED=0 go build -trimpath -o /out/reindex ./cmd/reindex
FROM alpine:3.22 AS final
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
USER app
COPY --from=build /out/ /bin/
ENTRYPOINT ["/bin/worker"]
