
# syntax=docker/dockerfile:1

ARG GO_VERSION=1.27.1
FROM golang:${GO_VERSION} AS build
WORKDIR /app

RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    go mod download

RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=bind,target=. \
    CGO_ENABLED=0 go build -o /bin/worker .

FROM alpine:3.22 AS final

RUN apk --update add ca-certificates tzdata && update-ca-certificates

ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser
USER appuser

COPY --from=build /bin/worker /bin/

EXPOSE 8080

ENTRYPOINT ["/bin/worker"]
