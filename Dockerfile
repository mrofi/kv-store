# syntax=docker/dockerfile:1

# Cross-compiles natively on the build host's platform (fast, no QEMU
# needed for the Go compile step itself) and produces a static binary
# for whatever platform buildx is targeting.
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/kvstore ./cmd/kvstore

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kvstore /kvstore

EXPOSE 8080
ENTRYPOINT ["/kvstore"]
