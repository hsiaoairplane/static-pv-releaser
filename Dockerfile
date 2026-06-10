# Build stage — run on the native build platform and cross-compile
FROM --platform=$BUILDPLATFORM golang:1.26 AS builder

# Provided automatically by buildx
ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

# Cache dependencies separately from the source
COPY go.mod go.sum ./
RUN go mod download

# Build a static binary so it runs on a scratch/distroless base
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /static-pv-releaser .

# Run stage
FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /static-pv-releaser /static-pv-releaser

USER 65532:65532

ENTRYPOINT ["/static-pv-releaser"]
