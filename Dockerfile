# Use a minimal base image with Go installed
FROM golang:1.26 AS builder

# Set the working directory
WORKDIR /app

# Copy the Go source code
COPY . .

# Build the Go binary
RUN go build -o static-pv-releaser main.go

# Run the webhook
CMD ["/app/static-pv-releaser"]
