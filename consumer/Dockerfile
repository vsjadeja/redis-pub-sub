# build stage
# Stage 1: Build
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Cache go modules
COPY ./go.mod ./go.sum ./
RUN go mod download

# Copy all source files
COPY . .

# Get build metadata
ARG VERSION

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X 'main.version=${VERSION}'" -o consumers cmd/consumer/main.go


# Stage 2: Run
FROM alpine:3.18

WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/consumers .

# Expose health check port
EXPOSE 8083

# Run server
CMD ["./consumers"]
