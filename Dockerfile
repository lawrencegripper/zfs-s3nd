FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /out/zfs-s3nd ./cmd/zfs-s3nd

FROM debian:bookworm-slim
RUN sed -i 's/Components: main/Components: main contrib non-free-firmware/' /etc/apt/sources.list.d/debian.sources \
  && apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates zfsutils-linux \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /out/zfs-s3nd /usr/local/bin/zfs-s3nd
COPY migrations ./migrations
ENV DATABASE_PATH=/data/catalog.db
ENV HTTP_PORT=3000
ENV SSH_PORT=2222
EXPOSE 3000 2222
CMD ["zfs-s3nd", "serve"]
