FROM golang:1.17-alpine3.13 as stage1

RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.ustc.edu.cn/g' /etc/apk/repositories
RUN apk add --no-cache \
    ffmpeg \
    gcc \
    musl-dev

WORKDIR /s
RUN go env -w GOPROXY=https://goproxy.cn,direct

COPY . ./

RUN go mod download
RUN go install github.com/mattn/go-sqlite3
RUN go build -o rtsp-simple-server .
RUN ls

FROM scratch AS export-stage
COPY --from=stage1 /s/rtsp-simple-server .
COPY --from=stage1 /s/rtsp-simple-server.yml .
# COPY --from=stage1 /s/Dockerfile .
# COPY --from=stage1 /s/docker-compose.yml .