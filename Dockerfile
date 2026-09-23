# syntax=docker/dockerfile:1

# ---- 构建阶段 ----
FROM golang:1.25-alpine AS builder

WORKDIR /src

# 先复制依赖清单，利用层缓存：源码变更时不必重新下载依赖。
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# 纯静态二进制：CGO_ENABLED=0 让镜像可以直接跑在 alpine/scratch 上。
#
# SQLite 驱动选用 modernc.org/sqlite（**纯 Go 实现**），正是为了保住这一点：
# 常见的 mattn/go-sqlite3 需要 CGO，会引入 glibc/musl 动态依赖，
# 从而失去「单文件静态二进制」这个部署优势（也拖慢构建）。
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath -ldflags="-s -w" \
      -o /out/p2psession-server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath -ldflags="-s -w" \
      -o /out/p2p-node ./cmd/p2p-node

# ---- 运行阶段 ----
FROM alpine:3.20

# wget 供 HEALTHCHECK；ca-certificates 供将来出站 TLS；tzdata 让日志时间正确。
RUN apk add --no-cache ca-certificates wget tzdata && \
    addgroup -S app && adduser -S -G app app && \
    # 数据目录必须在镜像里就建好并归 app 所有：
    # 容器以非 root（app）运行，无法自己在 / 下创建目录；
    # 而 Docker 初始化**具名卷**时会继承镜像中该路径的属主，
    # 因此这一步决定了挂载后 app 能否写入 SQLite 文件。
    mkdir -p /data && chown app:app /data

COPY --from=builder /out/p2psession-server /usr/local/bin/p2psession-server
# 把客户端 CLI 也放进去：方便在容器内直接做 create/join 冒烟测试。
COPY --from=builder /out/p2p-node /usr/local/bin/p2p-node

ENV P2PS_LISTEN=":60000" \
    P2PS_TRUST_PROXY_HEADERS="true" \
    # 默认启用 SQLite 持久化：配合下面的 VOLUME，`docker run -v x:/data` 即可
    # 让会话跨重启存活。想回到「重启即清空」的纯内存模式，
    # 设 P2PS_STORAGE_BACKEND=memory 即可。
    P2PS_STORAGE_BACKEND="sqlite" \
    P2PS_SQLITE_PATH="/data/p2psession.db"

# 声明为卷：数据落在容器外，docker rm 不会连数据一起删掉。
VOLUME ["/data"]

USER app
EXPOSE 60000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:60000/v1/health || exit 1

ENTRYPOINT ["/usr/local/bin/p2psession-server"]
