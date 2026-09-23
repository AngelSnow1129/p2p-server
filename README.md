# p2psession

以**服务端为核心的 P2P 会话协调 + 中继服务**。

节点通过服务端完成认证、发现与授权，然后在同一会话内互发数据。
服务端**看不到业务明文**（成对密钥加密），只负责信令与转发。

- **Go 单二进制**，零运行时依赖
- **两种存储后端**：内存（默认）与 SQLite（单文件持久化）
- **一条命令部署**：`docker run` 挂一个卷即可
- 完整设计文档见 [DESIGN.md](./DESIGN.md)

---

## 1. 快速开始

**推荐路径是直接跑二进制**：产物是**单个静态链接文件**（无 libc 依赖、无
运行时、无容器），一条 `install.sh` 交给 systemd 托管即可。

三种方式按推荐度排列：

| 方式 | 适用 | 依赖 |
|---|---|---|
| **[二进制 + systemd](#11-二进制--systemd推荐)** | 生产、长期自托管 | 无（纯静态二进制） |
| [手动跑二进制](#12-手动运行适合试用) | 试用、内网临时跑 | 无 |
| [Docker](#13-docker备选) | 已有容器编排体系 | Docker |

### 1.1 二进制 + systemd（推荐）

```bash
# 1) 构建（需要 Go 1.25+；也可直接下载官方归档，见下）
make dist                      # 产出 dist/ 下各平台归档 + SHA256SUMS

# 2) 安装（自动识别平台 → 校验 SHA256 → 建用户 → 装 unit → 启动 → 健康检查）
sudo ./deploy/install.sh

# 3) 确认
curl http://127.0.0.1:60000/v1/health        # → ok
systemctl status p2psession
```

安装脚本做的事（每一步都幂等，可重复执行）：

| 步骤 | 内容 |
|---|---|
| 1–3 | 定位/下载归档 → **校验 SHA256** → 解包 |
| 4 | 创建系统用户 `p2psession`（无 shell、无家目录）与目录 |
| 5 | 安装二进制到 `/usr/local/bin`，**替换前备份** |
| 6 | 装 systemd unit 与 `/etc/p2psession/p2psession.env`（自动生成随机 `P2PS_TICKET_KEY`） |
| 7 | 启动服务并**等待健康检查通过**才算成功 |

安全约束（都有实测）：

- **校验和不匹配即中止安装**；拿不到校验和也拒绝安装（除非显式 `--insecure-skip-verify`）。
- **失败会回滚**：二进制替换前备份，启动或自检失败则还原。
- **绝不覆盖既有配置**——升级时若覆盖 `/etc/p2psession/p2psession.env`，
  线上 `P2PS_TICKET_KEY` 会被抹掉，导致所有在途票据立即失效。

其他用法：

```bash
sudo ./deploy/install.sh --check          # 只获取+校验+解包，不改动系统（免 root）
sudo ./deploy/install.sh --upgrade        # 只换二进制并重启，保留配置与数据
sudo ./deploy/install.sh --url <归档URL>   # 从网络安装（自动尝试同目录 SHA256SUMS）
sudo ./deploy/install.sh --uninstall      # 卸载服务与二进制（保留数据与配置）
sudo ./deploy/install.sh --uninstall --purge   # 连带删除数据、配置与用户
```

不想自己编译？下载预编译归档后本地安装：

```bash
# 归档内已含二进制 + README + VERSION
sudo ./deploy/install.sh --url https://github.com/<owner>/<repo>/releases/download/v1.0.0/p2psession-1.0.0-linux-amd64.tar.gz
```

### 1.2 手动运行（适合试用）

```bash
make build                     # 产出 bin/p2psession-server、bin/p2p-node
./bin/p2psession-server --version

# 内存存储（重启即清空）
./bin/p2psession-server

# SQLite 持久化
P2PS_STORAGE_BACKEND=sqlite P2PS_SQLITE_PATH=./data/p2psession.db ./bin/p2psession-server
```

也可以直接用 Go 跑（零配置，内存存储）：

```bash
go run ./cmd/server
```

### 1.3 Docker（备选）

已有容器编排体系时可用；否则**二进制更简单**（不需要 Docker daemon、
不需要拉基础镜像，安装包只有几 MB）。

```bash
docker compose up -d --build
curl http://127.0.0.1:60000/v1/health
```

镜像**默认启用 SQLite**，数据落在具名卷 `p2p-data`（容器内 `/data`），
因此重新部署或重启后**会话仍在**。想回到无状态模式：

```bash
echo 'P2PS_STORAGE_BACKEND=memory' >> .env
docker compose up -d
```

### 1.4 带公网入口（Cloudflare 隧道，零账号）

下面以 Docker 为例；**跑二进制时把 `--url http://relay:60000` 换成
`--url http://127.0.0.1:60000` 直接执行 `cloudflared` 即可。**

```bash
docker compose --profile quick up -d --build
docker compose logs cloudflared-quick | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com'
```

固定域名的生产隧道：

```bash
export CLOUDFLARE_TUNNEL_TOKEN=xxxx
docker compose --profile named up -d --build
```

---

## 2. 客户端用法

CLI 源码在 `cmd/p2p-node`（容器内已包含该二进制）。

```bash
# 生成身份（Ed25519，保存到 .p2p-node/identity.json）
go run ./cmd/p2p-node init

# 创建会话：打印 Join Code
go run ./cmd/p2p-node create --mode pair

# 用 Join Code 加入并进入交互控制台
go run ./cmd/p2p-node join P2P-XXXX-XXXX-...
```

> ⚠️ **选项要放在子命令之后**：第一个参数必须是子命令
> （`cmd, args := os.Args[1], os.Args[2:]`），因此是
> `p2p-node join -s <url> <code>`，**不是** `p2p-node -s <url> join <code>`。

指定服务器与身份文件（注意 `-f` 只在 `init` / `whoami` / `join` 上注册，
`create` **不接受** `-f`，它不需要本地身份）：

```bash
go run ./cmd/p2p-node join -s https://your-host -f ./my-id.json P2P-XXXX-...
```

`create` 选项（全部在子命令之后）：

| 选项 | 说明 |
|---|---|
| `-s <url>` | 服务器地址（默认 `http://127.0.0.1:60000`） |
| `--mode pair\|group` | 会话模式（pair 固定 2 人；group 用 `--max-members`） |
| `--max-members N` | group 模式成员上限（默认 10） |
| `--join-limit N` | 最多加入次数（0 = 不限） |
| `--ttl 30m` | 会话存活时长 |
| `--idle-timeout 5m` | 全员离线后自动过期 |
| `--require-approval` | 新成员需 owner 批准 |
| `--json` | 以 JSON 输出（便于脚本消费） |

控制台命令：`/to <member> <msg>`、`/all <msg>`、`/list`、`/stream`、`/help`、`/exit`。

---

## 3. 存储后端

| 后端 | 配置 | 适用 |
|---|---|---|
| `memory` | 默认 | 本地开发、临时演示（重启即清空） |
| `sqlite` | `P2PS_STORAGE_BACKEND=sqlite` | 单机自托管（容器；镜像内默认） |

```bash
# 裸机 + SQLite
P2PS_STORAGE_BACKEND=sqlite \
P2PS_SQLITE_PATH=./data/p2psession.db \
go run ./cmd/server
```

要点：

- SQLite 驱动使用 **`modernc.org/sqlite`（纯 Go）**，因此 `CGO_ENABLED=0`
  静态编译依然成立（镜像 44.7 MB，`ldd` 显示非动态可执行）。
- SQLite 是**单写者**模型，**不解决多实例**问题——它是本地文件。
  水平扩展请换 Redis/Postgres 并共享 `P2PS_TICKET_KEY`（见 DESIGN.md §18.2）。
- 两个后端跑**同一套一致性测试**（`internal/storage/store_conformance_test.go`），
  避免实现漂移。

### 备份与恢复

```bash
# 整卷打包（停机备份，最简单可靠）
docker compose stop relay
docker run --rm -v p2psession_p2p-data:/d -v "$PWD:/b" alpine \
  tar czf /b/p2p-backup-$(date +%F).tgz -C /d .
docker compose start relay
```

> ⚠️ **不要只拷 `p2psession.db`**。WAL 模式下最新数据可能还在
> `p2psession.db-wal` 里，单独拷主库会丢最近事务。整目录打包，
> 或用 `sqlite3 .backup`（会正确处理检查点）。

---

## 4. HTTP API 摘要

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/v1/health` | 健康检查 |
| `GET` | `/v1/stats` | 统计（可配 `P2PS_ADMIN_TOKEN` 保护） |
| `POST` | `/v1/sessions` | 创建会话 |
| `GET` | `/v1/sessions/{id}` | 查询会话 |
| `DELETE` | `/v1/sessions/{id}` | 删除会话 |
| `POST` | `/v1/sessions/{id}/join` | 加入（已知道 session ID） |
| `POST` | `/v1/session/join` | 加入（用 join code） |
| `POST` | `/v1/sessions/{id}/leave` | 离开 |
| `GET` | `/v1/sessions/{id}/members` | 成员列表 |
| `GET` | `/v1/sessions/{id}/members/{mid}` | 单个成员 |
| `POST` | `/v1/sessions/{id}/revoke-token` | 撤销 token（不再接受新加入） |
| `POST` | `/v1/sessions/{id}/rotate-token` | 轮换 token 与 join code |
| `POST` | `/v1/sessions/{id}/approve` | 批准待审批成员 |
| `POST` | `/v1/sessions/{id}/reject` | 拒绝待审批成员 |
| `GET` | `/v1/relays` | 可用中继列表 |
| `GET` | `/metrics` | Prometheus 文本指标 |
| `GET` | `/ws/control` | 控制面 WebSocket（JSON） |
| `GET` | `/ws/relay` | 数据面 WebSocket（二进制） |

---

## 5. 配置

全部通过 `P2PS_` 前缀环境变量（12-factor）。完整样例见
[`.env.example`](./.env.example)。常用的几个：

| 变量 | 默认 | 说明 |
|---|---|---|
| `P2PS_LISTEN` | `:60000` | 监听地址（高端口，免 root） |
| `P2PS_TICKET_KEY` | 空 | 票据 HMAC 密钥（**多实例必配**；留空则随机生成，重启后旧票据失效） |
| `P2PS_STORAGE_BACKEND` | `memory`（镜像内 `sqlite`） | `memory` / `sqlite` |
| `P2PS_SQLITE_PATH` | `/data/p2psession.db` | SQLite 文件路径 |
| `P2PS_DEFAULT_TTL` | `30m` | 会话存活时长 |
| `P2PS_MEMBER_GRACE` | `5m` | 离线成员宽限期（重连恢复窗口） |
| `P2PS_ADMIN_TOKEN` | 空 | `/v1/stats`、`/metrics` 的 Bearer |
| `P2PS_TRUST_PROXY_HEADERS` | `false` | 信任 `X-Forwarded-For` / `CF-Connecting-IP` |

---

## 6. 开发与测试

```bash
go build ./...                    # 编译
go vet ./...                      # 静态检查
go test ./...                     # 单元 + 端到端
go test -race ./...               # 竞态检测（推荐在提交前跑）
go test ./internal/storage/ -v    # 存储一致性测试（双后端）
go test ./e2e/ -v                 # 端到端（真实 WS 双端互通）
```

存储一致性测试用**同一套用例**同时跑内存与 SQLite 两个后端
（`forEachBackend`），新增存储实现只需加进 `backends()` 即可获得全部覆盖。

---

## 7. 文档索引

| 文档 | 内容 |
|---|---|
| [DESIGN.md](./DESIGN.md) | 完整设计：协议、状态机、加密、存储、部署、安全审计 |
| [cloudflare/README.md](./cloudflare/README.md) | Cloudflare Worker / Durable Objects 版本 |

---

## 8. 已知限制

- **无 NAT 穿透**：当前是纯中继模式，协议已预留 `candidates` /
  `candidate_offer` 信令，便于后续接入打洞（见 DESIGN.md §7）。
- **元数据对服务端可见**：谁和谁通信、时序、包大小可见；仅**内容**加密。
- **SQLite 仅限单机**：多容器共写同一文件会互相阻塞，不要放在共享文件系统上。
- **无应用层全局限流**：已按 IP 做了创建/加入频率限制，但分布式限流需要外部存储。
