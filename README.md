# p2psession

[![ci](https://github.com/AngelSnow1129/p2p-server/actions/workflows/ci.yml/badge.svg)](https://github.com/AngelSnow1129/p2p-server/actions/workflows/ci.yml)
[![release](https://github.com/AngelSnow1129/p2p-server/actions/workflows/release.yml/badge.svg)](https://github.com/AngelSnow1129/p2p-server/actions/workflows/release.yml)
[![release](https://img.shields.io/github/v/release/AngelSnow1129/p2p-server)](https://github.com/AngelSnow1129/p2p-server/releases)
[![go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![platforms](https://img.shields.io/badge/platform-linux%20%7C%20macOS%20%7C%20windows-555)](https://github.com/AngelSnow1129/p2p-server/releases)

以**服务端为核心的 P2P 会话协调 + 中继服务**。节点凭一个 Join Code/Token
通过服务端完成认证、发现与授权，然后在同一会话内双向通信；服务端**看不到
业务明文**（端到端成对密钥加密），只负责信令与转发。

- **单个静态二进制**，纯 Go 实现（含 SQLite 驱动），无 libc / 无运行时依赖
- **一条命令安装**：`curl … | sudo bash`，自动校验 SHA256、systemd 托管
- **两种存储后端**：内存（零配置）与 SQLite（单文件持久化，重启会话仍在）
- **默认高端口 60000**，非 root 运行，systemd 全套加固
- 控制面（JSON）/ 数据面（二进制）双通道，按 IP 限流，Prometheus 指标

完整设计见 [DESIGN.md](./DESIGN.md)。

---

## 目录

1. [工作原理](#工作原理)
2. [快速开始](#2-快速开始)
3. [客户端用法](#3-客户端用法)
4. [存储后端与备份](#4-存储后端与备份)
5. [HTTP / WebSocket API](#5-http--websocket-api)
6. [鉴权模型](#6-鉴权模型)
7. [配置参考](#7-配置参考)
8. [运维与故障排查](#8-运维与故障排查)
9. [开发与测试](#9-开发与测试)
10. [项目结构](#10-项目结构)
11. [安全说明与已知限制](#11-安全说明与已知限制)

---

## 工作原理

```
   节点 A ──┐                         ┌── 节点 B
            │  ① join（出示 Token）    │
            ├─▶ /ws/control (JSON) ───┤
            │     认证 / 发现 / 信令    │
            │                         │
            │  ② 端到端成对密钥交换     │
            │     （服务器只见密文）     │
            ├─▶ /ws/relay (二进制) ◀───┤
            │     单播 / 组播 / 广播     │
   节点 C ──┘                         └── （同一会话更多成员）
                         ▲
                         │  ③ 仅转发密文帧；
                         │     X25519+ChaCha20-Poly1305
                         │     服务器无解密密钥
                    p2psession
```

几个关键概念：

- **Session（会话）**：成员共享同一个 Join Token 才能进入的安全域。
  两种模式——`pair`（恒为 2 人）与 `group`（成员上限可调，默认 10、最大 1000）。
- **Join Token / Join Code**：入会凭证。服务器只存它的 **SHA-256 哈希**，
  原始值只在创建时显示一次。Join Code 是 Token 的人工可读转写形式
  （大小写/连字符不敏感），二者等价。
- **身份（NodeID）**：每节点一把 Ed25519 密钥；NodeID 是公钥指纹。
  加入时用私钥对 challenge 签名（join proof），做到「身份与能力分离」。
- **连接票据（Ticket）**：join 成功后下发的短期 HMAC 票据，用于升级
  WebSocket。它无状态、可被任意实例校验——这是未来水平扩展的基础。
- **三个 ID 各司其职**：SessionID（会话）、NodeID（节点长期身份）、
  MemberID/Index（会话内成员标识与 16 位数据面寻址下标；掉线重连会复用）。

> 当前是**纯中继**模式（协议已预留 NAT 打洞信令，见 DESIGN.md §7）。

---

## 2. 快速开始

四种方式按推荐度排列：

| 方式 | 适用 | 依赖 |
|---|---|---|
| **[一键安装（Releases）](#21-一键安装推荐)** | 生产、长期自托管 | 仅 curl + systemd |
| [从源码构建安装](#22-从源码构建) | 开发机、离线环境 | Go 1.25+ |
| [手动运行](#23-手动运行试用) | 试用、内网临时跑 | 无（有二进制即可） |
| [Docker](#24-docker备选) | 已有容器编排体系 | Docker |

### 2.1 一键安装（推荐）

无需克隆仓库、无需 Go、无需 Docker。一条命令下载当前平台的静态二进制、
校验 SHA256、创建专用系统用户、安装加固的 systemd unit 并启动：

```bash
curl -fsSL https://raw.githubusercontent.com/AngelSnow1129/p2p-server/main/deploy/install.sh | sudo bash
```

安装后：

```bash
curl http://127.0.0.1:60000/v1/health        # → ok
systemctl status p2psession
```

常用操作（管道方式传参用 `bash -s --`）：

```bash
BASE=https://raw.githubusercontent.com/AngelSnow1129/p2p-server/main/deploy/install.sh

curl -fsSL "$BASE" | sudo bash -s -- --version v1.0.0   # 安装指定版本
curl -fsSL "$BASE" | sudo bash -s -- --upgrade          # 升级（保留配置与数据）
curl -fsSL "$BASE" | sudo bash -s -- --uninstall        # 卸载（保留数据与配置）
curl -fsSL "$BASE" | sudo bash -s -- --uninstall --purge  # 连数据/配置/用户一起删
```

> 想先审计脚本再执行：`curl -fsSL -o install.sh "$BASE"` → `less install.sh`
> → `sudo bash install.sh`。

安装脚本每一步幂等、可重复执行：

| 步骤 | 内容 |
|---|---|
| 1–3 | 检测 `<os>/<arch>` 与版本 → 从 Releases 下载归档 → **校验 SHA256** → 解包 |
| 4 | 创建系统用户 `p2psession`（无 shell、无家目录）与 `/var/lib/p2psession` |
| 5 | 安装到 `/usr/local/bin`，**替换前备份**；立即跑 `--version` 自检 |
| 6 | 安装 unit 与 `/etc/p2psession/p2psession.env`（首次生成随机 `P2PS_TICKET_KEY`） |
| 7 | `systemctl enable --now` 并**轮询健康检查直到通过**才算成功 |

安全保证：校验和缺失/不匹配即中止（除非显式 `--insecure-skip-verify`）；
启动失败自动回滚旧二进制；**升级绝不覆盖既有 env**（否则会抹掉线上
`P2PS_TICKET_KEY`，使所有在途票据失效）。

支持的安装目标平台（见 [Releases](https://github.com/AngelSnow1129/p2p-server/releases)）：
`linux/amd64`、`linux/arm64`、`linux/armv7`、`darwin/amd64`、`darwin/arm64`、
`windows/amd64`。systemd 托管仅在 Linux 上生效；macOS/Windows 安装二进制后
需以前台方式运行。

### 2.2 从源码构建

```bash
make dist                      # 产出 dist/ 下各平台归档 + SHA256SUMS
sudo ./deploy/install.sh       # 检测到本地 dist/ 会优先使用，无需联网
```

其他本地用法：

```bash
./deploy/install.sh --check                 # 只下载/校验/解包，不改系统（免 root）
sudo ./deploy/install.sh --from x.tar.gz    # 用指定本地归档安装
```

### 2.3 手动运行（试用）

```bash
make build                                  # → bin/p2psession-server、bin/p2p-node
./bin/p2psession-server --version

./bin/p2psession-server                     # 默认内存存储，:60000，重启即清空

# 或用 SQLite 持久化
P2PS_STORAGE_BACKEND=sqlite \
P2PS_SQLITE_PATH=./data/p2psession.db \
./bin/p2psession-server
```

也可以直接 `go run ./cmd/server`（零配置，内存存储）。

### 2.4 Docker（备选）

已有容器编排体系时可用；否则二进制更省（无需 daemon、镜像 44.7 MB vs
归档约 7 MB）。

```bash
docker compose up -d --build
curl http://127.0.0.1:60000/v1/health
```

镜像默认启用 SQLite，数据在具名卷 `p2p-data`（容器内 `/data`），
重新部署后会话仍在。想回到无状态：`echo 'P2PS_STORAGE_BACKEND=memory' >> .env`。

### 2.5 公网入口（Cloudflare 隧道，零账号）

二进制部署时直接让 cloudflared 指向本地端口即可：

```bash
cloudflared tunnel --no-autoupdate --url http://127.0.0.1:60000
```

Docker 方式见 `docker compose --profile quick up`（随机临时域名）；
固定域名用 `--profile named` 并设置 `CLOUDFLARE_TUNNEL_TOKEN`。
放在 Nginx/Caddy 之后时，记得放行 `Upgrade` 头并关闭 `/ws` 的响应缓冲。

---

## 3. 客户端用法

安装后系统里有命令行客户端 `p2p-node`（容器内位于
`/usr/local/bin/p2p-node`；开发时用 `go run ./cmd/p2p-node`）。

```bash
p2p-node init                          # 生成 Ed25519 身份（.p2p-node/identity.json）
p2p-node whoami                        # 查看本机 NodeID
p2p-node create --mode pair            # 创建会话，打印 Join Code
p2p-node join P2P-XXXX-XXXX-…          # 用 Join Code 加入并进入交互控制台
```

> ⚠️ **选项必须放在子命令之后**：第一个参数是子命令。
> 正确：`p2p-node join -s <url> <code>`；
> 错误：`p2p-node -s <url> join <code>`（会报 `unknown command "-s"`）。

连接非默认服务器：

```bash
p2p-node join -s https://your-host:60000 P2P-XXXX-...
p2p-node join -s https://your-host:60000 --token <JOIN_TOKEN>   # 用 token 而非 code
```

`create` 选项：

| 选项 | 说明 |
|---|---|
| `-s <url>` | 服务器地址（默认 `http://127.0.0.1:60000`） |
| `--mode pair\|group` | 会话模式（pair 固定 2 人） |
| `--max-members N` | group 上限（默认 10，最大 1000） |
| `--join-limit N` | 最多加入次数（0 = 不限） |
| `--ttl 30m` | 会话存活时长 |
| `--idle-timeout 10m` | 全员离线后自动过期 |
| `--require-approval` | 新成员需 owner 批准 |
| `--json` | 以 JSON 输出（便于脚本消费） |

> 注意：`create` **不接受 `-f`**（它不需要本地身份，仅发起 HTTP 请求）；
> `-f <身份文件>` 只在 `init` / `whoami` / `join` 上可用。

交互控制台命令：`/to <member> <msg>`、`/all <msg>`、`/list`、`/stream`、
`/help`、`/exit`（`/quit`）。

应用代码可直接引用客户端 SDK：`pkg/sessionclient`（含连接、密钥交换、
收发、生命周期管理）。

---

## 4. 存储后端与备份

| 后端 | 启用方式 | 适用 | 重启后 |
|---|---|---|---|
| `memory` | 默认 | 本地开发、临时演示 | 会话清空 |
| `sqlite` | `P2PS_STORAGE_BACKEND=sqlite` | 单机自托管（systemd / 镜像默认） | **会话保留** |

- SQLite 驱动是 **`modernc.org/sqlite`（纯 Go）**，因此 `CGO_ENABLED=0`
  静态链接成立（`ldd` 报告非动态可执行）。
- WAL 模式 + `immediate` 事务；连接池固定 1 条连接，写策略清晰、无需处理
  `SQLITE_BUSY`。心跳**不写库**，信令写入量很低。
- 两个后端跑**同一套一致性测试**（`internal/storage/store_conformance_test.go`，
  `forEachBackend` 双后端执行），防止实现漂移。
- SQLite 是**单写者**模型，**不解决多实例**；不要放在 NFS 等共享文件系统上。
  水平扩展需换 Redis/Postgres 并共享 `P2PS_TICKET_KEY`（DESIGN.md §19.2）。

### 备份

**二进制 / systemd 部署**（数据在 `/var/lib/p2psession`）：

```bash
# 在线一致性快照（推荐；.backup 会正确处理 WAL 检查点）
sudo -u p2psession sqlite3 /var/lib/p2psession/p2psession.db \
  ".backup '/var/backups/p2p-$(date +%F-%H%M).db'"

# 或停机整目录打包
sudo systemctl stop p2psession
sudo tar czf /var/backups/p2p-$(date +%F).tgz -C /var/lib p2psession
sudo systemctl start p2psession
```

Docker 部署把上面的 `/var/lib/p2psession` 换成具名卷 `p2psession_p2p-data`。

> ⚠️ **不要只拷 `p2psession.db`**：WAL 模式下最新事务可能还在
> `p2psession.db-wal` 里，单独拷主库会丢数据。整目录打包或用 `.backup`。
>
> 恢复后若 `P2PS_TICKET_KEY` 与备份时不一致，旧连接票据会验签失败，
> 客户端需重新 join——生产环境请把该密钥固定下来。

---

## 5. HTTP / WebSocket API

基址默认 `http://127.0.0.1:60000`。

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/v1/health` | 健康检查（恒返回纯文本 `ok`，匿名可查） |
| `GET` | `/v1/stats` | 运行统计 + `build` 信息（受 Admin Token 保护） |
| `GET` | `/metrics` | Prometheus 文本指标（含 `p2psession_build_info`，受保护） |
| `POST` | `/v1/sessions` | 创建会话 |
| `GET` | `/v1/sessions/{id}` | 查询会话 |
| `DELETE` | `/v1/sessions/{id}` | 删除会话（owner / admin） |
| `POST` | `/v1/sessions/{id}/join` | 加入（已知 session ID） |
| `POST` | `/v1/session/join` | 加入（用 join code） |
| `POST` | `/v1/sessions/{id}/leave` | 离开 |
| `GET` | `/v1/sessions/{id}/members` | 成员列表 |
| `GET` | `/v1/sessions/{id}/members/{mid}` | 单个成员 |
| `POST` | `/v1/sessions/{id}/revoke-token` | 撤销 token（阻止**新**加入，不踢在线成员） |
| `POST` | `/v1/sessions/{id}/rotate-token` | 轮换 token 与 join code |
| `POST` | `/v1/sessions/{id}/approve` | 批准待审批成员（owner / admin） |
| `POST` | `/v1/sessions/{id}/reject` | 拒绝待审批成员（owner / admin） |
| `GET` | `/v1/relays` | 可用中继列表 |
| `GET` | `/ws/control` | 控制面 WebSocket（JSON 文本帧） |
| `GET` | `/ws/relay` | 数据面 WebSocket（二进制 DataFrame） |

`POST /v1/sessions` 请求体（全部字段可选）：

```json
{
  "mode": "group",
  "max_members": 10,
  "join_limit": 0,
  "ttl": "30m",
  "idle_timeout": "10m",
  "require_approval": false
}
```

线协议（控制消息枚举、二进制 DataFrame 布局、关闭码）见 DESIGN.md §10。

---

## 6. 鉴权模型

两层，刻意分开：

1. **入会鉴权（Join Token）**：创建会话返回一次性 `join_token` / `join_code`。
   服务器只存其 SHA-256 哈希；加入时出示原值并附 Ed25519 join proof。
   Token 可被撤销（阻止新加入）或轮换（旧 code 立即失效）。
2. **管理鉴权（Admin Token）**：设置 `P2PS_ADMIN_TOKEN` 后，
   `/v1/stats`、`/metrics` 以及 owner 级管理操作需要
   `Authorization: Bearer <token>`。留空则这些端点匿名可查——**公网部署请务必设置**。

> 版本/commit 等构建信息只出现在受保护的 `/metrics`、`/v1/stats`，
> **不**出现在公开的 `/v1/health`，避免向匿名者暴露可用于反推未修补状态的信息。

连接票据（join 后下发的 HMAC ticket）用于 WebSocket 升级，是无状态自校验的：
多实例共享同一 `P2PS_TICKET_KEY` 即可互相认票。

---

## 7. 配置参考

全部通过 `P2PS_` 前缀环境变量配置（12-factor）。systemd 部署编辑
`/etc/p2psession/p2psession.env`（权限 `0640 root:p2psession`）；
完整带注释样例见 [`.env.example`](./.env.example)。

### 网络与存储

| 变量 | 默认 | 说明 |
|---|---|---|
| `P2PS_LISTEN` | `:60000` | 监听地址（高端口，免 root；绑 80/443 需放开 `CAP_NET_BIND_SERVICE`） |
| `P2PS_STORAGE_BACKEND` | `memory`（镜像内 `sqlite`） | `memory` / `sqlite` |
| `P2PS_SQLITE_PATH` | `/data/p2psession.db`（systemd 用 `/var/lib/p2psession/p2psession.db`） | SQLite 文件路径 |
| `P2PS_STATIC_DIR` | 空 | 可选静态文件目录 |
| `P2PS_ALLOWED_ORIGINS` | 空（不校验） | 浏览器 Origin 白名单，逗号分隔；`*` 放行全部 |
| `P2PS_TRUST_PROXY_HEADERS` | `false` | 信任 `X-Forwarded-For` / `CF-Connecting-IP`（位于反代后开启） |

### 安全

| 变量 | 默认 | 说明 |
|---|---|---|
| `P2PS_TICKET_KEY` | 空（随机生成） | 连接票据 HMAC 密钥（base64url 32B）。**单实例建议固定、多实例必须一致** |
| `P2PS_ADMIN_TOKEN` | 空 | `/v1/stats`、`/metrics` 与管理操作的 Bearer；公网部署建议设置 |

生成密钥：`openssl rand -base64 32 | tr '+/' '-_' | tr -d '='`

### 会话生命周期

| 变量 | 默认 | 说明 |
|---|---|---|
| `P2PS_DEFAULT_TTL` | `30m` | 会话默认存活时长 |
| `P2PS_MEMBER_GRACE` | `5m` | 离线成员保留 MemberID 的宽限期（重连恢复窗口） |
| `P2PS_TICKET_TTL` | `10m` | 连接票据有效期（覆盖 join→建 WS 的窗口） |
| `P2PS_HEARTBEAT` | `15s` | 要求客户端心跳间隔 |
| `P2PS_IDLE_TIMEOUT` | `45s` | 连接读空闲断开阈值（须 ≥ 心跳） |
| `P2PS_PRUNE_INTERVAL` | `60s` | 过期会话/超宽限成员的清扫间隔 |

### 限流与帧

| 变量 | 默认 | 说明 |
|---|---|---|
| `P2PS_SESSION_RATE_PER_MIN` | `30` | 每 IP 创建会话频率（次/分） |
| `P2PS_JOIN_RATE_PER_MIN` | `120` | 每 IP 加入频率（次/分） |
| `P2PS_MSG_RATE_PER_SEC` | `25` | 控制面令牌桶速率（条/秒） |
| `P2PS_MSG_RATE_BURST` | `50` | 控制面令牌桶突发容量 |
| `P2PS_MAX_CONTROL_FRAME` | `65536` | 控制面单帧上限（字节） |
| `P2PS_MAX_RELAY_FRAME` | `65536` | 数据面单帧上限（字节） |
| `P2PS_RELAY_BYTES_PER_SEC` | `524288` | 每连接中继带宽（字节/秒，0 = 不限） |
| `P2PS_RELAY_ENABLED` | `true` | 数据面中继总开关（false 时只做信令/发现） |
| `P2PS_MAX_CANDIDATES` | `16` | 每成员候选地址上限 |
| `P2PS_MAX_SESSIONS` | `0`（不限） | 单实例会话总数上限 |

---

## 8. 运维与故障排查

```bash
systemctl status p2psession                 # 状态
journalctl -u p2psession -f                 # 实时日志（结构化，含版本与存储后端）
curl http://127.0.0.1:60000/v1/health       # 存活探针
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:60000/v1/stats
```

| 现象 | 可能原因 | 处理 |
|---|---|---|
| 健康检查不通 | 服务未起 / 端口不对 | `systemctl status`、`ss -ltnp \| grep :60000` |
| 服务起不来 `200/CHDIR` 或权限拒绝 | 数据目录属主错 | `chown -R p2psession:p2psession /var/lib/p2psession` |
| `218/CAPABILITIES` | 想绑特权端口但能力集已清空 | 用 `:60000`，或在 unit 放开 `CAP_NET_BIND_SERVICE` |
| 启动即退出（配置错误） | env 语法不对 | env 是纯 `KEY=VALUE`，**不要写 `export`、值不要加引号** |
| 重启后客户端都要重新 join | `P2PS_TICKET_KEY` 没固定 | 在 env 写入固定密钥后重启 |
| 升级后端口/行为没变 | 升级保留旧 env，覆盖了 unit 新默认 | 按需手动改 `/etc/p2psession/p2psession.env` |
| `NRestarts` 持续增长 | 崩溃循环 | `journalctl -u p2psession -n 100` 看首个错误 |
| 60000 已被监听 | 端口冲突 | `ss -ltnp \| grep :60000`，或改 `P2PS_LISTEN` |
| 安装脚本报校验和失败 | 归档损坏/被篡改，或 release 缺 SHA256SUMS | 重新下载；自担风险可 `--insecure-skip-verify` |

systemd unit 的加固项（`ProtectSystem=strict`、空 capability 集、
`NoNewPrivileges`、`RestrictAddressFamilies` 等）与文件布局见
DESIGN.md §15.4 / §15.6。

---

## 9. 开发与测试

```bash
make build                 # 本机二进制 → bin/
make dist                  # 6 平台归档 + SHA256SUMS → dist/
make check                 # gofmt 检查 + go vet + go test（提交前门禁）
make test-race             # go test -race -count=1 ./...
make fmt                   # 格式化
make version               # 查看将注入的版本信息
```

直接用 go 工具：

```bash
go build ./...
go vet ./...
go test ./...
go test -race ./...
go test ./internal/storage/ -v      # 存储一致性（memory + sqlite 双后端）
go test ./e2e/ -v                   # 端到端：真实 HTTP + 双 WebSocket 互通
```

约 40 个 Go 文件、1.3 万行代码，含 90 个测试/基准函数；端到端用
`httptest.NewServer` 在随机端口起真实服务，不依赖固定端口。

发布：推送 `vX.Y.Z` tag 即由 `.github/workflows/release.yml` 自动构建并上传
归档（先把版本写入 `VERSION` 文件），日常 CI 见 `ci.yml`。流程详见
DESIGN.md §15.5.1。

---

## 10. 项目结构

```
p2psession/
├── cmd/
│   ├── server/            服务器入口（配置装配、优雅退出）
│   └── p2p-node/          节点 CLI 与交互控制台
├── internal/
│   ├── protocol/          线协议：控制消息 + 二进制 DataFrame
│   ├── auth/              Join Token / HMAC 票据 / Ed25519 身份
│   ├── storage/           Store 接口 + memory / sqlite 两实现 + 一致性测试
│   ├── session/           会话业务编排（创建/加入/审批/撤销/清扫）
│   ├── member/            在线连接中心（顶替、慢消费者保护）
│   ├── relay/             数据面授权与转发
│   ├── server/            HTTP 路由、中间件、REST 与 WebSocket 处理器
│   ├── config/            P2PS_ 环境变量解析与校验
│   ├── metrics/           Prometheus 文本指标（含 build_info）
│   └── version/           ldflags 注入的版本信息
├── pkg/sessionclient/     可复用客户端 SDK
├── deploy/
│   ├── install.sh         一键安装/升级/卸载（网络引导）
│   └── systemd/           加固 unit + env 样例
├── .github/workflows/     ci.yml（测试）、release.yml（发版）
├── Dockerfile / docker-compose.yml   备选容器方案
├── Makefile / VERSION     构建与版本
├── DESIGN.md              完整设计文档
└── README.md
```

---

## 11. 安全说明与已知限制

**已内建的防护**

- 业务内容端到端加密（X25519 + ChaCha20-Poly1305，成对密钥），服务器无解密密钥
- 入会需 Token + Ed25519 签名；Token 只存哈希，可撤销/轮换
- 非 root 运行；systemd 只读根、空能力集、私有 /tmp、syscall 白名单等加固
- 按 IP 的创建/加入限流、控制面令牌桶、帧大小与带宽上限、慢消费者断连
- 安装包强制 SHA256 校验；管理端点可加 Bearer 保护

**已知限制**

- **无 NAT 穿透**：当前为纯中继；协议已预留 `candidates` / `candidate_offer`
  信令（DESIGN.md §7）。
- **元数据对服务端可见**：谁与谁通信、时序、包大小可见；仅**内容**加密。
  中继节点本身应被视为可信基础设施。
- **SQLite 仅限单机**：库级写锁，不要多进程共写或放共享文件系统。
- **无分布式全局限流**：现有按 IP 限流是单实例的；多实例需外部存储。
- **无内置用户体系/ACL**：准入靠持有 Join Token，适合「邀请制」小群组，
  不是开放注册型 IM。
