# Cloudflare Workers + Durable Objects 方案

这是 Go 参考实现在边缘的等价物，用于验证核心设计判断：

> **一个 Session = 一个 Durable Object**

## 为什么 DO 适合做 Session 协调者

| Go 参考实现 | Cloudflare DO |
|---|---|
| `session.Service` + `member.Hub` | 单个 `SessionHub` 实例 |
| 一把锁保护房间映射 | DO 单线程事件循环，无需锁 |
| 内存态 + 可选外部存储 | `state.storage`（DO 自带持久化） |
| 实例间需要共享票据密钥 | DO 自动单实例路由；票据密钥用 secret 共享 |

DO 的输入门（input gate）保证「同一时刻只有一个事件在处理中」，因此成员表
可以用普通 `Map` 维护——这正是 Go 版里那把锁在边缘的等价物。

## 与 Go 版的一致性

协议常量、成员状态、帧布局、鉴权语义都与 Go 版**逐字段对齐**：
`worker.js` 里 `FRAME_MAGIC`、`DST_UNICAST/MULTICAST/BROADCAST`、
`MEMBER_ACTIVE/PENDING` 均与 `internal/protocol` 保持一致，因此
**同一个 Go 客户端 SDK 可以直接连 Worker 版本**（REST 与 WS 路径相同）。

几个容易写错、已在实现中显式处理的点：

1. **DO ID 必须由 sessionId 稳定派生**
   用 `idFromName(sessionId)` 而不是 `newUniqueId()`。否则「再次加入同一会话」
   会命中不同实例，成员表分裂。

2. **来源下标必须由服务器盖写**
   数据面帧里的 `src index` 在转发时被改写为发送方真实下标，
   客户端无法伪造来源（与 Go 版 `relay.Forward` 一致）。

3. **`await` 之间必须重新校验状态**
   DO 只保证同步段不交错。所有 `await storage.put(...)` 之后，
   代码都重新读取成员状态再决策，避免基于过期快照做授权。

4. **撤销只阻止新加入**
   已授权成员的重连（同 NodeID）不受 `revoked` 影响，否则一次误撤销
   会把所有在线成员永久踢出。

## 部署

```bash
cd cloudflare
npm install -g wrangler      # 或用 npx

# 票据 HMAC 密钥（必配；与 Go 版 P2PS_TICKET_KEY 等价作用）
wrangler secret put P2PS_TICKET_SECRET

wrangler deploy
```

部署后：

```bash
# 创建会话
curl -s -X POST https://p2psession-relay.<account>.workers.dev/v1/sessions \
  -H 'content-type: application/json' -d '{"mode":"pair"}'

# 客户端加入（与 Go 版相同的请求体）
p2p-node join P2P-XXXX-...
```

## 已知限制（与 Go 版对比）

| 能力 | Go 服务 | Worker/DO |
|---|---|---|
| 会话发现（join code → session_id） | 服务端索引 | 需额外 KV（见下） |
| 数据面带宽整形 | 每连接令牌桶 | 用 DO 内计数器近似，无精确整形 |
| 单会话成员上限 | 1000 | 受 DO 内存与 CPU 时间限制，建议 ≤ 100 |
| 多 Relay 选择 | `/v1/relays` 列表 | 可用 Smart Placement / 多 DO |

**join code → session_id 索引**：Worker 版目前要求调用方直接提供
`session_id`。若需要「只凭 join code 加入」的完整体验，加一层 KV：

```toml
[[kv_namespaces]]
binding = "SESSION_INDEX"
id = "<kv-namespace-id>"
```

在 `#init` 成功后写入 `SESSION_INDEX.put(joinCode, sessionId)`，
在 `/v1/session/join` 时先查 KV 解析出 sessionId 再转发到对应 DO。
Go 版把这个索引放在了内存存储里（`GetByJoinCode`），位置不同、语义一致。

## 何时选哪个

- **选 Go 服务**：需要完整功能、精确限流、大房间、可观测性、自托管；
- **选 Worker/DO**：无服务器运维、全球边缘接入、会话数量多但单会话规模小、
  希望零基础设施。

两者协议兼容，因此可以并存：边缘用 DO 做接入，重负载会话下沉到 Go 集群。
