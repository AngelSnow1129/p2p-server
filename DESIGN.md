# p2psession 设计文档

> 以服务端为核心、客户端极简配对的 P2P Session / Relay 系统。
>
> 本目录（`p2psession/`）是独立于仓库中 `p2prelay` 房间模型的第二套实现：
> 面向「Session + Token」语义，服务器负责配对、成员发现与中继，客户端只需
> `create` 与 `join` 两条命令。

---

## 目录

| # | 交付项 | 章节 |
|---|---|---|
| 1 | 总体架构 | [§1](#1-总体架构) |
| 2 | Pair Session 模型 | [§2](#2-session-模型) |
| 3 | Group Session 模型 | [§2](#2-session-模型) |
| 4 | Session Token 设计 | [§3](#3-session-token-设计) |
| 5 | Member 模型 | [§4](#4-member-模型) |
| 6 | 双向通信模型 | [§5](#5-双向通信模型) |
| 7 | Relay 模型 | [§6](#6-relay-模型) |
| 8 | NAT Traversal | [§7](#7-nat-traversal三阶段演进) |
| 9 | 加密模型 | [§8](#8-加密模型) |
| 10 | 状态机 | [§9](#9-状态机) |
| 11 | 协议 | [§10](#10-协议规范) |
| 12 | API | [§11](#11-http-api) |
| 13 | 数据库 | [§12](#12-存储与数据库) |
| 14 | Go 项目结构 | [§13](#13-go-项目结构) |
| 15 | 核心 Go 代码 | [§14](#14-核心实现要点) |
| 16 | **二进制部署与运维** | [§15](#15-二进制部署与运维推荐方式) |
| 17 | Docker（备选） | [§16](#16-docker备选方案) |
| 18 | Docker Compose | [§17](#17-docker-compose) |
| 19 | Cloudflare Worker/DO 方案 | [§18](#18-cloudflare-worker--durable-objects) |
| 20 | Relay 部署方案 | [§19](#19-relay-部署方案) |
| 21 | 测试 | [§20](#20-测试) |
| 22 | Benchmark | [§21](#21-benchmark) |
| 23 | 安全审计 | [§22](#22-安全审计) |
| 24 | MVP 路线 | [§23](#23-mvp-路线) |
| — | **八个设计问题回答** | [§24](#24-设计问题回答) |
| — | **两个完整时序图** | [§25](#25-完整时序图) |

---

## 1. 总体架构

核心设计理念：

```
Session = 一个逻辑通信空间
Token   = 加入这个 Session 的能力凭证
Client  = Session Member
Server  = Session Coordinator + Relay
```

系统把「我要连接哪一个 IP？」彻底替换成「我要加入哪一个 Session？」——
客户端不管理 Peer IP、不填 Peer ID、不填公钥、不配置 Relay 地址、不写 ACL。

```
                         ┌─────────────── Go Server ───────────────┐
                         │                                        │
   p2p-node create ──────┼─▶ REST  /v1/sessions      【控制面】     │
   （返回 Session+Code） │    Session / Token / 成员发现 / 授权     │
                         │                                        │
   p2p-node join  ───────┼─▶ WS    /ws/control       【控制面】    │
    ─ bind 成员           │    member_list / member_joined         │
    ─ 候选交换            │    candidate_* / key_exchange / 状态    │
                         │                                        │
                         │    WS    /ws/relay         【数据面】    │
   A ◀═══ 加密帧 ═══════▶│    DataFrame 转发（不解析载荷）          │
                         │                                        │
                         └────────────────┬───────────────────────┘
                                          │
                    ① 直连优先（信令就绪，UDP 打洞见 §7）
                    ② 失败自动回退中继（服务器只见密文）
                                          │
                              Node A ◀────┴────▶ Node B
```

### 1.1 Control Plane / Data Plane 彻底分离

| | 控制面 | 数据面 |
|---|---|---|
| 端点 | `POST /v1/*`、`WS /ws/control` | `WS /ws/relay` |
| 载荷 | JSON 文本帧 | 二进制 DataFrame |
| 内容 | Session、Token、成员、候选、连接状态、心跳 | 端到端加密的业务数据 |
| 服务器可见性 | 全部（本就是协调信息） | **只见密文、长度、时间** |
| 代码 | `internal/{session,server,auth,storage}` | `internal/{relay,member}` + `protocol/frame.go` |

分离带来的实际收益（不是概念堆砌）：

1. **密钥材料不过数据面**：X25519 公钥走控制面（`key_exchange`），业务密文
   走数据面。服务器即使记录了全部控制面流量，拿到的也只是公钥。
2. **互不阻塞**：控制面心跳与成员事件不会排在大块业务数据后面；两条独立
   WebSocket 各自有读写循环与独立写锁（`Client.ctrlMu` / `relayMu`）。
3. **可分别扩容**：将来把 `relay` 独立成无状态转发层时，控制面无需变动。

### 1.2 服务器是重点

代码量分布印证了「服务端优先」的取舍（29 个非测试源文件，8226 行）：

| 层 | 文件数 | 说明 |
|---|---|---|
| 服务端核心 | 13 | `session`/`member`/`relay`/`server`/`storage`/`auth`/`protocol`/`metrics`/`config` |
| 客户端 SDK | 7 | `pkg/sessionclient`（含加密与流） |
| CLI | 3 | `cmd/server`、`cmd/p2p-node`（main + console） |

---

## 2. Session 模型

### 2.1 两种模式（对外只暴露这两种）

| | `mode = pair` | `mode = group` |
|---|---|---|
| 成员上限 | **恒为 2**（不接受调用方覆盖） | `max_members`（默认 10，硬顶 1000） |
| 典型场景 | A ↔ B 点对点 | 多人协作 |
| 就绪信号 | 第 2 人加入即 `session_ready` | 达上限时 `session_ready` |
| 语义 | 强绑定：一个 Token 精确对应一条链路 | 松散：Token 是「房间入场券」 |

`pair` 的成员上限刻意**不可配置**。否则会出现「mode=pair 但 5 人」的语义
分裂——那本质上是 group 会话，却带着 pair 的隐含承诺。

### 2.2 逻辑拓扑 vs 物理拓扑

这是本设计最关键的区分：

```
逻辑拓扑（谁可以和谁说话）        物理拓扑（实际怎么走）
                                  
  A ◀──▶ B                        A ══ P2P 直连 ══▶ B
  A ◀──▶ C                        A ── 中继 ──────▶ C
  A ◀──▶ D                        B ══ P2P 直连 ══▶ D
  B ◀──▶ C                        C ── 中继 ──────▶ D
  B ◀──▶ D
  C ◀──▶ D
```

- **逻辑上**：会话内每个成员都能与其它成员双向通信（默认能力，见 §5）；
- **物理上**：每条连接独立决定 `direct` 还是 `relay`，**不强制 Full Mesh**。

设计上把「拓扑」与「传输」彻底解耦为两个正交维度：

```
Session.Mode    ∈ {pair, group}          ← 规模语义
Connection.Kind ∈ {direct, relay}        ← 单条边的传输方式
```

`mesh` 作为第三种拓扑**不在对外 API 中暴露**：它只是「所有边都 direct」的
一种退化情形，由 NAT 穿透结果自然产生，无需用户指定。

### 2.3 成员状态

```go
CREATED ──▶ WAITING ──▶ JOINING ──▶ READY ──▶ CONNECTING ──▶ CONNECTED ──▶ ACTIVE
                                                                    │
                                                          (全员离线且超时)
                                                                    ▼
                                                                EXPIRED
```

映射到代码：

| 概念状态 | 实现 | 位置 |
|---|---|---|
| `CREATED` | 会话记录落库，无成员 | `storage.Create` |
| `WAITING` | `len(members) == 0` | — |
| `JOINING` | `pending`（需审批时） | `MemberRecord.Status` |
| `PAIR_READY` | `session_ready` 广播 | `server.handleJoin` |
| `CONNECTING` | `peer_state = negotiating` | 客户端上报 |
| `DIRECT_CONNECTED` / `RELAY_CONNECTED` | `peer_state = direct` / `relay` | 客户端上报 |
| `ACTIVE` | 正常数据流 | — |
| `EXPIRED` | `ExpiresAt` 到期或被清扫 | `session.Prune` |

**`PAIR_READY` 的触发条件在服务端实现为一句话**：当活跃成员数达到
`max_members` 时广播 `session_ready`（`internal/server/handlers.go`）。
pair 模式下第 2 人加入即满足条件，无需为 pair 写特殊分支。

---

## 3. Session Token 设计

### 3.1 身份与能力的严格分离

| | Node Identity | Session Token |
|---|---|---|
| 回答的问题 | 「我是谁」 | 「我被允许加入哪个会话」 |
| 形态 | Ed25519 长期密钥对 | 32 字节随机数（256 位熵） |
| 生命周期 | 长期（落盘 0600 权限） | 会话级（可过期/撤销/轮换/限额） |
| 是否秘密 | 公钥公开，私钥本地 | 是（泄露即可被加入） |
| 是否用于加密 | 用于 `join proof` 签名 | **绝不用于数据加密** |

`Member` 由 **Token + Node Identity 共同决定**，而不是「一个 token 对应一个
匿名 socket」：

```
Session ABC
  Token       TTT
  Node A 公钥 KA  ──▶  Member A
  Node B 公钥 KB  ──▶  Member B
```

### 3.2 为什么不用 6 位数字短码

6 位数字只有 `10^6 ≈ 2^19.9` 种可能。以 120 次/分钟/IP 的加入限流计算，
单 IP 穷举期望时间约为 `10^6 / (2 × 120)` ≈ 69 分钟——**这不是安全凭证**。

本系统采用分层设计：

| 形态 | 熵 | 用途 | 抗暴力 |
|---|---|---|---|
| `join_token`（Base64URL，43 字符） | 256 位 | `--token` 机器使用 | 2^256，不可枚举 |
| `join_code`（`P2P-XXXX-XXXX-…`，52 字符 base32） | **与 token 等熵（256 位）** | 人工转写、粘贴 | 同上 |
| `session_id`（`sess_` + 随机） | 约 96 位 | 标识符（非凭证） | 不用于鉴权 |

关键点：**`join_code` 不是短码，而是同一份 256 位熵的另一种编码**
（32 字节 → 52 个 base32 字符 → 13 组 4 字符）。人工转写体验靠
「分组 + 大小写不敏感 + 忽略连字符」实现，而不是靠削减熵：

```go
// internal/auth/token.go
func (t *JoinToken) JoinCode() string { return EncodeJoinCode(t.raw) }

// 解析宽容：忽略前缀大小写、连字符、空格
func DecodeToken(in string) ([]byte, error) { /* P2P- / base32 / base64url */ }
```

> 这一处曾经有真实 bug：早期实现对输入无条件 `ToUpper`，导致 base64url
> 形式的 token 被解析成完全不同的字节（大小写敏感）。测试
> `TestJoinTokenRoundTrip` 抓到了它，修复方式是按字符集区分编码族。

### 3.3 存储形态：只存哈希

服务器**从不保存原始 token**：

```go
TokenHash = hex(SHA256(raw_token))   // 64 字符
```

```go
// internal/session/service.go
gotHash := hashTokenRaw(raw)
if !auth.ConstantTimeEqualHex(gotHash, rec.TokenHash) {
    return nil, ErrUnauthorized
}
```

即使数据库被拖库，攻击者也无法直接拿哈希当 token 使用（需原像攻击）。
比较用恒定时间实现，避免通过响应时间泄露前缀匹配长度。

### 3.4 join proof：证明「持有该公钥对应的私钥」

仅有公钥不足以确认身份——任何人都能声称自己是「Node A」。因此加入时必须
附带签名：

```
sign( Ed25519, "p2psession:join:v1\n" || SHA256( tokenHash || 0x00 || nonce ) )
```

```go
// internal/auth/identity.go
func JoinProofMessage(tokenHash string, nonce []byte) []byte {
    h := sha256.New()
    h.Write([]byte(DomainSepJoin))   // 域分隔，防跨协议挪用
    h.Write([]byte(tokenHash))
    h.Write([]byte{0})
    h.Write(nonce)
    return h.Sum(nil)
}
```

绑定 `tokenHash` 而非 `sessionID` 的原因：客户端用 join code 加入时**只知道
code，不知道 session_id**，无法参与以 session_id 为输入的计算。tokenHash
可由 code 自行派生（`sha256(DecodeToken(code))`），且不同会话的 tokenHash
不同，签名无法跨会话复用。

### 3.5 Token 生命周期能力

| 能力 | 机制 | 实现 |
|---|---|---|
| 高熵 | 32 字节 `crypto/rand` | `auth.NewJoinToken` |
| 不可猜测 | 256 位 | 同上 |
| 可过期 | 会话 `ExpiresAt` | `storage.SessionRecord.Expired` |
| 可撤销 | `Revoked` 标记 | `POST /revoke-token` |
| 可轮换 | 换哈希 + 换 code | `POST /rotate-token` |
| 限制加入次数 | `JoinLimit` / `JoinsUsed` | `storage.Join` 原子校验 |
| 限制成员数 | `MaxMembers` | 同上 |
| 可限制 IP 频率 | 每 IP 固定窗口 | `server.bucket` |

**撤销与轮换的语义差异**（容易混淆，故显式区分）：

- **Revoke**：停止接受**新**加入；已在线成员不受影响；老成员重连仍可恢复
  （否则一次误撤销会把所有在线成员永久踢出）。
- **Rotate**：直接换掉凭证，旧 token **与旧 join code 立即失效**，同时清除
  `Revoked` 标记。适用于「code 疑似泄露但不想解散会话」。

---

## 4. Member 模型

### 4.1 数据结构

```go
// internal/storage/store.go
type MemberRecord struct {
    ID           string              // mbr_… 会话内稳定标识（重连不变）
    SessionID    string              // 所属会话（冗余，便于反查）
    Index        uint16              // 数据面寻址下标（会话内唯一，可复用）
    NodeID       string              // sha256(Ed25519 pub)[:16]
    PublicKey    string              // Ed25519 公钥 hex
    Role         string              // owner | member
    Status       string              // active | pending | denied | left
    Capabilities []string            // ["udp","tcp","quic","relay"]
    Candidates   []protocol.Candidate// 网络候选（§7）
    ConnectionID string              // 当前在线连接（离线为空）
    JoinedAt     time.Time
    LastSeen     time.Time
    LeaveAt      time.Time           // 宽限期起算点
}
```

### 4.2 三个 ID 的分工（§24 问题 5「掉线重连」的答案）

| ID | 稳定性 | 用途 |
|---|---|---|
| `SessionID` | 会话级 | 定位会话（REST 路径、DO 键） |
| `MemberID` | **跨重连稳定**（同一 NodeID） | 逻辑寻址：`session://<sid>/<mid>` |
| `Index` | 跨重连稳定 | **数据面 16 位寻址**，避免在每帧里放长字符串 |
| `ConnectionID` | **每次连接新建** | 区分同一成员的多条连接；顶替判定 |

`Index` 存在的理由很实际：数据面每帧都要带上「发给谁」，若用 `mbr_` +
24 字符的 MemberID，每帧多出 25 字节开销。改为 2 字节下标后，64 KiB 帧的
头部开销不到 0.05%；同时服务器仍需维护 `Index → MemberID` 映射并校验
（下标由服务器分配，客户端无法自选）。

`Index` 分配使用**最小可用值**（`freeIndex`），成员离开后下标可复用，
长时间运行的会话不会耗尽 65535 个下标空间。

### 4.3 NodeID 派生

```go
func NodeIDFromPublic(pub ed25519.PublicKey) string {
    sum := sha256.Sum256(pub)
    return hex.EncodeToString(sum[:16])   // 128 位，32 字符
}
```

截断到 128 位而非全长：ID 需要出现在日志、命令行与成员列表里，
128 位在会话规模下的碰撞概率可忽略，而可读性显著更好。

### 4.4 成员视图与对外暴露

```go
type MemberInfo struct {
    MemberID     string
    Index        uint16
    NodeID       string
    PublicKey    string
    Capabilities []string
    Candidates   []Candidate
    IsOwner      bool
    Role         string
    Status       string   // active | pending
    Online       bool     // 由运行时连接中心判定，非持久化字段
}
```

注意 `Online` **不来自持久化记录**——`ConnectionID` 只能说明「上次连接是谁」，
真正的在线状态由 `member.Hub` 的运行时视图决定：

```go
info.Online = hub.HasMember(m.SessionID, m.ID)
```

### 4.5 同一成员的多条连接与顶替

```go
// internal/member/hub.go
func (h *Hub) Attach(sessionID, memberID, connID, kind string,
                     index uint16, approved bool) (*Conn, *Conn)
```

- 控制面与数据面各保留**一条当前连接**（`om.control` / `om.relay`）；
- 新连接接入时旧连接被 `kick("duplicate connection")`，其写泵会发出
  `4003` 关闭帧——这样客户端能明确知道「我被顶替了」而非「网络抖动」；
- `Detach` 只在**当前连接**匹配时才清空字段，防止旧连接的关闭事件误伤
  新连接：

```go
case KindControl:
    if om.control == c { om.control = nil }   // 关键：身份比对
```

这一条解决了「一个 Node 自己和自己建立会话/中继」的经典问题。

---

## 5. 双向通信模型

### 5.1 默认能力：sender + receiver

**不做**单向模型（A 是 sender、B 是 receiver）。会话内每个 Member 都同时
具备收发能力，且模型完全对称：

```
A ◀──▶ B     A ◀──▶ C     B ◀──▶ C     全部双向
```

连接抽象为逻辑流（`Stream`），接口与 `net.Conn` 同构：

| 操作 | 方法 | 说明 |
|---|---|---|
| Send | `Write([]byte)` | 自动分片（16 KiB）+ 加密 + 按 `StreamID` 复用连接 |
| Receive | `Read` / `ReadMessage` | 按序投递，乱序缓冲 |
| Close | `Close()` | 半关闭并通知对端（`FlagClose`） |
| Reset | `Reset(reason)` | 异常中止（`FlagReset`） |
| Ping | 控制面 `ping` / 数据面 `FlagPing` | 心跳 |

```go
// 最小使用示例（SDK 用户视角）
c, _ := sessionclient.Join(ctx, cfg, handler)   // 只需 Join Code
c.SendTo(peerID, []byte("hello"))               // 双向：对端也能 SendTo 回来
s, _ := c.OpenStream(peerID)                    // 需要长连接时开一条流
```

### 5.2 流 ID 分配：无中心分配器的冲突规避

双方各自分配流 ID，若共享同一数字空间就会撞号（A 的 `1` 与 B 的 `1`
在彼此映射表里冲突，导致两条独立流的序号交错、触发去重丢包）。

解决办法是按 `MemberID` 字典序**固定奇偶段**：

```go
func (c *Client) allocStreamIDLocked(peerID string) uint32 {
    if c.selfID() < peerID { start = &c.nextEven }  // 小的一方用偶数段
    else                   { start = &c.nextOdd  }  // 大的一方用奇数段
    ...
}
```

同一对成员的比较结果恒定，因此两个方向永远落在不同段内——无需协商即可
保证 ID 不冲突。

### 5.3 广播与组播：为什么客户端侧是「逐对加密」

协议的数据面帧支持三种目标类型：

```go
DstUnicast   = 1  // A → B
DstMulticast = 2  // A → {B, C}
DstBroadcast = 3  // A → 所有人
```

但 SDK 的 `Broadcast` / `Multicast` 实现是**N 次独立单播**：

```go
// pkg/sessionclient/group.go
for _, id := range ordered {
    if err := c.sendOneShot(id, payload); err != nil { ... }
}
```

原因是密码学约束而非偷懒：本系统的数据加密是**成对密钥**（每对成员一把
X25519 派生密钥，§8）。同一份密文无法被多个持有不同密钥的接收方解开，
因此「一次加密、多人投递」在成对密钥模型下不成立。

服务器侧的 `DstBroadcast` / `DstMulticast` 仍然保留并实现（`relay.Forwarder`
会为每个目标重编码为单播帧），它的价值在于：

- 当应用层自行协商了 **group key** 时，可一次上行、由服务器扇出，
  省去 N-1 次上行带宽；
- 为将来的 group key 机制预留协议位置，无需改动帧格式。

---

## 6. Relay 模型

### 6.1 服务器不接触明文

```
Client A                       Server Relay                    Client B
   │                                │                              │
   │  DataFrame(密文) ─────────────▶│                              │
   │                                │  校验：A ∈ Session?          │
   │                                │  校验：B ∈ Session?          │
   │                                │  盖写 src index = A 的真实值 │
   │                                │  ── 转发 ──────────────────▶ │
   │                                │                              │
   ▼                                ▼                              ▼
 AEAD 加密                       只能看到：                      AEAD 解密
 (成对密钥)                       SessionID / MemberID            (成对密钥)
                                  Packet Size / Timestamp
                                  ← 无业务明文 →
```

服务器**不解密、也无法解密**。它以明文可见的信息仅限于：会话 ID、成员 ID、
帧大小、时序，这些足以做计费与限流，但不足以还原任何业务语义。

### 6.2 授权：三重校验

```go
// internal/relay/relay.go
func (f *Forwarder) Forward(sessionID, senderMemberID string, raw []byte) (int, error) {
    if !f.memb.IsActiveMember(sessionID, senderMemberID) { return 0, ErrNotAuthorized }
    srcIndex, ok := f.memb.MemberIndex(sessionID, senderMemberID)
    if !ok { return 0, ErrNotAuthorized }
    ...
    fr.SrcIndex = srcIndex   // ← 强制盖写来源
```

| 校验 | 目的 | 绕过后果 |
|---|---|---|
| 发送方是会话活跃成员 | 防止外部注入 | 任何人可向任意会话灌流 |
| 目标在会话内且已审批 | 防止跨会话投递 | 会话隔离失效 |
| **`SrcIndex` 由服务器盖写** | 防止伪造来源 | 可冒充他人身份（密钥层会二次拦截） |

来源盖写是纵深防御：即便客户端伪造 `SrcIndex`，服务器也会覆盖为真实值；
接收方解密时还需通过 AEAD 认证（只有真持有成对密钥的对端才解得开），
两道防线独立成立。

### 6.3 单播 / 组播 / 广播的实现

```go
switch fr.DstType {
case protocol.DstUnicast:   // 直接转发，目标下标取自客户端指定值
case protocol.DstMulticast: // 为每个目标重编码为「指向该目标的单播帧」
case protocol.DstBroadcast: // 编码一次，由 member.Hub 扇出（除发送方）
}
```

组播的重编码是刻意的：接收方看到的永远是「一个指向自己的单播帧」，
无需再解析目标列表——把复杂度留在服务器（一处），而不是每个客户端（N 处）。

### 6.4 会话内中继（借鉴 Magic Wormhole Transit Relay）

Relay 不需要理解完整会话，只需三元组：

```
session + source_member + target_member
```

服务器校验 `source ∈ Session ∧ target ∈ Session` 后建立转发管道。
这正是 Magic Wormhole Transit Relay 用 transit key/handoff 做匹配的同一
思路：**端点匹配交给协调层，Relay 只负责搬运**。

本实现与该思路的差异：不需要预先分配 transit key，因为 `session+member`
已经由控制面的认证建立，中继授权直接复用该状态。

### 6.5 慢消费者保护

中继转发是**非阻塞入队**，队列满即判定慢消费者：

```go
func (c *Conn) Send(f Frame) bool {
    select {
    case c.out <- f:
        return true
    default:
        if c.dropped.Add(1) >= slowDropLimit {   // 累计 64 帧
            c.kick("slow consumer")
        }
        return false
    }
}
```

为什么必须这样：若阻塞等待，一个卡住的 TCP 连接就会占住读循环，
进而拖垮整个会话的事件分发（Head-of-Line Blocking）。丢帧 + 踢出是
正确的取舍——慢消费者自己有问题，不应连累他人。

### 6.6 Relay 侧资源限制

| 限制 | 配置 | 默认 |
|---|---|---|
| 单帧用户数据 | `P2PS_MAX_RELAY_FRAME` | 64 KiB |
| 每连接中继带宽 | `P2PS_RELAY_BYTES_PER_SEC` | 512 KiB/s（令牌桶） |
| 控制面消息速率 | `P2PS_MSG_RATE_PER_SEC` | 25 条/s，突发 50 |
| 中继总开关 | `P2PS_RELAY_ENABLED` | true（false 时只做信令） |

---

## 7. NAT Traversal（三阶段演进）

按需求「先把 Token 配对 + 双向 Relay 做稳，再逐步加 NAT」的顺序，
当前实现完成了**阶段一 + 阶段二的数据通道**，阶段三的打洞算法作为下一步。

### 阶段一（已完成）：Token 配对 + 双向中继

```
Client A ──▶ Server ──▶ Client B        服务器中继，双向通信
```

### 阶段二（数据通道已完成）：候选发现与交换

候选模型已就位，服务器只做校验与透传：

```go
type Candidate struct {
    Type     string // host | srflx | relay | prflx（ICE 风格）
    IP       string
    Port     int
    Proto    string // udp | tcp | quic
    Priority uint32 // 越大越优先
}
```

控制面消息：`candidate_offer` / `candidate_answer` / `punch_result`。
服务器处理逻辑（`internal/server/ws.go`）：

```go
case protocol.MsgCandidateOffer, protocol.MsgCandidateAnswer:
    if len(m.Candidates) > s.cfg.MaxCandidates {   // 上限 16，防滥用
        ws.sendError(protocol.ErrPayloadTooLarge, "too many candidates")
        return true
    }
    ws.forwardToPeer(m)                             // 只透传，不解释
    _ = s.svc.UpdateCandidates(...)                 // 落库供新成员查询
```

**服务器不解释候选语义**，这是刻意的：NAT 穿透策略（打洞时序、优先级
排序、竞速规则）属于客户端，服务器只做信令管道。好处是穿透算法可以独立
演进（换成 ICE、改用 QUIC），服务器无需改动。

### 阶段三（下一步）：UDP 打洞与连接竞速

```
A                          Server                          B
│  candidate_offer{host, srflx} ──▶ │ ──▶ │
│                                   │ ◀── │ candidate_answer{host, srflx}
│ ◀────────────── 候选透传 ─────────│
│                                                    │
│ ┌──── 并行发起 UDP 打洞（双方同时发包）────────────────┐
│ │  A → B(候选列表)    同时    B → A(候选列表)         │
│ │  成功 ⇛ peer_state=direct ──▶ 服务器广播状态        │
│ │  超时 ⇛ 保持 relay 路径不变（不中断现有会话）       │
│ └───────────────────────────────────────────────────┘
```

设计约束（务必遵守）：

1. **Relay 兜底不因打洞失败而中断**：`relay` 路径全程可用，打洞是
   「锦上添花」。切换失败时静默回退，用户无感。
2. **连接竞速而非顺序尝试**：多个候选并行发包（trickle），先成功的胜出，
   避免串行尝试带来的秒级延迟。
3. **状态由客户端上报**：服务器不猜连接状态，只转发 `peer_state`。

---

## 8. 加密模型

### 8.1 三层密钥的职责

| 密钥 | 算法 | 用途 | 谁能看到 |
|---|---|---|---|
| Node Identity | Ed25519 | 身份认证、`join proof` 签名 | 公钥公开 |
| X25519 临时密钥 | ECDH | 协商成对会话密钥 | 公钥经服务器透传 |
| Pairwise Session Key | HKDF-SHA256 → ChaCha20-Poly1305 | 业务数据加密 | **仅双方** |

### 8.2 Join Token 不是加密密钥

这一点是明确的红线。Join Token 只承担 `Authorization / Capability`：

- 不参与任何密钥派生；
- 不用于加密业务数据；
- 泄露的后果限于「可加入会话」，而非「可解密历史流量」。

数据加密完全由客户端在建立连接后自行协商：

```
Node Identity Key  +  Ephemeral X25519  +  Session binding  →  Pairwise Session Key
```

### 8.3 成对密钥（Pairwise Key）派生

Group 会话中**不使用共享 SessionKey**：

```
A ◀── Key_AB ──▶ B
A ◀── Key_AC ──▶ C
B ◀── Key_BC ──▶ D
```

理由：若会话中某个成员被攻破，不应因此获得其他所有 Pair 的数据密钥。
成对密钥把泄露面限制在「该成员参与的那几条边」。

派生实现：

```go
// pkg/sessionclient/crypto.go
shared, _ := self.priv.ECDH(peerPub)            // X25519

// 双方 NodeID 按字典序串接，保证两侧 info 一致
a, b := selfNodeID, peerNodeID
if a > b { a, b = b, a }
info := keyInfoPrefix + "\x00" + a + "\x00" + b

salt := sha256.Sum256([]byte(sessionID))
key, _ := hkdf.Key(sha256.New, shared, salt[:], info, 32)
cipher, _ := chacha20poly1305.New(key)
```

三个绑定项各自防一类问题：

| 绑定 | 防止 |
|---|---|
| `shared`（X25519） | 无密钥者解密 |
| `sessionID` 作 salt | 跨会话密钥复用（同一对节点在不同会话里密钥不同） |
| 双方 NodeID（有序） | 密钥换位复用 / 中间人把两条边拼成一条 |

A → B 与 B → A 派生出**同一把密钥**（NodeID 排序保证对称），因此双向
通信共用一条流密钥，无需为每个方向各派生一次。

### 8.4 数据帧格式

```
明文 → AEAD 加密 → nonce(12B) || ciphertext+tag
     → 放入 DataFrame 的 Payload
```

| 参数 | 值 | 理由 |
|---|---|---|
| 算法 | ChaCha20-Poly1305 | 纯软件实现快且不依赖 AES-NI，边缘/容器环境表现一致 |
| 密钥长度 | 32 字节 | 算法要求 |
| Nonce | **每帧随机 12 字节**（随密文一起传输） | 避免状态同步；随机 nonce 的碰撞风险在单键 2^32 帧内可忽略 |
| 认证 | Poly1305 tag（16 字节） | 提供完整性，篡改立即可见 |

接收方处理：

```go
nonce, ct := data[:12], data[12:]
pt, err := pk.aead.Open(nil, nonce, ct, nil)
if err != nil { /* 密钥不匹配或数据被篡改 */ }
```

### 8.5 密钥交换消息

X25519 公钥经控制面 `key_exchange` 透传，服务器无法从中获得任何可用信息：

```json
{ "type": "key_exchange", "target_id": "mbr_…",
  "payload": { "kex": "x25519", "pub": "<hex>", "node_id": "…" } }
```

服务器处理（与其他控制消息同一条路径）：校验目标在会话内 → 透传。
**不解析 payload**——这是「服务器不参与密钥协商」的实现保证。

### 8.6 收敛控制：避免密钥交换风暴

朴素实现会导致 `A→B→A→B…` 无限对发。这里用**两个各自幂等的标记**：

| 标记 | 触发时机 | 语义 |
|---|---|---|
| `helloSent[peer]` | 发现成员时主动发一次 | 每对端最多主动发一次 |
| `helloReplied[peer]` | 收到对方公钥后回发一次 | 每对端最多回一次 |

```go
// 幂等后：A 先发 → B 收并回发 → A 收（已发过，不再回）→ 收敛
```

为什么用两个标记而不是一个：若只用 `helloSent`，当 A 的主动 hello **丢失**时，
A 已标记「发过」，收到 B 的公钥后不再回应，B 永远拿不到 A 的公钥 → 卡死。
独立的 `replied` 标记保证「至少能回一次」，从而自愈。

### 8.7 敏感数据擦除

共享密钥与派生密钥在本函数返回前显式清零：

```go
zero(shared)
zero(key)
```

Go 的 GC 不保证及时回收，显式擦除可缩短密钥在内存中的存活窗口
（对内存转储类攻击有实际意义）。

### 8.8 尚未实现的加密增强

| 能力 | 状态 | 说明 |
|---|---|---|
| 前向保密（PFS） | 部分 | 每次 Join 生成新 X25519 临时密钥，故单次会话内前向保密成立；跨会话重连会复用新密钥 |
| 密钥轮换 | 未实现 | 长时间大流量会话应定期重协商 |
| 身份与密钥绑定校验 | 部分 | 客户端可选校验 `key_exchange` 中的 `node_id` 与服务器下发的成员公钥一致（防服务器替换公钥） |
| Group Key | 未实现 | 引入后可将 Broadcast 降为一次上行（§5.3） |

---

## 9. 状态机

### 9.1 服务端会话状态

```
                 Create()
                    │
                    ▼
              ┌──────────┐  Join(member)   ┌────────────┐
              │  WAITING │ ──────────────▶ │  JOINING   │ (requireApproval)
              └──────────┘                 └────────────┘
                    │                            │ Approve()
       activeCount == maxMembers                 │
                    ▼                            ▼
              ┌───────────┐              ┌─────────────┐
              │PAIR_READY │◀─────────────│   ACTIVE    │
              └───────────┘              └─────────────┘
                    │                            │
                    │                            │ 全员离线 && 超 IdleTimeout
                    │                            ▼
                    │                     ┌─────────────┐
                    └────────────────────▶│  EXPIRED    │ → Prune 删除
                                          └─────────────┘
```

### 9.2 成员状态

```
                    Join(新成员)
                         │
          ┌──────────────┴──────────────┐
          │ requireApproval?            │
          ▼ false                       ▼ true（首个成员除外）
    ┌──────────┐                 ┌───────────┐
    │  ACTIVE  │◀────Approve()───│  PENDING  │
    └──────────┘                 └───────────┘
          │                            │ Reject()
          │ 连接断开（Abort）           ▼
          ▼                      ┌───────────┐
    ┌───────────┐                │  DENIED   │ → 连接关闭
    │  OFFLINE  │ 宽限期内重连    └───────────┘
    │(保留记录)  │──────▶ ACTIVE（复用 MemberID/Index）
    └───────────┘
          │ 超 MemberGrace
          ▼
    ┌───────────┐
    │  EXPIRED  │ → 记录移除，Index 释放
    └───────────┘
```

### 9.3 连接状态

```
    ┌──────────────┐  Attach(new)   ┌──────────┐
    │  CONNECTING  │ ──────────────▶│   OPEN   │
    └──────────────┘                └──────────┘
                                        │
                     ┌──────────────────┼──────────────────┐
                     ▼                  ▼                  ▼
              ┌────────────┐    ┌─────────────┐    ┌──────────────┐
              │ REPLACED   │    │  SLOW_KICK  │    │    CLOSED    │
              │  (4003)    │    │   (4005)    │    │  (正常/掉线)  │
              └────────────┘    └─────────────┘    └──────────────┘
```

关闭码语义（应用私有区间 4000–4999，客户端据此决定是否重连）：

| 码 | 名称 | 客户端应做什么 |
|---|---|---|
| 4000 | `CloseProtocol` | 协议错误，修复后重连 |
| 4001 | `CloseAuth` | 鉴权失败，不要重试（除非换 token） |
| 4002 | `CloseSessionFull` | 会话已满，等他人离开 |
| 4003 | `CloseDuplicate` | **被同身份新连接顶替**（正常现象，不要重连） |
| 4004 | `CloseIdle` | 空闲超时，可立即重连 |
| 4005 | `ClosePolicy` | 限流/慢消费者，退避后重连 |
| 4006 | `CloseSessionGone` | 会话已删除/过期，不要再重连 |

---

## 10. 协议规范

### 10.1 传输与端点

| 端点 | 帧类型 | 用途 | 鉴权 |
|---|---|---|---|
| `POST /v1/*` | JSON | 会话创建/加入/管理 | Token（join）/ 票据（owner 操作） |
| `WS /ws/control` | 文本（JSON） | 成员发现、候选、密钥交换、状态、心跳 | `?ticket=` 或 `Authorization: Bearer` |
| `WS /ws/relay` | 二进制（DataFrame） | 加密业务数据转发 | 同上（且要求成员已 `active`） |

### 10.2 控制消息（按需求清单逐项对应）

| 需求清单 | 本协议实现 | 方向 |
|---|---|---|
| `SESSION_CREATE` / `SESSION_CREATED` | `POST /v1/sessions`（REST） | C→S |
| `SESSION_JOIN` / `SESSION_JOINED` | `POST /v1/sessions/{id}/join` + `joined` | C→S / S→C |
| `SESSION_LEAVE` / `SESSION_CLOSED` | `leave` / `session_closed` | C→S / S→C |
| `MEMBER_JOIN` / `MEMBER_LEFT` / `MEMBER_LIST` | `member_joined` / `member_left` / `member_list` | S→C |
| `PEER_INFO` | 包含在 `member_list` / `member_joined` 的 `member` 字段 | S→C |
| `CANDIDATE_OFFER` / `CANDIDATE_ANSWER` | `candidate_offer` / `candidate_answer` | 双向 |
| `PUNCH_START` / `PUNCH_RESULT` | `punch_start` / `punch_result` | S→C / 双向 |
| `DIRECT_CONNECT` / `DIRECT_CONNECTED` | `peer_state`（`state: direct`） | 双向／上报 |
| `RELAY_OFFER` / `RELAY_CONNECT` / `RELAY_CONNECTED` | **数据面直连即中继**：无需单独协商，`/ws/relay` 建立后即可收发（`peer_state: relay`） | — |
| `OPEN_STREAM` / `DATA` / `CLOSE_STREAM` / `RESET_STREAM` | `stream_open` / DataFrame / `stream_close` + `FlagClose` / `FlagReset` | 双向 |
| `PING` / `PONG` | `ping` / `pong` | 双向 |
| `ERROR` | `error` | S→C |

**关于 `RELAY_*` 的说明**：原需求列了 `RELAY_OFFER/RELAY_CONNECT/RELAY_CONNECTED`
三步协商。本实现省略了这套握手，原因是：**Relay 是本系统的默认数据通道，
而不是需要双方同意的降级路径**。数据面 WebSocket 在认证后立即可用，
「是否走中继」由客户端是否直连成功决定，对端无需参与。少一套状态机就少
一类状态不一致 bug。若将来引入「中继需对端显式同意」（如计量计费场景），
可在此增加 `relay_offer` 消息，协议留有空间。

### 10.3 消息信封

```jsonc
{ "type": "member_joined",
  "session_id": "sess_…",
  "member_id": "mbr_…",           // 事件主体
  "target_id": "mbr_…",           // 客户端上行时指定目标
  "from": "mbr_…",                // 服务器转发时盖写的来源
  "member": { /* MemberInfo */ },
  "members": [ /* MemberInfo[] */ ],
  "candidates": [ /* Candidate[] */ ],
  "transport": "direct|relay",
  "state": "negotiating|direct|relay|closed",
  "stream_id": 3,
  "ts": 1758600000000,
  "payload": { /* 对服务器不透明的 JSON */ },
  "code": "unauthorized",
  "message": "…" }
```

`payload` 的设计是有意的**不透明扩展位**：`key_exchange` 的 X25519 公钥、
候选附加信息都放这里，服务器只校验成员关系后透传，不解析内容。这样将来
新增「应用自定义控制信息」时不需要改服务器。

### 10.4 数据面 DataFrame（二进制）

```
偏移   0        1        2        4        5          9        11      12
     +--------+--------+--------+--------+----------+--------+-------+----------+
     | magic  | flags  | src idx| dsttype| streamID | seq16  | 保留  | 目标+载荷 |
     |  1B    |  1B    |  2B    |  1B    |   4B     |  2B    |  1B   |          |
     +--------+--------+--------+--------+----------+--------+-------+----------+
```

- `magic = 0x21`（高半字节 `0x2` 标识「数据面/版本 1」）。
- 所有整数**大端**。
- `src idx` 由服务器转发时**盖写**为真实发送方下标。
- 目标编码：
  - `UNICAST(1)`：紧随 2 字节目标下标 → 头部共 14 字节
  - `MULTICAST(2)`：紧随 1 字节数量 + 2×N 字节下标
  - `BROADCAST(3)`：无附加目标 → 头部共 12 字节
- `flags`：`FlagReliable=0x01`、`FlagClose=0x02`、`FlagReset=0x04`、`FlagPing=0x08`。

```go
// 解析只做结构校验，不触碰 Payload——它是密文。
func ParseFrame(b []byte) (*Frame, error) {
    if len(b) < frameFixedHeader { return nil, errors.New("frame too short") }
    if b[0] != frameMagic { return nil, fmt.Errorf("bad magic 0x%02x", b[0]) }
    ...
}
```

> 排查记录：`EncodeFrame` 的单播分支早期漏了 `off += 2`，导致 payload
> 覆盖了目标下标字段（编码后目标变成 `25966`）。`TestFrameUnicastRoundTrip`
> 抓到了它——这正是「协议层必须双向往返测试」的理由。

### 10.5 错误码

| 码 | 含义 | HTTP 对应 |
|---|---|---|
| `bad_request` | 入参非法 | 400 |
| `unauthorized` | token/签名/身份校验失败 | 401 |
| `session_not_found` | 会话不存在 | 404 |
| `session_expired` | 会话过期 | 410 |
| `session_closed` | 会话已关闭 | 410 |
| `session_full` | 成员已满 | 409 |
| `join_limit_reached` | 加入次数用尽 | 409 |
| `token_revoked` | token 已撤销 | 403 |
| `pending_approval` | 等待审批 | 403 |
| `approval_denied` | 审批被拒 | 403 |
| `member_not_found` | 成员不存在/离线 | 404 |
| `not_session_member` | 非会话成员 | 403 |
| `rate_limited` | 触发限流 | 429 |
| `payload_too_large` | 帧超限 | 413 |
| `relay_denied` | 中继被拒/关闭 | 403 |
| `duplicate_connection` | 连接被顶替 | — |
| `unsupported` | 未知消息类型 | — |
| `server_error` | 服务器内部错误 | 500 |

---

## 11. HTTP API

| 方法 & 路径 | 说明 | 鉴权 |
|---|---|---|
| `POST /v1/sessions` | 创建会话，返回 `session_id` + `join_token` + `join_code` | 无（受每 IP 限流） |
| `GET /v1/sessions/{id}` | 会话详情（含成员，**不含 token**） | 无 |
| `DELETE /v1/sessions/{id}` | 解散会话 | owner 票据 |
| `POST /v1/sessions/{id}/join` | 按 session_id 加入 | Token + join proof |
| `POST /v1/session/join` | 按 `join_code` 加入 | Token + join proof |
| `POST /v1/sessions/{id}/leave` | 离开 | 票据/member_id |
| `GET /v1/sessions/{id}/members` | 成员列表（含 pending） | 无 |
| `GET /v1/sessions/{id}/members/{mid}` | 单个成员 | 无 |
| `POST /v1/sessions/{id}/revoke-token` | 撤销 token | owner 票据 |
| `POST /v1/sessions/{id}/rotate-token` | 轮换 token（返回新凭据） | owner 票据 |
| `POST /v1/sessions/{id}/approve` | 批准 pending 成员 | owner 票据 |
| `POST /v1/sessions/{id}/reject` | 拒绝/移除成员 | owner 票据 |
| `GET /v1/health` | 存活探针 | 无 |
| `GET /v1/stats` | 运行统计 | 可选 AdminToken |
| `GET /metrics` | Prometheus 文本 | 可选 AdminToken |
| `GET /v1/relays` | Relay 列表（多 Relay 接入点） | 无 |

**创建会话**

```bash
curl -s -X POST http://127.0.0.1:60000/v1/sessions \
  -H 'content-type: application/json' \
  -d '{"mode":"pair"}'
```

```json
{ "session_id": "sess_…",
  "join_token": "…43 字符 base64url…",
  "join_code": "P2P-7K4X-9M2P-…",
  "mode": "pair", "max_members": 2, "join_limit": 0,
  "require_approval": false,
  "expires_at": "2026-09-23T08:00:00Z",
  "created_at": "2026-09-23T07:30:00Z",
  "join_hint": "p2p-node join P2P-7K4X-…" }
```

`join_token` 与 `join_code` **仅此刻返回一次**（服务器只存哈希），
响应中不含任何可反推原始 token 的字段。

**加入会话**

```jsonc
// POST /v1/sessions/{id}/join 或 /v1/session/join
{ "join_code": "P2P-7K4X-…",        // 或 session_id + join_token
  "node_id": "…32 hex…",
  "public_key": "…64 hex…",
  "nonce": "…64 hex…",
  "signature": "…128 hex…",         // join proof
  "capabilities": ["udp","tcp","relay"],
  "candidates": [] }
```

```jsonc
// 响应
{ "session_id": "sess_…", "member_id": "mbr_…", "index": 2,
  "is_owner": false, "status": "active", "rejoined": false,
  "ticket": "…",                    // 用于两条 WS 的升级
  "members": [ /* 已生效的其它成员 → 自动发现 */ ],
  "mode": "pair", "heartbeat_ms": 15000,
  "control_url": "/ws/control", "relay_url": "/ws/relay" }
```

`members` 字段就是「自动发现」的实现：**客户端无需任何额外查询即可知道
房间里还有谁**。后加入者一次拿到全部既有成员；先加入者通过后续的
`member_joined` 广播获知新人。

**票据（Connection Ticket）**

join 成功后签发，用于两条 WebSocket 升级：

```
ticket = base64url(payload) + "." + base64url(HMAC-SHA256(key, payload))
payload = { session_id, member_id, connection_id, role, exp }
```

无状态自校验——任意实例共享 `P2PS_TICKET_KEY` 即可验证，**无需查询共享存储**。
这是水平扩展的前提（§19）。

---

## 12. 存储与数据库

### 12.1 分层

```
session.Service  ──▶  storage.Store (接口)  ──▶  MemoryStore（默认）
                                            └─▶  RedisStore / PostgresStore（可插拔）
```

`session.Service` 只依赖接口，因此替换持久化不影响业务逻辑。

### 12.2 关键接口约束

```go
type Store interface {
    Create(ctx, *SessionRecord) error
    Get(ctx, sessionID) (*SessionRecord, error)
    GetByJoinCode(ctx, code) (*SessionRecord, error)
    Join(ctx, sessionID, JoinRequest) (*JoinOutcome, error)   // 必须原子
    UpdateMember(ctx, sessionID, *MemberRecord) error
    RemoveMember(ctx, sessionID, memberID) error
    MarkOffline(ctx, sessionID, memberID, connectionID, now) error
    Revoke(ctx, sessionID, now) error
    RotateToken(ctx, sessionID, newHash, newCode, now) error
    Close(ctx, sessionID) error
    Delete(ctx, sessionID) error
    List(ctx, ListFilter) ([]*SessionRecord, error)
}
```

**`Join` 必须原子**——容量/加入次数/过期/撤销的校验与成员写入不能被并发
穿插，否则并发加入会突破 `max_members`。测试
`TestJoinIsAtomicUnderConcurrency` 用 10 个并发 join 验证「只能成功 3 个」。

### 12.3 内存实现的并发模型

单个 `sync.RWMutex` 保护全部会话：信令服务器的临界区极短（map 查找 +
结构体替换），粗锁比细粒度锁更易证明正确；数据面转发**不经过这里**
（走 `member.Hub` 的连接队列），因此不会成为吞吐瓶颈。

所有出参是**深拷贝**，调用方可以安全长期持有：

```go
func cloneSession(s *SessionRecord) *SessionRecord { /* members 逐个 clone */ }
```

测试 `TestSnapshotsAreIsolated` 验证「改快照不影响存储内部状态」。

### 12.4 SQLite 实现（单机持久化）

`SQLiteStore`（`internal/storage/sqlite.go`）把会话状态落到**单个数据库文件**，
适用于自托管单机部署（`docker run -v p2psession-data:/data`）。

```
session.Service  ──▶  storage.Store (接口)  ──▶  MemoryStore（默认，重启即清空）
                                            ├─▶  SQLiteStore（持久化，单文件）
                                            └─▶  RedisStore / PostgresStore（多实例，可插拔）
```

**方案选择**：默认仍是内存实现；持久化是**显式选择**
（`P2PS_STORAGE_BACKEND=sqlite`）。镜像里默认已切到 SQLite，
因为容器场景下「重新部署丢会话」通常是意外而非期望。

#### 驱动选择：为什么是 `modernc.org/sqlite`

| 驱动 | CGO | 影响 |
|---|---|---|
| `mattn/go-sqlite3` | **需要** | 失去 `CGO_ENABLED=0` 静态编译，镜像必须带 glibc/musl，构建变慢 |
| `modernc.org/sqlite` | **不需要** | 纯 Go 实现，保住「单文件静态二进制」这一部署优势 ✓ |

已实测验证：`CGO_ENABLED=0 GOOS=linux go build` 产出
`statically linked` 可执行文件（`ldd` 报告 `not a dynamic executable`），
容器镜像 44.7 MB。

#### Schema

时间统一存 **Unix 纳秒整数**：SQLite 无原生时间类型，整数便于比较与索引，
也避免时区/格式歧义（`0` 表示零值时间，如未设置过期）。

```sql
CREATE TABLE sessions (
    id               TEXT PRIMARY KEY,
    mode             TEXT NOT NULL,
    join_code        TEXT NOT NULL DEFAULT '',
    join_code_norm   TEXT NOT NULL DEFAULT '',   -- 归一化 code（大小写/连字符不敏感）
    token_hash       TEXT NOT NULL DEFAULT '',
    join_limit       INTEGER NOT NULL DEFAULT 0,
    joins_used       INTEGER NOT NULL DEFAULT 0,
    max_members      INTEGER NOT NULL DEFAULT 0,
    require_approval INTEGER NOT NULL DEFAULT 0,
    owner_member_id  TEXT NOT NULL DEFAULT '',
    revoked          INTEGER NOT NULL DEFAULT 0,
    closed           INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL,
    expires_at       INTEGER NOT NULL DEFAULT 0,
    idle_timeout     INTEGER NOT NULL DEFAULT 0
);

-- 只对非空 code 建唯一索引：让多条空 code 共存（未使用 code 的会话）
CREATE UNIQUE INDEX idx_sessions_join_code
    ON sessions (join_code_norm) WHERE join_code_norm <> '';

CREATE TABLE members (
    session_id    TEXT NOT NULL,
    id            TEXT NOT NULL,
    member_index  INTEGER NOT NULL,
    node_id       TEXT NOT NULL,
    public_key    TEXT NOT NULL,
    role          TEXT NOT NULL,
    status        TEXT NOT NULL,
    capabilities  TEXT NOT NULL DEFAULT '[]',   -- JSON
    candidates    TEXT NOT NULL DEFAULT '[]',   -- JSON
    connection_id TEXT NOT NULL DEFAULT '',
    joined_at     INTEGER NOT NULL,
    last_seen     INTEGER NOT NULL,
    leave_at      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, id),
    FOREIGN KEY (session_id) REFERENCES sessions (id) ON DELETE CASCADE
);
CREATE INDEX  idx_members_node  ON members (session_id, node_id);
CREATE UNIQUE INDEX idx_members_index ON members (session_id, member_index);
```

**会话与成员分表**（而非把成员序列化成一个 JSON 列）是刻意的：
成员变更（审批、候选更新、上下线）只写一行 `members`，
不必重写整个会话——这与 cfp2p 里「每次 touch 全量重写」的写入放大
是同一个问题的两种解法（那里靠按键分片，这里靠关系表）。

#### 并发与原子性

连接池固定 **1 条连接**（`SetMaxOpenConns(1)`）。这是刻意的：
SQLite 写锁是库级的，多连接只会把锁竞争搬进 Go 的连接池，
换来的是需要处理 `SQLITE_BUSY` 分支；而本服务的信令写入极少
（**心跳不写库**），单连接不会成为瓶颈，却让正确性一目了然。

DSN 参数（`sqliteDSN`）：

| 参数 | 作用 |
|---|---|
| `journal_mode(WAL)` | 允许读写并行；崩溃后自动恢复 |
| `synchronous(NORMAL)` | WAL 下已足够安全（断电最多丢最后若干事务，不损坏库） |
| `foreign_keys(ON)` | SQLite 默认关闭外键，必须显式开启才能级联删除 |
| `busy_timeout(5000)` | 竞争时等待而非立刻报错 |
| `_txlock=immediate` | 写事务一开始就取写锁，避免「读→升级写锁」阶段的失败 |

**原子加入**：`Join` 复用 `SessionRecord` 上的策略方法
（`freeIndex` / `MemberByNodeID` / `ActiveMembers` / `Expired`），
只把「读 → 变更 → 写回」包进一个 `immediate` 写事务：

```go
func (s *SQLiteStore) Join(ctx, sessionID, req) (*JoinOutcome, error) {
    return s.inTx(ctx, func(q querier) error {
        rec := loadSession(q, sessionID)   // 读
        // ... 与 MemoryStore 完全相同的策略判断 ...
        insertMemberQ(q, sessionID, m)     // 写
    })
}
```

**为什么不在 SQL 里重写这些规则**：容量、撤销、审批、下标复用这类规则
只能有一个权威实现。若在 SQL 的 `WHERE` 条件里再写一遍，两条路径迟早
漂移——例如「pending 成员不占名额」这条规则极易漏掉，而存储层的
行为不一致是最难排查的一类缺陷。因此**策略留在 Go 里，SQL 只负责存取**。

#### 一致性测试（关键设计）

`store_conformance_test.go` 用**同一套用例同时跑两个后端**
（`forEachBackend`），而不是给 SQLite 单独写一份：

| 用例 | 覆盖的语义 |
|---|---|
| `CreateAndGet` | 重复 ID / 重复 code 冲突、字段往返 |
| `GetByJoinCodeIsCaseInsensitive` | 人工转写容错 |
| `JoinEnforcesCapacity` | 容量上限 |
| `JoinIsAtomicUnderConcurrency` | **10 并发只能成功 3 个** |
| `RejoinReusesMemberIDAndIndex` | 重连恢复原 MemberID/Index，且不新增行 |
| `JoinRejectsExpiredRevokedClosed` | 四种拒绝路径 + `ErrNotFound` 一致性 |
| `JoinLimit` | 加入次数上限（含 `joins_used` 已持久化） |
| `PendingDoesNotConsumeCapacity` | 审批不占名额（死结防护） |
| `MarkOfflineOnlyForCurrentConnection` | 旧连接不误伤新连接 |
| `MemberLifecycle` | UpdateMember / RemoveMember / **下标复用** |
| `RotateToken` / `RotateTokenRejectsTakenCode` | 轮换与冲突回滚 |
| `SnapshotsAreIsolated` | 深拷贝语义 |
| `List` / `ListExpiredBefore` | 枚举、过滤、成员重载（Prune 的入口） |
| `DeleteReleasesJoinCode` | code 释放 + 无孤儿成员行 |
| `ContextCancellation` | 关停时不再写库 |

新增存储实现（Redis/Postgres）只要加进 `backends()` 即可获得全部覆盖。

### 12.5 若换成外部存储（PostgreSQL 参考 schema）

```sql
CREATE TABLE sessions (
    id               TEXT PRIMARY KEY,
    mode             TEXT NOT NULL,              -- pair | group
    join_code        TEXT UNIQUE NOT NULL,
    token_hash       TEXT NOT NULL,              -- sha256(raw token) hex
    join_limit       INT  NOT NULL DEFAULT 0,
    joins_used       INT  NOT NULL DEFAULT 0,
    max_members      INT  NOT NULL,
    require_approval BOOL NOT NULL DEFAULT false,
    owner_member_id  TEXT,
    revoked          BOOL NOT NULL DEFAULT false,
    closed           BOOL NOT NULL DEFAULT false,
    created_at       TIMESTAMPTZ NOT NULL,
    expires_at       TIMESTAMPTZ NOT NULL,
    idle_timeout     INTERVAL
);
CREATE INDEX idx_sessions_expires ON sessions (expires_at);

CREATE TABLE members (
    id            TEXT PRIMARY KEY,
    session_id    TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    index         SMALLINT NOT NULL,             -- 数据面下标
    node_id       TEXT NOT NULL,
    public_key    TEXT NOT NULL,
    role          TEXT NOT NULL,                 -- owner | member
    status        TEXT NOT NULL,                 -- active | pending | denied | left
    capabilities  JSONB DEFAULT '[]',
    candidates    JSONB DEFAULT '[]',
    connection_id TEXT,
    joined_at     TIMESTAMPTZ NOT NULL,
    last_seen     TIMESTAMPTZ NOT NULL,
    leave_at      TIMESTAMPTZ,
    UNIQUE (session_id, index),
    UNIQUE (session_id, node_id)                 -- 同一节点在会话内唯一
);
CREATE INDEX idx_members_session ON members (session_id);
```

原子加入用一行 SQL 解决（`SELECT ... FOR UPDATE` 或条件插入）：

```sql
-- 伪代码：在事务内锁定会话行 → 校验 → 插入成员 → 递增 joins_used
BEGIN;
SELECT * FROM sessions WHERE id = $1 FOR UPDATE;
-- 校验 revoked / expires_at / joins_used / 活跃成员数
INSERT INTO members (...) VALUES (...) RETURNING id, index;
UPDATE sessions SET joins_used = joins_used + 1 WHERE id = $1;
COMMIT;
```

### 12.6 默认值的选择：内存 vs SQLite

会话是**短生命周期、强实时**的状态：

- 典型 TTL 30 分钟，且大多数会话活跃期只有几分钟；
- 成员表变化频繁（join/leave/offline），持久化写入成为纯负担；
- 数据面延迟敏感，任何存储往返都会加在关键路径上。

因此**库的默认值仍是内存实现**（`go run ./cmd/server` 零依赖启动），
持久化是显式选择。两种后端的取舍：

| 场景 | 建议后端 | 理由 |
|---|---|---|
| 本地开发 / 临时演示 | `memory` | 零配置，重启即清空反而是优点 |
| 单机自托管（容器） | `sqlite` | 重新部署/重启后会话仍在，客户端无需重新 join |
| 多实例水平扩展 | Redis/Postgres | SQLite 是本地文件，多进程不能共写同一份 |

**镜像内的默认值是 `sqlite`**：容器场景下「重新部署丢会话」通常是意外
而非期望。想显式回到无状态行为，设 `P2PS_STORAGE_BACKEND=memory`。

> 注意：SQLite **不解决多实例问题**。它是单文件本地存储，
> 多个进程同时写同一份会互相阻塞（即便在共享文件系统上）。
> 需要多实例时（§19.2），仍须换用 Redis/Postgres 并共享 `P2PS_TICKET_KEY`
> ——**票据是无状态的，因此数据面转发可以留在任意实例上进行**
> （只要该实例持有对应连接）。

### 12.7 卷、备份与恢复

容器内数据库路径由 `P2PS_SQLITE_PATH` 决定（默认 `/data/p2psession.db`），
`/data` 是 compose 里的具名卷 `p2p-data`。

```bash
# 查看数据库文件（含 WAL/SHM 附属文件）
docker compose exec relay ls -la /data

# 在线备份：用 sqlite3 的 .backup（一致性快照，无需停机）
docker compose exec relay sh -c \
  'command -v sqlite3 >/dev/null || echo "镜像未含 sqlite3，见下方 tar 方式"'

# 离线备份（停机，最简单可靠）：整卷打包
docker compose stop relay
docker run --rm -v p2psession_p2p-data:/d -v "$PWD:/b" alpine \
  tar czf /b/p2p-backup-$(date +%F).tgz -C /d .
docker compose start relay

# 恢复：解包回卷
docker compose stop relay
docker run --rm -v p2psession_p2p-data:/d -v "$PWD:/b" alpine \
  sh -c 'rm -f /d/* && tar xzf /b/p2p-backup-2026-09-23.tgz -C /d'
docker compose start relay
```

**备份要点**：

- **不要只拷 `p2psession.db` 而忽略 `-wal` / `-shm`**。WAL 模式下最新数据
  可能还在 `-wal` 里，单独拷主库会丢最近的事务。要么整目录打包，
  要么用 `sqlite3 .backup`（它会做检查点）。
- **停机备份是可接受的选择**：会话是短生命周期状态，丢几分钟通常
  只意味着客户端重新 join 一次。
- **恢复后票据密钥必须一致**，否则已下发的连接票据（HMAC 自校验）
  会验签失败。若 `P2PS_TICKET_KEY` 是启动时随机生成的，恢复后
  客户端需重新 join。生产环境请显式配置该密钥。

**可选的自动备份**（compose 加一个 sidecar）：

```yaml
  backup:
    image: alpine
    restart: unless-stopped
    depends_on: [relay]
    volumes:
      - p2p-data:/data:ro
      - ./backups:/backups
    # 每 6 小时做一次一致性快照（.backup 会正确处理 WAL）
    entrypoint: ["/bin/sh", "-c"]
    command: >
      apk add --no-cache sqlite >/dev/null &&
      while true; do
        sqlite3 /data/p2psession.db ".backup /backups/p2p-$$(date +%F-%H%M).db";
        ls -1t /backups/*.db | tail -n +25 | xargs -r rm -f;
        sleep 21600;
      done
```

> `:ro` 挂载 + `.backup` 是可行的：SQLite 的备份 API 通过读事务取得
> 一致快照，不要求写权限。保留最近 24 份，避免磁盘无限增长。

---

## 13. Go 项目结构

```
p2psession/
├── go.mod
├── README.md                        # 快速上手（二进制优先）
├── DESIGN.md                        # 本文档
├── VERSION                          · 权威版本号（make dist 注入）
├── Makefile                        · build / dist / install / check
├── Dockerfile                      · 备选：多阶段构建，静态二进制，非 root
├── docker-compose.yml              · 备选：服务器 + 可选 cloudflared 隧道
├── .env.example                    · 全部 P2PS_ 配置项
├── deploy/                         · 二进制部署（推荐方式）
│   ├── install.sh                  · 一键安装/升级/卸载（校验 SHA256、失败回滚）
│   └── systemd/
│       ├── p2psession.service      · 加固 unit（非 root、只读根、空能力集）
│       └── p2psession.env.example  · systemd 环境文件样例
│
├── cmd/
│   ├── server/main.go              · 服务器入口（配置、装配、优雅退出）
│   └── p2p-node/
│       ├── main.go                 · CLI：init / create / join / whoami
│       └── console.go              · 交互控制台（/to /all /list /stream）
│
├── internal/
│   ├── protocol/                   · 线协议（控制消息 + 数据帧）
│   │   ├── protocol.go             · 常量、错误码、关闭码
│   │   ├── message.go              · 控制消息信封、MemberInfo、Candidate
│   │   └── frame.go                · 二进制 DataFrame 编解码
│   ├── auth/                       · 密码学原语
│   │   ├── token.go                · Join Token / Join Code / 哈希
│   │   ├── ticket.go               · HMAC 连接票据
│   │   └── identity.go             · Ed25519 身份与 join proof
│   ├── storage/                    · 持久化抽象（含两个后端）
│   │   ├── store.go                · Store 接口 + 记录结构
│   │   ├── memory.go               · 单锁内存实现（深拷贝快照）
│   │   └── sqlite.go               · SQLite 实现（单文件持久化）
│   ├── session/service.go          · 会话业务编排（创建/加入/审批/撤销/清扫）
│   ├── member/hub.go               · 在线连接中心（presence/顶替/慢消费者）
│   ├── relay/relay.go              · 数据面授权与转发
│   ├── server/                     · HTTP + WebSocket 装配
│   │   ├── server.go               · 路由、中间件、限流、优雅退出
│   │   ├── handlers.go             · REST 处理器
│   │   ├── ws.go                   · /ws/control 与 /ws/relay
│   │   └── alias.go                · 类型别名
│   ├── config/config.go            · P2PS_ 环境变量
│   ├── version/version.go          · 构建期注入的版本信息（ldflags）
│   └── metrics/metrics.go          · Prometheus 文本指标（含 build_info）
│
├── pkg/sessionclient/              · 客户端 SDK（对外可复用）
│   ├── client.go                   · Join、连接、生命周期、访问器
│   ├── loops.go                    · 控制面/数据面读循环、发送、成员/密钥管理
│   ├── crypto.go                   · X25519 + HKDF 成对密钥
│   ├── aead.go                     · ChaCha20-Poly1305
│   ├── stream.go                   · 逻辑流（分片/排序/关闭/重置）
│   ├── group.go                    · Broadcast / Multicast
│   ├── create.go                   · 创建会话（REST）
│   ├── owner.go                    · owner 操作（审批/撤销/轮换）
│   └── api.go                      · SendTo / KexReady / Ping
│
├── e2e/e2e_test.go                 · 端到端（真实 httptest + 双客户端 SDK）
└── cloudflare/
    ├── worker.js                   · Worker + Durable Object 等价实现
    ├── wrangler.toml
    └── README.md
```

**依赖方向**（严格单向，无环）：

```
cmd ──▶ server ──▶ session ──▶ storage
              ├─▶ member        （不依赖 session）
              ├─▶ relay  ──▶ member + session.Membership（窄接口）
              └─▶ auth / protocol / config / metrics
pkg/sessionclient ──▶ auth / protocol
```

`relay` 通过窄接口 `Membership` 依赖 `session`，而不是直接引用
`session.Service`——这样 relay 可独立测试，也避免 `session ↔ relay` 循环。

---

## 14. 核心实现要点

### 14.1 连接生命周期（服务端）

每条 WebSocket 两个 goroutine：读循环串行处理上行消息，写泵独占写操作
并消费该连接的出站队列。

```go
func (ws *wsSession) serveControl() {
    done := make(chan struct{})
    defer close(done)
    go ws.writePump(done)          // 独占写
    ws.sendMemberList()            // 首帧即下发成员快照 → 自动发现
    for {
        _, data, err := ws.conn.ReadMessage()
        if err != nil { return }
        msg, _ := protocol.Decode(data)
        if !ws.handleControlMsg(msg) { return }
    }
}
```

> 调试记录：早期版本在 `writePump` 里轮询 `st.peer` 字段，而读循环在认证
> 成功时写该字段 —— `-race` 报出真实竞争。修复方式是用一个**容量为 1 的
> channel 发布会话**，利用 channel 的 happens-before 语义替代轮询；
> 同时 `contextTODO()` 每条消息都启动 goroutine 等超时，改为
> `storeCtx()` + `defer cancel()`，避免高频消息下的 goroutine 堆积。

### 14.2 自动发现的两个方向

「仅凭同一个 Token 自动配对」需要两条路径都成立：

| 场景 | 机制 | 代码位置 |
|---|---|---|
| **后加入者**发现既有成员 | join 响应 / `member_list` 携带快照 | `handlers.go` join 响应 |
| **先加入者**发现新成员 | 服务器广播 `member_joined` | `handlers.go` 广播段 |

第二条是关键——没有它，先加入的 A 只在建连时拿到一次快照，之后若不重连
就永远不知道 B 来了。这是「同一个 Token 自动配对」能否成立的**决定性一环**。

```go
// internal/server/handlers.go — join 成功后
if res.Member.Status == protocol.MemberActive {
    info := memberInfo(res.Member, s.hub)
    s.hub.BroadcastControl(res.Session.ID, res.Member.ID, &protocol.Message{
        Type: protocol.MsgMemberJoined, Member: &info,
    })
    // 成员到齐 → 通知全体可以开始协商（pair 的第 2 人加入即触发）
    if res.Session.MaxMembers > 0 && active >= res.Session.MaxMembers {
        s.hub.BroadcastControl(res.Session.ID, "", &protocol.Message{
            Type: protocol.MsgSessionReady, SessionID: res.Session.ID,
        })
    }
}
```

### 14.3 审批语义的一处关键决策

会话开启 `require_approval` 时，**首个加入者（owner）自动生效**：

```go
// internal/storage/memory.go
isFirst := rec.OwnerMemberID == ""
pending := rec.RequireApproval && !isFirst
```

若不加这个例外，owner 自己会卡在 `pending`，而唯一有权审批的人正是他自己
——会话永久不可用。这个 bug 在单元测试中被发现并记录。

配套决策：**pending 成员不占 `MaxMembers` 名额**。否则「满员 + 需审批」会
形成死结（无人能再加入，owner 也无从审批）。

### 14.4 宽限期与重连恢复

成员掉线（网络中断）与主动离开语义不同：

| 行为 | 服务端处理 | MemberID |
|---|---|---|
| `Close()`（发 leave） | 记录移除，Index 释放 | 重连得到新 ID |
| `Abort()`（静默断开） | 标记离线，保留记录 | **宽限期内重连复用原 ID** |

```go
// internal/session/service.go
func (s *Service) MarkOffline(ctx, sessionID, memberID, connectionID string) error

// Prune 中回收超宽限期的离线成员
if now.Sub(m.LeaveAt) > s.cfg.MemberGrace {
    s.store.RemoveMember(ctx, rec.ID, m.ID)
}
```

SDK 侧通过 `Abort()` 显式提供「模拟掉线」能力，使该路径可测试。

### 14.5 优雅退出

```go
// internal/server/server.go
func (s *Server) Shutdown(ctx context.Context, httpSrv *http.Server) error {
    n := s.hub.CloseAll("server shutting down")   // 先关 WS（带原因）
    ...
    return httpSrv.Shutdown(ctx)                  // 再排空 HTTP
}
```

顺序很关键：`http.Server.Shutdown` 会等待长连接自行结束，而 WebSocket
不会响应它——必须先主动关闭全部 WS 连接并给出带原因的 Close 帧，让客户端
能区分「服务端下线」与「网络故障」并据此决定重连策略。

---

## 15. 二进制部署与运维（推荐方式）

**二进制是首选的部署形态**：产物是单个静态链接文件，无 libc 依赖、
无运行时、无容器引擎，`install.sh` 一条命令交给 systemd 托管。

### 15.1 为什么二进制优先于容器

| 维度 | 静态二进制 | Docker |
|---|---|---|
| 依赖 | 无（`ldd` 报 `not a dynamic executable`） | 需要 Docker daemon + 基础镜像 |
| 安装体积 | 归档约 7.2 MB（gzip 后，含两个二进制+文档） | 镜像 44.7 MB |
| 启动路径 | 直接 `execve` | daemon → containerd → runc → 进程 |
| 升级 | 换文件 + `systemctl restart` | 重新构建/拉取镜像 + 重建容器 |
| 排错 | `journalctl -u p2psession` | 需先穿透容器层看日志 |
| 加固 | systemd 原生（`ProtectSystem=strict` 等） | 依赖镜像最小化 + 运行时参数 |

容器方案仍然保留（§16–§17）——已有容器编排体系时更省心。
但**没有任何理由为了跑一个单文件服务而引入 Docker**。

### 15.2 构建：Makefile

```bash
make build       # 本机二进制 → bin/
make dist        # 6 平台归档 + SHA256SUMS → dist/
make check       # fmt-check + vet + test（提交前）
make version     # 打印将注入二进制的版本信息
```

`make dist` 的产物（实测）：

```
dist/
├── p2psession-1.0.0-linux-amd64.tar.gz
├── p2psession-1.0.0-linux-arm64.tar.gz
├── p2psession-1.0.0-linux-armv7.tar.gz
├── p2psession-1.0.0-darwin-amd64.tar.gz
├── p2psession-1.0.0-darwin-arm64.tar.gz
├── p2psession-1.0.0-windows-amd64.zip
└── SHA256SUMS
```

每个归档内含 `p2psession-server`、`p2p-node`、`README.md`、`VERSION`。
`sha256sum -c SHA256SUMS` 可直接验证。

**多平台交叉编译之所以可行**，唯一前提是 `CGO_ENABLED=0`：

```make
export CGO_ENABLED := 0
```

这也正是 §12.4 选择 `modernc.org/sqlite`（纯 Go）而非
`mattn/go-sqlite3`（需 CGO）的根本原因——否则 SQLite 会把
「纯静态单文件」这个部署优势直接吃掉。

### 15.3 版本注入

版本信息由 ldflags 注入 `internal/version`：

```make
VERSION_PKG := p2psession/internal/version
LDFLAGS := -s -w \
    -X $(VERSION_PKG).Version=$(VERSION) \
    -X $(VERSION_PKG).Commit=$(COMMIT) \
    -X $(VERSION_PKG).BuildDate=$(BUILD_DATE)
```

版本号来源按优先级回退：命令行 `VERSION=` → `VERSION` 文件 →
`git describe`（若在 git 仓库内）→ `dev`。

**另外还有一条「回填」路径**：`internal/version.resolve()` 会用
`debug.ReadBuildInfo()` 从二进制内嵌元数据补全版本/commit/时间。
这样 `go install ./cmd/server` 或直接 `go build`（没走 Makefile）
产出的二进制也能报出可读版本，而不是笼统的 `dev`。
回填只在 ldflags **未**提供对应值时生效，不会覆盖显式注入。

`BuildDate` 固定用 UTC（`date -u`），避免不同时区产出不同二进制
而破坏可复现性。

暴露位置（实测输出）：

```bash
$ p2psession-server --version
1.0.0 (unknown, 2026-09-23T14:04:23Z, go1.25.12, linux/amd64)

$ curl -s localhost:60000/metrics | grep build_info
p2psession_build_info{build_date="...",commit="...",go_version="go1.25.12",goarch="amd64",goos="linux",version="1.0.0"} 1
```

> **为什么不放进公开的 `/v1/health`**：`/metrics` 与 `/v1/stats` 受
> `P2PS_ADMIN_TOKEN` 保护，而 `/v1/health` 是匿名可查的。
> commit 与 Go 版本可用于反推未修补状态，不该向匿名者暴露。
> `/v1/health` 因此保持纯文本 `ok`（也兼容既有健康检查脚本）。

### 15.4 systemd 单元加固

`deploy/systemd/p2psession.service` 的关键项与**实测生效值**：

| 配置 | 作用 | 实测结果 |
|---|---|---|
| `User=p2psession` | 非 root 运行 | 进程属主 = `p2psession` ✓ |
| `CapabilityBoundingSet=`（空） | 清空能力集 | `/proc/PID/status` 中 `CapEff=0` ✓ |
| `ProtectSystem=strict` | 根文件系统只读 | `ProtectSystem=strict` ✓ |
| `StateDirectory=p2psession` | systemd 建数据目录并保证属主 | `/var/lib/p2psession` 属主 `p2psession`、`0750` ✓ |
| `RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX` | 只允许必要套接字 | 服务正常收发 ✓ |
| `MemoryDenyWriteExecute=yes` | 禁止可写可执行内存 | 服务正常启动 ✓ |
| `PrivateTmp` / `ProtectHome` / `RestrictNamespaces` … | 隔离 | 全部生效 ✓ |

几点取舍说明：

- **`StateDirectory` 优于手写 `ReadWritePaths`**：它让 systemd 负责创建
  目录、设置属主与权限，unit 里不必再猜测 uid。
- **`CapabilityBoundingSet=` 为空**（而非省略）：省略意味着「不给也不收」，
  保留调用者原有的 capability；显式置空才是真正收缩。
  代价是**不能绑特权端口**——若确需 `:80`，须放开
  `CAP_NET_BIND_SERVICE`（unit 里已注释说明）。
- **刻意不设 `MemoryMax`**：SQLite 页缓存与 Go 堆随会话数增长，
  过早设限会让进程被 systemd 直接杀掉，表现为「莫名重启」，
  比 OOM 更难排查。需要限额时应先按实测量级设定。
- **`MemoryDenyWriteExecute=yes` 是安全的**，因为这是纯 Go、
  无 JIT、无插件的二进制。

unit 可用 `systemd-analyze verify` 校验。**踩坑记录**：
`StartLimitIntervalSec` 属于 `[Unit]` 而非 `[Service]`——放在 `[Service]`
会被静默忽略（verify 报 `Unknown key name`），而 `StartLimitBurst`
看似生效只因 `[Service]` 里保留了一个已废弃的同名别名。
两项都写在 `[Unit]` 才是面向未来的正确写法。

### 15.5 安装脚本

`deploy/install.sh` 把「校验 → 建用户 → 装二进制 → 装 unit → 启动 → 自检」
串成一条命令。三条设计原则（按重要性排序）：

1. **校验和不匹配即中止安装**。拿不到 `SHA256SUMS` 也**拒绝安装**，
   而不是静默跳过——「无法验证」与「验证通过」必须区分对待。
   确实要跳过需显式 `--insecure-skip-verify`（会大声警告）。
2. **失败不留半装状态**。二进制替换前备份；若新二进制 `--version`
   执行失败或服务未通过健康检查，自动回滚旧二进制。
3. **幂等且不覆盖既有配置**。升级时若覆盖
   `/etc/p2psession/p2psession.env`，线上 `P2PS_TICKET_KEY` 会被抹掉，
   导致所有在途连接票据立即失效（客户端被迫重新 join）。

已验证的路径（全部实跑）：

| 场景 | 结果 |
|---|---|
| 全新安装 | ✓ 服务 active，健康检查通过 |
| `--upgrade` | ✓ 二进制更新，`P2PS_TICKET_KEY` 与数据库均未被改动 |
| `--uninstall` | ✓ 服务与二进制移除，数据与配置保留 |
| 重复 `--uninstall`（幂等） | ✓ exit 0，不报错 |
| 篡改归档 | ✓ 中止安装，exit 1 |
| 缺失 `SHA256SUMS` | ✓ 拒绝安装，exit 1 |
| `--check`（免 root） | ✓ 只校验不改动系统 |

**踩坑记录（两个都真实踩到）**：

- `[ -f x ] && Y=z` 作为分支最后一条命令时，条件为假会让整个分支
  返回非零，`set -e` 随即**静默退出脚本**（用户看不到任何错误）。
  必须改写为 `if`。
- **EXIT trap 的返回值会覆盖脚本退出码**。清理函数若以
  `[ -n "$X" ] && rm ...` 结尾，未设该变量的分支（如卸载流程）返回 1，
  使**成功的卸载在调用方看来是失败**（exit 1）。清理函数末尾必须
  `return 0`。

### 15.6 运维手册

```bash
# 状态
systemctl status p2psession
systemctl show p2psession -p MainPID -p NRestarts -p SubState

# 日志（结构化，含版本与存储后端）
journalctl -u p2psession -f
journalctl -u p2psession --since "10 min ago"

# 健康与指标
curl http://127.0.0.1:60000/v1/health        # ok
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:60000/v1/stats   # 含 build 字段

# 升级（保留配置与数据）
sudo ./deploy/install.sh --upgrade

# 改配置
sudoedit /etc/p2psession/p2psession.env
sudo systemctl restart p2psession
```

**文件布局**：

| 路径 | 内容 | 属主/权限 |
|---|---|---|
| `/usr/local/bin/p2psession-server` | 服务器二进制 | `root:root` `0755` |
| `/usr/local/bin/p2p-node` | 客户端 CLI | `root:root` `0755` |
| `/etc/systemd/system/p2psession.service` | unit | `root:root` `0644` |
| `/etc/p2psession/p2psession.env` | 配置（含密钥） | `root:p2psession` `0640` |
| `/var/lib/p2psession/p2psession.db` | SQLite 数据 | `p2psession:p2psession` `0750` |

> `p2psession.env` 权限必须是 `0640` 且组为 `p2psession`：
> 里面是 HMAC 密钥与可能的管理员令牌，不能让同机其他用户读到。

**备份**（二进制部署下没有「卷」，直接备份目录）：

```bash
# 在线一致性快照（需 sqlite3；.backup 会正确处理 WAL 检查点）
sudo -u p2psession sqlite3 /var/lib/p2psession/p2psession.db \
  ".backup '/var/backups/p2p-$(date +%F-%H%M).db'"

# 或停机整目录打包
sudo systemctl stop p2psession
sudo tar czf /var/backups/p2p-$(date +%F).tgz -C /var/lib p2psession
sudo systemctl start p2psession
```

> ⚠️ **不要只拷 `p2psession.db`**。WAL 模式下最新事务可能还在
> `p2psession.db-wal` 里，单独拷主库会丢最近数据。

**恢复后必须保证 `P2PS_TICKET_KEY`一致**，否则已签发的连接票据验签失败。
若当初是留空（启动时随机生成），恢复后客户端需重新 join。

### 15.7 排错表

| 现象 | 原因 | 处理 |
|---|---|---|
| 服务起不来，`status=203/EXEC` | 二进制缺失或不可执行 | 重跑 `install.sh` |
| `status=200/CHDIR` 或权限拒绝 | 数据目录属主不对 | `chown p2psession:p2psession /var/lib/p2psession` |
| `218/CAPABILITIES` | 试图绑特权端口但能力集已清空 | 用 `:60000`，或放开 `CAP_NET_BIND_SERVICE` |
| 启动即退出、日志显示配置错误 | `p2psession.env` 语法错误 | 该文件是纯 `KEY=VALUE`，**不能写 `export`、值不能加引号** |
| 重启后客户端要重新 join | `P2PS_TICKET_KEY` 未固定 | 在环境文件里写入固定密钥 |
| `NRestarts` 持续增长 | 崩溃循环 | `journalctl -u p2psession -n 100` 看首个错误 |
| 端口占用 | 60000 已被监听 | `ss -ltnp \| grep :60000`，或改 `P2PS_LISTEN` |

---

## 16. Docker（备选方案）

多阶段构建，产出静态二进制（`CGO_ENABLED=0`），运行阶段基于 alpine、
非 root 用户：

```dockerfile
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/p2psession-server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/p2p-node ./cmd/p2p-node

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget tzdata && \
    addgroup -S app && adduser -S -G app app && \
    mkdir -p /data && chown app:app /data
COPY --from=builder /out/p2psession-server /usr/local/bin/
COPY --from=builder /out/p2p-node /usr/local/bin/
ENV P2PS_LISTEN=":60000" \
    P2PS_TRUST_PROXY_HEADERS="true" \
    P2PS_STORAGE_BACKEND="sqlite" \
    P2PS_SQLITE_PATH="/data/p2psession.db"
VOLUME ["/data"]
USER app
EXPOSE 60000
HEALTHCHECK CMD wget -qO- http://127.0.0.1:60000/v1/health || exit 1
ENTRYPOINT ["/usr/local/bin/p2psession-server"]
```

设计要点：

- **镜像内含 `p2p-node`**：可在容器内直接做 create/join 冒烟测试；
- **默认启用 SQLite 持久化**，数据落 `/data` 卷，容器重建不丢会话；
  想回到「重启即清空」，设 `P2PS_STORAGE_BACKEND=memory`；
- **`mkdir -p /data && chown app:app /data` 不可省略**：容器以非 root
  （`app`）运行，无法自行在 `/` 下建目录；而 Docker 初始化**具名卷**时
  会继承镜像中该路径的属主。漏掉这一步会出现
  「SQLite 因无写权限启动失败」，且**只在挂卷时**复现（不挂卷用镜像内
  目录则正常），是最容易踩空的一类问题；
- **依赖层缓存**：`go.mod` 单独 COPY，源码变更不触发重新下载依赖；
- **健康检查走 `/v1/health`** 而非 `/v1/stats`（后者可能配置了 AdminToken）。

构建与运行（已实测）：

```bash
docker build -t p2psession-server .
docker run -d --name relay -p 60000:60000 \
  -v p2psession-data:/data \
  -e P2PS_TICKET_KEY=$(openssl rand -base64 32) \
  p2psession-server
```

实测结果：镜像 44.7 MB（静态链接、无动态依赖）；容器内 `/data` 文件属主为
`app`；healthcheck 在约 6 秒后转 `healthy`；**创建会话 → `docker restart` →
`GET /v1/sessions/{id}` 仍返回该会话**，持久化生效。

---

## 17. Docker Compose

```bash
# 仅服务器
docker compose up -d --build

# + Cloudflare 临时隧道（零账号，域名随机）
docker compose --profile quick up -d --build
docker compose logs cloudflared-quick | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com'

# + Cloudflare 命名隧道（固定域名，生产）
export CLOUDFLARE_TUNNEL_TOKEN=xxxx
docker compose --profile named up -d --build
```

配置项（`.env.example` 摘要）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `P2PS_LISTEN` | `:60000` | 监听地址 |
| `P2PS_TICKET_KEY` | 空 | 票据 HMAC 密钥（**多实例必配**，base64url 32B） |
| `P2PS_DEFAULT_TTL` | `30m` | 会话默认存活 |
| `P2PS_MEMBER_GRACE` | `5m` | 离线成员宽限期（重连恢复窗口） |
| `P2PS_TICKET_TTL` | `10m` | 连接票据有效期 |
| `P2PS_HEARTBEAT` / `P2PS_IDLE_TIMEOUT` | `15s` / `45s` | 心跳 / 空闲断开 |
| `P2PS_MAX_SESSIONS` | `0`（不限） | 单实例会话上限 |
| `P2PS_MAX_RELAY_FRAME` | `65536` | 单帧用户数据上限 |
| `P2PS_RELAY_BYTES_PER_SEC` | `524288` | 每连接中继带宽 |
| `P2PS_MSG_RATE_PER_SEC` / `_BURST` | `25` / `50` | 控制面令牌桶 |
| `P2PS_SESSION_RATE_PER_MIN` / `P2PS_JOIN_RATE_PER_MIN` | `30` / `120` | 每 IP 频率限制 |
| `P2PS_RELAY_ENABLED` | `true` | 中继总开关 |
| `P2PS_ALLOWED_ORIGINS` | 空 | 浏览器 Origin 白名单 |
| `P2PS_TRUST_PROXY_HEADERS` | `false` | 信任 `X-Forwarded-For` / `CF-Connecting-IP` |
| `P2PS_ADMIN_TOKEN` | 空 | `/v1/stats`、`/metrics` 的 Bearer |
| `P2PS_PRUNE_INTERVAL` | `60s` | 清扫间隔 |
| `P2PS_STORAGE_BACKEND` | `memory`（镜像内为 `sqlite`） | 存储后端：`memory` / `sqlite` |
| `P2PS_SQLITE_PATH` | `/data/p2psession.db` | SQLite 文件路径（仅 sqlite 后端） |

> 踩坑记录：compose 会对**所有服务**做变量展开（与 profile 无关），
> 因此命名隧道处不能用 `${VAR:?}` 强制插值——否则只跑 quick profile 时
> 也会强制要求 `CLOUDFLARE_TUNNEL_TOKEN`。已改为 `${VAR:-}`。
> 同理 `env_file` 用 `required: false`，使 `.env` 可选。

---

## 18. Cloudflare Worker / Durable Objects

完整实现见 `cloudflare/worker.js`（约 870 行）+ `wrangler.toml` + `README.md`。

核心映射：**一个 Session = 一个 Durable Object**

| Go 参考实现 | Cloudflare DO |
|---|---|
| `session.Service` + `member.Hub` | 单个 `SessionHub` 实例 |
| 一把 `sync.RWMutex` 保护房间 | DO 单线程事件循环（无需锁） |
| 内存存储 | `state.storage`（DO 自带持久化） |
| `P2PS_TICKET_KEY` 共享配置 | `wrangler secret put P2PS_TICKET_SECRET` |

DO 的输入门（input gate）保证同一时刻只有一个事件在处理中，因此成员表可以
用普通 `Map` 维护——这正是 Go 版那把锁在边缘的等价物。

**协议完全对齐**：`FRAME_MAGIC`、`DST_UNICAST/MULTICAST/BROADCAST`、
`MEMBER_ACTIVE/PENDING`、REST/WS 路径均与 Go 版一致，因此**同一个 Go 客户端
SDK 可以直接连接 Worker 版本**。

三个容易写错、已在实现中显式处理的点：

1. **DO ID 必须由 sessionId 稳定派生**
   ```js
   env.SESSION_HUB.get(env.SESSION_HUB.idFromName(sessionId))   // ✅
   // 不能用 newUniqueId()：那样「再次加入同一会话」会命中不同实例
   ```
   配套约束：Session ID 的**所有权在 Worker 入口**，DO 只接收并使用
   （`#init` 要求 `body.session_id` 必填）。早期版本让 DO 自己生成 ID，
   导致「返回给客户端的 session_id」与「实际持有状态的 DO」指向两个实例。

2. **来源下标必须由服务器盖写** —— 与 Go 版 `relay.Forward` 同语义。

3. **`await` 之间必须重新校验状态** —— DO 只保证同步段不交错，
   所有 `await storage.put()` 之后都重新读取成员状态再决策。

**已知限制**：join code → session_id 的索引需要额外 KV 命名空间
（Go 版放在内存存储的 `GetByJoinCode`）；单会话成员数建议 ≤ 100
（受 DO 内存与 CPU 时间限制）。

**何时选哪个**：

- 选 **Go 服务**：完整功能、精确限流、大房间、可观测性、可自托管；
- 选 **Worker/DO**：无服务器运维、全球边缘接入、会话多但单会话规模小。

两者协议兼容，可以并存：边缘用 DO 接入，重负载会话下沉到 Go 集群。

---

## 19. Relay 部署方案

### 19.1 单实例（默认）

```
              ┌──────────────┐
   clients ──▶│ p2psession   │  :60000
              │ (内存会话)    │
              └──────────────┘
```

零依赖、零配置。适用于中小规模。
默认内存后端下重启会清空会话（客户端自动重连）；用 **SQLite 后端**
（`P2PS_STORAGE_BACKEND=sqlite`，镜像内已默认）可让会话跨重启存活：

```
              ┌──────────────┐
   clients ──▶│ p2psession   │  :60000
              │ (SQLite 会话) │──▶ /data/p2psession.db（卷）
              └──────────────┘
```

单实例 SQLite 的边界：它是**单写者**模型（库级写锁），
适合一台机器上的自托管；不要把它挂到 NFS 等多机共享文件系统上
让多个容器同时写。

### 19.2 水平扩展

关键洞察：**连接票据是无状态自校验的（HMAC）**，因此：

```
                 ┌────────────┐
    clients ──▶  │ LB / DNS   │
                 └─────┬──────┘
          ┌────────────┼────────────┐
          ▼            ▼            ▼
     ┌────────┐  ┌────────┐  ┌────────┐
     │ relay1 │  │ relay2 │  │ relay3 │   共享 P2PS_TICKET_KEY
     └───┬────┘  └───┬────┘  └───┬────┘
         └───────────┼───────────┘
                     ▼
            ┌──────────────────┐
            │ Redis / Postgres │  会话与成员状态（storage.Store 实现）
            └──────────────────┘
```

三条要求：

1. **共享 `P2PS_TICKET_KEY`**：任意实例都能验证任意票据，无需查询；
2. **共享 `storage.Store`**：换成 Redis/Postgres 实现（§12.5）；
3. **WebSocket 需会话亲和**：数据面连接是长连接，必须落到持有该连接的实例
   （LB 用 consistent hash on `session_id`，或客户端直连指定实例）。

> 注意：3 是连接亲和而非状态亲和。状态已在共享存储里，任何实例都能处理
> 「加入」请求；只有**数据转发**必须与持有该成员 WS 连接的实例同处。

### 19.3 多 Relay 与选择（阶段五）

`GET /v1/relays` 已提供接入点：

```json
{ "relays": [ { "id": "local", "url": "https://…", "region": "local",
                "healthy": true, "priority": 0 } ] }
```

客户端可据此做延迟探测与选择（借鉴 Tailscale DERP 的 relay map 思路）：

```
Relay US ─┐
Relay EU ─┼─▶ 客户端测量 RTT / 丢包 / 可用性 ──▶ 选择最优
Relay Asia┘
```

### 19.4 Cloudflare 隧道部署

WebSocket 在 Cloudflare 网络下无需特殊配置。两种模式（见 §17）：

- **Quick Tunnel**（零账号）：域名随机、不保证持久，**仅用于演示**；
- **Named Tunnel**（生产）：固定域名，在 Zero Trust 后台配置
  `relay.example.com → http://relay:60000`。

反代（Nginx）需注意放行 Upgrade 头并关闭该 location 的响应缓冲：

```nginx
location /ws {
    proxy_pass http://127.0.0.1:60000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_read_timeout 3600s;
}
```

---

## 20. 测试

### 20.1 测试规模

| 层级 | 文件 | 用例数 |
|---|---|---|
| auth（token/ticket/identity） | 3 | 16 |
| protocol（消息/帧） | 1 | 9 |
| storage（原子性/快照隔离） | 1 | 15 |
| session（策略/审批/清扫） | 1 | 18 |
| relay（授权/单播/组播/广播） | 1 | 12 |
| **e2e（真实服务器 + 双 SDK）** | 1 | 12 |
| **合计** | 8 | **70**（约 2500 行测试代码） |

### 20.2 e2e 覆盖的场景

| 用例 | 验证内容 |
|---|---|
| `PairJoinDiscoverAndRelay` | 仅凭同一 Join Code 加入 → 双向发现 → 双向中继 |
| `GroupSessionMultipleMembers` | 4 成员自动发现 + 单播/组播/广播语义 |
| `PairCapacityEnforced` | pair 第 3 人被拒 |
| `JoinLimitAndExpiry` | 加入次数上限、TTL |
| `TokenRevocationBlocksNewJoinsOnly` | 撤销阻止新加入、不影响在线的 owner |
| `RotateTokenInvalidatesOldCode` | 轮换后旧 code 失效、新 code 可用 |
| `ApprovalFlow` | owner 自动生效、guest 进 pending、批准后进数据面 |
| `ReconnectRestoresMemberIdentity` | `Abort()` 掉线 → 重连复用 MemberID |
| `ServerNeverSeesPlaintext` | 服务器统计中不含明文标记 |
| `StreamIsBidirectionalAndOrdered` | 同流多消息顺序正确 |
| `GroupLeaveNotifiesPeers` | 离开通知 |
| `JoinErrors` | 错误凭据/缺身份/非法 scheme |

### 20.3 测试中发现并修复的真实缺陷

这是本设计最值得记录的部分——以下都由测试/`-race` 抓出，而不是推测：

| # | 缺陷 | 发现方式 | 影响 |
|---|---|---|---|
| 1 | 无条件 `ToUpper` 破坏 base64url token 解析 | `TestJoinTokenRoundTrip` | token 解析成完全不同的字节，加入失败 |
| 2 | `EncodeFrame` 单播分支漏 `off += 2` | `TestFrameUnicastRoundTrip` | payload 覆盖目标下标，编码后目标变成 `25966` |
| 3 | 撤销 token 会踢掉所有在线成员 | `TestRejoinRestoresMemberAndSurvivesRevoke` | 一次误撤销毁掉整个会话 |
| 4 | 审批开启时 owner 自己卡在 pending | `TestApproveAndReject` | 会话永久不可用 |
| 5 | 旧 join code 泄露「是否曾存在」 | `TestRotateTokenInvalidatesOld` | 信息泄露（应统一返回 unauthorized） |
| 6 | 流关闭竞态吞掉最后一条消息 | `TestE2E_GroupSessionMultipleMembers` | `select` 随机选中 closed 分支丢弃已到达数据 |
| 7 | 双方流 ID 从同一空间分配而撞号 | 同上 | 两条独立流序号交错，触发去重丢包 |
| 8 | 数据帧先于 `stream_open` 到达被丢弃 | 同上 | 控制面/数据面跨通道无顺序保证 |
| 9 | `Detach` 持写锁时又取读锁 → 自死锁 | 代码审查 + `-race` | `sync.RWMutex` 不可重入 |
| 10 | `writePump` 轮询 `st.peer` 与读循环竞争 | `-race` | 真实数据竞争 |
| 11 | `Hub.each` 回调在锁外读 `om.approved` | `-race` | 真实数据竞争（改用值快照） |
| 12 | SDK `self` 字段被读循环写、被调用方读 | `-race` | 真实数据竞争（加 `selfMu`） |
| 13 | 密钥交换无限对发（A→B→A→B…） | 运行时观察 | 把连接打满（改为双幂等标记） |
| 14 | 每条控制消息启动 goroutine 等超时 | 代码审查 | 高频消息下 goroutine 堆积 |

### 20.4 运行

```bash
go test ./...                 # 全部单元 + e2e
go test -race ./...           # 竞态检测（推荐 CI 必跑）
go test -run TestE2E -v ./e2e/# 只跑端到端
go test -cover ./internal/... # 覆盖率
```

---

## 21. Benchmark

```bash
go test -run '^$' -bench . -benchtime 300ms ./internal/protocol/ ./internal/relay/
```

本机（Intel i3-9100T）实测：

| 基准 | 结果 | 说明 |
|---|---|---|
| `BenchmarkEncodeFrame` | 306 ns/op，1152 B/op，1 alloc | 1 KiB 载荷编码 |
| `BenchmarkParseFrame` | 55 ns/op，66 B/op，2 allocs | 解析只做头部校验 |
| `BenchmarkForwardUnicast` | 600 ns/op，**1705 MB/s**，1218 B/op，3 allocs | 端到端转发（授权+盖写+编码+入队） |

解读：

- **转发吞吐 ~1.7 GB/s 单核**：瓶颈不在 Go 侧，而在网络与出站队列；
  按 512 KiB/s 的每连接限流计算，单核理论上可服务约 3400 条满速中继连接。
- **解析仅 55 ns**：因为只校验头部、不触碰 payload（payload 是密文，
  服务器无需也不应解析）。
- **内存分配可控**：转发路径 3 次分配/帧（目标列表、编码缓冲、队列帧），
  若需进一步优化可引入 `sync.Pool` 复用编码缓冲。

> 基准编写注意：`Forward` 的入队是**非阻塞**的，若消费者跑在独立
> goroutine 里、生产者跑得更快，出站队列会填满并触发慢消费者保护把目标
> 踢出会话，基准就会因「目标不在会话内」失败。正确做法是在同一 goroutine
> 内即时消费——这是基准自身的负载问题，不是被测代码的缺陷。

---

## 22. 安全审计

### 21.1 威胁模型

假设攻击者能够：窃听网络、重放/篡改 WebSocket 帧、伪装成任意节点尝试加入、
暴力枚举 join code、在被授权后做出恶意行为（灌流、慢速读、伪造来源）。
假设攻击者**不能**：读取服务器内存、读取其他客户端磁盘上的私钥。

不在威胁模型内：服务器被完全攻陷（那时会话元数据必泄露）、客户端设备被
攻陷（私钥泄露）、流量分析攻击。

### 21.2 已实现的防护

| # | 威胁 | 防护 | 代码位置 |
|---|---|---|---|
| 1 | 伪造身份 | Ed25519 `join proof`，绑定 tokenHash + nonce | `auth/identity.go` |
| 2 | 重放加入请求 | nonce 由客户端随机生成，绑定 tokenHash | 同上 |
| 3 | 签名跨会话挪用 | 签名内容绑定 `tokenHash`（不同会话哈希不同） | 同上 |
| 4 | 签名跨协议挪用 | 域分隔前缀 `p2psession:join:v1\n` | 同上 |
| 5 | 暴力枚举 join code | 256 位熵（52 字符 base32），**非短码** | `auth/token.go` |
| 6 | 拖库后直接复用凭证 | 只存 `SHA256(token)`，不存明文 | `session/service.go` |
| 7 | 时序侧信道 | 恒定时间比较 `ConstantTimeEqualHex` | `auth/token.go` |
| 8 | 伪造数据来源 | 服务器**盖写** `SrcIndex` 为已认证身份 | `relay/relay.go` |
| 9 | 冒充他人解密 | AEAD 认证：只有持有成对密钥者能构造合法密文 | `sessionclient/crypto.go` |
| 10 | 跨会话投递 | 每次转发校验发送方与目标均为本会话 active 成员 | `relay/relay.go` |
| 11 | 未授权成员进入数据面 | pending 成员挂载 `/ws/relay` 被拒 | `server/ws.go` |
| 12 | 服务器读取业务明文 | 端到端加密，服务器无密钥（§8） | `sessionclient/crypto.go` |
| 13 | 服务器替换公钥（MITM） | `key_exchange` 携带 `node_id`，客户端可核对成员公钥 | `sessionclient/loops.go` |
| 14 | 资源耗尽（连接） | 每 IP 创建/加入固定窗口限流 | `server/server.go` |
| 15 | 资源耗尽（消息） | 每连接令牌桶（25/s，突发 50） | `server/ws.go` |
| 16 | 资源耗尽（带宽） | 每连接中继令牌桶（默认 512 KiB/s） | `server/ws.go` |
| 17 | 资源耗尽（内存） | 三层帧大小限制 + 候选数上限 16 + 乱序缓冲上限 256 | `protocol/frame.go` 等 |
| 18 | 慢消费者拖垮会话 | 出站队列满累计 64 帧即踢出 | `member/hub.go` |
| 19 | 同身份顶替他人连接 | 新连接顶替旧连接并给出 4003 明确语义 | `member/hub.go` |
| 20 | 旧连接误伤新连接 | `Detach`/`MarkOffline` 比对 ConnectionID | `member/hub.go`、`storage/memory.go` |
| 21 | 跨站 WebSocket 劫持 | Origin 白名单 | `server/server.go` |
| 22 | 密钥交换风暴 | 双幂等标记 | `sessionclient/loops.go` |
| 23 | 敏感密钥驻留内存 | 共享/派生密钥显式清零 | `sessionclient/crypto.go` |
| 24 | 部署后权限过大 | 容器非 root、静态二进制、私钥文件 0600 | `Dockerfile`、`auth/identity.go` |
| 25 | 管理接口暴露 | `/v1/stats`、`/metrics` 可选 Bearer | `server/server.go` |
| 26 | 信息泄露（code 是否存在） | 按 code 查找失败统一返回 `unauthorized` | `session/service.go` |
| 27 | 会话状态分裂（DO） | Session ID 由 Worker 入口统一分配 | `cloudflare/worker.js` |
| 28 | 未初始化即使用（DO） | `#init` 要求 `session_id` 必填 | 同上 |

### 21.3 已知限制与上线前建议

| 项 | 现状 | 建议 |
|---|---|---|
| **节点准入** | 任何人可自建身份加入 | 引入 CA 签发的准入令牌，或在 `auth` 层加公钥白/黑名单 |
| **密钥轮换** | 仅每次 Join 生成新临时密钥 | 长会话应定期重协商（如每 2^32 帧或每 24h） |
| **前向保密** | 单次会话内成立，跨重连不保留 | 需要更强保证时上 Noise Protocol 或双棘轮 |
| **重放窗口** | nonce 未做服务器侧缓存去重 | 加入 nonce 短期缓存（TTL 5min）拒绝重复 |
| **TLS** | 依赖外部（隧道/反代） | 生产务必 `wss://`，不要裸 `ws://` 跨公网 |
| **审计日志** | 仅结构化日志 | 接入集中日志，记录 join/approve/revoke 事件 |
| **限流持久性** | 每实例内存计数 | 多实例需 Redis 令牌桶，否则限流可被 LB 轮询绕过 |
| **DoS 深度防护** | 应用层限流 | 前置 Cloudflare / WAF（L3/L4 由基础设施承担） |
| **成员上限** | 硬顶 1000（Index 空间 65535） | 超大房间需分片或改用 32 位寻址 |

### 21.4 密码学选择说明

| 选择 | 理由 |
|---|---|
| **Ed25519** | 签名快、密钥/签名短（32/64 字节）、无参数选择陷阱、标准库原生支持 |
| **X25519** | ECDH 简单安全，无曲线参数协商攻击面 |
| **HKDF-SHA256** | 标准 KDF，显式区分 salt（sessionID）与 info（双方 NodeID） |
| **ChaCha20-Poly1305** | 纯软件快、不依赖 AES-NI、12 字节 nonce 与帧布局契合 |
| **随机 nonce（每帧）** | 免状态同步；单键 2^32 帧内碰撞概率可忽略（会话级远低于此） |
| 未用 **Noise Protocol** | 需要多轮握手与更复杂状态机，当前「每对一次性 ECDH」已满足需求；若需前向保密/身份隐藏可迁移 |

> **明确声明**：Join Token 只承担 `Authorization / Capability`，
> **不是**数据加密密钥，不参与任何密钥派生。

---

## 23. MVP 路线

严格按「先稳基础，再上穿透」的优先级推进：

### 阶段一：Token 配对 + 双向 Relay（**已完成**）

```
Server: Session 创建/加入/离开/成员列表 + WebSocket 控制面 + 双向中继
Client: A join same token ──▶ Server ──▶ B        双向传输
```

验收：

- [x] `p2p-node create` 返回 Session ID + Join Code
- [x] `p2p-node join <CODE>` 无需任何手工配置即可加入
- [x] 双方自动发现并双向通信（服务器只见密文）
- [x] pair（≤2）与 group（N）两种模式
- [x] 容量/次数/过期/撤销/轮换/审批策略生效
- [x] 掉线重连恢复原 MemberID
- [x] e2e + `-race` 全绿

### 阶段二：候选发现 + STUN（**数据通道已完成，STUN 待接入**）

- [x] `Candidate` 模型与 `candidate_offer/answer` 透传
- [x] 服务器落库候选供新成员查询
- [ ] 客户端侧 STUN 绑定获取 `srflx` 候选
- [ ] 候选优先级排序与筛选

### 阶段三：NAT 打洞 + 直连 + 竞速回退

- [ ] UDP 打洞（双方同时发包）
- [ ] 多候选并行竞速，先成功者胜出
- [ ] `peer_state` 上报 `direct` / `relay`
- [ ] **Relay 路径全程保持可用，打洞失败不中断现有会话**

### 阶段四：Group 增强

- [x] `max_members` 与多人成员表
- [x] 单播 / 组播 / 广播（帧层 + SDK 层）
- [ ] Group Key（使 Broadcast 能一次上行、由服务器扇出）
- [ ] 大规模会话的分片或树形拓扑

### 阶段五：多 Relay 与选择

- [x] `GET /v1/relays` 接口
- [ ] 多区域 Relay 注册与健康检查
- [ ] 客户端 RTT / 丢包探测与自动选路
- [ ] Cloudflare Smart Placement / 多 DO 就近接入

### 阶段六：运维能力

- [x] Prometheus 指标（`/metrics`）
- [x] 结构化日志（`log/slog`）
- [x] 优雅退出（带原因的 Close 帧）
- [x] Docker / Compose / Cloudflare 三种部署
- [ ] 分布式追踪（OpenTelemetry）
- [ ] 管理后台（会话与成员可视化）

---

## 24. 设计问题回答

### 问题 1：如何让两个客户端仅凭同一个 Token 自动发现并配对？

**答案：把「发现」实现为服务端的推模式，而不是客户端的轮询。**

三步机制：

1. **后加入者**在 `POST /join` 的响应里直接拿到 `members` 快照
   （`session.Peers` = 除自己外的 active 成员）——一次往返即完成发现；
2. **先加入者**通过服务器广播的 `member_joined` 获知新人——
   这是关键，没有它先加入者将永远停留在建连时的快照上；
3. 双方拿到对端 `MemberID` 后**各自主动**发起 X25519 密钥交换
   （`helloPeers()`），不依赖「谁先谁后」的时序假设。

```go
// 关键代码：internal/server/handlers.go
if res.Member.Status == protocol.MemberActive {
    info := memberInfo(res.Member, s.hub)
    s.hub.BroadcastControl(res.Session.ID, res.Member.ID,
        &protocol.Message{Type: protocol.MsgMemberJoined, Member: &info})
}
```

客户端无需填写 IP、端口、Peer ID、公钥、Relay 地址或 ACL——所有这些信息
都由服务器在加入时下发，或由服务端在两成员间透传。

### 问题 2：三个客户端使用同一个 Token，如何自动发现另外两个？

**答案：机制与问题 1 完全相同，不需要额外设计——只是快照与广播的成员数从 1 变成 2。**

```
B 加入时：响应携带 [A]           → B 发现 1 个
C 加入时：响应携带 [A, B]        → C 发现 2 个
          服务器向 A、B 广播 C   → A、B 各发现 1 个（累计 2 个）
```

e2e 用例 `TestE2E_GroupSessionMultipleMembers` 逐成员断言了这一点：
第 i 个加入者必须恰好看到 i 个对端。

客户端侧的收敛由「发现即主动发起密钥交换」保证：

```go
func (c *Client) helloPeers() {
    for _, m := range c.Peers() { c.sendHelloTo(m) }   // 幂等
}
```

服务器侧 O(N) 广播，客户端侧 O(N) 交换，均为线性——不需要任何中心化的
「成员列表同步协议」。

### 问题 3：是否允许 A↔B、A↔C、B↔C 全部双向？给出协议设计。

**答案：允许，且这是默认能力。协议上通过「成对密钥 + 逻辑流」实现。**

逻辑层：会话内任意两成员都可双向通信（§5）。物理层每条边独立决定
`direct` 或 `relay`，不强制 Full Mesh（§2.2）。

协议设计要点：

| 层面 | 机制 |
|---|---|
| 寻址 | `MemberID`（逻辑）+ `Index`（数据面 16 位） |
| 加密 | **成对密钥**：`Key_AB ≠ Key_AC ≠ Key_BC`（§8.3） |
| 复用 | 一条 `/ws/relay` 上按 `StreamID` 复用出多条逻辑流 |
| 流 ID 冲突 | 按 MemberID 字典序固定奇偶段（§5.2） |
| 定向 | `DstUnicast` + 目标 Index |
| 组播 | `DstMulticast` + 目标列表（服务器重编码为逐个单播） |
| 广播 | `DstBroadcast`（服务器扇出） |

**为什么成对密钥而不是共享 SessionKey**：如果三者共享一把密钥，任一成员
被攻破就等价于获得全部边的解密能力。成对密钥把泄露面限制在「该成员参与
的边」——A 被攻破不影响 B↔C 的通信。

### 问题 4：100 个客户端加入同一 Session，应该用哪种策略？

**答案：Direct + Relay 混合，绝不 Full Mesh。**

连接数量对比（N = 100）：

| 策略 | 连接数 | 资源消耗 | 结论 |
|---|---|---|---|
| Full Mesh | `N(N-1)/2 = 4950` | 每客户端 99 条连接；总内存与握手成本 O(N²) | **不可接受** |
| Hub & Spoke | `N = 100`（全部经服务器） | 服务器带宽 O(N × 流量)，单点瓶颈 | 可接受但不优 |
| **Direct + Relay** | 按需建立：默认 0~k 条 | 每条边独立决策 | **本设计采用** |

本设计的实际行为：

- **控制面**是全连接的：每个成员都从服务器拿到完整成员表（100 条记录，
  每条约 200 字节 ≈ 20 KB），这是轻量且必要的；
- **数据面按需建立**：只有当 A 真的要发给 B 时才建立 A↔B 的边
  （`OpenStream` → `sendOneShot`），无交互的成员对之间**零连接**；
- **每条边独立选路**：直连成功走 `direct`，失败走 `relay`；
- **服务器承担扇出**：广播时服务器一次编码、多次投递（而非客户端 N 次上行）。

资源量化（100 成员、10% 的边有活跃流量 ≈ 500 条边）：

| 项 | 估算 |
|---|---|
| 控制面连接 | 100 条 WS（每客户端 1 条） |
| 数据面连接 | 100 条 WS（每客户端 1 条，复用承载全部边） |
| 服务器内存 | 100 × (2 × 4 KB 缓冲) ≈ 0.8 MB + 成员表 ≈ 0.1 MB |
| 服务器带宽 | 仅中继部分：若 30% 的边走中继，则 150 条边 × 平均速率 |
| 直连边 | 350 条边不经服务器（P2P），服务器零成本 |

关键点：**每客户端恒定 2 条 WebSocket**（一条控制面、一条数据面），
与会话规模无关。边是逻辑概念，复用在同一条 WS 上——这是把
「O(N²) 连接」降为「O(N) 连接」的根本手段。

> 若成员数继续增长到数百以上，应引入 Group Key（使广播一次上行）
> 与会话分片（子房间）。

### 问题 5：客户端掉线再上线，是否自动恢复原 Session？如何处理三个 ID？

**答案：自动恢复，前提是「静默掉线」而非「主动离开」。**

三个 ID 的处理：

| ID | 掉线重连后 | 处理方式 |
|---|---|---|
| `SessionID` | 不变 | 客户端用同一 Join Code/Token 重新加入，落到同一会话 |
| `MemberID` | **不变** | 以 `NodeID` 为键查找既有成员记录并复用（关键） |
| `Index` | **不变** | 随 MemberID 一起复用（数据面寻址连续性） |
| `ConnectionID` | **变** | 每次连接新建，用于顶替判定与离线标记 |

实现（`storage/memory.go` 的 rejoin 分支）：

```go
if existing := rec.MemberByNodeID(req.NodeID); existing != nil {
    existing.ConnectionID = req.ConnectionID   // 新连接
    existing.LastSeen = now
    existing.LeaveAt = time.Time{}             // 清除宽限期
    return &JoinOutcome{Rejoined: true, ...}   // MemberID/Index 保持
}
```

两条互补的路径：

1. **连接级软恢复**（秒级）：客户端 `Abort()` 掉线后立即重连，服务器在
   `MemberGrace`（默认 5 分钟）内保留记录；
2. **成员级硬恢复**（无需宽限期）：即使超宽限期记录已被清扫，重连时
   服务器按 `NodeID` 重新匹配；若会话仍在且 `requireApproval=false`，
   会得到**新的** MemberID（此时 Index 可能变化，但对端通过
   `member_left` + `member_joined` 事件重新学习）。

**重要区分**：`Close()` 会发送 `leave`，成员被**立即移除**，重连得到新 ID；
`Abort()` 不发送任何通知，服务器只能观察到 TCP 断开，才会走宽限期路径。
SDK 显式提供两个方法，避免语义混淆：

```go
func (c *Client) Close() error { return c.closeWith(nil) }        // 发 leave
func (c *Client) Abort() error { return c.closeWith(nil, true) }  // 静默断开
```

此外，重连**不受 token 撤销影响**（该成员此前已被授权）：

```go
existing := rec.MemberByNodeID(p.NodeID)
if rec.Revoked && existing == nil {   // 只有新成员才被撤销拦住
    return nil, ErrTokenRevoked
}
```

### 问题 6：同一个 Token 是否可以无限加入？

**答案：不可以。由三个独立维度共同限制，任一触发即拒绝。**

| 维度 | 字段 | 语义 | 默认 |
|---|---|---|---|
| **成员数上限** | `MaxMembers` | 同时**在线有效**的成员数 | pair=2，group=10 |
| **加入次数上限** | `JoinLimit` / `JoinsUsed` | 累计成功加入次数（含已离开者） | 0（不限） |
| **时间上限** | `ExpiresAt` | 会话整体存活期 | 30 分钟 |
| **空闲上限** | `IdleTimeout` | 全员离线后自动回收 | 0（不启用） |

三者的分工值得强调：`MaxMembers` 限制**并发规模**，`JoinLimit` 限制
**累计消耗**（防止「反复加入-离开」绕过成员上限），`ExpiresAt` 限制
**时间窗口**。只有 `MaxMembers` 一个维度是不够的——攻击者可以不断加入又
离开，每次都能获得一个新 MemberID 并占用资源。

校验在存储层**原子完成**（`storage.Join`），与写入同处一个临界区：

```go
if rec.Revoked { return nil, ErrRevoked }
if rec.JoinLimit > 0 && rec.JoinsUsed >= rec.JoinLimit { return nil, ErrJoinLimit }
if !pending && rec.MaxMembers > 0 && rec.ActiveMembers() >= rec.MaxMembers {
    return nil, ErrFull
}
```

`TestJoinIsAtomicUnderConcurrency` 用 10 个并发 join 验证「max=3 时恰好
成功 3 个」——若校验与写入不原子，这个断言会失败。

设计细节：**pending（待审批）成员不占 `MaxMembers` 名额**，否则
「满员 + 需审批」会形成死结（无人能再加入，owner 也无从审批）。

### 问题 7：Token 被第三方获得以后怎么办？

**答案：五层防护，从「立即止损」到「长期隔离」逐级递进。**

| 层级 | 手段 | 操作 | 效果 |
|---|---|---|---|
| 1. **预算控制** | `MaxMembers` + `JoinLimit` | 创建时设定 | 限制损害的**上限** |
| 2. **时间控制** | 短 `ExpiresAt` | 创建时设定 | 会话自动过期，泄露窗口有限 |
| 3. **即时撤销** | `POST /revoke-token` | owner 一键操作 | **立即**停止新加入 |
| 4. **凭证轮换** | `POST /rotate-token` | owner 一键操作 | 换新 token + 新 code，旧凭据彻底失效 |
| 5. **会话重建** | `DELETE /sessions/{id}` + 新建 | 解散重来 | 彻底隔离，代价是与会话成员重新沟通 |

三个关键设计决策：

**(a) 撤销不踢在线成员。** 撤销是为了阻止**新的**未授权加入，而不是
惩罚已授权成员。若撤销同时踢人在线成员，一次误操作就能毁掉正在进行的
协作——`TestRejoinRestoresMemberAndSurvivesRevoke` 专门验证了这一点。

**(b) 轮换与撤销分开。** 撤销后 token 仍然存在（只是被标记），轮换则
**生成全新的 256 位 token 与新的 join code**。前者适合「暂时不想让人加入」，
后者适合「确认泄露，必须换新」：

```go
func (s *Service) Rotate(ctx, sessionID string) (*auth.JoinToken, error) {
    token, _ := auth.NewJoinToken()
    s.store.RotateToken(ctx, sessionID, token.Hash(), token.JoinCode(), s.now())
    return token, nil   // 新凭据仅此刻可见一次
}
```

**(c) 不泄露「该 code 是否曾经存在」。** 按 join code 查找失败时统一返回
`unauthorized`，而不是 `session_not_found`——否则攻击者可以区分
「猜错的 code」与「曾经存在但已轮换的 code」，从而缩小搜索空间：

```go
case p.JoinCode != "":
    rec, err = s.store.GetByJoinCode(ctx, p.JoinCode)
    if errors.Is(err, storage.ErrNotFound) {
        return nil, ErrUnauthorized   // 不区分「不存在」与「已失效」
    }
```

**Session Owner 的角色**：owner 是首个加入者（`rec.OwnerMemberID`），
持有 `role=owner` 的连接票据，因此能执行撤销/轮换/审批/解散。这些操作
都要求 **owner 票据**（`authorizeOwner`），而不是仅凭 member_id 声明。

### 问题 8：是否可以让 Session Owner 审批新成员？

**答案：可以，作为可选的 `require_approval` 能力，默认关闭。**

开启方式：

```bash
p2p-node create --require-approval          # 或 REST: {"require_approval": true}
```

流程：

```
Session Owner                     Server                      Pending Member
     │                              │                               │
     │                              │◀── join（进 pending 状态）─────│
     │◀── 通过 /members 看到 pending │                               │
     │    （PendingMembers()）       │                               │
     │── POST /approve ────────────▶│                               │
     │    （owner 票据）             │── joined（本端生效）─────────▶│
     │                              │── member_joined（广播全房间）─▶│
     │                              │                               │
     │                              │  同时允许挂载 /ws/relay        │
```

设计决策（每一条都对应一个会出问题的边界）：

**(a) owner 自动生效，不受审批约束。**

```go
isFirst := rec.OwnerMemberID == ""
pending := rec.RequireApproval && !isFirst
```

否则唯一有权审批的人卡在 pending，会话永久不可用。

**(b) pending 成员不占成员名额**，但仍消耗一次 `JoinLimit`。
前者避免「满员 + 需审批」死结，后者防止「反复申请拖垮 owner」。

**(c) pending 成员可以连控制面，不能进数据面。**

```go
// internal/server/ws.go
if mrec.Status != protocol.MemberActive {
    _ = conn.WriteControl(websocket.CloseMessage,
        websocket.FormatCloseMessage(protocol.ClosePolicy, protocol.ErrPendingApproval), ...)
    return
}
```

这样待审批者能挂在线上**实时**收到批准结果（SDK 收到 `joined` 事件后
才建立数据面连接），而不是反复轮询 REST：

```go
// pkg/sessionclient/client.go — Join 时先不连数据面
if joinRes.Status == protocol.MemberActive {
    if err := c.ensureRelay(ctx, joinRes); err != nil { ... }
}
// 收到 joined 事件后再补建
```

**(d) pending 成员不出现在其他人的成员列表里。** `peersOf()` 只返回
`Status == MemberActive` 的成员，`infosOf()` 同理。否则未审批者会出现在
他人的对端列表中被尝试连接。

**(e) 批准时重新检查容量。** pending 期间名额可能已被他人占满：

```go
if rec.MaxMembers > 0 && rec.ActiveMembers() >= rec.MaxMembers {
    return nil, ErrSessionFull
}
```

---

## 25. 完整时序图

### 24.1 Pair Session（含失败回退）

```
  Client A                Server                    Client B
     │                       │                          │
     │ 1. POST /v1/sessions  │                          │
     │   {mode:"pair"}       │                          │
     │──────────────────────▶│                          │
     │                       │ 生成 256 位 token         │
     │                       │ 存 sha256(token)          │
     │◀──────────────────────│                          │
     │  {session_id,          │                          │
     │   join_code: P2P-…,    │                          │
     │   max_members: 2}      │                          │
     │                       │                          │
     │  （用户把 P2P-… 给对方，无需任何其他信息）           │
     │                       │                          │
     │ 2. POST /join         │                          │
     │   {join_code, node_id,│                          │
     │    pubkey, nonce, sig}│                          │
     │──────────────────────▶│                          │
     │                       │ 校验 token（恒定时间）     │
     │                       │ 校验 join proof          │
     │                       │ 原子写入成员（首个→owner） │
     │                       │ 签发 ticket              │
     │◀──────────────────────│                          │
     │  {member_id, index:1, │                          │
     │   status:"active",    │                          │
     │   ticket, members:[]} │                          │
     │                       │                          │
     │ 3. WS /ws/control?ticket=…                       │
     │──────────────────────▶│                          │
     │◀── member_list (空) ──│                          │
     │ 4. WS /ws/relay       │                          │
     │──────────────────────▶│                          │
     │                       │                          │
     │                       │        5. POST /join（第二个成员）
     │                       │◀─────────────────────────│
     │                       │ 校验通过，index:2         │
     │◀── member_joined(B) ──│                          │
     │                       │── 响应 {members:[A]} ───▶│
     │                       │── member_list [A,B] ────▶│  (WS 建立后)
     │                       │                          │
     │                       │      ★ PAIR_READY ★      │
     │◀══ session_ready ═════│══════════════════════════│  活跃数达到 max_members
     │                       │                          │
     │ 6. Candidate Exchange（服务器只透传）              │
     │── key_exchange{pub} ─▶│─────────────────────────▶│
     │◀──────────────────────│◀── key_exchange{pub} ─────│
     │  双方派生 Key_AB       │                          │
     │                       │                          │
     │ 7. NAT Traversal（阶段三）                        │
     │  A →→ B(候选列表)  同时  B →→ A(候选列表)          │
     │                       │                          │
     │ ╔═══════════════════════════════════════════════╗ │
     │ ║  成功：Direct P2P 建立                          ║ │
     │ ║  A ◀═══════ UDP/QUIC 直连 ═══════════▶ B       ║ │
     │ ║  peer_state=direct ──▶ 服务器记录状态           ║ │
     │ ╚═══════════════════════════════════════════════╝ │
     │                       │                          │
     │ ╔═════════════════ 失败路径 ════════════════════╗ │
     │ ║  打洞超时 → 保持既有 relay 路径，会话不中断      ║ │
     │ ║  peer_state=relay                             ║ │
     │ ║                                               ║ │
     │ ║  A ── 加密 DataFrame ──▶ Server ──▶ B          ║ │
     │ ║  A ◀── 加密 DataFrame ── Server ◀── B          ║ │
     │ ║                                               ║ │
     │ ║  服务器可见：会话ID/成员ID/帧大小/时序           ║ │
     │ ║  服务器不可见：业务明文（AEAD 加密）             ║ │
     │ ╚═══════════════════════════════════════════════╝ │
     │                       │                          │
     │ 8. 数据流（逻辑流复用同一条 /ws/relay）             │
     │── stream_open{sid:0} ▶│─────────────────────────▶│
     │── DATA[sid:0,seq:0] ─▶│ (盖写 srcIdx) ──────────▶│
     │── DATA[sid:0,seq:1] ─▶│─────────────────────────▶│
     │── DATA[flags:Close] ─▶│─────────────────────────▶│
     │                       │                          │
     │ 9. 离开                │                          │
     │── leave ─────────────▶│ 移除成员记录              │
     │                       │── member_left ──────────▶│
```

### 24.2 Group Session

```
  Client A          Client B          Server          Client C          Client D
     │                  │                │                │                │
     │ ① CREATE                         │                │                │
     │─ POST /sessions ─▶│                │                │                │
     │  {mode:"group",   │                │                │                │
     │   max_members:4}  │                │                │                │
     │◀── session_id, join_code ─────────│                │                │
     │                  │                │                │                │
     │ ② JOIN (A)                        │                │                │
     │─ POST /join ─────────────────────▶│                │                │
     │  index:1, role:owner              │                │                │
     │◀── members:[] ────────────────────│                │                │
     │─ WS /control, /relay ────────────▶│                │                │
     │                  │                │                │                │
     │ ③ JOIN (B)          │             │                │                │
     │                  │─ POST /join ──▶│                │                │
     │◀─ member_joined(B) ───────────────│                │                │
     │                  │◀─ members:[A] ─│                │                │
     │─ key_exchange ──▶│─ 透传 ────────▶│                │                │
     │◀── 透传 ─────────│◀─ key_exchange ─│                │                │
     │                  │  ⇒ Key_AB      │                │                │
     │                  │                │                │                │
     │ ④ JOIN (C)                         │                │                │
     │                  │                │◀─ POST /join ──│                │
     │◀─ member_joined(C) ───────────────│                │                │
     │                  │◀─ member_joined(C) ─────────────│                │
     │                  │                │── members:[A,B] ───────────────▶│
     │  ⇄ key_exchange (A↔C, B↔C) 双向收敛（双幂等标记）⇄   │                │
     │                  │                │  ⇒ Key_AC, Key_BC               │
     │                  │                │                │                │
     │ ⑤ JOIN (D)                         │                │                │
     │                  │                │◀─ POST /join ───────────────────│
     │◀─ member_joined(D) ───────────────│                │                │
     │                  │◀─ member_joined(D) ─────────────│                │
     │                  │                │◀─ member_joined(D) ─────────────│
     │                  │                │── members:[A,B,C] ─────────────▶│
     │  ⇄ key_exchange ×3（A↔D, B↔D, C↔D）⇄  │                │                │
     │                  │                │                │                │
     │                  │                │ ★ 活跃数 4 == max_members ★      │
     │◀══ session_ready ═│═ session_ready ═│═ session_ready ═│═ session_ready ═│
     │                  │                │                │                │
     │ ⑥ 数据面：每对各自决定 direct / relay（不强制 Full Mesh）            │
     │                  │                │                │                │
     │ A ◀══ Direct ═══▶ B                │                │                │
     │ A ── Relay ───────────────────────▶│────────────────▶ C              │
     │ B ◀══ Direct ════════════════════════════════════▶ D                │
     │ C ── Relay ───────────────────────▶│────────────────▶ D              │
     │                  │                │                │                │
     │ ⑦ 单播 / 组播 / 广播                │                │                │
     │ A → B        (DstUnicast, dst=2)  │                │                │
     │ A → {C,D}    (DstMulticast, dst=[3,4])             │                │
     │ A → 所有人    (DstBroadcast) ──────│─ 扇出（除 A）──▶│                │
     │                  │                │                │                │
     │ ⑧ 离开             │                │                │                │
     │                  │─ leave ───────▶│                │                │
     │◀─ member_left(B) ─────────────────│                │                │
     │                  │                │─ member_left(B) ▶│                │
     │  与 B 相关的流被关闭，Key_AB 被丢弃  │                │                │
```

---

## 附：快速验证清单

```bash
# 1. 全部测试（含竞态检测）
cd p2psession && go test -race ./...

# 2. 端到端（真实服务器 + 双客户端 SDK）
go test -run TestE2E -v ./e2e/

# 3. 基准
go test -run '^$' -bench . -benchtime 300ms ./internal/protocol/ ./internal/relay/

# 4. 手动跑一对节点（三个终端）
go run ./cmd/server                                   # 终端 1
go run ./cmd/p2p-node create                           # 终端 2 → 拿到 P2P-…
go run ./cmd/p2p-node join P2P-…                       # 终端 2
go run ./cmd/p2p-node join P2P-…                       # 终端 3
# 在任一终端输入文本，另一端即收到（经中继、端到端加密）

# 5. 容器
docker compose up -d --build
```

