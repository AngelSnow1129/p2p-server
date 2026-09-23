package sessionclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"p2psession/internal/auth"
	"p2psession/internal/protocol"
)

// 分片与缓冲参数。
const (
	// maxStreamChunk 为单次 Write 的自动分片大小（留出帧头与加密开销余量）。
	maxStreamChunk = 16 * 1024
	// maxReorderBuffer 为单流乱序缓冲上限（防恶意对端耗尽内存）。
	maxReorderBuffer = 256
	// readTimeout 为控制面/数据面读超时（超出即认为链路异常）。
	readTimeout = 90 * time.Second
	// writeTimeout 为单次写超时。
	writeTimeout = 15 * time.Second
)

// SDK 错误。
var (
	// ErrStreamClosed 流已正常关闭。
	ErrStreamClosed = errors.New("stream closed")
	// ErrStreamReset 流被异常重置。
	ErrStreamReset = errors.New("stream reset")
	// ErrJoinFailed 加入会话失败。
	ErrJoinFailed = errors.New("join failed")
	// ErrNotConnected 尚未建立会话连接。
	ErrNotConnected = errors.New("not connected")
	// ErrUnknownPeer 对端不在会话内。
	ErrUnknownPeer = errors.New("unknown peer")
	// ErrClosed 客户端已关闭。
	ErrClosed = errors.New("client closed")
)

// MemberInfo 是服务器下发的成员描述。
type MemberInfo = protocol.MemberInfo

// Candidate 是网络候选（第二阶段 NAT 穿透使用）。
type Candidate = protocol.Candidate

// Handler 接收会话事件。可嵌入 BaseHandler 只覆盖关心的方法。
type Handler interface {
	// OnReady 在控制面与数据面都就绪、且已收到成员列表后触发一次。
	OnReady(c *Client, self MemberInfo, peers []MemberInfo)
	// OnMemberJoined 新成员加入（已审批生效）。
	OnMemberJoined(c *Client, m MemberInfo)
	// OnMemberLeft 成员离开/被移除。
	OnMemberLeft(c *Client, memberID string)
	// OnStream 对端开了一条逻辑流，应用可在此开始读取。
	OnStream(c *Client, s *Stream)
	// OnPeerState 对端连接状态变化（direct / relay / negotiating）。
	OnPeerState(c *Client, peerID, state string)
	// OnError 服务器错误或本地错误。
	OnError(c *Client, code, msg string)
	// OnClose 连接彻底关闭。
	OnClose(c *Client, err error)
}

// BaseHandler 提供全部空实现。
type BaseHandler struct{}

func (BaseHandler) OnReady(*Client, MemberInfo, []MemberInfo) {}
func (BaseHandler) OnMemberJoined(*Client, MemberInfo)        {}
func (BaseHandler) OnMemberLeft(*Client, string)              {}
func (BaseHandler) OnStream(*Client, *Stream)                 {}
func (BaseHandler) OnPeerState(*Client, string, string)       {}
func (BaseHandler) OnError(*Client, string, string)           {}
func (BaseHandler) OnClose(*Client, error)                    {}

// Config 是 Dial 或 Join 的参数。
type Config struct {
	// ServerURL 为服务器基址，如 http://127.0.0.1:60000 或 wss://relay.example.com。
	ServerURL string

	// Identity 为节点长期 Ed25519 身份（必填，身份即 NodeID）。
	Identity *auth.Identity

	// 加入凭据三选一：
	//   SessionID + JoinToken（机器形式）
	//   JoinCode（人类形式 P2P-XXXX-…，也可直接当 token 用）
	SessionID string
	JoinToken string
	JoinCode  string

	// Capabilities 为本节点能力声明，如 ["udp","tcp","relay"]。
	Capabilities []string

	// Candidates 为初始网络候选（可选；第二阶段 NAT 穿透使用）。
	Candidates []Candidate

	// HTTPClient 可选，默认带 30s 超时的客户端。
	HTTPClient *http.Client
	// Dialer 可选，WebSocket 拨号器。
	Dialer *websocket.Dialer
	// Header 可选，附加到所有 HTTP/WS 请求（如自定义鉴权网关）。
	Header http.Header

	// HeartbeatInterval 为控制面心跳间隔（0 用服务器建议值）。
	HeartbeatInterval time.Duration
	// Relay 为数据面地址（默认由服务器返回的 RelayURL 推导）。
	RelayURL string
}

// Client 是一条已加入会话的完整客户端（控制面 + 数据面 + 端到端加密）。
type Client struct {
	cfg Config
	h   Handler

	// sessionID 为服务器分配的会话 ID（MemberInfo 不含该字段）。
	sessionID string

	// selfMu 保护 self 中会被控制面读循环改写的字段（Status/Index/IsOwner）。
	//
	// 只有这些字段会在运行期变化：MemberID/NodeID/PublicKey 在 Join 时确定后
	// 不再改动。但 Self() 返回的是整个结构体副本，读它会连带读到正在被写的
	// Status，因此 Self() 也必须持锁——否则 -race 会报真实竞争。
	selfMu sync.RWMutex
	self   MemberInfo

	httpc *http.Client

	ctrl  *websocket.Conn
	relay *websocket.Conn

	// wsMu 保护连接与其票据字段（审批通过后需要新建数据面连接）。
	wsMu      sync.Mutex
	ticket    string
	relayPath string

	crypto *cryptoState

	// 控制面写锁与数据面写锁分离：两条 WS 独立，避免相互阻塞。
	ctrlMu  sync.Mutex
	relayMu sync.Mutex

	peersMu sync.RWMutex
	// peers 以 MemberID 为键。
	peers map[string]MemberInfo
	// byIndex 由数据面下标反查成员 ID（数据面寻址用）。
	byIndex map[uint16]string

	streamsMu sync.RWMutex
	// streams 以 StreamID 为键（同一对端间复用，故全局唯一即可）。
	streams map[uint32]*Stream
	// closedStreams 记录「刚关闭的流 ID → 关闭时刻」，用于丢弃迟到的
	// stream_open（跨通道重排保护，见 loops.go 的 onStreamOpen）。
	closedStreams map[uint32]time.Time

	// nextEven/nextOdd 为本地流 ID 的两个奇偶段计数器。
	//
	// 为什么要分奇偶：流 ID 是每个对端各自分配的，若双方都用同一个数字空间，
	// A 的「1」与 B 的「1」会在彼此的映射表里撞车——B 注册自己的流 1 会覆盖
	// 从 A 收到的流 1，两条独立流的序号随即交错，触发去重丢包。
	// 按 MemberID 字典序固定奇偶后，同一对之间两个方向永远落在不同段内。
	nextEven uint32
	nextOdd  uint32

	hbInterval atomic.Int64

	// relayRunning 保证数据面读循环只启动一次（审批通过后可能补建）。
	relayRunning atomic.Bool

	// kexMu/helloSent/helloReplied 控制 X25519 公钥交换的收敛：
	//   helloSent     —— 主动向该对端发过 hello（发现成员时触发）；
	//   helloReplied  —— 收到该对端公钥后回过一次 hello。
	// 两个标记各自幂等，既避免无限对发，又保证「主动 hello 丢失」时仍能收敛。
	kexMu        sync.Mutex
	helloSent    map[string]bool
	helloReplied map[string]bool

	// pong 用于 Ping 等待心跳应答（缓冲 1：并发 Ping 时后者不阻塞读循环）。
	pong chan struct{}

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  error

	readyOnce sync.Once
	ready     chan struct{}

	// 统计
	stats Stats
}

// Stats 是客户端侧计数。
type Stats struct {
	FramesSent     atomic.Uint64
	FramesReceived atomic.Uint64
	BytesSent      atomic.Uint64
	BytesReceived  atomic.Uint64
	DecryptErrors  atomic.Uint64
}

// Join 执行「REST 加入 → 连接两条 WebSocket → 交换密钥」的完整流程。
func Join(ctx context.Context, cfg Config, h Handler) (*Client, error) {
	if cfg.Identity == nil {
		return nil, fmt.Errorf("%w: identity required", ErrJoinFailed)
	}
	if cfg.ServerURL == "" {
		return nil, fmt.Errorf("%w: server url required", ErrJoinFailed)
	}
	if h == nil {
		h = BaseHandler{}
	}

	httpc := cfg.HTTPClient
	if httpc == nil {
		httpc = &http.Client{Timeout: 30 * time.Second}
	}

	// 1. 推导 token 的原始字节与存储哈希（join proof 需绑定 tokenHash）。
	rawToken, err := resolveToken(cfg)
	if err != nil {
		return nil, err
	}
	tokenHash := hashTokenRaw(rawToken)

	// 2. 生成 join proof（对 tokenHash + 随机 nonce 签名）。
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	pub := cfg.Identity.Public()
	sig := cfg.Identity.SignJoin(tokenHash, nonce)

	// 3. REST 加入。
	joinRes, err := doJoin(ctx, httpc, cfg, rawToken, pub, nonce, sig)
	if err != nil {
		return nil, err
	}

	c := &Client{
		cfg:          cfg,
		h:            h,
		httpc:        httpc,
		sessionID:    joinRes.SessionID,
		peers:        make(map[string]MemberInfo),
		byIndex:      make(map[uint16]string),
		streams:      make(map[uint32]*Stream),
		helloSent:    make(map[string]bool),
		helloReplied: make(map[string]bool),
		pong:         make(chan struct{}, 1),
		closed:       make(chan struct{}),
		ready:        make(chan struct{}),
	}
	c.self = MemberInfo{
		MemberID:     joinRes.MemberID,
		Index:        joinRes.Index,
		NodeID:       cfg.Identity.NodeID(),
		PublicKey:    hex.EncodeToString(pub),
		Capabilities: cfg.Capabilities,
		IsOwner:      joinRes.IsOwner,
		Status:       joinRes.Status,
	}

	// 4. 初始化端到端加密状态。
	cs, err := newCryptoState(c.selfNodeID(), joinRes.SessionID)
	if err != nil {
		return nil, err
	}
	c.crypto = cs

	// 5. 记录服务器下发的既有成员（自动发现的种子）。
	for _, m := range joinRes.Members {
		c.peers[m.MemberID] = m
		c.byIndex[m.Index] = m.MemberID
	}

	hb := cfg.HeartbeatInterval
	if hb <= 0 && joinRes.HeartbeatMs > 0 {
		hb = time.Duration(joinRes.HeartbeatMs) * time.Millisecond
	}
	if hb <= 0 {
		hb = 15 * time.Second
	}
	c.hbInterval.Store(int64(hb))

	// 6. 连接控制面。
	if err := c.connectControl(ctx, joinRes); err != nil {
		_ = c.closeWith(err)
		return nil, err
	}

	// 7. 数据面：仅对已生效成员连接。
	//
	// 等待 owner 审批（status=pending）的成员必须能连上控制面等待结果，
	// 但服务器会拒绝其数据面连接；因此这里延后到收到 joined 事件再连，
	// 否则「需要审批」的会话在 SDK 侧根本无法加入。
	if joinRes.Status == protocol.MemberActive {
		if err := c.ensureRelay(ctx, joinRes); err != nil {
			_ = c.closeWith(err)
			return nil, err
		}
		go c.relayReadLoop()
	}

	// 8. 后台泵与心跳。
	go c.controlReadLoop()
	go c.heartbeatLoop()

	// 9. 与既有成员交换密钥（对端在收到后会回发，从而双向就绪）。
	c.helloPeers()

	return c, nil
}

// resolveToken 从三种凭据形态中解析出原始 token 字节。
func resolveToken(cfg Config) ([]byte, error) {
	switch {
	case cfg.JoinToken != "":
		raw, err := auth.DecodeToken(cfg.JoinToken)
		if err != nil {
			return nil, fmt.Errorf("%w: malformed join token", ErrJoinFailed)
		}
		return raw, nil
	case cfg.JoinCode != "":
		raw, err := auth.DecodeToken(cfg.JoinCode)
		if err != nil {
			return nil, fmt.Errorf("%w: malformed join code", ErrJoinFailed)
		}
		return raw, nil
	case cfg.SessionID != "":
		return nil, fmt.Errorf("%w: join_token or join_code required (session_id alone cannot prove authorization)", ErrJoinFailed)
	default:
		return nil, fmt.Errorf("%w: no join credential provided", ErrJoinFailed)
	}
}

// hashTokenRaw 与服务器侧的存储哈希规则一致：sha256(raw) 的 hex。
func hashTokenRaw(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// ---- REST ------------------------------------------------------------------

type joinRequest struct {
	SessionID string `json:"session_id,omitempty"`
	JoinCode  string `json:"join_code,omitempty"`
	JoinToken string `json:"join_token"`

	NodeID    string `json:"node_id"`
	PublicKey string `json:"public_key"`
	Nonce     string `json:"nonce"`
	Signature string `json:"signature"`

	Capabilities []string    `json:"capabilities,omitempty"`
	Candidates   []Candidate `json:"candidates,omitempty"`
}

type joinResponse struct {
	SessionID   string       `json:"session_id"`
	MemberID    string       `json:"member_id"`
	Index       uint16       `json:"index"`
	IsOwner     bool         `json:"is_owner"`
	Status      string       `json:"status"`
	Rejoined    bool         `json:"rejoined"`
	Ticket      string       `json:"ticket"`
	Members     []MemberInfo `json:"members"`
	Mode        string       `json:"mode"`
	HeartbeatMs int          `json:"heartbeat_ms"`
	ControlURL  string       `json:"control_url"`
	RelayURL    string       `json:"relay_url"`
}

func doJoin(ctx context.Context, httpc *http.Client, cfg Config,
	rawToken []byte, pub []byte, nonce, sig []byte) (*joinResponse, error) {

	base, err := normalizeBase(cfg.ServerURL)
	if err != nil {
		return nil, err
	}

	// 把原始 token 还原为机器形式字符串发给服务器（服务器只存哈希）。
	tok, err := auth.FromRaw(rawToken)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJoinFailed, err)
	}

	body := joinRequest{
		SessionID:    cfg.SessionID,
		JoinCode:     cfg.JoinCode,
		JoinToken:    tok.TokenString(),
		NodeID:       cfg.Identity.NodeID(),
		PublicKey:    hex.EncodeToString(pub),
		Nonce:        hex.EncodeToString(nonce),
		Signature:    hex.EncodeToString(sig),
		Capabilities: cfg.Capabilities,
		Candidates:   cfg.Candidates,
	}

	// 路径优先用 session_id；只有 join code 时走通用端点。
	endpoint := base + "/v1/session/join"
	if cfg.SessionID != "" {
		endpoint = base + "/v1/sessions/" + url.PathEscape(cfg.SessionID) + "/join"
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	applyHeader(req.Header, cfg.Header)

	resp, err := httpc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJoinFailed, err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: HTTP %d: %s", ErrJoinFailed, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var out joinResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("%w: bad response: %v", ErrJoinFailed, err)
	}
	if out.Ticket == "" || out.MemberID == "" {
		return nil, fmt.Errorf("%w: incomplete join response", ErrJoinFailed)
	}
	return &out, nil
}

// ---- WebSocket -------------------------------------------------------------

// connectControl 建立控制面连接。
func (c *Client) connectControl(ctx context.Context, jr *joinResponse) error {
	base, err := normalizeBase(c.cfg.ServerURL)
	if err != nil {
		return err
	}
	controlPath := jr.ControlURL
	if controlPath == "" {
		controlPath = "/ws/control"
	}

	c.wsMu.Lock()
	c.ticket = jr.Ticket
	c.relayPath = jr.RelayURL
	c.wsMu.Unlock()

	hdr := http.Header{}
	applyHeader(hdr, c.cfg.Header)

	ctrl, _, err := c.dialer().DialContext(ctx,
		wsURLOf(base, controlPath)+"?ticket="+url.QueryEscape(jr.Ticket), hdr)
	if err != nil {
		return fmt.Errorf("control websocket: %w", err)
	}
	c.ctrl = ctrl
	return nil
}

// ensureRelay 建立数据面连接（幂等；等待审批的成员在批准后调用）。
func (c *Client) ensureRelay(ctx context.Context, jr *joinResponse) error {
	base, err := normalizeBase(c.cfg.ServerURL)
	if err != nil {
		return err
	}

	c.wsMu.Lock()
	if c.relay != nil {
		c.wsMu.Unlock()
		return nil // 已连接
	}
	ticket := c.ticket
	relayPath := c.relayPath
	c.wsMu.Unlock()

	if ticket == "" {
		if jr != nil {
			ticket = jr.Ticket
			relayPath = jr.RelayURL
		}
	}
	if relayPath == "" {
		relayPath = "/ws/relay"
	}
	if c.cfg.RelayURL != "" {
		relayPath = c.cfg.RelayURL
	}

	hdr := http.Header{}
	applyHeader(hdr, c.cfg.Header)

	relay, _, err := c.dialer().DialContext(ctx,
		wsURLOf(base, relayPath)+"?ticket="+url.QueryEscape(ticket), hdr)
	if err != nil {
		return fmt.Errorf("relay websocket: %w", err)
	}

	c.wsMu.Lock()
	c.relay = relay
	c.wsMu.Unlock()
	return nil
}

func (c *Client) dialer() *websocket.Dialer {
	if c.cfg.Dialer != nil {
		return c.cfg.Dialer
	}
	return websocket.DefaultDialer
}

// normalizeBase 归一化服务器基址（补 scheme、去尾斜杠）。
func normalizeBase(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("%w: empty server url", ErrJoinFailed)
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("%w: bad server url: %v", ErrJoinFailed, err)
	}
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	case "http", "https":
	default:
		return "", fmt.Errorf("%w: unsupported scheme %q", ErrJoinFailed, u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

// wsURLOf 把 REST 基址与路径转换为 WebSocket 地址。
func wsURLOf(base, path string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	return u.String()
}

func applyHeader(dst http.Header, src http.Header) {
	if src == nil {
		return
	}
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

// ---- 访问器 ----------------------------------------------------------------

// Self 返回本节点在会话中的成员信息快照。
func (c *Client) Self() MemberInfo {
	c.selfMu.RLock()
	defer c.selfMu.RUnlock()
	return c.self
}

// updateSelf 在锁内更新本节点的可变字段（统计/索引）。
func (c *Client) updateSelf(fn func(*MemberInfo)) {
	c.selfMu.Lock()
	defer c.selfMu.Unlock()
	fn(&c.self)
}

// selfID 返回本节点成员 ID（Join 后不变，无需持锁读整结构体）。
func (c *Client) selfID() string { return c.self.MemberID }

// selfNodeID 返回本节点 NodeID（Join 后不变）。
func (c *Client) selfNodeID() string { return c.self.NodeID }

// selfIndex 返回本节点数据面下标（审批后可能变化，需持锁）。
func (c *Client) selfIndex() uint16 {
	c.selfMu.RLock()
	defer c.selfMu.RUnlock()
	return c.self.Index
}

// selfStatus 返回本节点当前状态（active / pending）。
func (c *Client) selfStatus() string {
	c.selfMu.RLock()
	defer c.selfMu.RUnlock()
	return c.self.Status
}

// SessionIDOf 返回会话 ID。
func (c *Client) SessionIDOf() string { return c.sessionID }

// Peers 返回当前已知的对端快照。
func (c *Client) Peers() []MemberInfo {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	out := make([]MemberInfo, 0, len(c.peers))
	for _, m := range c.peers {
		if m.MemberID == c.selfID() {
			continue
		}
		out = append(out, m)
	}
	return out
}

// Peer 按成员 ID 查找对端。
func (c *Client) Peer(memberID string) (MemberInfo, bool) {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	m, ok := c.peers[memberID]
	return m, ok
}

// WaitReady 等待控制面就绪（已收到成员列表）。
func (c *Client) WaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-c.closed:
		return fmt.Errorf("%w: %v", ErrClosed, c.closeErr)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Done 在客户端关闭时关闭。
func (c *Client) Done() <-chan struct{} { return c.closed }

// Close 优雅关闭客户端：先发送 leave 通知服务器本端主动离开（成员记录随即
// 从会话中移除），再断开两条连接。
//
// 若需要模拟「网络掉线」而非主动离开，用 Abort。
func (c *Client) Close() error {
	return c.closeWith(nil)
}

// Abort 强制断开连接且**不发送 leave**，用于模拟网络掉线/进程崩溃。
//
// 语义差异很重要：
//   - Close（发送 leave）→ 服务器移除成员，重连会得到新的 MemberID；
//   - Abort（静默断开）  → 服务器把成员标记为离线并保留记录，宽限期内重连
//     可复用原 MemberID 与数据面下标。
//
// 后者正是「掉线再上线自动恢复原会话」所依赖的服务端行为。
func (c *Client) Abort() error { return c.closeWith(nil, true) }

func (c *Client) closeWith(err error, abort ...bool) error {
	silent := len(abort) > 0 && abort[0]
	c.closeOnce.Do(func() {
		c.closeErr = err
		// 尽力通知服务器本端离开（Abort 时跳过，模拟网络中断）。
		if !silent && c.ctrl != nil {
			msg := &protocol.Message{Type: protocol.MsgLeave}
			if data, e := msg.Encode(); e == nil {
				c.ctrlMu.Lock()
				_ = c.ctrl.SetWriteDeadline(time.Now().Add(time.Second))
				_ = c.ctrl.WriteMessage(websocket.TextMessage, data)
				c.ctrlMu.Unlock()
			}
		}
		if c.ctrl != nil {
			_ = c.ctrl.Close()
		}
		if c.relay != nil {
			_ = c.relay.Close()
		}
		// 关闭全部流。
		c.streamsMu.Lock()
		for _, s := range c.streams {
			s.once.Do(func() { close(s.closed) })
		}
		c.streams = make(map[uint32]*Stream)
		c.streamsMu.Unlock()

		close(c.closed)
		c.h.OnClose(c, err)
	})
	return nil
}
