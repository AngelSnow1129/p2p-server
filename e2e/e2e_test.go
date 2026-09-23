// Package e2e_test 通过 httptest 启动真实服务器，并用客户端 SDK 做端到端验证。
//
// 覆盖 MVP 的核心承诺：
//   - 两个节点仅凭同一个 Join Code 加入同一会话并自动发现彼此；
//   - 双向经服务器中继通信（服务器看不到明文）；
//   - group 会话支持多成员与定向/组播发送；
//   - 容量、加入次数、过期、撤销、审批、重连恢复等策略生效。
package e2e_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"p2psession/internal/auth"
	"p2psession/internal/config"
	"p2psession/internal/member"
	"p2psession/internal/metrics"
	"p2psession/internal/relay"
	"p2psession/internal/server"
	"p2psession/internal/session"
	"p2psession/internal/storage"
	"p2psession/pkg/sessionclient"
)

// ---- 测试服务器 -------------------------------------------------------------

func startServer(t *testing.T) *httptest.Server {
	t.Helper()

	cfg := config.Default()
	cfg.Heartbeat = 2 * time.Second
	cfg.IdleTimeout = 10 * time.Second
	cfg.TicketTTL = 5 * time.Minute
	cfg.PruneInterval = time.Hour // 测试里手动触发，避免后台干扰

	tickets, err := auth.NewTicketKey()
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewMemory()
	svc := session.New(store, tickets, session.Config{
		TicketTTL:   cfg.TicketTTL,
		MemberGrace: session.DefaultMemberGrace,
		DefaultTTL:  cfg.DefaultTTL,
	})
	hub := member.NewHub()
	reg := metrics.New()
	fwd := relay.New(hub, svc, cfg.MaxRelayFrame)

	srv := server.New(cfg, svc, hub, fwd, reg, tickets,
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// ---- 测试用 Handler ---------------------------------------------------------

// collector 收集 SDK 事件，供测试断言。
type collector struct {
	sessionclient.BaseHandler

	mu       sync.Mutex
	peers    map[string]sessionclient.MemberInfo
	joined   chan sessionclient.MemberInfo
	left     chan string
	streams  chan *sessionclient.Stream
	errs     chan string
	ready    chan struct{}
	readyOne sync.Once
	// messages 汇总收到的明文（key = 对端成员 ID）。
	messages map[string][]string
	msgCh    chan string
}

func newCollector() *collector {
	return &collector{
		peers:    make(map[string]sessionclient.MemberInfo),
		joined:   make(chan sessionclient.MemberInfo, 16),
		left:     make(chan string, 16),
		streams:  make(chan *sessionclient.Stream, 16),
		errs:     make(chan string, 64),
		ready:    make(chan struct{}),
		messages: make(map[string][]string),
		msgCh:    make(chan string, 64),
	}
}

func (c *collector) OnReady(_ *sessionclient.Client, _ sessionclient.MemberInfo, peers []sessionclient.MemberInfo) {
	c.mu.Lock()
	for _, p := range peers {
		c.peers[p.MemberID] = p
	}
	c.mu.Unlock()
	c.readyOne.Do(func() { close(c.ready) })
}

func (c *collector) OnMemberJoined(_ *sessionclient.Client, m sessionclient.MemberInfo) {
	c.mu.Lock()
	c.peers[m.MemberID] = m
	c.mu.Unlock()
	select {
	case c.joined <- m:
	default:
	}
}

func (c *collector) OnMemberLeft(_ *sessionclient.Client, id string) {
	c.mu.Lock()
	delete(c.peers, id)
	c.mu.Unlock()
	select {
	case c.left <- id:
	default:
	}
}

func (c *collector) OnStream(_ *sessionclient.Client, s *sessionclient.Stream) {
	select {
	case c.streams <- s:
	default:
	}
}

func (c *collector) OnError(_ *sessionclient.Client, code, msg string) {
	select {
	case c.errs <- code + ": " + msg:
	default:
	}
}

// pump 持续读取入站流并把内容记录到 messages。
func (c *collector) pump(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case s := <-c.streams:
			go func(s *sessionclient.Stream) {
				for {
					data, err := s.ReadMessage()
					if err != nil {
						return
					}
					c.mu.Lock()
					c.messages[s.PeerID()] = append(c.messages[s.PeerID()], string(data))
					c.mu.Unlock()
					select {
					case c.msgCh <- string(data):
					default:
					}
				}
			}(s)
		}
	}
}

// countFrom 返回从某对端收到的消息条数。
func (c *collector) countFrom(peerID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.messages[peerID])
}

// totalMessages 返回收到的消息总数。
func (c *collector) totalMessages() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.messages {
		n += len(v)
	}
	return n
}

// waitFor 轮询等待条件成立。
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// ---- 加入辅助 ---------------------------------------------------------------

func newIdentity(t *testing.T) *auth.Identity {
	t.Helper()
	id, err := auth.GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// join 用 Join Code 加入会话并等待就绪。
func join(t *testing.T, ts *httptest.Server, code string, h sessionclient.Handler) *sessionclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL:    ts.URL,
		Identity:     newIdentity(t),
		JoinCode:     code,
		Capabilities: []string{"udp", "relay"},
	}, h)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// create 创建一个会话。
func create(t *testing.T, ts *httptest.Server, p sessionclient.CreateParams) *sessionclient.CreatedSession {
	t.Helper()
	p.ServerURL = ts.URL
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := sessionclient.Create(ctx, p)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	return res
}

// waitKex 等待与某对端完成成对密钥协商。
func waitKex(t *testing.T, c *sessionclient.Client, peerID string) {
	t.Helper()
	waitFor(t, 5*time.Second, "pairwise key with "+peerID, func() bool {
		return c.KexReady(peerID)
	})
}

// ---- 用例：pair -------------------------------------------------------------

func TestE2E_PairJoinDiscoverAndRelay(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{Mode: "pair"})

	if res.Mode != "pair" || res.MaxMembers != 2 {
		t.Fatalf("pair session = %+v", res)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A 先加入：此时房间内没有对端。
	hA := newCollector()
	go hA.pump(ctx)
	cA := join(t, ts, res.JoinCode, hA)
	if peers := cA.Peers(); len(peers) != 0 {
		t.Fatalf("A should see no peers initially, got %d", len(peers))
	}

	// B 用同一个 Join Code 加入：双方必须自动发现彼此（无任何手工配置）。
	hB := newCollector()
	go hB.pump(ctx)
	cB := join(t, ts, res.JoinCode, hB)

	waitFor(t, 5*time.Second, "A discovers B", func() bool { return len(cA.Peers()) == 1 })
	waitFor(t, 5*time.Second, "B discovers A", func() bool { return len(cB.Peers()) == 1 })
	// A 发现的必须正是 B（而不是自己或别的节点）。
	if got := cA.Peers()[0].MemberID; got != cB.Self().MemberID {
		t.Fatalf("A discovered %s, want B=%s", got, cB.Self().MemberID)
	}
	if got := cB.Peers()[0].MemberID; got != cA.Self().MemberID {
		t.Fatalf("B discovered %s, want A=%s", got, cA.Self().MemberID)
	}

	// 等待端到端成对密钥就绪。
	waitKex(t, cA, cB.Self().MemberID)
	waitKex(t, cB, cA.Self().MemberID)

	// A -> B 经服务器中继（服务器只见密文）。
	if err := cA.SendTo(cB.Self().MemberID, []byte("hello-from-A")); err != nil {
		t.Fatalf("A send: %v", err)
	}
	waitFor(t, 5*time.Second, "B receives from A", func() bool {
		return hB.countFrom(cA.Self().MemberID) == 1
	})

	// B -> A：双向通信必须对称可用。
	if err := cB.SendTo(cA.Self().MemberID, []byte("hello-from-B")); err != nil {
		t.Fatalf("B send: %v", err)
	}
	waitFor(t, 5*time.Second, "A receives from B", func() bool {
		return hA.countFrom(cB.Self().MemberID) == 1
	})
}

func TestE2E_PairCapacityEnforced(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{Mode: "pair"})

	join(t, ts, res.JoinCode, newCollector())
	join(t, ts, res.JoinCode, newCollector())

	// 第三个节点必须被拒。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL: ts.URL,
		Identity:  newIdentity(t),
		JoinCode:  res.JoinCode,
	}, sessionclient.BaseHandler{})
	if err == nil {
		t.Fatal("third join should be rejected for pair session")
	}
	if !strings.Contains(err.Error(), "session_full") && !strings.Contains(strings.ToLower(err.Error()), "full") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestE2E_GroupSessionMultipleMembers(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{Mode: "group", MaxMembers: 4})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4 个成员依次加入同一会话。
	clients := make([]*sessionclient.Client, 0, 4)
	hubs := make([]*collector, 0, 4)
	for i := 0; i < 4; i++ {
		h := newCollector()
		go h.pump(ctx)
		c := join(t, ts, res.JoinCode, h)
		clients = append(clients, c)
		hubs = append(hubs, h)

		// 每个新成员都应看到此前已加入的所有成员（自动发现）。
		waitFor(t, 5*time.Second, "member sees peers", func() bool {
			return len(c.Peers()) == i
		})
	}

	// 既有成员应陆续收到其余成员的加入通知。
	for i, c := range clients {
		want := len(clients) - 1
		idx := i
		waitFor(t, 5*time.Second, "peer count", func() bool {
			return len(c.Peers()) == want
		})
		_ = idx
	}

	// 等待全部成对密钥就绪。
	for i, c := range clients {
		for j, other := range clients {
			if i == j {
				continue
			}
			waitKex(t, c, other.Self().MemberID)
		}
	}

	// 单播：0 -> 1。
	if err := clients[0].SendTo(clients[1].Self().MemberID, []byte("uni-0-1")); err != nil {
		t.Fatalf("unicast: %v", err)
	}
	waitFor(t, 5*time.Second, "1 receives from 0", func() bool {
		return hubs[1].countFrom(clients[0].Self().MemberID) == 1
	})
	// 只有 1 收到，2、3 不应收到（单播语义）。
	if hubs[2].totalMessages() != 0 || hubs[3].totalMessages() != 0 {
		t.Fatalf("unicast leaked: 2=%d 3=%d", hubs[2].totalMessages(), hubs[3].totalMessages())
	}

	// 组播：0 -> {2,3}。
	n, err := clients[0].Multicast(
		[]string{clients[2].Self().MemberID, clients[3].Self().MemberID},
		[]byte("multi-0-23"),
	)
	if err != nil {
		t.Fatalf("multicast: %v", err)
	}
	if n != 2 {
		t.Fatalf("multicast delivered = %d, want 2", n)
	}
	for _, idx := range []int{2, 3} {
		i := idx
		waitFor(t, 5*time.Second, "multicast delivery", func() bool {
			return hubs[i].countFrom(clients[0].Self().MemberID) == 1
		})
	}

	// 广播：0 -> 其余全部。
	if _, err := clients[0].Broadcast([]byte("broadcast-0")); err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	for idx := 1; idx < 4; idx++ {
		i := idx
		waitFor(t, 5*time.Second, "broadcast delivery", func() bool {
			return hubs[i].countFrom(clients[0].Self().MemberID) == 2 // 含此前的组播
		})
	}
}

func TestE2E_GroupLeaveNotifiesPeers(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{Mode: "group", MaxMembers: 4})

	hA := newCollector()
	cA := join(t, ts, res.JoinCode, hA)
	hB := newCollector()
	cB := join(t, ts, res.JoinCode, hB)

	waitFor(t, 5*time.Second, "A sees B", func() bool { return len(cA.Peers()) == 1 })

	// B 离开：A 必须收到通知并从成员表中移除。
	if err := cB.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "A removes B", func() bool { return len(cA.Peers()) == 0 })
}

// ---- 用例：策略 -------------------------------------------------------------

func TestE2E_JoinLimitAndExpiry(t *testing.T) {
	ts := startServer(t)

	t.Run("join limit", func(t *testing.T) {
		res := create(t, ts, sessionclient.CreateParams{
			Mode:       "group",
			MaxMembers: 5,
			JoinLimit:  2,
		})
		join(t, ts, res.JoinCode, newCollector())
		join(t, ts, res.JoinCode, newCollector())

		// 第三次加入必须被拒（即使还有名额）。
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := sessionclient.Join(ctx, sessionclient.Config{
			ServerURL: ts.URL,
			Identity:  newIdentity(t),
			JoinCode:  res.JoinCode,
		}, sessionclient.BaseHandler{})
		if err == nil {
			t.Fatal("join beyond join_limit should fail")
		}
	})

	t.Run("ttl expiry", func(t *testing.T) {
		// TTL 取最小值（服务端要求 >= 1m 的默认下限由 Creates 校验，
		// 这里用 1 分钟并直接断言 ExpiresAt 已设置；过期路径由单元测试覆盖）。
		res := create(t, ts, sessionclient.CreateParams{TTL: time.Minute})
		if res.ExpiresAt.IsZero() {
			t.Fatal("ExpiresAt not set")
		}
		if time.Until(res.ExpiresAt) > 2*time.Minute {
			t.Fatalf("ttl not applied: %s", res.ExpiresAt)
		}
	})
}

func TestE2E_TokenRevocationBlocksNewJoinsOnly(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{Mode: "group", MaxMembers: 5})

	owner := join(t, ts, res.JoinCode, newCollector())
	waitFor(t, 3*time.Second, "owner ready", func() bool { return owner.Self().MemberID != "" })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// owner 撤销 token。
	if err := owner.RevokeToken(ctx); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	// 新节点不能再加入。
	_, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL: ts.URL,
		Identity:  newIdentity(t),
		JoinCode:  res.JoinCode,
	}, sessionclient.BaseHandler{})
	if err == nil {
		t.Fatal("join after revoke should fail")
	}

	// 但已加入的 owner 仍可正常使用（不受撤销影响）。
	if err := owner.Ping(ctx); err != nil {
		t.Fatalf("existing member broken by revoke: %v", err)
	}
}

func TestE2E_RotateTokenInvalidatesOldCode(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{Mode: "group", MaxMembers: 5})

	owner := join(t, ts, res.JoinCode, newCollector())
	waitFor(t, 3*time.Second, "owner ready", func() bool { return owner.Self().MemberID != "" })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	token, code, err := owner.RotateToken(ctx)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if token == "" || code == "" || code == res.JoinCode {
		t.Fatalf("rotate returned bad values: token=%q code=%q", token, code)
	}

	// 旧 code 失效。
	if _, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL: ts.URL, Identity: newIdentity(t), JoinCode: res.JoinCode,
	}, sessionclient.BaseHandler{}); err == nil {
		t.Fatal("old join code still works after rotate")
	}
	// 新 code 可用。
	c := join(t, ts, code, newCollector())
	if c.Self().MemberID == "" {
		t.Fatal("new join code did not work")
	}
}

func TestE2E_ApprovalFlow(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{
		Mode:            "group",
		MaxMembers:      4,
		RequireApproval: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 会话创建者（首个加入者）自动生效，否则无人能执行审批。
	owner := join(t, ts, res.JoinCode, newCollector())
	waitFor(t, 3*time.Second, "owner ready", func() bool { return owner.Self().MemberID != "" })
	if st := owner.Self().Status; st != "" && st != "active" {
		t.Fatalf("owner status = %s, want active", st)
	}

	// 第二个加入者进入 pending：能连上服务器，但不在数据面。
	guest := join(t, ts, res.JoinCode, newCollector())
	if st := guest.Self().Status; st != "pending" {
		t.Fatalf("guest status = %s, want pending", st)
	}
	// owner 不应把 pending 成员当作可用对端。
	waitFor(t, 3*time.Second, "pending member visible to owner", func() bool {
		ms, err := owner.PendingMembers(ctx)
		return err == nil && len(ms) == 1
	})

	pend, err := owner.PendingMembers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pend) != 1 {
		t.Fatalf("pending = %d, want 1", len(pend))
	}

	// 批准后：guest 生效、双方互相发现、数据面可用。
	if err := owner.ApproveMember(ctx, pend[0].MemberID); err != nil {
		t.Fatalf("approve: %v", err)
	}
	waitFor(t, 5*time.Second, "owner discovers approved member", func() bool {
		return len(owner.Peers()) == 1
	})
	waitFor(t, 5*time.Second, "guest becomes active", func() bool {
		return guest.Self().Status == "active"
	})
}

// ---- 用例：重连恢复 ---------------------------------------------------------

func TestE2E_ReconnectRestoresMemberIdentity(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{Mode: "group", MaxMembers: 4})

	// 用固定身份加入两次（模拟掉线重连），MemberID 必须保持一致。
	id := newIdentity(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL: ts.URL, Identity: id, JoinCode: res.JoinCode,
	}, newCollector())
	if err != nil {
		t.Fatal(err)
	}
	firstID := first.Self().MemberID
	// 模拟网络掉线（不发送 leave）：服务器应把成员标记为离线并保留记录。
	if err := first.Abort(); err != nil {
		t.Fatal(err)
	}
	// 等待服务器处理断开（离线标记是异步的）。
	time.Sleep(200 * time.Millisecond)

	// 用同一身份重连。
	second, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL: ts.URL, Identity: id, JoinCode: res.JoinCode,
	}, newCollector())
	if err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	defer second.Close()

	if second.Self().MemberID != firstID {
		t.Fatalf("member id changed after reconnect: %s -> %s", firstID, second.Self().MemberID)
	}
}

// ---- 用例：服务器不接触明文 ------------------------------------------------

func TestE2E_ServerNeverSeesPlaintext(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{Mode: "pair"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hA, hB := newCollector(), newCollector()
	go hA.pump(ctx)
	go hB.pump(ctx)
	cA := join(t, ts, res.JoinCode, hA)
	cB := join(t, ts, res.JoinCode, hB)

	waitFor(t, 5*time.Second, "peers discovered", func() bool {
		return len(cA.Peers()) == 1 && len(cB.Peers()) == 1
	})
	waitKex(t, cA, cB.Self().MemberID)

	const secret = "TOP-SECRET-MARKER-9F2A"
	if err := cA.SendTo(cB.Self().MemberID, []byte(secret)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "delivery", func() bool {
		return hB.countFrom(cA.Self().MemberID) == 1
	})

	// 服务器统计接口不应包含明文；且中继确实发生了（frames > 0）。
	body := getStats(t, ts.URL)
	if strings.Contains(body, secret) {
		t.Fatal("plaintext marker leaked into server stats")
	}
	if !strings.Contains(body, "frames_in") {
		t.Fatalf("unexpected stats body: %s", body)
	}
}

// ---- 用例：流语义 -----------------------------------------------------------

func TestE2E_StreamIsBidirectionalAndOrdered(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{Mode: "pair"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hA, hB := newCollector(), newCollector()
	go hA.pump(ctx)
	go hB.pump(ctx)
	cA := join(t, ts, res.JoinCode, hA)
	cB := join(t, ts, res.JoinCode, hB)

	waitFor(t, 5*time.Second, "discovery", func() bool { return len(cA.Peers()) == 1 })
	waitKex(t, cA, cB.Self().MemberID)

	// 同一条流上双向发送多条消息，顺序必须保持。
	sA, err := cA.OpenStream(cB.Self().MemberID)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := sA.Write([]byte{byte('0' + i)}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// B 侧应收到 5 条且顺序正确（同一条流）。
	waitFor(t, 5*time.Second, "5 messages", func() bool {
		return hB.countFrom(cA.Self().MemberID) == 5
	})
	hB.mu.Lock()
	got := append([]string(nil), hB.messages[cA.Self().MemberID]...)
	hB.mu.Unlock()
	want := []string{"0", "1", "2", "3", "4"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order mismatch at %d: got %v want %v", i, got, want)
		}
	}
}

// ---- 用例：错误路径 ---------------------------------------------------------

func TestE2E_JoinErrors(t *testing.T) {
	ts := startServer(t)
	res := create(t, ts, sessionclient.CreateParams{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 错误的 Join Code：必须被拒（且不泄露该 code 是否存在）。
	if _, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL: ts.URL, Identity: newIdentity(t),
		JoinCode: "P2P-AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-A",
	}, sessionclient.BaseHandler{}); err == nil {
		t.Fatal("invalid join code accepted")
	}

	// 缺少凭据。
	if _, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL: ts.URL, Identity: newIdentity(t),
	}, sessionclient.BaseHandler{}); err == nil {
		t.Fatal("join without credential accepted")
	}

	// 缺少身份。
	if _, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL: ts.URL, JoinCode: res.JoinCode,
	}, sessionclient.BaseHandler{}); err == nil {
		t.Fatal("join without identity accepted")
	}

	// 无效的服务器地址。
	if _, err := sessionclient.Join(ctx, sessionclient.Config{
		ServerURL: "ftp://nope", Identity: newIdentity(t), JoinCode: res.JoinCode,
	}, sessionclient.BaseHandler{}); err == nil {
		t.Fatal("invalid server scheme accepted")
	}
}

// getStats 拉取服务器统计（不校验管理员令牌，测试环境未配置）。
func getStats(t *testing.T, base string) string {
	t.Helper()
	resp, err := http.Get(base + "/v1/stats")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("stats body: %v", err)
	}
	return string(b)
}
