package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"p2psession/internal/auth"
	"p2psession/internal/protocol"
	"p2psession/internal/storage"
)

// newTestService 构造一个使用内存存储的服务与已生成的节点身份。
func newTestService(t *testing.T) (*Service, *auth.Identity, *auth.Identity) {
	t.Helper()
	tickets, err := auth.NewTicketKey()
	if err != nil {
		t.Fatal(err)
	}
	svc := New(storage.NewMemory(), tickets, DefaultConfig())
	idA, err := auth.GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	idB, _ := auth.GenerateIdentity()
	return svc, idA, idB
}

// joinParams 用真实身份构造一次合法加入请求（含 join proof）。
func joinParams(t *testing.T, id *auth.Identity, sessionID, code, token string) JoinParams {
	t.Helper()
	// join proof 绑定的正是客户端可从 token 自行派生的哈希。
	tokenHash, err := auth.HashTokenString(token)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 32)
	copy(nonce, []byte("test-nonce-0123456789abcdef"))
	sig := id.SignJoin(tokenHash, nonce)

	return JoinParams{
		SessionID:    sessionID,
		JoinCode:     code,
		Token:        token,
		NodeID:       id.NodeID(),
		PublicKey:    id.Public(),
		Nonce:        nonce,
		Signature:    sig,
		ConnectionID: auth.RandomID("con_", 8),
		Capabilities: []string{"udp", "relay"},
	}
}

func TestCreatePairSession(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()

	res, err := svc.Create(ctx, CreateParams{Mode: protocol.ModePair})
	if err != nil {
		t.Fatal(err)
	}
	// pair 恒为 2 名成员，即使调用方要求更多。
	if res.Session.MaxMembers != 2 {
		t.Fatalf("pair max members = %d, want 2", res.Session.MaxMembers)
	}
	if res.Session.Mode != protocol.ModePair {
		t.Fatalf("mode = %s", res.Session.Mode)
	}
	if res.JoinCode == "" || res.Token.TokenString() == "" {
		t.Fatal("join code/token missing")
	}
	if res.Session.JoinCode != res.JoinCode {
		t.Fatal("join code not stored on session")
	}
	// 服务器只存哈希，绝不存原始 token。
	if res.Session.TokenHash == res.Token.TokenString() {
		t.Fatal("raw token stored on session")
	}
	if res.Session.TokenHash != res.Token.Hash() {
		t.Fatal("token hash mismatch")
	}
}

func TestCreateRejectsBadParams(t *testing.T) {
	svc, _, _ := newTestService(t)
	ctx := context.Background()

	if _, err := svc.Create(ctx, CreateParams{Mode: "mesh"}); !errors.Is(err, ErrBadParams) {
		t.Fatalf("unknown mode err = %v", err)
	}
	if _, err := svc.Create(ctx, CreateParams{Mode: protocol.ModeGroup, MaxMembers: MaxGroupMembers + 1}); !errors.Is(err, ErrBadParams) {
		t.Fatalf("oversized group err = %v", err)
	}
	if _, err := svc.Create(ctx, CreateParams{TTL: 100 * 24 * time.Hour}); !errors.Is(err, ErrBadParams) {
		t.Fatalf("oversized ttl err = %v", err)
	}
	if _, err := svc.Create(ctx, CreateParams{JoinLimit: -1}); !errors.Is(err, ErrBadParams) {
		t.Fatalf("negative join limit err = %v", err)
	}
}

func TestJoinWithJoinCodeAutoDiscoversPeers(t *testing.T) {
	svc, idA, idB := newTestService(t)
	ctx := context.Background()

	res, err := svc.Create(ctx, CreateParams{Mode: protocol.ModePair})
	if err != nil {
		t.Fatal(err)
	}

	// A 用 join code 加入（用户只需这一个字符串）。
	ja, err := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode))
	if err != nil {
		t.Fatalf("A join: %v", err)
	}
	if !ja.IsOwner {
		t.Fatal("first joiner should be owner")
	}
	if len(ja.Peers) != 0 {
		t.Fatalf("A should see no peers yet, got %d", len(ja.Peers))
	}
	if ja.Ticket == "" {
		t.Fatal("ticket missing")
	}

	// B 用同一 join code 加入：必须自动看到 A（无需任何手工配置）。
	jb, err := svc.Join(ctx, joinParams(t, idB, "", res.JoinCode, res.JoinCode))
	if err != nil {
		t.Fatalf("B join: %v", err)
	}
	if len(jb.Peers) != 1 {
		t.Fatalf("B peers = %d, want 1", len(jb.Peers))
	}
	if jb.Peers[0].ID != ja.Member.ID {
		t.Fatalf("B discovered %s, want %s", jb.Peers[0].ID, ja.Member.ID)
	}
	// 数据面下标必须互不相同且非 0。
	if ja.Member.Index == 0 || jb.Member.Index == 0 || ja.Member.Index == jb.Member.Index {
		t.Fatalf("bad indices: %d / %d", ja.Member.Index, jb.Member.Index)
	}
}

func TestJoinRejectsBadCredentials(t *testing.T) {
	svc, idA, idB := newTestService(t)
	ctx := context.Background()

	resA, _ := svc.Create(ctx, CreateParams{})
	resB, _ := svc.Create(ctx, CreateParams{})

	// 用 B 的 token 加入 A 的会话：必须拒绝。
	params := joinParams(t, idA, "", resA.JoinCode, resB.JoinCode)
	if _, err := svc.Join(ctx, params); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-session token err = %v, want ErrUnauthorized", err)
	}

	// 冒充他人 NodeID：必须拒绝。
	params = joinParams(t, idA, "", resA.JoinCode, resA.JoinCode)
	params.NodeID = idB.NodeID() // 与 PublicKey 不匹配
	if _, err := svc.Join(ctx, params); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("spoofed node id err = %v, want ErrUnauthorized", err)
	}

	// 篡改签名：必须拒绝。
	params = joinParams(t, idA, "", resA.JoinCode, resA.JoinCode)
	params.Signature = make([]byte, 64)
	if _, err := svc.Join(ctx, params); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("bad signature err = %v, want ErrUnauthorized", err)
	}

	// 缺少凭据。
	if _, err := svc.Join(ctx, JoinParams{NodeID: "x"}); !errors.Is(err, ErrBadParams) {
		t.Fatalf("missing credential err = %v, want ErrBadParams", err)
	}
}

func TestPairCapacityEnforced(t *testing.T) {
	svc, idA, idB := newTestService(t)
	ctx := context.Background()
	res, _ := svc.Create(ctx, CreateParams{Mode: protocol.ModePair})

	if _, err := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Join(ctx, joinParams(t, idB, "", res.JoinCode, res.JoinCode)); err != nil {
		t.Fatal(err)
	}

	// 第三人必须被拒（pair 上限 2）。
	idC, _ := auth.GenerateIdentity()
	if _, err := svc.Join(ctx, joinParams(t, idC, "", res.JoinCode, res.JoinCode)); !errors.Is(err, ErrSessionFull) {
		t.Fatalf("err = %v, want ErrSessionFull", err)
	}
}

func TestGroupSessionSupportsN(t *testing.T) {
	svc, idA, _ := newTestService(t)
	ctx := context.Background()
	res, _ := svc.Create(ctx, CreateParams{Mode: protocol.ModeGroup, MaxMembers: 5})

	if _, err := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode)); err != nil {
		t.Fatal(err)
	}
	// 再加入 4 个不同节点，全部应成功且能发现彼此。
	for i := 0; i < 4; i++ {
		id, _ := auth.GenerateIdentity()
		jr, err := svc.Join(ctx, joinParams(t, id, "", res.JoinCode, res.JoinCode))
		if err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
		// 第 N 个加入者应看到前面所有 active 成员（自动发现）。
		if len(jr.Peers) != i+1 {
			t.Fatalf("joiner %d sees %d peers, want %d", i, len(jr.Peers), i+1)
		}
	}

	// 第 6 个必须被拒。
	idX, _ := auth.GenerateIdentity()
	if _, err := svc.Join(ctx, joinParams(t, idX, "", res.JoinCode, res.JoinCode)); !errors.Is(err, ErrSessionFull) {
		t.Fatalf("err = %v, want ErrSessionFull", err)
	}
}

func TestRejoinRestoresMemberAndSurvivesRevoke(t *testing.T) {
	svc, idA, _ := newTestService(t)
	ctx := context.Background()
	res, _ := svc.Create(ctx, CreateParams{Mode: protocol.ModePair})

	first, err := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode))
	if err != nil {
		t.Fatal(err)
	}
	// 模拟掉线。
	if err := svc.MarkOffline(ctx, res.SessionID, first.Member.ID, first.Member.ConnectionID); err != nil {
		t.Fatal(err)
	}
	// 撤销 token 后，老成员仍应能重连恢复原身份（此前已被授权）。
	if err := svc.Revoke(ctx, res.SessionID); err != nil {
		t.Fatal(err)
	}

	again, err := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode))
	if err != nil {
		t.Fatalf("rejoin after revoke: %v", err)
	}
	if !again.Rejoined {
		t.Fatal("expected Rejoined=true")
	}
	if again.Member.ID != first.Member.ID {
		t.Fatalf("member id changed: %s -> %s", first.Member.ID, again.Member.ID)
	}
	if again.Member.Index != first.Member.Index {
		t.Fatalf("index changed: %d -> %d", first.Member.Index, again.Member.Index)
	}
}

func TestLeaveFreesCapacity(t *testing.T) {
	svc, idA, idB := newTestService(t)
	ctx := context.Background()
	res, _ := svc.Create(ctx, CreateParams{Mode: protocol.ModePair})

	ja, _ := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode))
	jb, _ := svc.Join(ctx, joinParams(t, idB, "", res.JoinCode, res.JoinCode))

	if err := svc.Leave(ctx, res.SessionID, jb.Member.ID); err != nil {
		t.Fatal(err)
	}
	// 离开后名额应释放，第三人可以加入。
	idC, _ := auth.GenerateIdentity()
	if _, err := svc.Join(ctx, joinParams(t, idC, "", res.JoinCode, res.JoinCode)); err != nil {
		t.Fatalf("join after leave: %v", err)
	}
	// A 应看到自己还在（未被误删）。
	rec, _ := svc.Get(ctx, res.SessionID)
	if rec.MemberByID(ja.Member.ID) == nil {
		t.Fatal("owner member disappeared")
	}
}

func TestApproveAndReject(t *testing.T) {
	svc, idA, idB := newTestService(t)
	ctx := context.Background()
	res, _ := svc.Create(ctx, CreateParams{
		Mode:            protocol.ModeGroup,
		MaxMembers:      5,
		RequireApproval: true,
	})

	owner, err := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode))
	if err != nil {
		t.Fatal(err)
	}
	// owner 即会话创建者，不受审批策略约束：否则他会卡在 pending，
	// 而唯一有权审批的人正是他自己，会话将永久无法使用。
	if owner.Member.Status != protocol.MemberActive {
		t.Fatalf("owner status = %s, want active", owner.Member.Status)
	}
	if owner.Member.Role != protocol.RoleOwner {
		t.Fatalf("owner role = %s", owner.Member.Role)
	}

	// 第二个加入者才需要审批。
	pend, err := svc.Join(ctx, joinParams(t, idB, "", res.JoinCode, res.JoinCode))
	if err != nil {
		t.Fatal(err)
	}
	if pend.Member.Status != protocol.MemberPending {
		t.Fatalf("status = %s, want pending", pend.Member.Status)
	}
	// pending 成员不能出现在他人的 peers 列表里（数据面隔离）。
	rec, _ := svc.Get(ctx, res.SessionID)
	if rec.ActiveMembers() != 1 {
		t.Fatalf("active = %d, want 1", rec.ActiveMembers())
	}

	// 非 owner 无权批准。
	if _, err := svc.Approve(ctx, res.SessionID, pend.Member.ID, owner.Member.ID); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("non-owner approve err = %v, want ErrNotOwner", err)
	}

	// owner 批准后成员生效。
	approved, err := svc.Approve(ctx, res.SessionID, owner.Member.ID, pend.Member.ID)
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	if approved.Status != protocol.MemberActive {
		t.Fatalf("status after approve = %s", approved.Status)
	}
	rec, _ = svc.Get(ctx, res.SessionID)
	if rec.ActiveMembers() != 2 {
		t.Fatalf("active = %d, want 2", rec.ActiveMembers())
	}

	// 拒绝另一个成员。
	idC, _ := auth.GenerateIdentity()
	pc, _ := svc.Join(ctx, joinParams(t, idC, "", res.JoinCode, res.JoinCode))
	if err := svc.Reject(ctx, res.SessionID, owner.Member.ID, pc.Member.ID); err != nil {
		t.Fatalf("reject: %v", err)
	}
	m, _ := svc.Member(ctx, res.SessionID, pc.Member.ID)
	if m.Status != protocol.MemberDenied {
		t.Fatalf("status = %s, want denied", m.Status)
	}
	// owner 不能移除自己。
	if err := svc.Reject(ctx, res.SessionID, owner.Member.ID, owner.Member.ID); !errors.Is(err, ErrBadParams) {
		t.Fatalf("self reject err = %v, want ErrBadParams", err)
	}
}

func TestExpiryAndPrune(t *testing.T) {
	svc, idA, _ := newTestService(t)
	ctx := context.Background()

	now := time.Now()
	svc.SetNow(func() time.Time { return now })

	res, _ := svc.Create(ctx, CreateParams{TTL: time.Minute})
	if _, err := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode)); err != nil {
		t.Fatal(err)
	}

	// 时间推进到过期之后：Get 与 Join 都必须报告过期。
	later := now.Add(2 * time.Minute)
	svc.SetNow(func() time.Time { return later })

	if _, err := svc.Get(ctx, res.SessionID); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("get err = %v, want ErrSessionExpired", err)
	}
	if _, err := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode)); !errors.Is(err, ErrSessionExpired) {
		t.Fatalf("join err = %v, want ErrSessionExpired", err)
	}

	// Prune 应回收过期会话。
	pruned, err := svc.Prune(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pruned.SessionsExpired != 1 {
		t.Fatalf("expired sessions = %d, want 1", pruned.SessionsExpired)
	}
	if _, err := svc.Get(ctx, res.SessionID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("session not deleted: %v", err)
	}
}

func TestPruneReclaimsOfflineMembersAfterGrace(t *testing.T) {
	svc, idA, idB := newTestService(t)
	ctx := context.Background()

	now := time.Now()
	svc.SetNow(func() time.Time { return now })

	res, _ := svc.Create(ctx, CreateParams{Mode: protocol.ModeGroup, MaxMembers: 5})
	ja, _ := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode))
	jb, _ := svc.Join(ctx, joinParams(t, idB, "", res.JoinCode, res.JoinCode))

	// B 离线。
	if err := svc.MarkOffline(ctx, res.SessionID, jb.Member.ID, jb.Member.ConnectionID); err != nil {
		t.Fatal(err)
	}

	// 未超宽限期：记录保留（重连窗口）。
	pruned, _ := svc.Prune(ctx)
	if pruned.MembersExpired != 0 {
		t.Fatalf("premature member expiry: %d", pruned.MembersExpired)
	}

	// 超过宽限期：回收成员但会话仍在（A 还在）。
	svc.SetNow(func() time.Time { return now.Add(DefaultMemberGrace + time.Minute) })
	pruned, _ = svc.Prune(ctx)
	if pruned.MembersExpired != 1 {
		t.Fatalf("members expired = %d, want 1", pruned.MembersExpired)
	}
	rec, err := svc.Get(ctx, res.SessionID)
	if err != nil {
		t.Fatalf("session should survive: %v", err)
	}
	if len(rec.Members) != 1 || rec.Members[0].ID != ja.Member.ID {
		t.Fatalf("remaining members = %+v", rec.Members)
	}
}

func TestPruneIdleSession(t *testing.T) {
	svc, idA, _ := newTestService(t)
	ctx := context.Background()

	now := time.Now()
	svc.SetNow(func() time.Time { return now })

	res, _ := svc.Create(ctx, CreateParams{IdleTimeout: time.Minute, TTL: time.Hour})
	ja, _ := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode))
	_ = svc.MarkOffline(ctx, res.SessionID, ja.Member.ID, ja.Member.ConnectionID)

	// 全员离线超过 IdleTimeout：整个会话应被回收。
	svc.SetNow(func() time.Time { return now.Add(3 * time.Minute) })
	pruned, _ := svc.Prune(ctx)
	if pruned.SessionsExpired != 1 {
		t.Fatalf("idle session not reclaimed: %+v", pruned)
	}
}

func TestRotateTokenInvalidatesOld(t *testing.T) {
	svc, idA, idB := newTestService(t)
	ctx := context.Background()
	res, _ := svc.Create(ctx, CreateParams{Mode: protocol.ModeGroup, MaxMembers: 5})
	if _, err := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode)); err != nil {
		t.Fatal(err)
	}

	newToken, err := svc.Rotate(ctx, res.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	// 旧 code 必须失效。
	if _, err := svc.Join(ctx, joinParams(t, idB, "", res.JoinCode, res.JoinCode)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("old code err = %v, want ErrUnauthorized", err)
	}
	// 新 code 可用。
	if _, err := svc.Join(ctx, joinParams(t, idB, "", newToken.JoinCode(), newToken.JoinCode())); err != nil {
		t.Fatalf("new code rejected: %v", err)
	}
}

func TestUpdateCandidatesAndTouch(t *testing.T) {
	svc, idA, _ := newTestService(t)
	ctx := context.Background()
	res, _ := svc.Create(ctx, CreateParams{})
	ja, _ := svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode))

	cands := []protocol.Candidate{
		{Type: "host", IP: "192.168.1.10", Port: 5000, Proto: "udp", Priority: 100},
		{Type: "srflx", IP: "203.0.113.7", Port: 41234, Proto: "udp", Priority: 50},
	}
	if err := svc.UpdateCandidates(ctx, res.SessionID, ja.Member.ID, cands); err != nil {
		t.Fatal(err)
	}
	m, err := svc.Member(ctx, res.SessionID, ja.Member.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Candidates) != 2 || m.Candidates[0].IP != "192.168.1.10" {
		t.Fatalf("candidates = %+v", m.Candidates)
	}

	// 超出上限必须被拒。
	tooMany := make([]protocol.Candidate, protocol.DefaultMaxCandidates+1)
	if err := svc.UpdateCandidates(ctx, res.SessionID, ja.Member.ID, tooMany); !errors.Is(err, ErrBadParams) {
		t.Fatalf("err = %v, want ErrBadParams", err)
	}

	// Touch 只接受当前连接 ID。
	if err := svc.Touch(ctx, res.SessionID, ja.Member.ID, "stale"); !errors.Is(err, ErrStaleConnection) {
		t.Fatalf("stale touch err = %v, want ErrStaleConnection", err)
	}
	if err := svc.Touch(ctx, res.SessionID, ja.Member.ID, ja.Member.ConnectionID); err != nil {
		t.Fatalf("touch: %v", err)
	}
}

func TestStats(t *testing.T) {
	svc, idA, idB := newTestService(t)
	ctx := context.Background()
	res, _ := svc.Create(ctx, CreateParams{Mode: protocol.ModeGroup, MaxMembers: 5})
	svc.Join(ctx, joinParams(t, idA, "", res.JoinCode, res.JoinCode))
	svc.Join(ctx, joinParams(t, idB, "", res.JoinCode, res.JoinCode))

	st, err := svc.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Sessions != 1 || st.Members != 2 || st.Online != 2 {
		t.Fatalf("stats = %+v", st)
	}
}
