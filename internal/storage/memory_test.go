package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"p2psession/internal/protocol"
)

func newSession(id, code string, max int) *SessionRecord {
	return &SessionRecord{
		ID:         id,
		Mode:       protocol.ModePair,
		JoinCode:   code,
		TokenHash:  "hash-" + id,
		MaxMembers: max,
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(time.Hour),
	}
}

func joinReq(nodeID string) JoinRequest {
	return JoinRequest{
		NodeID:       nodeID,
		PublicKey:    "pub-" + nodeID,
		ConnectionID: "con-" + nodeID,
		Now:          time.Now(),
	}
}

func TestCreateAndGet(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()

	rec := newSession("sess_1", "P2P-AAAA-BBBB", 2)
	if err := st.Create(ctx, rec); err != nil {
		t.Fatal(err)
	}
	// 重复 ID 必须冲突。
	if err := st.Create(ctx, newSession("sess_1", "P2P-CCCC", 2)); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate id err = %v, want ErrConflict", err)
	}
	// 重复 join code 必须冲突。
	if err := st.Create(ctx, newSession("sess_2", "P2P-AAAA-BBBB", 2)); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate code err = %v, want ErrConflict", err)
	}

	got, err := st.Get(ctx, "sess_1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "sess_1" || got.MaxMembers != 2 {
		t.Fatalf("got %+v", got)
	}
	// 未知会话返回 ErrNotFound。
	if _, err := st.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestGetByJoinCodeIsCaseInsensitive(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	if err := st.Create(ctx, newSession("sess_1", "P2P-4K7X-9M2P", 2)); err != nil {
		t.Fatal(err)
	}
	// 大小写与连字符都不敏感（人工转写容错）。
	for _, code := range []string{"P2P-4K7X-9M2P", "p2p-4k7x-9m2p", "P2P4K7X9M2P", "  P2P-4k7x-9m2p  "} {
		got, err := st.GetByJoinCode(ctx, code)
		if err != nil {
			t.Fatalf("code %q: %v", code, err)
		}
		if got.ID != "sess_1" {
			t.Fatalf("code %q resolved to %s", code, got.ID)
		}
	}
	if _, err := st.GetByJoinCode(ctx, "P2P-NOPE"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestJoinEnforcesCapacity(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	if err := st.Create(ctx, newSession("sess_1", "P2P-A", 2)); err != nil {
		t.Fatal(err)
	}

	a, err := st.Join(ctx, "sess_1", joinReq("node_a"))
	if err != nil {
		t.Fatal(err)
	}
	if !a.IsOwner {
		t.Fatal("first member should be owner")
	}
	if a.Member.Role != protocol.RoleOwner {
		t.Fatalf("role = %s, want owner", a.Member.Role)
	}
	if _, err := st.Join(ctx, "sess_1", joinReq("node_b")); err != nil {
		t.Fatal(err)
	}
	// 第三人必须被拒（pair 上限 2）。
	if _, err := st.Join(ctx, "sess_1", joinReq("node_c")); !errors.Is(err, ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
}

func TestJoinIsAtomicUnderConcurrency(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	if err := st.Create(ctx, newSession("sess_1", "P2P-A", 3)); err != nil {
		t.Fatal(err)
	}

	// 10 个并发加入，只能有 3 个成功：容量校验必须与写入原子。
	const n = 10
	var ok, full int
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			_, err := st.Join(ctx, "sess_1", joinReq(string(rune('a'+i))))
			done <- err
		}(i)
	}
	for i := 0; i < n; i++ {
		err := <-done
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrFull):
			full++
		default:
			t.Fatalf("unexpected err: %v", err)
		}
	}
	if ok != 3 {
		t.Fatalf("successful joins = %d, want 3", ok)
	}
	if full != n-3 {
		t.Fatalf("rejected joins = %d, want %d", full, n-3)
	}
}

func TestRejoinReusesMemberIDAndIndex(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	if err := st.Create(ctx, newSession("sess_1", "P2P-A", 4)); err != nil {
		t.Fatal(err)
	}
	first, err := st.Join(ctx, "sess_1", joinReq("node_a"))
	if err != nil {
		t.Fatal(err)
	}
	// 同一 NodeID 重连：必须复用 MemberID 与 Index（恢复原会话）。
	again, err := st.Join(ctx, "sess_1", joinReq("node_a"))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Rejoined {
		t.Fatal("expected Rejoined=true")
	}
	if again.Member.ID != first.Member.ID {
		t.Fatalf("member id changed: %s vs %s", again.Member.ID, first.Member.ID)
	}
	if again.Member.Index != first.Member.Index {
		t.Fatalf("index changed: %d vs %d", again.Member.Index, first.Member.Index)
	}
	// 不应占用额外名额。
	rec, _ := st.Get(ctx, "sess_1")
	if rec.ActiveMembers() != 1 {
		t.Fatalf("active members = %d, want 1", rec.ActiveMembers())
	}
}

func TestJoinRejectsExpiredRevokedClosed(t *testing.T) {
	ctx := context.Background()

	t.Run("expired", func(t *testing.T) {
		st := NewMemory()
		rec := newSession("s", "C", 2)
		rec.ExpiresAt = time.Now().Add(-time.Minute)
		_ = st.Create(ctx, rec)
		if _, err := st.Join(ctx, "s", joinReq("n")); !errors.Is(err, ErrExpired) {
			t.Fatalf("err = %v, want ErrExpired", err)
		}
	})

	t.Run("revoked", func(t *testing.T) {
		st := NewMemory()
		_ = st.Create(ctx, newSession("s", "C", 2))
		if err := st.Revoke(ctx, "s", time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Join(ctx, "s", joinReq("n")); !errors.Is(err, ErrRevoked) {
			t.Fatalf("err = %v, want ErrRevoked", err)
		}
	})

	t.Run("closed", func(t *testing.T) {
		st := NewMemory()
		_ = st.Create(ctx, newSession("s", "C", 2))
		if err := st.Close(ctx, "s"); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Join(ctx, "s", joinReq("n")); !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	})
}

func TestJoinLimit(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	rec := newSession("s", "C", 10)
	rec.JoinLimit = 2
	if err := st.Create(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Join(ctx, "s", joinReq("n1")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Join(ctx, "s", joinReq("n2")); err != nil {
		t.Fatal(err)
	}
	// 第三次加入即使有名额也必须被拒。
	if _, err := st.Join(ctx, "s", joinReq("n3")); !errors.Is(err, ErrJoinLimit) {
		t.Fatalf("err = %v, want ErrJoinLimit", err)
	}
}

func TestPendingDoesNotConsumeCapacity(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	rec := newSession("s", "C", 2)
	// 审批策略来自会话记录（单一权威来源），而非每个 JoinRequest。
	rec.RequireApproval = true
	if err := st.Create(ctx, rec); err != nil {
		t.Fatal(err)
	}
	// 首个加入者即 owner，不受审批约束（否则无人能批准）。
	first, err := st.Join(ctx, "s", joinReq("n1"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Member.Status != protocol.MemberActive {
		t.Fatalf("owner status = %s, want active", first.Member.Status)
	}

	// 后续加入者进入 pending。
	out, err := st.Join(ctx, "s", joinReq("n2"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Member.Status != protocol.MemberPending {
		t.Fatalf("status = %s, want pending", out.Member.Status)
	}
	// pending 不占名额：active 仍只有 owner 一个。
	rec2, _ := st.Get(ctx, "s")
	if rec2.ActiveMembers() != 1 {
		t.Fatalf("active = %d, want 1", rec2.ActiveMembers())
	}
	// 即使名额被 active 占满，pending 仍可继续加入（不触发 ErrFull）。
	if _, err := st.Join(ctx, "s", joinReq("n3")); err != nil {
		t.Fatalf("third pending rejected: %v", err)
	}
}

func TestMarkOfflineOnlyForCurrentConnection(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	_ = st.Create(ctx, newSession("s", "C", 2))
	out, _ := st.Join(ctx, "s", joinReq("n1"))

	// 错误连接 ID 的离线通知必须被忽略（防止旧连接误伤新连接）。
	if err := st.MarkOffline(ctx, "s", out.Member.ID, "stale-con", time.Now()); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	rec, _ := st.Get(ctx, "s")
	if !rec.MemberByID(out.Member.ID).Online() {
		t.Fatal("member should still be online")
	}

	// 正确连接 ID：标记离线但保留记录（宽限期）。
	if err := st.MarkOffline(ctx, "s", out.Member.ID, out.Member.ConnectionID, time.Now()); err != nil {
		t.Fatal(err)
	}
	rec, _ = st.Get(ctx, "s")
	m := rec.MemberByID(out.Member.ID)
	if m == nil {
		t.Fatal("member record removed too early")
	}
	if m.Online() {
		t.Fatal("member should be offline")
	}
	if m.LeaveAt.IsZero() {
		t.Fatal("LeaveAt not set")
	}
}

func TestRotateToken(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	_ = st.Create(ctx, newSession("s", "P2P-OLD1-OLD2", 2))
	_ = st.Revoke(ctx, "s", time.Now())

	if err := st.RotateToken(ctx, "s", "newhash", "P2P-NEW1-NEW2", time.Now()); err != nil {
		t.Fatal(err)
	}
	rec, _ := st.Get(ctx, "s")
	if rec.TokenHash != "newhash" {
		t.Fatalf("hash = %s", rec.TokenHash)
	}
	if rec.Revoked {
		t.Fatal("Revoked flag should be cleared by rotate")
	}
	// 新 code 可反查，旧 code 失效。
	if _, err := st.GetByJoinCode(ctx, "P2P-NEW1-NEW2"); err != nil {
		t.Fatalf("new code not resolvable: %v", err)
	}
	if _, err := st.GetByJoinCode(ctx, "P2P-OLD1-OLD2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old code still resolvable: %v", err)
	}
}

func TestSnapshotsAreIsolated(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	_ = st.Create(ctx, newSession("s", "C", 2))
	_, _ = st.Join(ctx, "s", joinReq("n1"))

	snap, _ := st.Get(ctx, "s")
	// 修改快照不得影响存储内部状态（深拷贝语义）。
	snap.Members[0].NodeID = "tampered"
	snap.Members[0].Candidates = append(snap.Members[0].Candidates, protocol.Candidate{Type: "host"})
	snap.Members = append(snap.Members, &MemberRecord{ID: "ghost"})

	again, _ := st.Get(ctx, "s")
	if again.Members[0].NodeID == "tampered" {
		t.Fatal("snapshot mutation leaked into store")
	}
	if len(again.Members) != 1 {
		t.Fatalf("member count = %d, want 1", len(again.Members))
	}
}

func TestList(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	_ = st.Create(ctx, newSession("s1", "C1", 2))
	_ = st.Create(ctx, newSession("s2", "C2", 2))
	_ = st.Close(ctx, "s2")

	all, err := st.List(ctx, ListFilter{})
	if err != nil || len(all) != 2 {
		t.Fatalf("all = %d (err %v)", len(all), err)
	}
	closed, err := st.List(ctx, ListFilter{ClosedOnly: true})
	if err != nil || len(closed) != 1 || closed[0].ID != "s2" {
		t.Fatalf("closed = %+v (err %v)", closed, err)
	}
	limited, err := st.List(ctx, ListFilter{Limit: 1})
	if err != nil || len(limited) != 1 {
		t.Fatalf("limited = %d (err %v)", len(limited), err)
	}
}

func TestDeleteReleasesJoinCode(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	_ = st.Create(ctx, newSession("s1", "P2P-SAME", 2))
	if err := st.Delete(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	// code 索引必须一并释放，否则新会话无法复用该 code。
	if err := st.Create(ctx, newSession("s2", "P2P-SAME", 2)); err != nil {
		t.Fatalf("code not released after delete: %v", err)
	}
	if st.Count() != 1 {
		t.Fatalf("count = %d, want 1", st.Count())
	}
}
