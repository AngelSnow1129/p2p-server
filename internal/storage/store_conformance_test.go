package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"p2psession/internal/protocol"
)

// 本文件是**存储一致性测试**：同一套用例同时跑 MemoryStore 与 SQLiteStore。
//
// 为什么必须双后端跑同一套用例：存储层的语义（容量、原子性、重连复用、
// 审批不占名额、快照隔离……）是 session 包的隐含契约。若只给 SQLite 单独
// 写一份测试，两份用例会各自漂移，而「某个后端悄悄放宽了容量校验」这类
// 缺陷只在生产并发下才暴露。把用例写成后端无关的，是唯一能长期守住
// 一致性的做法。
//
// 新存储实现（Redis/Postgres…）只要加进 backends() 即可获得全部覆盖。

// backend 描述一个可测的存储实现。
type backend struct {
	name string
	// new 创建一个空存储。返回的 store 会在测试结束时清理。
	new func(t *testing.T) Store
}

// backends 返回全部待测后端。
func backends(t *testing.T) []backend {
	return []backend{
		{
			name: "memory",
			new:  func(t *testing.T) Store { return NewMemory() },
		},
		{
			name: "sqlite",
			new: func(t *testing.T) Store {
				// 每个子测试用独立临时库，避免相互污染。
				path := filepath.Join(t.TempDir(), "test.db")
				st, err := NewSQLite(path)
				if err != nil {
					t.Fatalf("open sqlite: %v", err)
				}
				t.Cleanup(func() {
					if err := st.CloseDB(); err != nil {
						t.Errorf("close sqlite: %v", err)
					}
				})
				return st
			},
		},
	}
}

// forEachBackend 对每个后端运行 fn（子测试名 = 后端名）。
//
// 用子测试而非循环内断言：失败时能一眼看出是哪个后端坏了。
func forEachBackend(t *testing.T, fn func(t *testing.T, st Store)) {
	t.Helper()
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			fn(t, b.new(t))
		})
	}
}

// ---- 测试数据辅助 ----------------------------------------------------------
//
// newSession / joinReq 由 memory_test.go 提供（同包共用），
// 避免两处定义漂移。

// mustCount 读取会话总数。
//
// Count() 刻意**不在** Store 接口里：它只服务于指标与测试，
// 不该强迫未来每个存储实现（如 Redis）都去实现一次全量计数
// ——那在多实例下是一次 O(N) 扫描。因此这里用可选接口探测。
func mustCount(t *testing.T, st Store) int {
	t.Helper()
	c, ok := st.(interface{ Count() int })
	if !ok {
		t.Fatalf("store %T does not expose Count()", st)
	}
	return c.Count()
}

// ---- 一致性用例 ------------------------------------------------------------

func TestConformanceCreateAndGet(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()

		if err := st.Create(ctx, newSession("sess_1", "P2P-AAAA-BBBB", 2)); err != nil {
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
		if got.Mode != protocol.ModePair || got.TokenHash != "hash-sess_1" {
			t.Fatalf("scalar fields not round-tripped: %+v", got)
		}
		if _, err := st.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}

func TestConformanceGetByJoinCodeIsCaseInsensitive(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("sess_1", "P2P-4K7X-9M2P", 2)); err != nil {
			t.Fatal(err)
		}
		for _, code := range []string{
			"P2P-4K7X-9M2P", "p2p-4k7x-9m2p", "P2P4K7X9M2P", "  P2P-4k7x-9m2p  ",
		} {
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
	})
}

func TestConformanceJoinEnforcesCapacity(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("sess_1", "P2P-A", 2)); err != nil {
			t.Fatal(err)
		}

		a, err := st.Join(ctx, "sess_1", joinReq("node_a"))
		if err != nil {
			t.Fatal(err)
		}
		if !a.IsOwner || a.Member.Role != protocol.RoleOwner {
			t.Fatalf("first member should be owner: %+v", a.Member)
		}
		if _, err := st.Join(ctx, "sess_1", joinReq("node_b")); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Join(ctx, "sess_1", joinReq("node_c")); !errors.Is(err, ErrFull) {
			t.Fatalf("err = %v, want ErrFull", err)
		}
	})
}

// 容量校验必须与写入原子（store.go 接口注释的硬要求）。
func TestConformanceJoinIsAtomicUnderConcurrency(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("sess_1", "P2P-A", 3)); err != nil {
			t.Fatal(err)
		}

		const n = 10
		var mu sync.Mutex
		var ok, full int
		var other []error
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, err := st.Join(ctx, "sess_1", joinReq(fmt.Sprintf("node_%02d", i)))
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					ok++
				case errors.Is(err, ErrFull):
					full++
				default:
					other = append(other, err)
				}
			}(i)
		}
		wg.Wait()

		if len(other) > 0 {
			t.Fatalf("unexpected errors: %v", other)
		}
		if ok != 3 {
			t.Fatalf("successful joins = %d, want 3", ok)
		}
		if full != n-3 {
			t.Fatalf("rejected joins = %d, want %d", full, n-3)
		}
		// 最终成员数也必须正好是上限，不能多也不能少。
		rec, err := st.Get(ctx, "sess_1")
		if err != nil {
			t.Fatal(err)
		}
		if got := len(rec.Members); got != 3 {
			t.Fatalf("final members = %d, want 3", got)
		}
	})
}

func TestConformanceRejoinReusesMemberIDAndIndex(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("sess_1", "P2P-A", 4)); err != nil {
			t.Fatal(err)
		}
		first, err := st.Join(ctx, "sess_1", joinReq("node_a"))
		if err != nil {
			t.Fatal(err)
		}
		again, err := st.Join(ctx, "sess_1", joinReq("node_a"))
		if err != nil {
			t.Fatal(err)
		}
		if !again.Rejoined {
			t.Fatal("expected Rejoined=true")
		}
		if again.Member.ID != first.Member.ID || again.Member.Index != first.Member.Index {
			t.Fatalf("member identity changed: %s/%d vs %s/%d",
				again.Member.ID, again.Member.Index, first.Member.ID, first.Member.Index)
		}
		rec, _ := st.Get(ctx, "sess_1")
		if rec.ActiveMembers() != 1 {
			t.Fatalf("active members = %d, want 1", rec.ActiveMembers())
		}
		if len(rec.Members) != 1 {
			t.Fatalf("member rows = %d, want 1 (rejoin must not insert)", len(rec.Members))
		}
	})
}

func TestConformanceJoinRejectsExpiredRevokedClosed(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()

		t.Run("expired", func(t *testing.T) {
			rec := newSession("s", "C", 2)
			rec.ExpiresAt = time.Now().Add(-time.Minute)
			if err := st.Create(ctx, rec); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Join(ctx, "s", joinReq("n")); !errors.Is(err, ErrExpired) {
				t.Fatalf("err = %v, want ErrExpired", err)
			}
		})

		t.Run("revoked", func(t *testing.T) {
			if err := st.Create(ctx, newSession("s2", "C2", 2)); err != nil {
				t.Fatal(err)
			}
			if err := st.Revoke(ctx, "s2", time.Now()); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Join(ctx, "s2", joinReq("n")); !errors.Is(err, ErrRevoked) {
				t.Fatalf("err = %v, want ErrRevoked", err)
			}
		})

		t.Run("closed", func(t *testing.T) {
			if err := st.Create(ctx, newSession("s3", "C3", 2)); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(ctx, "s3"); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Join(ctx, "s3", joinReq("n")); !errors.Is(err, ErrClosed) {
				t.Fatalf("err = %v, want ErrClosed", err)
			}
		})

		// 对不存在的会话，各操作都应返回 ErrNotFound 而非静默成功。
		t.Run("missing session", func(t *testing.T) {
			if _, err := st.Join(ctx, "ghost", joinReq("n")); !errors.Is(err, ErrNotFound) {
				t.Fatalf("join err = %v, want ErrNotFound", err)
			}
			if err := st.Revoke(ctx, "ghost", time.Now()); !errors.Is(err, ErrNotFound) {
				t.Fatalf("revoke err = %v, want ErrNotFound", err)
			}
			if err := st.Close(ctx, "ghost"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("close err = %v, want ErrNotFound", err)
			}
			if err := st.Delete(ctx, "ghost"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("delete err = %v, want ErrNotFound", err)
			}
			if err := st.MarkOffline(ctx, "ghost", "m", "c", time.Now()); !errors.Is(err, ErrNotFound) {
				t.Fatalf("markoffline err = %v, want ErrNotFound", err)
			}
			if err := st.UpdateMember(ctx, "ghost", &MemberRecord{ID: "m"}); !errors.Is(err, ErrNotFound) {
				t.Fatalf("updatemember err = %v, want ErrNotFound", err)
			}
			if err := st.RemoveMember(ctx, "ghost", "m"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("removemember err = %v, want ErrNotFound", err)
			}
			if err := st.RotateToken(ctx, "ghost", "h", "C", time.Now()); !errors.Is(err, ErrNotFound) {
				t.Fatalf("rotate err = %v, want ErrNotFound", err)
			}
		})
	})
}

func TestConformanceJoinLimit(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
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
		if _, err := st.Join(ctx, "s", joinReq("n3")); !errors.Is(err, ErrJoinLimit) {
			t.Fatalf("err = %v, want ErrJoinLimit", err)
		}
		// JoinsUsed 必须被持久化（重启后限额仍然有效）。
		got, _ := st.Get(ctx, "s")
		if got.JoinsUsed != 2 {
			t.Fatalf("joins_used = %d, want 2", got.JoinsUsed)
		}
	})
}

func TestConformancePendingDoesNotConsumeCapacity(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		rec := newSession("s", "C", 2)
		rec.RequireApproval = true
		if err := st.Create(ctx, rec); err != nil {
			t.Fatal(err)
		}
		first, err := st.Join(ctx, "s", joinReq("n1"))
		if err != nil {
			t.Fatal(err)
		}
		if first.Member.Status != protocol.MemberActive {
			t.Fatalf("owner status = %s, want active", first.Member.Status)
		}

		out, err := st.Join(ctx, "s", joinReq("n2"))
		if err != nil {
			t.Fatal(err)
		}
		if out.Member.Status != protocol.MemberPending {
			t.Fatalf("status = %s, want pending", out.Member.Status)
		}
		rec2, _ := st.Get(ctx, "s")
		if rec2.ActiveMembers() != 1 {
			t.Fatalf("active = %d, want 1", rec2.ActiveMembers())
		}
		// pending 不占名额：第三人仍可加入（不触发 ErrFull）。
		if _, err := st.Join(ctx, "s", joinReq("n3")); err != nil {
			t.Fatalf("third pending rejected: %v", err)
		}
	})
}

func TestConformanceMarkOfflineOnlyForCurrentConnection(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("s", "C", 2)); err != nil {
			t.Fatal(err)
		}
		out, err := st.Join(ctx, "s", joinReq("n1"))
		if err != nil {
			t.Fatal(err)
		}

		// 旧连接的离线通知必须被忽略（不能误伤新连接）。
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
		if m.Online() || m.LeaveAt.IsZero() {
			t.Fatalf("offline state wrong: online=%v leaveAt=%v", m.Online(), m.LeaveAt)
		}
	})
}

func TestConformanceMemberLifecycle(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("s", "C", 4)); err != nil {
			t.Fatal(err)
		}
		out, err := st.Join(ctx, "s", joinReq("n1"))
		if err != nil {
			t.Fatal(err)
		}

		// UpdateMember：整体替换（审批路径）。
		m := *out.Member
		m.Status = protocol.MemberActive
		m.Candidates = []protocol.Candidate{{Type: "srflx", IP: "1.2.3.4", Port: 5000, Proto: "udp", Priority: 9}}
		m.Capabilities = []string{"relay", "udp"}
		if err := st.UpdateMember(ctx, "s", &m); err != nil {
			t.Fatal(err)
		}
		got, _ := st.Get(ctx, "s")
		cur := got.MemberByID(m.ID)
		if cur.Status != protocol.MemberActive {
			t.Fatalf("status = %s", cur.Status)
		}
		if len(cur.Candidates) != 1 || cur.Candidates[0].IP != "1.2.3.4" {
			t.Fatalf("candidates not round-tripped: %+v", cur.Candidates)
		}
		if len(cur.Capabilities) != 2 || cur.Capabilities[1] != "udp" {
			t.Fatalf("capabilities not round-tripped: %+v", cur.Capabilities)
		}

		// 更新不存在的成员 → ErrMemberMissing。
		ghost := m
		ghost.ID = "mbr_nope"
		if err := st.UpdateMember(ctx, "s", &ghost); !errors.Is(err, ErrMemberMissing) {
			t.Fatalf("update ghost err = %v, want ErrMemberMissing", err)
		}

		// RemoveMember：删除后下标可被复用。
		removedIdx := cur.Index
		if err := st.RemoveMember(ctx, "s", m.ID); err != nil {
			t.Fatal(err)
		}
		if err := st.RemoveMember(ctx, "s", m.ID); !errors.Is(err, ErrMemberMissing) {
			t.Fatalf("second remove err = %v, want ErrMemberMissing", err)
		}
		next, err := st.Join(ctx, "s", joinReq("n2"))
		if err != nil {
			t.Fatal(err)
		}
		if next.Member.Index != removedIdx {
			t.Fatalf("index not reused: got %d want %d", next.Member.Index, removedIdx)
		}
	})
}

func TestConformanceRotateToken(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("s", "P2P-OLD1-OLD2", 2)); err != nil {
			t.Fatal(err)
		}
		if err := st.Revoke(ctx, "s", time.Now()); err != nil {
			t.Fatal(err)
		}

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
		if _, err := st.GetByJoinCode(ctx, "P2P-NEW1-NEW2"); err != nil {
			t.Fatalf("new code not resolvable: %v", err)
		}
		if _, err := st.GetByJoinCode(ctx, "P2P-OLD1-OLD2"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("old code still resolvable: %v", err)
		}
	})
}

// RotateToken 到已被占用的 code 必须被拒绝，且不得破坏原状态。
func TestConformanceRotateTokenRejectsTakenCode(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("a", "P2P-AAAA", 2)); err != nil {
			t.Fatal(err)
		}
		if err := st.Create(ctx, newSession("b", "P2P-BBBB", 2)); err != nil {
			t.Fatal(err)
		}
		if err := st.RotateToken(ctx, "b", "h2", "P2P-AAAA", time.Now()); !errors.Is(err, ErrConflict) {
			t.Fatalf("err = %v, want ErrConflict", err)
		}
		// b 的状态必须保持不变（事务回滚）。
		rec, err := st.Get(ctx, "b")
		if err != nil {
			t.Fatal(err)
		}
		if rec.TokenHash != "hash-b" || rec.JoinCode != "P2P-BBBB" {
			t.Fatalf("state corrupted by failed rotate: hash=%s code=%s", rec.TokenHash, rec.JoinCode)
		}
		// a 仍可用原 code 反查。
		if _, err := st.GetByJoinCode(ctx, "P2P-AAAA"); err != nil {
			t.Fatalf("a lost its code: %v", err)
		}
	})
}

func TestConformanceSnapshotsAreIsolated(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("s", "C", 2)); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Join(ctx, "s", joinReq("n1")); err != nil {
			t.Fatal(err)
		}

		snap, err := st.Get(ctx, "s")
		if err != nil {
			t.Fatal(err)
		}
		// 修改快照不得影响存储内部状态（深拷贝语义）。
		snap.Members[0].NodeID = "tampered"
		snap.Members[0].Candidates = append(snap.Members[0].Candidates,
			protocol.Candidate{Type: "host"})
		snap.Members = append(snap.Members, &MemberRecord{ID: "ghost"})
		snap.OwnerMemberID = "tampered"

		again, err := st.Get(ctx, "s")
		if err != nil {
			t.Fatal(err)
		}
		if again.Members[0].NodeID == "tampered" {
			t.Fatal("snapshot mutation leaked into store")
		}
		if len(again.Members) != 1 {
			t.Fatalf("member count = %d, want 1", len(again.Members))
		}
		if again.OwnerMemberID == "tampered" {
			t.Fatal("scalar mutation leaked into store")
		}
	})
}

func TestConformanceList(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("s1", "C1", 2)); err != nil {
			t.Fatal(err)
		}
		if err := st.Create(ctx, newSession("s2", "C2", 2)); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(ctx, "s2"); err != nil {
			t.Fatal(err)
		}

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

		// 会话成员的完整重载：List 必须返回成员，否则 Prune 无法判断
		// 「离线成员是否超宽限期」。
		if _, err := st.Join(ctx, "s1", joinReq("n1")); err != nil {
			t.Fatal(err)
		}
		all, _ = st.List(ctx, ListFilter{})
		var withMember *SessionRecord
		for _, r := range all {
			if r.ID == "s1" {
				withMember = r
			}
		}
		if withMember == nil || len(withMember.Members) != 1 {
			t.Fatalf("List did not load members: %+v", withMember)
		}
	})
}

// List(ExpiredBefore) 是 Prune 的入口，必须只返回「已过期」的会话。
func TestConformanceListExpiredBefore(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		now := time.Now()

		live := newSession("live", "C-LIVE", 2)
		live.ExpiresAt = now.Add(time.Hour)
		if err := st.Create(ctx, live); err != nil {
			t.Fatal(err)
		}
		dead := newSession("dead", "C-DEAD", 2)
		dead.ExpiresAt = now.Add(-time.Minute)
		if err := st.Create(ctx, dead); err != nil {
			t.Fatal(err)
		}

		got, err := st.List(ctx, ListFilter{ExpiredBefore: now})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "dead" {
			t.Fatalf("expired filter = %+v, want only 'dead'", got)
		}
	})
}

func TestConformanceDeleteReleasesJoinCode(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx := context.Background()
		if err := st.Create(ctx, newSession("s1", "P2P-SAME", 2)); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Join(ctx, "s1", joinReq("n1")); err != nil {
			t.Fatal(err)
		}
		if err := st.Delete(ctx, "s1"); err != nil {
			t.Fatal(err)
		}
		// code 索引必须一并释放。
		if err := st.Create(ctx, newSession("s2", "P2P-SAME", 2)); err != nil {
			t.Fatalf("code not released after delete: %v", err)
		}
		if mustCount(t, st) != 1 {
			t.Fatalf("count = %d, want 1", mustCount(t, st))
		}
		// 成员行也必须被清掉（不能留下孤儿行）。
		got, err := st.Get(ctx, "s2")
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Members) != 0 {
			t.Fatalf("new session inherited %d orphan members", len(got.Members))
		}
	})
}

// 上下文取消必须被尊重（服务关停时不应继续写库）。
func TestConformanceContextCancellation(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st Store) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // 立即取消

		if err := st.Create(ctx, newSession("s", "C", 2)); !errors.Is(err, context.Canceled) {
			t.Fatalf("create err = %v, want context.Canceled", err)
		}
		if _, err := st.Get(ctx, "s"); !errors.Is(err, context.Canceled) {
			t.Fatalf("get err = %v, want context.Canceled", err)
		}
		if _, err := st.List(ctx, ListFilter{}); !errors.Is(err, context.Canceled) {
			t.Fatalf("list err = %v, want context.Canceled", err)
		}
	})
}
