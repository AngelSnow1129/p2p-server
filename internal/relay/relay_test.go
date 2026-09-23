package relay

import (
	"errors"
	"testing"
	"time"

	"p2psession/internal/member"
	"p2psession/internal/protocol"
)

// fakeMembership 是 relay 所需的最小成员视图（避免依赖 session 包）。
type fakeMembership struct {
	index map[string]uint16
	// active 限定哪些成员处于生效状态。
	active map[string]bool
}

func newFakeMembership(pairs ...interface{}) *fakeMembership {
	f := &fakeMembership{index: map[string]uint16{}, active: map[string]bool{}}
	for i := 0; i+1 < len(pairs); i += 2 {
		id := pairs[i].(string)
		idx := uint16(pairs[i+1].(int))
		f.index[id] = idx
		f.active[id] = true
	}
	return f
}

func (f *fakeMembership) IsActiveMember(_, memberID string) bool { return f.active[memberID] }

func (f *fakeMembership) MemberIndex(_, memberID string) (uint16, bool) {
	idx, ok := f.index[memberID]
	return idx, ok
}

// harness 搭起一个 hub + forwarder，并挂载若干在线成员。
type harness struct {
	fwd   *Forwarder
	hub   *member.Hub
	conns map[string]*member.Conn
}

func newHarness(t *testing.T, members ...string) *harness {
	t.Helper()
	hub := member.NewHub()
	memb := &fakeMembership{index: map[string]uint16{}, active: map[string]bool{}}
	h := &harness{fwd: New(hub, memb, protocol.DefaultMaxRelayFrame), hub: hub, conns: map[string]*member.Conn{}}
	for i, id := range members {
		idx := uint16(i + 1)
		memb.index[id] = idx
		memb.active[id] = true
		// 数据面连接（relay）与一个控制面连接一并挂上，避免事件无处投递。
		rc, _ := hub.Attach("s1", id, "con-relay-"+id, member.KindRelay, idx, true)
		_, _ = hub.Attach("s1", id, "con-ctrl-"+id, member.KindControl, idx, true)
		h.conns[id] = rc
	}
	return h
}

// recvFrame 读取某成员收到的下一帧二进制并解析。
func (h *harness) recvFrame(t *testing.T, memberID string) *protocol.Frame {
	t.Helper()
	select {
	case f := <-h.conns[memberID].Recv():
		if f.Binary == nil {
			t.Fatalf("%s: expected binary frame", memberID)
		}
		parsed, err := protocol.ParseFrame(f.Binary)
		if err != nil {
			t.Fatalf("%s: parse: %v", memberID, err)
		}
		return parsed
	case <-time.After(time.Second):
		t.Fatalf("%s: timed out waiting for frame", memberID)
		return nil
	}
}

// assertNoFrame 确认某成员在短时间内没有收到任何帧。
func (h *harness) assertNoFrame(t *testing.T, memberID string) {
	t.Helper()
	select {
	case f := <-h.conns[memberID].Recv():
		t.Fatalf("%s: unexpected frame: %x", memberID, f.Binary)
	case <-time.After(60 * time.Millisecond):
	}
}

func encode(t *testing.T, f *protocol.Frame) []byte {
	t.Helper()
	data, err := protocol.EncodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestForwardUnicastRewritesSource(t *testing.T) {
	h := newHarness(t, "mA", "mB")

	// A 上传：Dst 为 B 的下标(2)，SrcIndex 故意填一个假值。
	raw := encode(t, &protocol.Frame{
		DstType:  protocol.DstUnicast,
		Dst:      []uint16{h.fwd.memb.(*fakeMembership).index["mB"]},
		SrcIndex: 999, // 伪造来源
		StreamID: 7,
		Seq:      1,
		Payload:  []byte("encrypted"),
	})

	n, err := h.fwd.Forward("s1", "mA", raw)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("delivered = %d, want 1", n)
	}

	got := h.recvFrame(t, "mB")
	// 来源下标必须被服务器改写为 A 的真实下标(1)，客户端伪造无效。
	if got.SrcIndex != 1 {
		t.Fatalf("src index = %d, want 1 (spoof not overwritten)", got.SrcIndex)
	}
	if got.StreamID != 7 || got.Seq != 1 {
		t.Fatalf("header mismatch: %+v", got)
	}
	if string(got.Payload) != "encrypted" {
		t.Fatalf("payload mismatch: %q", got.Payload)
	}
	// A 自己不应收到自己发的帧。
	h.assertNoFrame(t, "mA")
}

func TestForwardRejectsUnauthorizedSender(t *testing.T) {
	h := newHarness(t, "mA", "mB")
	raw := encode(t, &protocol.Frame{DstType: protocol.DstBroadcast, Payload: []byte("x")})

	// 非本会话成员：拒绝，且不投递给任何在线成员。
	if _, err := h.fwd.Forward("s1", "evil", raw); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("err = %v, want ErrNotAuthorized", err)
	}
	h.assertNoFrame(t, "mB")
	if h.fwd.DeniedFrames.Load() == 0 {
		t.Fatal("denied counter not incremented")
	}
}

func TestForwardRejectsUnknownTarget(t *testing.T) {
	h := newHarness(t, "mA", "mB")

	// 目标下标不存在。
	raw := encode(t, &protocol.Frame{
		DstType: protocol.DstUnicast,
		Dst:     []uint16{4242},
		Payload: []byte("x"),
	})
	if _, err := h.fwd.Forward("s1", "mA", raw); !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("err = %v, want ErrTargetNotFound", err)
	}

	// 目标是自己：拒绝（避免无意义的自发自收掩盖客户端 bug）。
	raw = encode(t, &protocol.Frame{
		DstType: protocol.DstUnicast,
		Dst:     []uint16{1},
		Payload: []byte("x"),
	})
	if _, err := h.fwd.Forward("s1", "mA", raw); !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("self-send err = %v, want ErrTargetNotFound", err)
	}
}

func TestForwardRejectsBadAndOversizedFrames(t *testing.T) {
	h := newHarness(t, "mA", "mB")

	// 非帧数据。
	if _, err := h.fwd.Forward("s1", "mA", []byte("not-a-frame")); !errors.Is(err, ErrBadFrame) {
		t.Fatalf("err = %v, want ErrBadFrame", err)
	}

	// 超出单帧上限：构造超长载荷。
	big := make([]byte, protocol.DefaultMaxRelayFrame+1)
	raw := encode(t, &protocol.Frame{
		DstType: protocol.DstBroadcast,
		Payload: big,
	})
	if _, err := h.fwd.Forward("s1", "mA", raw); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	h.assertNoFrame(t, "mB")
}

func TestForwardMulticastDeliversOnlyListed(t *testing.T) {
	h := newHarness(t, "mA", "mB", "mC")

	// A -> {B, C}
	raw := encode(t, &protocol.Frame{
		DstType:  protocol.DstMulticast,
		Dst:      []uint16{2, 3},
		StreamID: 11,
		Payload:  []byte("mc-payload"),
	})
	n, err := h.fwd.Forward("s1", "mA", raw)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("delivered = %d, want 2", n)
	}

	// 两个接收方看到的是「指向自己的单播帧」，且来源被改写。
	for _, id := range []string{"mB", "mC"} {
		got := h.recvFrame(t, id)
		if got.SrcIndex != 1 {
			t.Fatalf("%s: src = %d, want 1", id, got.SrcIndex)
		}
		if got.DstType != protocol.DstUnicast || len(got.Dst) != 1 {
			t.Fatalf("%s: dst not normalized to unicast: %+v", id, got)
		}
		if string(got.Payload) != "mc-payload" {
			t.Fatalf("%s: payload = %q", id, got.Payload)
		}
	}
}

func TestForwardMulticastSkipsSelfAndUnknown(t *testing.T) {
	h := newHarness(t, "mA", "mB")

	// 列表里混入自己与不存在的下标：只投递给 B，且不报错。
	raw := encode(t, &protocol.Frame{
		DstType: protocol.DstMulticast,
		Dst:     []uint16{1 /* self */, 2 /* B */, 999 /* unknown */},
		Payload: []byte("mixed"),
	})
	n, err := h.fwd.Forward("s1", "mA", raw)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("delivered = %d, want 1", n)
	}
	got := h.recvFrame(t, "mB")
	if string(got.Payload) != "mixed" {
		t.Fatalf("payload = %q", got.Payload)
	}
	h.assertNoFrame(t, "mA")
}

func TestForwardBroadcastReachesAllExceptSender(t *testing.T) {
	h := newHarness(t, "mA", "mB", "mC")

	raw := encode(t, &protocol.Frame{
		DstType:  protocol.DstBroadcast,
		StreamID: 21,
		Payload:  []byte("to-everyone"),
	})
	n, err := h.fwd.Forward("s1", "mA", raw)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("delivered = %d, want 2", n)
	}
	for _, id := range []string{"mB", "mC"} {
		got := h.recvFrame(t, id)
		if string(got.Payload) != "to-everyone" || got.SrcIndex != 1 {
			t.Fatalf("%s: %+v", id, got)
		}
	}
	h.assertNoFrame(t, "mA")
}

func TestForwardBroadcastFailsWhenAlone(t *testing.T) {
	h := newHarness(t, "mA")
	raw := encode(t, &protocol.Frame{DstType: protocol.DstBroadcast, Payload: []byte("x")})
	if _, err := h.fwd.Forward("s1", "mA", raw); !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("err = %v, want ErrTargetNotFound", err)
	}
}

func TestForwardCounters(t *testing.T) {
	h := newHarness(t, "mA", "mB")
	raw := encode(t, &protocol.Frame{
		DstType: protocol.DstUnicast,
		Dst:     []uint16{2},
		Payload: []byte("12345"),
	})
	if _, err := h.fwd.Forward("s1", "mA", raw); err != nil {
		t.Fatal(err)
	}
	if h.fwd.FramesIn.Load() != 1 || h.fwd.FramesOut.Load() != 1 {
		t.Fatalf("frame counters = %d/%d, want 1/1", h.fwd.FramesIn.Load(), h.fwd.FramesOut.Load())
	}
	if h.fwd.BytesIn.Load() != 5 || h.fwd.BytesOut.Load() != 5 {
		t.Fatalf("byte counters = %d/%d, want 5/5", h.fwd.BytesIn.Load(), h.fwd.BytesOut.Load())
	}
}

func BenchmarkForwardUnicast(b *testing.B) {
	hub := member.NewHub()
	memb := &fakeMembership{index: map[string]uint16{"mA": 1, "mB": 2}, active: map[string]bool{"mA": true, "mB": true}}
	fwd := New(hub, memb, protocol.DefaultMaxRelayFrame)

	rc, _ := hub.Attach("s1", "mB", "con", member.KindRelay, 2, true)

	raw, _ := protocol.EncodeFrame(&protocol.Frame{
		DstType:  protocol.DstUnicast,
		Dst:      []uint16{2},
		StreamID: 1,
		Payload:  make([]byte, 1024),
	})

	b.ReportAllocs()
	b.SetBytes(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := fwd.Forward("s1", "mA", raw); err != nil {
			b.Fatalf("forward: %v", err)
		}
		// 在同一 goroutine 内即时消费目标帧。
		//
		// 不能把消费放到独立 goroutine：Forward 的入队是非阻塞的，
		// 生产者一旦跑赢消费者，出站队列会填满并触发慢消费者保护把目标
		// 踢出会话，后续 Forward 就会因为「目标已不在会话内」而失败——
		// 那是基准自身的负载失衡，不是被测代码的问题。
		<-rc.Recv()
	}
}
