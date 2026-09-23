package protocol

import (
	"encoding/json"
	"testing"
)

func TestMessageRoundTrip(t *testing.T) {
	m := &Message{
		Type:     MsgKeyExchange,
		From:     "mbr_a",
		TargetID: "mbr_b",
		Payload:  json.RawMessage(`{"kex":"x25519","pub":"abcd"}`),
	}
	data, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != MsgKeyExchange || got.From != "mbr_a" || got.TargetID != "mbr_b" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if string(got.Payload) != `{"kex":"x25519","pub":"abcd"}` {
		t.Fatalf("payload mismatch: %s", got.Payload)
	}
}

func TestDecodeRejects(t *testing.T) {
	if _, err := Decode([]byte("{not json")); err == nil {
		t.Fatal("garbage accepted")
	}
	if _, err := Decode([]byte(`{"from":"x"}`)); err == nil {
		t.Fatal("message without type accepted")
	}
}

func TestNewError(t *testing.T) {
	m := NewError(ErrSessionFull, "full")
	if m.Type != MsgError || m.Code != ErrSessionFull || m.Message != "full" {
		t.Fatalf("bad error message: %+v", m)
	}
}

// ---- 数据面帧 ---------------------------------------------------------------

func TestFrameUnicastRoundTrip(t *testing.T) {
	f := &Frame{
		Flags:    FlagReliable,
		SrcIndex: 3,
		DstType:  DstUnicast,
		Dst:      []uint16{7},
		StreamID: 0xDEADBEEF,
		Seq:      42,
		Payload:  []byte("encrypted-bytes"),
	}
	data, err := EncodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 14+len(f.Payload) {
		t.Fatalf("frame len = %d, want %d", len(data), 14+len(f.Payload))
	}
	got, err := ParseFrame(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.SrcIndex != 3 || got.DstType != DstUnicast || got.Dst[0] != 7 ||
		got.StreamID != 0xDEADBEEF || got.Seq != 42 {
		t.Fatalf("header mismatch: %+v", got)
	}
	if string(got.Payload) != "encrypted-bytes" {
		t.Fatalf("payload mismatch: %q", got.Payload)
	}
	if got.HeaderLen() != 14 {
		t.Fatalf("header len = %d, want 14", got.HeaderLen())
	}
}

func TestFrameMulticastAndBroadcast(t *testing.T) {
	mc := &Frame{
		DstType:  DstMulticast,
		Dst:      []uint16{2, 5, 9},
		StreamID: 1,
		Payload:  []byte("x"),
	}
	data, err := EncodeFrame(mc)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseFrame(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Dst) != 3 || got.Dst[0] != 2 || got.Dst[2] != 9 {
		t.Fatalf("multicast dst mismatch: %+v", got.Dst)
	}

	bc := &Frame{DstType: DstBroadcast, StreamID: 2, Payload: []byte("y")}
	data, err = EncodeFrame(bc)
	if err != nil {
		t.Fatal(err)
	}
	got, err = ParseFrame(data)
	if err != nil {
		t.Fatal(err)
	}
	if got.DstType != DstBroadcast || len(got.Dst) != 0 {
		t.Fatalf("broadcast frame mismatch: %+v", got)
	}
}

func TestFrameValidation(t *testing.T) {
	// 单播必须恰好一个目标。
	if _, err := EncodeFrame(&Frame{DstType: DstUnicast, Dst: []uint16{1, 2}}); err == nil {
		t.Fatal("unicast with 2 dst accepted")
	}
	if _, err := EncodeFrame(&Frame{DstType: DstUnicast}); err == nil {
		t.Fatal("unicast with 0 dst accepted")
	}
	// 组播必须至少一个、且不超过 255。
	if _, err := EncodeFrame(&Frame{DstType: DstMulticast}); err == nil {
		t.Fatal("multicast with 0 dst accepted")
	}
	big := make([]uint16, 256)
	if _, err := EncodeFrame(&Frame{DstType: DstMulticast, Dst: big}); err == nil {
		t.Fatal("multicast with 256 dst accepted")
	}
	// 未知目标类型必须被拒。
	if _, err := EncodeFrame(&Frame{DstType: 99}); err == nil {
		t.Fatal("unknown dst type accepted")
	}
}

func TestParseFrameRejects(t *testing.T) {
	if _, err := ParseFrame(nil); err == nil {
		t.Fatal("empty frame accepted")
	}
	if _, err := ParseFrame(make([]byte, 5)); err == nil {
		t.Fatal("short frame accepted")
	}
	bad := make([]byte, 12)
	bad[0] = 0xFF
	if _, err := ParseFrame(bad); err == nil {
		t.Fatal("bad magic accepted")
	}
	// 单播但缺少目标下标。
	trunc := make([]byte, 12)
	trunc[0] = frameMagic
	trunc[4] = DstUnicast
	if _, err := ParseFrame(trunc); err == nil {
		t.Fatal("truncated unicast accepted")
	}
	// 组播数量为 0。
	mc := make([]byte, 13)
	mc[0] = frameMagic
	mc[4] = DstMulticast
	mc[12] = 0
	if _, err := ParseFrame(mc); err == nil {
		t.Fatal("multicast with count 0 accepted")
	}
}

// TestFrameSourceCannotBeTrustedByClient 确认帧头里的 SrcIndex 是可以被
// 服务器覆写的普通字段（客户端填什么都无意义）——语义由 relay 包保证。
func TestFrameSourceRewrite(t *testing.T) {
	f := &Frame{DstType: DstBroadcast, SrcIndex: 0 /* 客户端不知道自己的下标 */, Payload: []byte("d")}
	data, err := EncodeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := ParseFrame(data)
	if got.SrcIndex != 0 {
		t.Fatalf("src index = %d, want 0", got.SrcIndex)
	}
	// 服务器改写来源下标后，接收方看到的是真实发送者。
	got.SrcIndex = 12
	data2, _ := EncodeFrame(got)
	got2, _ := ParseFrame(data2)
	if got2.SrcIndex != 12 {
		t.Fatalf("rewritten src index = %d, want 12", got2.SrcIndex)
	}
}

func BenchmarkEncodeFrame(b *testing.B) {
	f := &Frame{
		DstType:  DstUnicast,
		Dst:      []uint16{5},
		StreamID: 1,
		Payload:  make([]byte, 1024),
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeFrame(f); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseFrame(b *testing.B) {
	f := &Frame{DstType: DstUnicast, Dst: []uint16{5}, StreamID: 1, Payload: make([]byte, 1024)}
	data, _ := EncodeFrame(f)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParseFrame(data); err != nil {
			b.Fatal(err)
		}
	}
}
