// Package relay 实现数据面的「授权 + 转发」：把端到端加密的 DataFrame
// 从发送方送达目标成员。服务器不解析、也无法解密 Payload。
//
// 安全模型（务必牢记）：
//   - 只在会话内转发：发送方与所有接收方都必须是同一 Session 的 active 成员，
//     否则返回 ErrNotAuthorized / ErrTargetNotFound；
//   - 帧头 SrcIndex 一律由本包按发送方真实身份盖写，客户端无法伪造来源；
//   - 广播/组播由服务器做一次解码与多次编码，避免客户端向全房间灌注。
//
// 与 member 包的分工：member 管「往哪条连接写」，relay 管「谁能发给谁」。
package relay

import (
	"errors"
	"sync/atomic"

	"p2psession/internal/member"
	"p2psession/internal/protocol"
)

// 转发错误。
var (
	// ErrNotAuthorized 发送方不是该会话的活跃成员。
	ErrNotAuthorized = errors.New("sender is not an active session member")
	// ErrTargetNotFound 目标成员不在会话内或已离线。
	ErrTargetNotFound = errors.New("target member not found")
	// ErrBadFrame 帧结构非法。
	ErrBadFrame = errors.New("invalid data frame")
	// ErrTooLarge 帧超出大小上限。
	ErrTooLarge = errors.New("frame too large")
)

// Membership 是 relay 需要的会话成员视图，由 session 服务实现。
//
// 之所以用窄接口而不是直接依赖 session.Service：让 relay 可被独立测试，
// 也避免 session ↔ relay 之间产生循环依赖。
type Membership interface {
	// IsActiveMember 判断 memberID 是否为该会话的活跃成员。
	IsActiveMember(sessionID, memberID string) bool
	// MemberIndex 返回成员的数据面下标。
	MemberIndex(sessionID, memberID string) (uint16, bool)
}

// Forwarder 负责数据面转发。
type Forwarder struct {
	mem  *member.Hub
	memb Membership
	// maxFrame 为单帧用户数据上限（字节，不含帧头）。
	maxFrame int
	// counters 为转发统计。
	FramesIn     atomic.Uint64
	FramesOut    atomic.Uint64
	BytesIn      atomic.Uint64
	BytesOut     atomic.Uint64
	DeniedFrames atomic.Uint64
	NoTarget     atomic.Uint64
}

// New 创建转发器。
func New(mem *member.Hub, memb Membership, maxFrame int) *Forwarder {
	if maxFrame <= 0 {
		maxFrame = protocol.DefaultMaxRelayFrame
	}
	return &Forwarder{mem: mem, memb: memb, maxFrame: maxFrame}
}

// Forward 校验并转发一帧。
//
// 入参 raw 为客户端上传的原始 DataFrame；发送方身份由调用方（已认证的连接）
// 提供，因此不可伪造。返回实际投递的接收方数量。
func (f *Forwarder) Forward(sessionID, senderMemberID string, raw []byte) (int, error) {
	if !f.memb.IsActiveMember(sessionID, senderMemberID) {
		f.DeniedFrames.Add(1)
		return 0, ErrNotAuthorized
	}
	srcIndex, ok := f.memb.MemberIndex(sessionID, senderMemberID)
	if !ok {
		f.DeniedFrames.Add(1)
		return 0, ErrNotAuthorized
	}

	fr, err := protocol.ParseFrame(raw)
	if err != nil {
		f.DeniedFrames.Add(1)
		return 0, ErrBadFrame
	}
	if len(fr.Payload) > f.maxFrame {
		f.DeniedFrames.Add(1)
		return 0, ErrTooLarge
	}
	f.FramesIn.Add(1)
	f.BytesIn.Add(uint64(len(fr.Payload)))

	// 盖写来源下标：客户端填什么都无效。
	fr.SrcIndex = srcIndex

	switch fr.DstType {
	case protocol.DstUnicast:
		return f.forwardUnicast(sessionID, senderMemberID, fr)
	case protocol.DstMulticast:
		return f.forwardMulticast(sessionID, senderMemberID, fr)
	case protocol.DstBroadcast:
		return f.forwardBroadcast(sessionID, senderMemberID, fr)
	default:
		return 0, ErrBadFrame
	}
}

func (f *Forwarder) forwardUnicast(sessionID, sender string, fr *protocol.Frame) (int, error) {
	targetID, ok := f.mem.LookupMemberByIndex(sessionID, fr.Dst[0])
	if !ok {
		f.NoTarget.Add(1)
		return 0, ErrTargetNotFound
	}
	if targetID == sender {
		// 允许自发自收在语义上无意义，且容易掩盖客户端 bug；直接拒绝。
		f.NoTarget.Add(1)
		return 0, ErrTargetNotFound
	}
	if !f.memb.IsActiveMember(sessionID, targetID) {
		f.NoTarget.Add(1)
		return 0, ErrTargetNotFound
	}
	// 单播帧重编码：SrcIndex 已盖写，目标下标保持客户端指定值。
	out, err := protocol.EncodeFrame(fr)
	if err != nil {
		return 0, ErrBadFrame
	}
	if !f.mem.Deliver(sessionID, targetID, member.Frame{Binary: out}) {
		f.NoTarget.Add(1)
		return 0, ErrTargetNotFound
	}
	f.count(len(fr.Payload))
	return 1, nil
}

func (f *Forwarder) forwardMulticast(sessionID, sender string, fr *protocol.Frame) (int, error) {
	delivered := 0
	for _, idx := range fr.Dst {
		targetID, ok := f.mem.LookupMemberByIndex(sessionID, idx)
		if !ok || targetID == sender || !f.memb.IsActiveMember(sessionID, targetID) {
			continue
		}
		// 为每个目标单独编码：帧头里的目标列表只保留该目标，
		// 这样接收方看到的永远是「一个明确的单播帧」，无需再解析列表。
		one := &protocol.Frame{
			Flags:    fr.Flags,
			SrcIndex: fr.SrcIndex,
			DstType:  protocol.DstUnicast,
			Dst:      []uint16{idx},
			StreamID: fr.StreamID,
			Seq:      fr.Seq,
			Payload:  fr.Payload,
		}
		out, err := protocol.EncodeFrame(one)
		if err != nil {
			continue
		}
		if f.mem.Deliver(sessionID, targetID, member.Frame{Binary: out}) {
			delivered++
		}
	}
	if delivered == 0 {
		f.NoTarget.Add(1)
		return 0, ErrTargetNotFound
	}
	f.count(int(uint64(len(fr.Payload)) * uint64(delivered)))
	return delivered, nil
}

func (f *Forwarder) forwardBroadcast(sessionID, sender string, fr *protocol.Frame) (int, error) {
	// 广播：目标集合为会话内全部在线成员（除发送方）。
	// 只编码一次，由 member.Hub 负责逐个投递，避免 O(N) 次编码。
	out, err := protocol.EncodeFrame(fr)
	if err != nil {
		return 0, ErrBadFrame
	}
	n := f.mem.Broadcast(sessionID, sender, member.Frame{Binary: out})
	if n == 0 {
		f.NoTarget.Add(1)
		return 0, ErrTargetNotFound
	}
	f.FramesOut.Add(1)
	f.BytesOut.Add(uint64(len(fr.Payload)) * uint64(n))
	return n, nil
}

func (f *Forwarder) count(payloadBytes int) {
	f.FramesOut.Add(1)
	f.BytesOut.Add(uint64(payloadBytes))
}
