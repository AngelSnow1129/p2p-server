package sessionclient

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"

	"p2psession/internal/protocol"
)

// ---- 读循环 ------------------------------------------------------------------

// controlReadLoop 处理控制面消息：成员发现、密钥交换、流通知、错误。
func (c *Client) controlReadLoop() {
	for {
		_ = c.ctrl.SetReadDeadline(time.Now().Add(readTimeout))
		mt, data, err := c.ctrl.ReadMessage()
		if err != nil {
			_ = c.closeWith(err)
			return
		}
		if mt != websocket.TextMessage {
			continue
		}
		var m protocol.Message
		if err := json.Unmarshal(data, &m); err != nil {
			c.h.OnError(c, protocol.ErrBadRequest, "invalid json from server: "+err.Error())
			continue
		}
		c.dispatchControl(&m)
	}
}

func (c *Client) dispatchControl(m *protocol.Message) {
	switch m.Type {
	case protocol.MsgMemberList:
		c.applyMemberList(m.Members)
		c.readyOnce.Do(func() { close(c.ready) })
		// 主动向快照中的所有对端发起密钥交换，而不是等对方先来。
		// 缺少这一步时，收敛就依赖「后加入者恰好先发起」这一时序假设；
		// 双方都主动发起才能让任意加入顺序都自愈（密钥派生是幂等的）。
		c.helloPeers()
		c.h.OnReady(c, c.self, c.Peers())

	case protocol.MsgJoined:
		// 审批通过：本端正式生效。
		if m.Member != nil {
			c.updateSelf(func(s *MemberInfo) {
				s.Status = m.Member.Status
				s.Index = m.Member.Index
			})
		}
		// 生效后才建立数据面连接（此前服务器会拒绝 pending 成员）。
		if err := c.ensureRelay(context.Background(), nil); err != nil {
			c.h.OnError(c, protocol.ErrRelayDenied, "relay connect after approval: "+err.Error())
		} else if !c.relayRunning.Swap(true) {
			go c.relayReadLoop()
		}
		c.sendMemberHello()
		c.h.OnMemberJoined(c, c.self)

	case protocol.MsgMemberJoined:
		if m.Member == nil {
			return
		}
		c.addPeer(*m.Member)
		c.h.OnMemberJoined(c, *m.Member)
		// 新成员加入：立刻做一次密钥交换，让对方也能与我通信。
		c.sendHelloTo(*m.Member)

	case protocol.MsgSessionReady:
		// 会话成员已集齐（pair 满员为典型场景）：此时客户端可开始
		// 候选交换与直连协商；本 SDK 已在此前完成密钥交换。
		c.applyMemberList(m.Members)
		c.h.OnPeerState(c, "", "session_ready")

	case protocol.MsgMemberLeft:
		c.removePeer(m.MemberID)
		c.h.OnMemberLeft(c, m.MemberID)

	case protocol.MsgKeyExchange:
		c.onKeyExchange(m)

	case protocol.MsgStreamOpen:
		c.onStreamOpen(m)

	case protocol.MsgStreamClose:
		c.onStreamClose(m)

	case protocol.MsgPeerState, protocol.MsgPeerStateNote:
		c.h.OnPeerState(c, m.From, m.State)

	case protocol.MsgCandidateOffer, protocol.MsgCandidateAnswer, protocol.MsgCandidateRelay:
		// 候选交换：第二阶段 NAT 穿透的接入点。SDK 默认只记录，
		// 由上层 Handler 决定是否发起打洞（当前阶段尚未实现 UDP）。
		c.h.OnPeerState(c, m.From, protocol.PeerStateNegotiating)

	case protocol.MsgPong:
		// 心跳应答：唤醒等待中的 Ping（非阻塞，避免无人等待时卡住读循环）。
		select {
		case c.pong <- struct{}{}:
		default:
		}

	case protocol.MsgError:
		c.h.OnError(c, m.Code, m.Message)

	default:
		// 未知消息类型不致命，仅上报。
		c.h.OnError(c, protocol.ErrUnsupported, "unexpected message: "+m.Type)
	}
}

// relayReadLoop 处理数据面二进制帧：解密后按 StreamID 分发。
func (c *Client) relayReadLoop() {
	for {
		_ = c.relay.SetReadDeadline(time.Now().Add(readTimeout))
		mt, data, err := c.relay.ReadMessage()
		if err != nil {
			_ = c.closeWith(err)
			return
		}
		if mt == websocket.TextMessage {
			// 数据面心跳应答。
			continue
		}
		if mt != websocket.BinaryMessage || len(data) == 0 {
			continue
		}
		frame, err := protocol.ParseFrame(data)
		if err != nil {
			c.h.OnError(c, protocol.ErrBadRequest, "bad data frame: "+err.Error())
			continue
		}
		c.stats.FramesReceived.Add(1)
		c.stats.BytesReceived.Add(uint64(len(frame.Payload)))
		c.onDataFrame(frame)
	}
}

// onDataFrame 解密并投递一帧业务数据。
func (c *Client) onDataFrame(f *protocol.Frame) {
	peerID := c.memberByIndex(f.SrcIndex)
	if peerID == "" {
		// 服务器改写了来源下标，理论上必然能反查；查不到说明状态不同步。
		c.h.OnError(c, protocol.ErrMemberNotFound, fmt.Sprintf("unknown source index %d", f.SrcIndex))
		return
	}

	// 控制标志帧：关闭/重置/心跳，无加密载荷。
	switch {
	case f.Flags&protocol.FlagClose != 0:
		c.onStreamClose(&protocol.Message{From: peerID, StreamID: f.StreamID})
		return
	case f.Flags&protocol.FlagReset != 0:
		if s := c.streamByID(f.StreamID); s != nil {
			s.markRemoteClosed(true)
		}
		return
	case f.Flags&protocol.FlagPing != 0:
		return
	}

	plaintext, err := c.crypto.decrypt(peerID, f.Payload)
	if err != nil {
		c.stats.DecryptErrors.Add(1)
		// 对端可能尚未完成密钥协商；静默丢弃比刷屏更合理，但仍上报一次。
		c.h.OnError(c, protocol.ErrUnauthorized, "decrypt from "+peerID+": "+err.Error())
		return
	}

	s := c.streamByID(f.StreamID)
	if s == nil {
		// 数据先于 stream_open 到达。
		//
		// 这不是异常：stream_open 走控制面 WS，数据走数据面 WS，两条连接
		// 相互独立，跨通道顺序无法保证（发送方刚 open 就 write 时尤其常见）。
		// 因此数据帧本身即可创建流——它的可信度来自两处：SrcIndex 由服务器
		// 按已认证会话盖写（发送方身份不可伪造），载荷经 AEAD 认证
		// （只有真持有成对密钥的对端才能构造）。
		s = c.registerInboundStream(peerID, f.StreamID)
		if s == nil {
			return
		}
	}
	s.deliver(f.Seq, plaintext)
}

// registerInboundStream 为来自对端的数据帧创建一个入站流（幂等）。
// 已存在时返回既有流；创建成功时通知应用。
func (c *Client) registerInboundStream(peerID string, streamID uint32) *Stream {
	c.streamsMu.Lock()
	if existing, ok := c.streams[streamID]; ok {
		c.streamsMu.Unlock()
		return existing
	}
	s := c.newStream(peerID, streamID)
	c.streams[streamID] = s
	c.streamsMu.Unlock()

	// 在锁外回调，避免应用在回调里操作流导致自死锁。
	c.h.OnStream(c, s)
	return s
}

// ---- 心跳 --------------------------------------------------------------------

func (c *Client) heartbeatLoop() {
	interval := time.Duration(c.hbInterval.Load())
	if interval <= 0 {
		interval = 15 * time.Second
	}
	// 协议要求客户端在半个周期内发一次心跳。
	tick := interval / 2
	if tick < time.Second {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
			_ = c.sendControl(&protocol.Message{Type: protocol.MsgPing, TS: time.Now().UnixMilli()})
			// 数据面同样保活（部分部署下两条连接独立计超时）。
			f, err := protocol.EncodeFrame(&protocol.Frame{
				Flags:   protocol.FlagPing,
				DstType: protocol.DstBroadcast,
			})
			if err == nil {
				c.relayMu.Lock()
				_ = c.relay.SetWriteDeadline(time.Now().Add(writeTimeout))
				_ = c.relay.WriteMessage(websocket.BinaryMessage, f)
				c.relayMu.Unlock()
			}
		}
	}
}

// ---- 发送 --------------------------------------------------------------------

// sendControl 发送一条控制消息（并发安全）。
func (c *Client) sendControl(m *protocol.Message) error {
	data, err := m.Encode()
	if err != nil {
		return err
	}
	select {
	case <-c.closed:
		return ErrClosed
	default:
	}
	c.ctrlMu.Lock()
	defer c.ctrlMu.Unlock()
	_ = c.ctrl.SetWriteDeadline(time.Now().Add(writeTimeout))
	return c.ctrl.WriteMessage(websocket.TextMessage, data)
}

// sendFrame 发送一帧数据面帧（并发安全）。
func (c *Client) sendFrame(f *protocol.Frame) error {
	data, err := protocol.EncodeFrame(f)
	if err != nil {
		return err
	}
	select {
	case <-c.closed:
		return ErrClosed
	default:
	}
	c.relayMu.Lock()
	defer c.relayMu.Unlock()
	_ = c.relay.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := c.relay.WriteMessage(websocket.BinaryMessage, data); err != nil {
		return err
	}
	c.stats.FramesSent.Add(1)
	c.stats.BytesSent.Add(uint64(len(f.Payload)))
	return nil
}

// sendStreamData 加密并发送一段流数据。
func (c *Client) sendStreamData(peerID string, streamID uint32, seq uint16, flags byte, payload []byte) error {
	enc, err := c.crypto.encrypt(peerID, payload)
	if err != nil {
		return err
	}
	idx, ok := c.indexByMember(peerID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownPeer, peerID)
	}
	// 上行帧的 SrcIndex 会被服务器盖写，这里填自己的下标即可。
	selfIdx := c.selfIndex()
	return c.sendFrame(&protocol.Frame{
		Flags:    flags,
		SrcIndex: selfIdx,
		DstType:  protocol.DstUnicast,
		Dst:      []uint16{idx},
		StreamID: streamID,
		Seq:      seq,
		Payload:  enc,
	})
}

// sendStreamClose 发送关闭/重置通知（空载荷，无加密必要）。
func (c *Client) sendStreamClose(peerID string, streamID uint32, reset bool) error {
	idx, ok := c.indexByMember(peerID)
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownPeer, peerID)
	}
	flags := protocol.FlagClose
	if reset {
		flags = protocol.FlagReset
	}
	return c.sendFrame(&protocol.Frame{
		Flags:    flags,
		SrcIndex: c.selfIndex(),
		DstType:  protocol.DstUnicast,
		Dst:      []uint16{idx},
		StreamID: streamID,
	})
}

// ---- 流管理 ------------------------------------------------------------------

// OpenStream 向对端开一条逻辑流（复用已有连接，无需新握手）。
//
// 若尚未与该对端完成密钥协商，返回 ErrNoSessionKey；调用方应等待
// Handler.OnPeerState / 稍后重试。
func (c *Client) OpenStream(peerID string) (*Stream, error) {
	if _, ok := c.Peer(peerID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownPeer, peerID)
	}
	if !c.crypto.ready(peerID) {
		return nil, fmt.Errorf("%w: %s", ErrNoSessionKey, peerID)
	}
	// 先占号再登记：allocStreamID 内部持锁并检查占用，避免并发 OpenStream 撞号。
	c.streamsMu.Lock()
	id := c.allocStreamIDLocked(peerID)
	s := c.newStream(peerID, id)
	c.streams[id] = s
	c.streamsMu.Unlock()

	// 通知对端：这里有一条新逻辑流。
	if err := c.sendControl(&protocol.Message{
		Type:     protocol.MsgStreamOpen,
		TargetID: peerID,
		StreamID: id,
	}); err != nil {
		s.once.Do(func() { close(s.closed) })
		c.dropStream(id)
		return nil, err
	}
	return s, nil
}

// allocStreamIDLocked 为「与 peerID 的一条新流」分配 ID。
// 调用方必须已持有 c.streamsMu 写锁。
//
// 奇偶由双方 MemberID 的字典序决定：小的一方用偶数段、大的一方用奇数段。
// 由于同一对成员的比较结果固定，两个方向必然落在不同奇偶段，
// 因此 A 的流 ID 与 B 的流 ID 永远不会相撞（无中心分配器的冲突规避）。
func (c *Client) allocStreamIDLocked(peerID string) uint32 {
	var start *uint32
	if c.selfID() < peerID {
		start = &c.nextEven
	} else {
		start = &c.nextOdd
	}
	if *start == 0 {
		if start == &c.nextEven {
			*start = 0
		} else {
			*start = 1
		}
	}
	// 步长 2 保证只在本段内取号；同时跳过已被占用的值（入站流可能已占用）。
	for i := 0; i < 1<<15; i++ {
		v := *start
		*start += 2
		if _, busy := c.streams[v]; !busy {
			return v
		}
	}
	// 段内耗尽（32768 条并发流）：退回线性探测，保证总能返回一个可用 ID。
	for v := uint32(0); ; v++ {
		if _, busy := c.streams[v]; !busy {
			return v
		}
	}
}

func (c *Client) streamByID(id uint32) *Stream {
	c.streamsMu.RLock()
	defer c.streamsMu.RUnlock()
	return c.streams[id]
}

// closedStreamTTL 决定「刚关闭的流 ID」在多长时间内拒绝被 stream_open 复活。
// 只需覆盖跨通道重排的窗口（毫秒~秒级），取 30s 足够宽松且不会长期占用内存。
const closedStreamTTL = 30 * time.Second

func (c *Client) dropStream(id uint32) {
	c.streamsMu.Lock()
	delete(c.streams, id)
	// 记录关闭时刻，供 onStreamOpen 拒绝迟到的 open；顺手清理过期项，
	// 避免这张表随会话时长无限增长。
	now := time.Now()
	if c.closedStreams == nil {
		c.closedStreams = make(map[uint32]time.Time)
	}
	c.closedStreams[id] = now
	for sid, at := range c.closedStreams {
		if now.Sub(at) > closedStreamTTL {
			delete(c.closedStreams, sid)
		}
	}
	c.streamsMu.Unlock()
}

// onStreamOpen 处理对端开的逻辑流。
func (c *Client) onStreamOpen(m *protocol.Message) {
	if m.From == "" {
		return
	}
	if _, ok := c.Peer(m.From); !ok {
		c.h.OnError(c, protocol.ErrMemberNotFound, "stream_open from unknown peer "+m.From)
		return
	}

	c.streamsMu.Lock()
	if _, exists := c.streams[m.StreamID]; exists {
		// 重复的 open：幂等忽略。
		c.streamsMu.Unlock()
		return
	}
	// 迟到的 open：若该流刚被关闭过，说明控制面的 stream_open 被数据面的
	// 数据/关闭帧反超了（两条 WebSocket 相互独立，跨通道顺序无保证）。
	//
	// 此时若照常创建，就会复活一条「已结束」的流——它不会再收到任何数据，
	// 也不会被应用主动关闭，等于每处理一条短消息就泄漏一条幽灵流。
	// 因此对刚关闭过的 ID 直接忽略（短消息流是「一条消息一条流」，ID 不会
	// 被立即复用；真正的复用会在 closedStreams 过期后进行）。
	if closedAt, wasClosed := c.closedStreams[m.StreamID]; wasClosed &&
		time.Since(closedAt) < closedStreamTTL {
		c.streamsMu.Unlock()
		return
	}
	s := c.newStream(m.From, m.StreamID)
	c.streams[m.StreamID] = s
	c.streamsMu.Unlock()

	c.h.OnStream(c, s)
}

// onStreamClose 处理对端的关闭/重置。
func (c *Client) onStreamClose(m *protocol.Message) {
	s := c.streamByID(m.StreamID)
	if s == nil {
		return
	}
	s.markRemoteClosed(false)
}

// ---- 成员与密钥 --------------------------------------------------------------

// addPeer 记录一个成员。
func (c *Client) addPeer(m MemberInfo) {
	c.peersMu.Lock()
	c.peers[m.MemberID] = m
	c.byIndex[m.Index] = m.MemberID
	c.peersMu.Unlock()
}

// removePeer 移除成员并清理其密钥与流。
func (c *Client) removePeer(memberID string) {
	c.peersMu.Lock()
	m, ok := c.peers[memberID]
	if ok {
		delete(c.byIndex, m.Index)
	}
	delete(c.peers, memberID)
	c.peersMu.Unlock()

	if ok {
		c.crypto.forget(memberID)
	}
	// 关闭与该对端相关的流。
	c.streamsMu.Lock()
	for id, s := range c.streams {
		if s.peerID == memberID {
			s.once.Do(func() { close(s.closed) })
			delete(c.streams, id)
		}
	}
	c.streamsMu.Unlock()
}

// applyMemberList 用服务器快照覆盖本地成员表。
func (c *Client) applyMemberList(list []MemberInfo) {
	selfID := c.selfID()
	var selfUpdate *MemberInfo

	c.peersMu.Lock()
	for _, m := range list {
		if m.MemberID == selfID {
			// 服务器视角的本节点信息更权威（如审批后状态变化）。
			// 记下来在锁外更新，避免 peersMu 与 selfMu 嵌套加锁。
			cp := m
			selfUpdate = &cp
			continue
		}
		// 只接受已生效成员：pending 成员不参与数据面。
		if m.Status != "" && m.Status != protocol.MemberActive {
			continue
		}
		c.peers[m.MemberID] = m
		c.byIndex[m.Index] = m.MemberID
	}
	c.peersMu.Unlock()

	if selfUpdate != nil {
		c.updateSelf(func(s *MemberInfo) {
			s.Status = selfUpdate.Status
			s.Index = selfUpdate.Index
		})
	}
}

// memberByIndex 由数据面下标反查成员 ID。
func (c *Client) memberByIndex(idx uint16) string {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	return c.byIndex[idx]
}

// indexByMember 由成员 ID 反查数据面下标。
func (c *Client) indexByMember(memberID string) (uint16, bool) {
	c.peersMu.RLock()
	defer c.peersMu.RUnlock()
	m, ok := c.peers[memberID]
	if !ok {
		return 0, false
	}
	return m.Index, true
}

// helloPeers 向当前已知的全部对端发起一次密钥交换。
func (c *Client) helloPeers() {
	for _, m := range c.Peers() {
		c.sendHelloTo(m)
	}
}

// sendHelloTo 向某对端发送本端 X25519 临时公钥（幂等：每个对端只发一次）。
//
// 「只发一次」不只是优化，更是正确性要求：onKeyExchange 收到对方公钥后会
// 调用本函数回报自己的公钥，若每次都无条件发送，双方将陷入
// A→B→A→B… 的密钥交换风暴（实测会把连接打满）。幂等后：
// A 先发 → B 收并回发 → A 收（此时 A 已发过，不再回）→ 收敛。
func (c *Client) sendHelloTo(m MemberInfo) {
	if m.MemberID == "" || m.MemberID == c.selfID() {
		return
	}
	c.kexMu.Lock()
	if c.helloSent[m.MemberID] {
		c.kexMu.Unlock()
		return
	}
	c.helloSent[m.MemberID] = true
	c.kexMu.Unlock()

	c.sendKeyExchange(m.MemberID, c.crypto.PublicKey())
}

// sendKeyExchange 把 X25519 公钥经控制面发给对端（服务器只透传，不理解内容）。
func (c *Client) sendKeyExchange(peerID string, x25519Pub []byte) {
	payload, err := json.Marshal(map[string]string{
		"kex":     "x25519",
		"pub":     hex.EncodeToString(x25519Pub),
		"node_id": c.selfNodeID(),
	})
	if err != nil {
		return
	}
	_ = c.sendControl(&protocol.Message{
		Type:     protocol.MsgKeyExchange,
		TargetID: peerID,
		Payload:  payload,
	})
}

// onKeyExchange 处理对端的密钥协商材料并派生密钥。
func (c *Client) onKeyExchange(m *protocol.Message) {
	if m.From == "" || len(m.Payload) == 0 {
		return
	}
	var body struct {
		Kex    string `json:"kex"`
		Pub    string `json:"pub"`
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(m.Payload, &body); err != nil {
		c.h.OnError(c, protocol.ErrBadRequest, "bad key_exchange payload")
		return
	}
	if body.Kex != "x25519" {
		c.h.OnError(c, protocol.ErrUnsupported, "unsupported kex: "+body.Kex)
		return
	}
	pub, err := hex.DecodeString(body.Pub)
	if err != nil {
		c.h.OnError(c, protocol.ErrBadRequest, "bad x25519 pubkey hex")
		return
	}
	// NodeID 优先取消息里声明的（与服务器下发的成员信息应一致）。
	nodeID := body.NodeID
	if nodeID == "" {
		if m, ok := c.Peer(m.From); ok {
			nodeID = m.NodeID
		}
	}
	if err := c.crypto.derive(m.From, nodeID, pub); err != nil {
		c.h.OnError(c, protocol.ErrUnauthorized, "key derivation failed: "+err.Error())
		return
	}
	// 收到对端公钥后回发本端公钥——但每个对端只回一次。
	//
	// 为什么不能直接复用 sendHelloTo 的幂等标记：那样会在「主动 hello 丢失」
	// 时卡死——A 已标记发过，收到 B 的 key 后不再回应，B 永远拿不到 A 的公钥。
	// 用一个独立的 replied 标记：最多回一次，既保证收敛又不产生风暴。
	c.replyHello(m.From)
	c.h.OnPeerState(c, m.From, protocol.PeerStateRelay)
}

// replyHello 首次收到某对端公钥时回发本端公钥（每对端至多一次）。
func (c *Client) replyHello(peerID string) {
	if peerID == "" || peerID == c.selfID() {
		return
	}
	c.kexMu.Lock()
	if c.helloReplied[peerID] {
		c.kexMu.Unlock()
		return
	}
	c.helloReplied[peerID] = true
	c.kexMu.Unlock()

	c.sendKeyExchange(peerID, c.crypto.PublicKey())
}

// sendMemberHello 在审批通过后重新向全房间打招呼。
func (c *Client) sendMemberHello() {
	c.helloPeers()
}

// randUint16 生成随机序号起点（降低长连接上的序号碰撞概率）。
func randUint16() uint16 {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	return binary.BigEndian.Uint16(b[:])
}
