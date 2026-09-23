// Package member 维护「在线连接」这一运行时状态，是控制面事件的分发中心。
//
// 与 storage/session 的分工：
//   - storage/session 管持久状态（会话、成员、token 策略）；
//   - 本包管易失状态（哪条连接在线、往哪写帧、谁该收到事件）。
//
// 关键机制：
//   - 每会话一个 hub，成员可有多条连接（多设备/重连），但只有最新的那条
//     被标记为「当前连接」，旧连接收到 duplicate 关闭（避免自己和自己建流）；
//   - 每个连接一条带缓冲出站队列，队列满即判定慢消费者并踢出，
//     防止单个卡住的客户端拖垮整个会话的事件分发；
//   - 成员离线进入宽限期，MemberID/Index 保留，重连可恢复原身份。
package member

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"p2psession/internal/protocol"
)

// 出站队列参数。
const (
	defaultSendQueue = 256
	// slowDropLimit 为累计丢帧阈值，达到即踢出（慢消费者保护）。
	slowDropLimit = 64
)

// Frame 是要写入某条连接的一帧；Text/Binary 二选一。
type Frame struct {
	Text   []byte
	Binary []byte
}

// Conn 是一条控制面或数据面连接的运行时句柄。
type Conn struct {
	// ID 是连接标识（con_…），与票据中的 ConnectionID 对应。
	ID string
	// SessionID / MemberID 为该连接绑定的身份。
	SessionID string
	MemberID  string
	// Kind 为 control / relay，用于区分两条 WebSocket。
	Kind string

	out    chan Frame
	closed chan struct{}
	once   sync.Once
	// kickReason 记录被踢原因（空表示未被踢）。
	kickReason atomic.Value
	dropped    atomic.Uint64
	lastSeen   atomic.Int64
}

// 连接类型。
const (
	KindControl = "control"
	KindRelay   = "relay"
)

func newConn(id, sessionID, memberID, kind string) *Conn {
	c := &Conn{
		ID:        id,
		SessionID: sessionID,
		MemberID:  memberID,
		Kind:      kind,
		out:       make(chan Frame, defaultSendQueue),
		closed:    make(chan struct{}),
	}
	c.kickReason.Store("")
	c.lastSeen.Store(time.Now().UnixNano())
	return c
}

// Done 在连接被关闭（顶替/慢消费者/会话结束）时关闭。
func (c *Conn) Done() <-chan struct{} { return c.closed }

// Recv 返回出站队列，由该连接的写泵独占消费。
func (c *Conn) Recv() <-chan Frame { return c.out }

// KickReason 返回被踢原因。
func (c *Conn) KickReason() string {
	if v, ok := c.kickReason.Load().(string); ok {
		return v
	}
	return ""
}

// Dropped 返回累计丢弃帧数。
func (c *Conn) Dropped() uint64 { return c.dropped.Load() }

// LastSeen 返回最近活动时间。
func (c *Conn) LastSeen() time.Time { return time.Unix(0, c.lastSeen.Load()) }

// Touch 刷新活动时间。
func (c *Conn) Touch() { c.lastSeen.Store(time.Now().UnixNano()) }

// Send 非阻塞投递一帧；队列满时计丢帧，超限踢出该连接。
func (c *Conn) Send(f Frame) bool {
	select {
	case c.out <- f:
		return true
	default:
		if c.dropped.Add(1) >= slowDropLimit {
			c.kick("slow consumer")
		}
		return false
	}
}

// SendJSON 序列化并发送一条控制消息。
func (c *Conn) SendJSON(m *protocol.Message) bool {
	data, err := m.Encode()
	if err != nil {
		return false
	}
	return c.Send(Frame{Text: data})
}

// kick 关闭连接并记录原因（幂等）。
func (c *Conn) kick(reason string) {
	c.kickReason.Store(reason)
	c.once.Do(func() { close(c.closed) })
}

// ---- Hub -------------------------------------------------------------------

// onlineMember 是某成员当前的在线视图。
type onlineMember struct {
	MemberID string
	// control/relay 为当前有效连接（可能为空：只连了其中一条）。
	control *Conn
	relay   *Conn
	// connID 为最近一次加入时生成的连接 ID。
	connID string
	// index 为数据面下标（转发时用于构造帧头）。
	index uint16
	// approved 表示该成员已通过审批（Session 启用审批时，pending 成员
	// 可以连控制面等待结果，但不得进入数据面，也不得出现在他人成员列表里）。
	approved bool
}

// Room 是单会话的在线连接集合。
type Room struct {
	sessionID string
	mu        sync.RWMutex
	members   map[string]*onlineMember
	// byIndex 用于数据面按 16 位下标快速定位成员。
	byIndex map[uint16]*onlineMember
}

// Hub 管理全部会话的在线状态。
type Hub struct {
	mu    sync.RWMutex
	rooms map[string]*Room
	// hooks 为事件回调（server 层用来触发持久化与指标）。
	onOffline func(sessionID, memberID, connID string)
}

// NewHub 创建在线连接中心。
func NewHub() *Hub {
	return &Hub{rooms: make(map[string]*Room)}
}

// SetOfflineHook 注册「成员所有连接都断开」时的回调。
// 回调在锁外执行，可安全地做 IO。
func (h *Hub) SetOfflineHook(fn func(sessionID, memberID, connID string)) {
	h.mu.Lock()
	h.onOffline = fn
	h.mu.Unlock()
}

func (h *Hub) room(sessionID string) *Room {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r != nil {
		return r
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if r := h.rooms[sessionID]; r != nil {
		return r
	}
	r = &Room{
		sessionID: sessionID,
		members:   make(map[string]*onlineMember),
		byIndex:   make(map[uint16]*onlineMember),
	}
	h.rooms[sessionID] = r
	return r
}

// Attach 挂载一条新连接；返回该连接句柄与被顶替的旧连接（可能为空）。
//
// index 为该成员的数据面下标（由会话成员记录提供），用于数据面寻址。
// approved 表示该成员当前已通过审批（未审批者仍在房间里等待，但不参与数据面）。
func (h *Hub) Attach(sessionID, memberID, connID, kind string, index uint16, approved bool) (*Conn, *Conn) {
	r := h.room(sessionID)
	c := newConn(connID, sessionID, memberID, kind)

	r.mu.Lock()
	om := r.members[memberID]
	if om == nil {
		om = &onlineMember{MemberID: memberID}
		r.members[memberID] = om
	}
	om.connID = connID
	om.index = index
	// 新连接上报的审批状态更权威（控制面重连时会重新判定）。
	om.approved = approved
	r.byIndex[index] = om

	var replaced *Conn
	switch kind {
	case KindControl:
		replaced = om.control
		om.control = c
	case KindRelay:
		replaced = om.relay
		om.relay = c
	}
	r.mu.Unlock()

	if replaced != nil {
		// 顶替旧连接：它会在写泵里收到 duplicate 关闭。
		replaced.kick("duplicate connection")
	}
	return c, replaced
}

// Detach 注销一条连接；若该成员已无任何连接，触发离线回调。
func (h *Hub) Detach(c *Conn) {
	if c == nil {
		return
	}
	h.mu.RLock()
	r := h.rooms[c.SessionID]
	h.mu.RUnlock()
	if r == nil {
		return
	}

	offline := false
	empty := false

	// 注意：sync.RWMutex 不可重入，因此锁内只读取状态、不再次加锁
	// （早期版本在持写锁时又调 r.mu.RLock 做二次确认，会自死锁）。
	r.mu.Lock()
	om := r.members[c.MemberID]
	if om != nil {
		// 只有当前连接才允许清空，避免旧连接的 Detach 误伤新连接。
		switch c.Kind {
		case KindControl:
			if om.control == c {
				om.control = nil
			}
		case KindRelay:
			if om.relay == c {
				om.relay = nil
			}
		}
		if om.control == nil && om.relay == nil {
			delete(r.byIndex, om.index)
			delete(r.members, c.MemberID)
			offline = true
		}
	}
	empty = len(r.members) == 0
	r.mu.Unlock()

	// 空房间回收：先摘索引（占住 h.mu），再在 r.mu 下确认仍为空，避免误删。
	if empty {
		h.mu.Lock()
		if cur, ok := h.rooms[c.SessionID]; ok && cur == r {
			r.mu.RLock()
			stillEmpty := len(r.members) == 0
			r.mu.RUnlock()
			if stillEmpty {
				delete(h.rooms, c.SessionID)
			}
		}
		h.mu.Unlock()
	}

	if offline {
		h.mu.RLock()
		hk := h.onOffline
		h.mu.RUnlock()
		if hk != nil {
			hk(c.SessionID, c.MemberID, c.ID)
		}
	}
}

// Deliver 向某成员投递一帧（control 与 relay 都投；relay 优先用于业务数据）。
func (h *Hub) Deliver(sessionID, memberID string, f Frame) bool {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return false
	}
	r.mu.RLock()
	om := r.members[memberID]
	r.mu.RUnlock()
	if om == nil {
		return false
	}
	ok := false
	// 数据帧按需投递：文本帧给控制面，二进制帧给数据面；
	// 若对应连接不存在则退回另一条，避免因只连一条 WS 而丢事件。
	if f.Binary != nil {
		if om.relay != nil {
			ok = om.relay.Send(f)
		} else if om.control != nil {
			ok = om.control.Send(f)
		}
		return ok
	}
	if om.control != nil {
		ok = om.control.Send(f)
	} else if om.relay != nil {
		ok = om.relay.Send(f)
	}
	return ok
}

// DeliverControl 向某成员的控制面投递一条 JSON 消息。
func (h *Hub) DeliverControl(sessionID, memberID string, m *protocol.Message) bool {
	data, err := m.Encode()
	if err != nil {
		return false
	}
	return h.Deliver(sessionID, memberID, Frame{Text: data})
}

// onlineView 是 onlineMember 的只读快照。
//
// 遍历时必须以值传递：如果直接把 *onlineMember 交给回调，回调会在房间锁之外
// 读取 approved/control/relay，而 Attach/Detach 会在写锁下改写这些字段——
// 这是 -race 会抓到的真实竞争。Conn 指针本身在构造后不再变化，可安全共享。
type onlineView struct {
	MemberID string
	control  *Conn
	relay    *Conn
	approved bool
	index    uint16
}

// Broadcast 向会话内除 exclude 外的所有已审批在线成员投递。
// 未通过审批的成员不接收任何数据面或广播流量。
func (h *Hub) Broadcast(sessionID, excludeMemberID string, f Frame) int {
	return h.each(sessionID, func(v onlineView) bool {
		if v.MemberID == excludeMemberID || !v.approved {
			return false
		}
		if f.Binary != nil {
			if v.relay != nil {
				v.relay.Send(f)
			} else if v.control != nil {
				v.control.Send(f)
			}
		} else if v.control != nil {
			v.control.Send(f)
		}
		return true
	})
}

// BroadcastControl 向会话内（可排除自己）所有成员投递控制消息。
func (h *Hub) BroadcastControl(sessionID, excludeMemberID string, m *protocol.Message) int {
	data, err := m.Encode()
	if err != nil {
		return 0
	}
	return h.Broadcast(sessionID, excludeMemberID, Frame{Text: data})
}

// each 在房间读锁内构造成员快照，然后在锁外回调（回调会做非阻塞投递，
// 不应在持有房间锁时执行，也不应再取房间锁）。
func (h *Hub) each(sessionID string, fn func(onlineView) bool) int {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return 0
	}
	r.mu.RLock()
	snapshot := make([]onlineView, 0, len(r.members))
	for _, om := range r.members {
		snapshot = append(snapshot, om.view())
	}
	r.mu.RUnlock()
	n := 0
	for _, v := range snapshot {
		if fn(v) {
			n++
		}
	}
	return n
}

// view 返回只读快照（调用方须持有房间锁）。
func (om *onlineMember) view() onlineView {
	return onlineView{
		MemberID: om.MemberID,
		control:  om.control,
		relay:    om.relay,
		approved: om.approved,
		index:    om.index,
	}
}

// LookupIndex 返回成员的数据面下标。
func (h *Hub) LookupIndex(sessionID, memberID string) (uint16, bool) {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return 0, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	om := r.members[memberID]
	if om == nil || !om.approved {
		return 0, false
	}
	return om.index, true
}

// LookupMemberByIndex 返回数据面下标对应的成员 ID（仅已审批成员）。
func (h *Hub) LookupMemberByIndex(sessionID string, index uint16) (string, bool) {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	om := r.byIndex[index]
	if om == nil || !om.approved {
		return "", false
	}
	return om.MemberID, true
}

// SetApproved 变更成员的审批状态（owner 批准/拒绝后由 server 层调用）。
// 返回 false 表示该成员当前无在线连接（无法即时变更，下次 Attach 时会带上新状态）。
func (h *Hub) SetApproved(sessionID, memberID string, approved bool) bool {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	om := r.members[memberID]
	if om == nil {
		return false
	}
	om.approved = approved
	return true
}

// IsApproved 返回成员是否已通过审批且在线。
func (h *Hub) IsApproved(sessionID, memberID string) bool {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	om := r.members[memberID]
	return om != nil && om.approved
}

// HasMember 返回该成员当前是否有在线连接。
func (h *Hub) HasMember(sessionID, memberID string) bool {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.members[memberID] != nil
}

// CloseMember 关闭某成员的全部连接（被移除/拒绝/离开时调用）。
func (h *Hub) CloseMember(sessionID, memberID, reason string) {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return
	}
	r.mu.RLock()
	om := r.members[memberID]
	conns := make([]*Conn, 0, 2)
	if om != nil {
		if om.control != nil {
			conns = append(conns, om.control)
		}
		if om.relay != nil {
			conns = append(conns, om.relay)
		}
	}
	r.mu.RUnlock()
	for _, c := range conns {
		c.kick(reason)
	}
}

// MakeApproved 把成员从等待区移动到数据面（批准时调用），
// 与 SetApproved 的区别是把 index 重新登记进 byIndex 索引。
func (h *Hub) MakeApproved(sessionID, memberID string, index uint16) bool {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	om := r.members[memberID]
	if om == nil {
		return false
	}
	// 释放旧下标（pending 期间不占索引）后再登记新下标。
	if old, ok := r.byIndex[om.index]; ok && old == om && om.index != index {
		delete(r.byIndex, om.index)
	}
	om.index = index
	om.approved = true
	r.byIndex[index] = om
	return true
}

// OnlineMembers 返回会话内在线成员 ID 列表。
func (h *Hub) OnlineMembers(sessionID string) []string {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.members))
	for id := range r.members {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// OnlineCount 返回会话内在线成员数。
func (h *Hub) OnlineCount(sessionID string) int {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members)
}

// Stats 汇总在线状态（指标用）。
type Stats struct {
	Rooms         int
	OnlineMembers int
	OnlineConns   int
	DroppedFrames uint64
}

// Stats 返回在线状态快照。
func (h *Hub) Stats() Stats {
	h.mu.RLock()
	rooms := make([]*Room, 0, len(h.rooms))
	for _, r := range h.rooms {
		rooms = append(rooms, r)
	}
	h.mu.RUnlock()

	var st Stats
	st.Rooms = len(rooms)
	for _, r := range rooms {
		r.mu.RLock()
		for _, om := range r.members {
			st.OnlineMembers++
			if om.control != nil {
				st.OnlineConns++
				st.DroppedFrames += om.control.Dropped()
			}
			if om.relay != nil {
				st.OnlineConns++
				st.DroppedFrames += om.relay.Dropped()
			}
		}
		r.mu.RUnlock()
	}
	return st
}

// CloseSession 关闭会话内全部连接（会话关闭/删除时调用）。
func (h *Hub) CloseSession(sessionID, reason string) {
	h.mu.RLock()
	r := h.rooms[sessionID]
	h.mu.RUnlock()
	if r == nil {
		return
	}
	r.mu.RLock()
	conns := make([]*Conn, 0, len(r.members)*2)
	for _, om := range r.members {
		if om.control != nil {
			conns = append(conns, om.control)
		}
		if om.relay != nil {
			conns = append(conns, om.relay)
		}
	}
	r.mu.RUnlock()
	for _, c := range conns {
		c.kick(reason)
	}
}

// CloseAll 关闭全部会话的全部连接（进程优雅退出时调用）。
// 返回被关闭的连接数；连接会收到带 reason 的 Close 帧而非静默断开，
// 从而让客户端能区分「服务端下线」与「网络故障」并据此重连。
func (h *Hub) CloseAll(reason string) int {
	h.mu.RLock()
	rooms := make([]*Room, 0, len(h.rooms))
	for _, r := range h.rooms {
		rooms = append(rooms, r)
	}
	h.mu.RUnlock()

	total := 0
	for _, r := range rooms {
		r.mu.RLock()
		conns := make([]*Conn, 0, len(r.members)*2)
		for _, om := range r.members {
			if om.control != nil {
				conns = append(conns, om.control)
			}
			if om.relay != nil {
				conns = append(conns, om.relay)
			}
		}
		r.mu.RUnlock()
		for _, c := range conns {
			c.kick(reason)
			total++
		}
	}
	return total
}

// ErrNotFound 成员或会话不存在。
var ErrNotFound = errors.New("member not found")

// UnmarshalMessage 便捷反序列化（供回调使用）。
func UnmarshalMessage(data []byte) (*protocol.Message, error) {
	var m protocol.Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
