package server

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"

	"p2psession/internal/auth"
	"p2psession/internal/member"
	"p2psession/internal/protocol"
	"p2psession/internal/relay"
)

// storeCtx 返回用于 WS 回调内持久化操作的短超时上下文。
//
// 调用方必须 defer cancel()：读循环不能无限期阻塞在存储操作上，
// 但也不能为每条消息遗留一个等待超时的 goroutine。
func storeCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}

// ---- 连接骨架 --------------------------------------------------------------

// wsSession 是一条已鉴权 WebSocket 的运行时状态。
type wsSession struct {
	srv    *Server
	conn   *websocket.Conn
	ticket *auth.Ticket
	kind   string // control | relay

	mc *member.Conn

	msgLimiter *rate.Limiter
	relayBW    *rate.Limiter

	closeReason string
}

// upgrade 校验票据并升级连接。
func (s *Server) upgrade(w http.ResponseWriter, r *http.Request, kind string) (*websocket.Conn, *auth.Ticket, bool) {
	t, err := s.ticketFrom(r)
	if err != nil {
		s.metrics.AuthFailures.Add(1)
		status := http.StatusUnauthorized
		if errors.Is(err, auth.ErrTicketExpired) {
			status = http.StatusGone
		}
		writeErr(w, status, protocol.ErrUnauthorized, "invalid or expired ticket")
		return nil, nil, false
	}
	if kind == member.KindRelay && !s.cfg.RelayEnabled {
		writeErr(w, http.StatusServiceUnavailable, protocol.ErrRelayDenied, "relay disabled")
		return nil, nil, false
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Debug("ws upgrade failed", "kind", kind, "err", err)
		return nil, nil, false
	}
	return conn, t, true
}

// ---- 控制面 /ws/control ----------------------------------------------------

func (s *Server) handleControlWS(w http.ResponseWriter, r *http.Request) {
	conn, t, ok := s.upgrade(w, r, member.KindControl)
	if !ok {
		return
	}
	s.metrics.ControlConnects.Add(1)

	conn.SetReadLimit(s.cfg.MaxControlFrame)

	// 读取成员记录：拿到数据面下标与审批状态。
	mrec, err := s.svc.Member(r.Context(), t.SessionID, t.MemberID)
	if err != nil {
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(protocol.CloseSessionGone, "session or member gone"),
			time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}
	approved := mrec.Status == protocol.MemberActive

	mc, replaced := s.hub.Attach(t.SessionID, t.MemberID, t.ConnectionID, member.KindControl, mrec.Index, approved)
	if replaced != nil {
		s.log.Info("control connection replaced", "session", t.SessionID, "member", t.MemberID)
	}

	ws := &wsSession{
		srv:        s,
		conn:       conn,
		ticket:     t,
		kind:       member.KindControl,
		mc:         mc,
		msgLimiter: newLimiter(s.cfg.MsgRatePerSec, s.cfg.MsgRateBurst),
	}
	if p := s.cfg.RelayBytesPerSec; p > 0 {
		ws.relayBW = rate.NewLimiter(rate.Limit(p), p)
	}
	ws.serveControl()
}

func (ws *wsSession) serveControl() {
	s := ws.srv
	defer func() {
		s.hub.Detach(ws.mc)
		s.metrics.ControlDisconnects.Add(1)
		_ = ws.conn.Close()
		s.log.Debug("control closed", "member", ws.ticket.MemberID, "reason", ws.closeReason)
	}()

	done := make(chan struct{})
	defer close(done)
	go ws.writePump(done)

	// 首帧：把当前成员列表与对端信息推给新连接，实现「自动发现」。
	ws.sendMemberList()

	_ = ws.conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
	for {
		mt, data, err := ws.conn.ReadMessage()
		if err != nil {
			ws.noteReadErr(err)
			return
		}
		_ = ws.conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		if mt != websocket.TextMessage {
			// 控制面只接受 JSON 文本帧；二进制帧属于数据面。
			continue
		}
		if !ws.msgLimiter.Allow() {
			s.metrics.RateLimited.Add(1)
			ws.fail(protocol.ClosePolicy, protocol.ErrRateLimited, "control message rate exceeded")
			return
		}
		msg, derr := protocol.Decode(data)
		if derr != nil {
			ws.sendError(protocol.ErrBadRequest, "invalid json: "+derr.Error())
			continue
		}
		if !ws.handleControlMsg(msg) {
			return
		}
	}
}

// handleControlMsg 返回 false 表示连接应终止。
func (ws *wsSession) handleControlMsg(m *protocol.Message) bool {
	s := ws.srv
	ctx, cancel := storeCtx()
	defer cancel()

	// 心跳与任何消息都刷新在线时间。
	ws.mc.Touch()

	switch m.Type {
	case protocol.MsgPing:
		ws.send(&protocol.Message{Type: protocol.MsgPong, TS: m.TS})
		// 顺带刷新持久化的 LastSeen（限速由 TrimInterval 隐式控制）。
		_ = s.svc.Touch(ctx, ws.ticket.SessionID, ws.ticket.MemberID, ws.ticket.ConnectionID)
		return true

	case protocol.MsgLeave:
		_ = s.svc.Leave(ctx, ws.ticket.SessionID, ws.ticket.MemberID)
		s.hub.CloseMember(ws.ticket.SessionID, ws.ticket.MemberID, "left")
		s.hub.BroadcastControl(ws.ticket.SessionID, ws.ticket.MemberID, &protocol.Message{
			Type:     protocol.MsgMemberLeft,
			MemberID: ws.ticket.MemberID,
		})
		ws.fail(websocket.CloseNormalClosure, "bye", "left session")
		return false

	case protocol.MsgCandidateOffer, protocol.MsgCandidateAnswer:
		// 候选交换：服务器只做校验与透传，不解释语义（NAT 穿透在客户端之间完成）。
		if m.TargetID == "" {
			ws.sendError(protocol.ErrBadRequest, "target_id required")
			return true
		}
		if len(m.Candidates) > s.cfg.MaxCandidates {
			ws.sendError(protocol.ErrPayloadTooLarge, "too many candidates")
			return true
		}
		if !ws.forwardToPeer(m) {
			return true
		}
		// 上报候选的同时持久化自己的候选（便于新成员加入时直接看到）。
		_ = s.svc.UpdateCandidates(ctx, ws.ticket.SessionID, ws.ticket.MemberID, m.Candidates)
		return true

	case protocol.MsgPunchResult, protocol.MsgPeerState,
		protocol.MsgKeyExchange, protocol.MsgStreamOpen, protocol.MsgStreamClose:
		// 打洞结果、连接状态、端到端密钥协商材料与逻辑流通知：
		// 服务器一律只做「校验成员关系 + 透传」，不解析内容——
		// 密钥材料对服务器不可理解，业务数据也不经这里。
		if m.TargetID == "" {
			ws.sendError(protocol.ErrBadRequest, "target_id required")
			return true
		}
		ws.forwardToPeer(m)
		return true

	default:
		ws.sendError(protocol.ErrUnsupported, "unsupported message type: "+m.Type)
		return true
	}
}

// forwardToPeer 把消息带上真实来源后透传给目标成员。
//
// 两类「投递不到」被刻意区别对待：
//
//   - 目标根本不是本会话成员 → 上报错误（调用方的用法确实错了）；
//   - 目标是成员但当前没有控制面连接 → **静默丢弃**。
//
// 后者是正常现象而非故障：服务器在 POST /join 成功时立即广播 member_joined，
// 而新成员此时还没建立 WebSocket（客户端拿到响应后才会连两条 WS）。这个
// 窗口内先加入者发出的 key_exchange 必然落空——但双方各自都会主动发起
// 密钥交换（helloSent / helloReplied 双幂等标记），因此消息丢失可自愈。
// 若把这种瞬时不可达当作错误上报，用户会在正常配对时看到大量误导性的
// "target member is offline"。
func (ws *wsSession) forwardToPeer(m *protocol.Message) bool {
	out := *m
	out.From = ws.ticket.MemberID
	out.SessionID = ws.ticket.SessionID

	if _, ok := ws.srv.svc.MemberIndexFor(ws.ticket.SessionID, m.TargetID); !ok {
		ws.sendError(protocol.ErrMemberNotFound, "target member not found")
		return false
	}
	if !ws.srv.hub.DeliverControl(ws.ticket.SessionID, m.TargetID, &out) {
		ws.srv.log.Debug("signaling dropped: peer has no control connection yet",
			"session", ws.ticket.SessionID,
			"from", ws.ticket.MemberID,
			"to", m.TargetID,
			"type", m.Type)
		return false
	}
	return true
}

// sendMemberList 推送当前会话成员快照（自动发现的入口）。
func (ws *wsSession) sendMemberList() {
	s := ws.srv
	ctx, cancel := storeCtx()
	defer cancel()
	members, err := s.svc.Members(ctx, ws.ticket.SessionID)
	if err != nil {
		return
	}
	out := make([]protocol.MemberInfo, 0, len(members))
	for _, m := range members {
		out = append(out, memberInfo(m, s.hub))
	}
	ws.send(&protocol.Message{
		Type:      protocol.MsgMemberList,
		SessionID: ws.ticket.SessionID,
		Members:   out,
	})
}

// ---- 数据面 /ws/relay ------------------------------------------------------

func (s *Server) handleRelayWS(w http.ResponseWriter, r *http.Request) {
	conn, t, ok := s.upgrade(w, r, member.KindRelay)
	if !ok {
		return
	}
	conn.SetReadLimit(int64(s.cfg.MaxRelayFrame) + 512)

	mrec, err := s.svc.Member(r.Context(), t.SessionID, t.MemberID)
	if err != nil {
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(protocol.CloseSessionGone, "member gone"),
			time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}
	// 只有已生效（已审批）成员才允许挂载数据面。
	if mrec.Status != protocol.MemberActive {
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(protocol.ClosePolicy, protocol.ErrPendingApproval),
			time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}

	mc, _ := s.hub.Attach(t.SessionID, t.MemberID, t.ConnectionID, member.KindRelay, mrec.Index, true)

	ws := &wsSession{
		srv:    s,
		conn:   conn,
		ticket: t,
		kind:   member.KindRelay,
		mc:     mc,
	}
	ws.serveRelay()
}

func (ws *wsSession) serveRelay() {
	s := ws.srv
	defer func() {
		s.hub.Detach(ws.mc)
		_ = ws.conn.Close()
		s.log.Debug("relay closed", "member", ws.ticket.MemberID, "reason", ws.closeReason)
	}()

	done := make(chan struct{})
	defer close(done)
	go ws.writePump(done)

	_ = ws.conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
	for {
		mt, data, err := ws.conn.ReadMessage()
		if err != nil {
			ws.noteReadErr(err)
			return
		}
		_ = ws.conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		ws.mc.Touch()

		if mt == websocket.TextMessage {
			// 数据面容忍心跳文本帧（部分客户端只连一条 WS）。
			if msg, derr := protocol.Decode(data); derr == nil && msg.Type == protocol.MsgPing {
				ws.send(&protocol.Message{Type: protocol.MsgPong, TS: msg.TS})
			}
			continue
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		if len(data) == 0 {
			continue
		}
		if s.relay == nil {
			ws.sendError(protocol.ErrRelayDenied, "relay disabled")
			continue
		}
		n, ferr := s.relay.Forward(ws.ticket.SessionID, ws.ticket.MemberID, data)
		if ferr != nil {
			s.metrics.RelayDenied.Add(1)
			code := protocol.ErrRelayDenied
			if errors.Is(ferr, relay.ErrTargetNotFound) {
				code = protocol.ErrMemberNotFound
			}
			ws.sendError(code, ferr.Error())
			continue
		}
		s.metrics.RelayFramesIn.Add(1)
		s.metrics.RelayFramesOut.Add(uint64(n))
	}
}

// ---- 写泵与关闭 ------------------------------------------------------------

func (ws *wsSession) writePump(done <-chan struct{}) {
	var peerDone <-chan struct{}
	if ws.mc != nil {
		peerDone = ws.mc.Done()
	}
	for {
		select {
		case <-done:
			return
		case <-peerDone:
			code := protocol.CloseDuplicate
			reason := ws.mc.KickReason()
			switch reason {
			case "slow consumer":
				code = protocol.ClosePolicy
			case "left", "bye":
				code = websocket.CloseNormalClosure
			}
			if reason == "" {
				reason = "closed"
			}
			_ = ws.conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(code, truncate(reason, 100)),
				time.Now().Add(time.Second))
			return
		case f := <-ws.mc.Recv():
			switch {
			case f.Text != nil:
				if err := ws.conn.WriteMessage(websocket.TextMessage, f.Text); err != nil {
					return
				}
			case f.Binary != nil:
				if err := ws.conn.WriteMessage(websocket.BinaryMessage, f.Binary); err != nil {
					return
				}
			}
		}
	}
}

func (ws *wsSession) send(m *protocol.Message) {
	if ws.mc == nil {
		data, err := m.Encode()
		if err != nil {
			return
		}
		_ = ws.conn.WriteMessage(websocket.TextMessage, data)
		return
	}
	_ = ws.mc.SendJSON(m)
}

func (ws *wsSession) sendError(code, msg string) {
	ws.send(protocol.NewError(code, msg))
}

func (ws *wsSession) fail(closeCode int, code, msg string) {
	ws.closeReason = code + ": " + msg
	if code != "" {
		ws.sendError(code, msg)
	}
	_ = ws.conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(closeCode, truncate(msg, 100)),
		time.Now().Add(2*time.Second))
}

func (ws *wsSession) noteReadErr(err error) {
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		ws.closeReason = "peer close " + ce.Text
		return
	}
	ws.closeReason = truncate(err.Error(), 120)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// hexOrEmpty 便捷 hex 编码（保留空串语义）。
func hexOrEmpty(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}
