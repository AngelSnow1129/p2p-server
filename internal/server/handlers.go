package server

import (
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"time"

	"p2psession/internal/auth"
	"p2psession/internal/member"
	"p2psession/internal/protocol"
	"p2psession/internal/session"
	"p2psession/internal/version"
)

// ---- 请求/响应 DTO ---------------------------------------------------------

type createSessionReq struct {
	Mode string `json:"mode"` // pair | group
	// MaxMembers 为成员上限（pair 忽略，恒为 2）。
	MaxMembers int `json:"max_members"`
	// JoinLimit 为最多加入次数（0 = 不限）。
	JoinLimit int `json:"join_limit"`
	// TTL 为会话存活时长（如 "30m"）。
	TTL string `json:"ttl"`
	// RequireApproval 为 true 时新成员需 owner 批准。
	RequireApproval bool `json:"require_approval"`
	// IdleTimeout 为全员离线后自动过期（如 "10m"）。
	IdleTimeout string `json:"idle_timeout"`
}

type createSessionResp struct {
	SessionID string `json:"session_id"`
	// JoinToken 为机器形式（--token 使用），仅此一次可见。
	JoinToken string `json:"join_token"`
	// JoinCode 为人类可传形式（P2P-XXXX-…）。
	JoinCode        string `json:"join_code"`
	Mode            string `json:"mode"`
	MaxMembers      int    `json:"max_members"`
	JoinLimit       int    `json:"join_limit"`
	RequireApproval bool   `json:"require_approval"`
	ExpiresAt       string `json:"expires_at"`
	CreatedAt       string `json:"created_at"`
	// 便捷字段：how-to 提示。
	JoinHint string `json:"join_hint"`
}

type joinReq struct {
	SessionID string `json:"session_id"`
	JoinCode  string `json:"join_code"`
	JoinToken string `json:"join_token"`

	NodeID    string `json:"node_id"`
	PublicKey string `json:"public_key"` // hex
	Nonce     string `json:"nonce"`      // hex
	Signature string `json:"signature"`  // hex

	Capabilities []string             `json:"capabilities"`
	Candidates   []protocol.Candidate `json:"candidates"`
}

type joinResp struct {
	SessionID string `json:"session_id"`
	MemberID  string `json:"member_id"`
	Index     uint16 `json:"index"`
	IsOwner   bool   `json:"is_owner"`
	Status    string `json:"status"` // active | pending
	Rejoined  bool   `json:"rejoined"`
	// Ticket 用于 /ws/control 与 /ws/relay 升级。
	Ticket string `json:"ticket"`
	// Members 为已生效的其它成员，客户端据此自动发现对端。
	Members []protocol.MemberInfo `json:"members"`
	// Mode 为 pair / group。
	Mode string `json:"mode"`
	// HeartbeatMs 为建议心跳间隔。
	HeartbeatMs int `json:"heartbeat_ms"`
	// ControlURL / RelayURL 为可以直接使用的 WebSocket 相对路径。
	ControlURL string `json:"control_url"`
	RelayURL   string `json:"relay_url"`
}

type memberActionReq struct {
	MemberID string `json:"member_id"`
	// ActorMemberID 为发起方成员 ID（owner 校验用）。
	ActorMemberID string `json:"actor_member_id"`
	// ActorTicket 也可用于证明操作者身份（优先于 ActorMemberID）。
	ActorTicket string `json:"actor_ticket"`
}

// ---- 创建 ------------------------------------------------------------------

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	ip := s.clientIP(r)
	if !s.ipCreate.allow(ip) {
		s.metrics.RateLimited.Add(1)
		writeErr(w, http.StatusTooManyRequests, protocol.ErrRateLimited, "too many sessions from this IP")
		return
	}

	var req createSessionReq
	if !s.decodeJSON(w, r, &req) {
		return
	}

	ttl := time.Duration(0)
	if req.TTL != "" {
		d, err := time.ParseDuration(req.TTL)
		if err != nil {
			writeErr(w, http.StatusBadRequest, protocol.ErrBadRequest, "invalid ttl: "+err.Error())
			return
		}
		ttl = d
	}
	if req.MaxMembers == 0 {
		// 未指定时按模式取默认：pair=2，group=10。
		if req.Mode == protocol.ModeGroup {
			req.MaxMembers = session.DefaultGroupMembers
		}
	}
	idle := time.Duration(0)
	if req.IdleTimeout != "" {
		d, err := time.ParseDuration(req.IdleTimeout)
		if err != nil {
			writeErr(w, http.StatusBadRequest, protocol.ErrBadRequest, "invalid idle_timeout: "+err.Error())
			return
		}
		idle = d
	}

	res, err := s.svc.Create(r.Context(), session.CreateParams{
		Mode:            req.Mode,
		MaxMembers:      req.MaxMembers,
		JoinLimit:       req.JoinLimit,
		TTL:             ttl,
		RequireApproval: req.RequireApproval,
		IdleTimeout:     idle,
	})
	if err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	s.metrics.SessionsCreated.Add(1)

	writeJSON(w, http.StatusCreated, createSessionResp{
		SessionID:       res.SessionID,
		JoinToken:       res.Token.TokenString(),
		JoinCode:        res.JoinCode,
		Mode:            res.Session.Mode,
		MaxMembers:      res.Session.MaxMembers,
		JoinLimit:       res.Session.JoinLimit,
		RequireApproval: res.Session.RequireApproval,
		ExpiresAt:       res.ExpiresAt.UTC().Format(time.RFC3339),
		CreatedAt:       res.Session.CreatedAt.UTC().Format(time.RFC3339),
		JoinHint:        "p2p-node join " + res.JoinCode,
	})
}

// ---- 查询 ------------------------------------------------------------------

func (s *Server) handleGetSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, err := s.svc.Get(r.Context(), id)
	if err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sessionView(rec, s.hub))
}

func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, err := s.svc.Get(r.Context(), id)
	if err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	out := make([]protocol.MemberInfo, 0, len(rec.Members))
	for _, m := range rec.Members {
		out = append(out, memberInfo(m, s.hub))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session_id": rec.ID,
		"members":    out,
	})
}

func (s *Server) handleMember(w http.ResponseWriter, r *http.Request) {
	rec, err := s.svc.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	m := rec.MemberByID(r.PathValue("mid"))
	if m == nil {
		writeErr(w, http.StatusNotFound, protocol.ErrMemberNotFound, "member not found")
		return
	}
	writeJSON(w, http.StatusOK, memberInfo(m, s.hub))
}

// ---- 加入 ------------------------------------------------------------------

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	ip := s.clientIP(r)
	if !s.ipJoin.allow(ip) {
		s.metrics.RateLimited.Add(1)
		writeErr(w, http.StatusTooManyRequests, protocol.ErrRateLimited, "too many join attempts")
		return
	}

	var req joinReq
	if !s.decodeJSON(w, r, &req) {
		return
	}
	// 路径参数优先于 body。
	if id := r.PathValue("id"); id != "" {
		req.SessionID = id
	}
	if req.SessionID == "" && req.JoinCode == "" {
		writeErr(w, http.StatusBadRequest, protocol.ErrBadRequest, "session_id or join_code required")
		return
	}

	pub, err := hex.DecodeString(req.PublicKey)
	if err != nil || len(pub) == 0 {
		writeErr(w, http.StatusBadRequest, protocol.ErrBadRequest, "public_key must be hex")
		return
	}
	nonce, err := hex.DecodeString(req.Nonce)
	if err != nil || len(nonce) == 0 {
		writeErr(w, http.StatusBadRequest, protocol.ErrBadRequest, "nonce must be hex")
		return
	}
	sig, err := hex.DecodeString(req.Signature)
	if err != nil || len(sig) == 0 {
		writeErr(w, http.StatusBadRequest, protocol.ErrBadRequest, "signature must be hex")
		return
	}
	if req.NodeID == "" {
		writeErr(w, http.StatusBadRequest, protocol.ErrBadRequest, "node_id required")
		return
	}

	connID := auth.RandomID("con_", 10)
	res, err := s.svc.Join(r.Context(), session.JoinParams{
		SessionID:    req.SessionID,
		JoinCode:     req.JoinCode,
		Token:        req.JoinToken,
		NodeID:       req.NodeID,
		PublicKey:    pub,
		Nonce:        nonce,
		Signature:    sig,
		ConnectionID: connID,
		Capabilities: req.Capabilities,
		Candidates:   req.Candidates,
	})
	if err != nil {
		s.metrics.JoinsRejected.Add(1)
		if errors.Is(err, session.ErrUnauthorized) {
			s.metrics.AuthFailures.Add(1)
		}
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	s.metrics.JoinsTotal.Add(1)
	if res.Rejoined {
		s.metrics.Rejoins.Add(1)
	}

	// 关键：把「新成员已生效」广播给房间内既有成员。
	//
	// 没有这一步，先加入的 A 永远不知道 B 来了——A 只在建立控制面连接时
	// 拿到一次成员快照，之后若不重连就无法发现新对端。这是「仅凭同一个
	// Token 自动发现彼此」的核心路径。
	active := 0
	for _, m := range res.Session.Members {
		if m.Status == protocol.MemberActive {
			active++
		}
	}
	if res.Member.Status == protocol.MemberActive {
		info := memberInfo(res.Member, s.hub)
		s.hub.BroadcastControl(res.Session.ID, res.Member.ID, &protocol.Message{
			Type:   protocol.MsgMemberJoined,
			Member: &info,
		})
		// 会话成员到齐（pair 满员）→ 通知全体可以开始协商。
		if res.Session.MaxMembers > 0 && active >= res.Session.MaxMembers {
			ready := &protocol.Message{
				Type:      protocol.MsgSessionReady,
				SessionID: res.Session.ID,
				Members:   infosOf(res.Session.Members, s.hub),
			}
			s.hub.BroadcastControl(res.Session.ID, "", ready)
		}
	}

	infos := make([]protocol.MemberInfo, 0, len(res.Peers))
	for _, m := range res.Peers {
		infos = append(infos, memberInfo(m, s.hub))
	}
	writeJSON(w, http.StatusOK, joinResp{
		SessionID:   res.Session.ID,
		MemberID:    res.Member.ID,
		Index:       res.Member.Index,
		IsOwner:     res.IsOwner,
		Status:      res.Status,
		Rejoined:    res.Rejoined,
		Ticket:      res.Ticket,
		Members:     infos,
		Mode:        res.Session.Mode,
		HeartbeatMs: int(res.Heartbeat / time.Millisecond),
		ControlURL:  "/ws/control",
		RelayURL:    "/ws/relay",
	})
}

// ---- 离开 / 解散 -----------------------------------------------------------

func (s *Server) handleLeave(w http.ResponseWriter, r *http.Request) {
	var req memberActionReq
	if !s.decodeJSON(w, r, &req) {
		return
	}
	sessionID := r.PathValue("id")
	memberID, ok := s.authenticateActor(w, r, &req, sessionID)
	if !ok {
		return
	}
	if err := s.svc.Leave(r.Context(), sessionID, memberID); err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	s.notifyMemberLeft(r, sessionID, memberID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "left", "member_id": memberID})
}

func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	// 解散会话需 owner 票据（或配置了 AdminToken 时用管理员令牌）。
	if !s.authorizeOwner(w, r, sessionID, "") {
		return
	}
	if err := s.svc.Close(r.Context(), sessionID); err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	s.hub.CloseSession(sessionID, "session closed by owner")
	writeJSON(w, http.StatusOK, map[string]string{"status": "closed", "session_id": sessionID})
}

// ---- token 管理 ------------------------------------------------------------

func (s *Server) handleRevoke(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	if !s.authorizeOwner(w, r, sessionID, "") {
		return
	}
	if err := s.svc.Revoke(r.Context(), sessionID); err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked", "session_id": sessionID})
}

func (s *Server) handleRotate(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	if !s.authorizeOwner(w, r, sessionID, "") {
		return
	}
	token, err := s.svc.Rotate(r.Context(), sessionID)
	if err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status":     "rotated",
		"session_id": sessionID,
		"join_token": token.TokenString(),
		"join_code":  token.JoinCode(),
	})
}

// ---- 审批 ------------------------------------------------------------------

func (s *Server) handleApprove(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	var req memberActionReq
	if !s.decodeJSON(w, r, &req) {
		return
	}
	actor, ok := s.authenticateActor(w, r, &req, sessionID)
	if !ok {
		return
	}
	m, err := s.svc.Approve(r.Context(), sessionID, actor, req.MemberID)
	if err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	// 让该成员进入数据面，并通知全房间。
	s.hub.MakeApproved(sessionID, m.ID, m.Index)
	info := memberInfo(m, s.hub)
	s.hub.BroadcastControl(sessionID, "", &protocol.Message{
		Type:   protocol.MsgMemberJoined,
		Member: &info,
	})
	// 直接告知被批准者本人。
	s.hub.DeliverControl(sessionID, m.ID, &protocol.Message{
		Type:   protocol.MsgJoined,
		Member: &info,
	})
	writeJSON(w, http.StatusOK, map[string]any{"status": "approved", "member": info})
}

func (s *Server) handleReject(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	var req memberActionReq
	if !s.decodeJSON(w, r, &req) {
		return
	}
	actor, ok := s.authenticateActor(w, r, &req, sessionID)
	if !ok {
		return
	}
	if err := s.svc.Reject(r.Context(), sessionID, actor, req.MemberID); err != nil {
		status, code := mapServiceErr(err)
		writeErr(w, status, code, err.Error())
		return
	}
	s.hub.SetApproved(sessionID, req.MemberID, false)
	s.hub.CloseMember(sessionID, req.MemberID, "removed by owner")
	writeJSON(w, http.StatusOK, map[string]string{"status": "rejected", "member_id": req.MemberID})
}

// ---- 健康与观测 ------------------------------------------------------------

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if !s.checkAdmin(w, r) {
		return
	}
	s.refreshGauges(r.Context())
	svcStats, _ := s.svc.Stats(r.Context())
	hubStats := s.hub.Stats()

	resp := map[string]any{
		"uptime_sec":    int(time.Since(s.started).Seconds()),
		"protocol":      "p2psession/1",
		"sessions":      svcStats.Sessions,
		"members":       svcStats.Members,
		"pending":       svcStats.Pending,
		"online":        hubStats.OnlineMembers,
		"online_conns":  hubStats.OnlineConns,
		"relay_enabled": s.cfg.RelayEnabled,
		// 构建信息放在这里（受 checkAdmin 保护）而不是公开的 /v1/health：
		// commit 与 go 版本可用于反推未修补状态，不该向匿名者暴露。
		"build": version.Fields(),
	}
	if s.relay != nil {
		resp["relay"] = map[string]any{
			"frames_in":  s.relay.FramesIn.Load(),
			"frames_out": s.relay.FramesOut.Load(),
			"bytes_in":   s.relay.BytesIn.Load(),
			"bytes_out":  s.relay.BytesOut.Load(),
			"denied":     s.relay.DeniedFrames.Load(),
			"no_target":  s.relay.NoTarget.Load(),
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !s.checkAdmin(w, r) {
		return
	}
	s.refreshGauges(r.Context())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, s.metrics.Render())
}

// handleRelays 返回可用 Relay 列表（阶段五多 Relay 的接入点）。
// 单实例部署时只返回自身；客户端可据此做延迟探测与选择。
func (s *Server) handleRelays(w http.ResponseWriter, r *http.Request) {
	self := map[string]any{
		"id":       "local",
		"url":      selfURL(r),
		"region":   "local",
		"healthy":  true,
		"priority": 0,
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"relays": []any{self},
	})
}

func selfURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

// ---- 内部辅助 --------------------------------------------------------------

// decodeJSON 读取并解析请求体；失败时已写好响应。
func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer func() { _, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20)) }()
	body := io.LimitReader(r.Body, 1<<20)
	dec := jsonDecoder(body)
	if err := dec.Decode(dst); err != nil {
		// 空 body 对某些端点（如 leave 用票据标识身份）是合法的。
		if errors.Is(err, io.EOF) {
			return true
		}
		writeErr(w, http.StatusBadRequest, protocol.ErrBadRequest, "invalid json: "+err.Error())
		return false
	}
	return true
}

// authenticateActor 解析操作者成员身份：优先票据，其次 body 中的 member_id。
func (s *Server) authenticateActor(w http.ResponseWriter, r *http.Request, req *memberActionReq, sessionID string) (string, bool) {
	if req.ActorTicket != "" {
		t, err := s.tickets.Verify(req.ActorTicket)
		if err != nil || t.SessionID != sessionID {
			s.metrics.AuthFailures.Add(1)
			writeErr(w, http.StatusUnauthorized, protocol.ErrUnauthorized, "invalid actor ticket")
			return "", false
		}
		return t.MemberID, true
	}
	if req.MemberID != "" && req.ActorMemberID != "" {
		return req.ActorMemberID, true
	}
	if req.ActorMemberID != "" {
		return req.ActorMemberID, true
	}
	// 退回到查询参数里的成员 ID（便于 curl 手工调用）。
	if mid := r.URL.Query().Get("member_id"); mid != "" {
		return mid, true
	}
	writeErr(w, http.StatusUnauthorized, protocol.ErrUnauthorized, "actor identity required")
	return "", false
}

// authorizeOwner 要求请求方是 owner（或持有 AdminToken）。
func (s *Server) authorizeOwner(w http.ResponseWriter, r *http.Request, sessionID, actorMemberID string) bool {
	if s.checkAdmin(w, r) && s.cfg.AdminToken != "" {
		return true // 管理员令牌已通过
	}
	if t, err := s.ticketFrom(r); err == nil {
		if t.SessionID != sessionID {
			writeErr(w, http.StatusForbidden, protocol.ErrUnauthorized, "ticket belongs to another session")
			return false
		}
		if t.Role == protocol.RoleOwner {
			return true
		}
		writeErr(w, http.StatusForbidden, "not_owner", "owner ticket required")
		return false
	}
	if actorMemberID != "" {
		rec, err := s.svc.Get(r.Context(), sessionID)
		if err != nil {
			status, code := mapServiceErr(err)
			writeErr(w, status, code, err.Error())
			return false
		}
		if m := rec.MemberByID(actorMemberID); m != nil && m.Role == protocol.RoleOwner {
			return true
		}
	}
	writeErr(w, http.StatusForbidden, "not_owner", "owner credentials required")
	return false
}

// checkAdmin 校验管理员令牌（未配置时放行）。
func (s *Server) checkAdmin(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.AdminToken == "" {
		return true
	}
	got := ""
	if h := r.Header.Get("Authorization"); len(h) > 7 && h[:7] == "Bearer " {
		got = h[7:]
	} else if t := r.URL.Query().Get("token"); t != "" {
		got = t
	}
	if got != s.cfg.AdminToken {
		writeErr(w, http.StatusUnauthorized, protocol.ErrUnauthorized, "admin token required")
		return false
	}
	return true
}

// notifyMemberLeft 通知房间内其余成员。
func (s *Server) notifyMemberLeft(r *http.Request, sessionID, memberID string) {
	s.hub.CloseMember(sessionID, memberID, "left")
	s.hub.BroadcastControl(sessionID, memberID, &protocol.Message{
		Type:     protocol.MsgMemberLeft,
		MemberID: memberID,
	})
}

// sessionView 组装会话的对外视图。
func sessionView(rec *sessionRecord, hub *member.Hub) map[string]any {
	members := make([]protocol.MemberInfo, 0, len(rec.Members))
	for _, m := range rec.Members {
		members = append(members, memberInfo(m, hub))
	}
	return map[string]any{
		"session_id":       rec.ID,
		"mode":             rec.Mode,
		"join_code":        rec.JoinCode,
		"max_members":      rec.MaxMembers,
		"join_limit":       rec.JoinLimit,
		"joins_used":       rec.JoinsUsed,
		"require_approval": rec.RequireApproval,
		"revoked":          rec.Revoked,
		"closed":           rec.Closed,
		"created_at":       rec.CreatedAt.UTC().Format(time.RFC3339),
		"expires_at":       rec.ExpiresAt.UTC().Format(time.RFC3339),
		"members":          members,
	}
}

// infosOf 批量转换成员记录（只保留已生效成员，供 session_ready 等广播使用）。
func infosOf(members []*memberRecord, hub *member.Hub) []protocol.MemberInfo {
	out := make([]protocol.MemberInfo, 0, len(members))
	for _, m := range members {
		if m.Status != protocol.MemberActive {
			continue
		}
		out = append(out, memberInfo(m, hub))
	}
	return out
}

// memberInfo 把持久化成员记录转换为对外视图（含在线状态与角色）。
func memberInfo(m *memberRecord, hub *member.Hub) protocol.MemberInfo {
	info := protocol.MemberInfo{
		MemberID:     m.ID,
		Index:        m.Index,
		NodeID:       m.NodeID,
		PublicKey:    m.PublicKey,
		Capabilities: m.Capabilities,
		Candidates:   m.Candidates,
		IsOwner:      m.Role == protocol.RoleOwner,
		Role:         m.Role,
		Status:       m.Status,
		JoinedAt:     m.JoinedAt.UTC().Format(time.RFC3339),
		LastSeen:     m.LastSeen.UTC().Format(time.RFC3339),
	}
	if hub != nil && m.SessionID != "" {
		// 在线状态来自运行时连接中心，而不是持久化记录。
		info.Online = hub.HasMember(m.SessionID, m.ID)
	}
	return info
}
