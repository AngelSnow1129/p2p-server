// Package session 实现会话（Session）的业务编排：
// 创建、加入、离开、审批、撤销、关闭、清扫，以及 token 校验与 join proof 验证。
//
// 本包不关心传输（HTTP/WebSocket）与在线连接状态（那是 server/member 包的事），
// 只负责「持久化状态 + 策略」这一层，因此可以独立测试。
package session

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"p2psession/internal/auth"
	"p2psession/internal/protocol"
	"p2psession/internal/storage"
)

// 默认策略。
const (
	// DefaultPairMembers 是 pair 模式的固定成员上限。
	DefaultPairMembers = 2
	// DefaultGroupMembers 是 group 模式未指定时的成员上限。
	DefaultGroupMembers = 10
	// MaxGroupMembers 是成员上限的硬顶（受 16 位下标空间与内存约束）。
	MaxGroupMembers = 1000
	// DefaultTTL 是会话默认存活时长。
	DefaultTTL = 30 * time.Minute
	// MaxTTL 是允许的最大存活时长。
	MaxTTL = 7 * 24 * time.Hour
	// DefaultMemberGrace 是成员离线后保留 MemberID 的宽限时长。
	DefaultMemberGrace = 5 * time.Minute
	// DefaultTicketTTL 是连接票据有效期。
	DefaultTicketTTL = 10 * time.Minute
)

// Config 是服务层策略配置。
type Config struct {
	// TicketTTL 为签发的连接票据有效期。
	TicketTTL time.Duration
	// MemberGrace 为成员离线后保留其 MemberID/Index 的时长（重连恢复窗口）。
	MemberGrace time.Duration
	// DefaultTTL 为未指定时的会话存活时长。
	DefaultTTL time.Duration
	// MaxSessions 为单实例会话总数上限（0 = 不限），用于防止资源耗尽。
	MaxSessions int
}

// DefaultConfig 返回带默认值的配置。
func DefaultConfig() Config {
	return Config{
		TicketTTL:   DefaultTicketTTL,
		MemberGrace: DefaultMemberGrace,
		DefaultTTL:  DefaultTTL,
		MaxSessions: 0,
	}
}

// Service 是会话业务入口。
type Service struct {
	store   storage.Store
	tickets *auth.TicketKey
	cfg     Config
	// now 可注入，便于测试模拟时间流逝。
	now func() time.Time
}

// New 创建服务。
func New(store storage.Store, tickets *auth.TicketKey, cfg Config) *Service {
	if cfg.TicketTTL <= 0 {
		cfg.TicketTTL = DefaultTicketTTL
	}
	if cfg.MemberGrace <= 0 {
		cfg.MemberGrace = DefaultMemberGrace
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = DefaultTTL
	}
	return &Service{store: store, tickets: tickets, cfg: cfg, now: time.Now}
}

// Store 返回底层存储（供管理接口/测试直接访问）。
func (s *Service) Store() storage.Store { return s.store }

// ---- 创建 ------------------------------------------------------------------

// CreateParams 是创建会话的入参。
type CreateParams struct {
	// Mode 为 pair / group（空则 pair）。
	Mode string
	// MaxMembers 为成员上限（pair 强制 2；group 默认 DefaultGroupMembers）。
	MaxMembers int
	// JoinLimit 为最多加入次数（0 = 不限，仅受 MaxMembers 约束）。
	JoinLimit int
	// TTL 为会话存活时长（0 用默认）。
	TTL time.Duration
	// RequireApproval 为 true 时新成员需 owner 批准。
	RequireApproval bool
	// IdleTimeout 为全员离线后自动过期时长（0 = 不启用）。
	IdleTimeout time.Duration
	// OwnerNodeID 为预声明的 owner 节点；留空则由首个加入者成为 owner。
	OwnerNodeID string
	// Capabilities 预留给创建者的能力声明（可空）。
	Capabilities []string
}

// CreateResult 是创建结果；Token 只在此时返回一次，服务器仅存其哈希。
type CreateResult struct {
	Session   *storage.SessionRecord
	Token     *auth.JoinToken
	JoinCode  string
	SessionID string
	ExpiresAt time.Time
}

// Create 创建会话。
func (s *Service) Create(ctx context.Context, p CreateParams) (*CreateResult, error) {
	now := s.now()

	mode := strings.ToLower(strings.TrimSpace(p.Mode))
	if mode == "" {
		mode = protocol.ModePair
	}
	switch mode {
	case protocol.ModePair, protocol.ModeGroup:
	default:
		return nil, fmt.Errorf("%w: unknown mode %q", ErrBadParams, mode)
	}

	maxMembers := p.MaxMembers
	if mode == protocol.ModePair {
		// pair 恒为 2：不接受调用方覆盖，避免出现「pair 但 5 人」的语义混乱。
		maxMembers = DefaultPairMembers
	} else if maxMembers <= 0 {
		maxMembers = DefaultGroupMembers
	}
	if maxMembers > MaxGroupMembers {
		return nil, fmt.Errorf("%w: max_members %d exceeds hard limit %d",
			ErrBadParams, maxMembers, MaxGroupMembers)
	}
	if p.JoinLimit < 0 {
		return nil, fmt.Errorf("%w: join_limit must be >= 0", ErrBadParams)
	}
	if p.JoinLimit > 0 && p.JoinLimit < maxMembers && mode == protocol.ModeGroup {
		// join_limit 小于成员上限时以 join_limit 为实际约束，属合法配置（更严格）。
		_ = p.JoinLimit
	}

	ttl := p.TTL
	if ttl <= 0 {
		ttl = s.cfg.DefaultTTL
	}
	if ttl > MaxTTL {
		return nil, fmt.Errorf("%w: ttl exceeds %s", ErrBadParams, MaxTTL)
	}

	token, err := auth.NewJoinToken()
	if err != nil {
		return nil, err
	}

	rec := &storage.SessionRecord{
		ID:              auth.RandomID("sess_", 12),
		Mode:            mode,
		JoinCode:        token.JoinCode(),
		TokenHash:       token.Hash(),
		JoinLimit:       p.JoinLimit,
		MaxMembers:      maxMembers,
		RequireApproval: p.RequireApproval,
		CreatedAt:       now,
		ExpiresAt:       now.Add(ttl),
		IdleTimeout:     p.IdleTimeout,
	}
	if err := s.store.Create(ctx, rec); err != nil {
		return nil, mapStoreErr(err)
	}
	return &CreateResult{
		Session:   rec,
		Token:     token,
		JoinCode:  rec.JoinCode,
		SessionID: rec.ID,
		ExpiresAt: rec.ExpiresAt,
	}, nil
}

// ---- 加入 ------------------------------------------------------------------

// JoinParams 是一次加入请求。
//
// Token 与 JoinCode 二选一：Token 为机器形式（--token），JoinCode 为人类形式。
type JoinParams struct {
	SessionID string
	JoinCode  string
	Token     string

	// 节点身份与证明（必填）。
	NodeID    string
	PublicKey []byte
	Nonce     []byte
	Signature []byte

	// ConnectionID 由服务器生成，标识本次连接。
	ConnectionID string
	Capabilities []string
	Candidates   []protocol.Candidate
}

// JoinResult 是加入结果。
type JoinResult struct {
	Session  *storage.SessionRecord
	Member   *storage.MemberRecord
	Peers    []*storage.MemberRecord // 其它 active 成员（用于自动发现）
	Rejoined bool
	IsOwner  bool
	// Ticket 为后续 /ws/control 与 /ws/relay 的连接票据。
	Ticket string
	// Status 为 active 或 pending（需审批）。
	Status string
	// Heartbeat 为建议心跳间隔。
	Heartbeat time.Duration
}

// Join 校验并加入会话。
func (s *Service) Join(ctx context.Context, p JoinParams) (*JoinResult, error) {
	now := s.now()

	// 1. 定位会话：join code 或 session id。
	var rec *storage.SessionRecord
	var err error
	switch {
	case p.SessionID != "":
		// 按 ID 查找：ID 是标识符，可以如实告知「不存在」。
		rec, err = s.store.Get(ctx, p.SessionID)
	case p.JoinCode != "":
		// 按 join code 查找：这是能力凭证，不存在与已失效不应区分，
		// 否则会泄露「某个 code 是否曾经存在」。
		rec, err = s.store.GetByJoinCode(ctx, p.JoinCode)
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrUnauthorized
		}
	default:
		return nil, fmt.Errorf("%w: session_id or join_code required", ErrBadParams)
	}
	if err != nil {
		return nil, mapStoreErr(err)
	}
	if rec.Closed {
		return nil, ErrSessionClosed
	}
	if rec.Expired(now) {
		return nil, ErrSessionExpired
	}

	// 2. 校验 token（能力凭证）。
	tokenStr := p.Token
	if tokenStr == "" {
		// 允许用 join code 本身作为 token（体验：p2p-node join P2P-XXXX）。
		tokenStr = p.JoinCode
	}
	raw, err := auth.DecodeToken(tokenStr)
	if err != nil {
		return nil, ErrUnauthorized
	}
	gotHash := hashTokenRaw(raw)
	if !auth.ConstantTimeEqualHex(gotHash, rec.TokenHash) {
		// token 与 session 不匹配，或 token 错误。
		return nil, ErrUnauthorized
	}

	// 撤销只阻止「新加入」，不影响已授权成员的重连：
	// 该成员此前已凭同一 token 通过授权，掉线重连应能恢复原身份，
	// 否则一次误撤销就会把所有在线成员永久踢出会话。
	existing := rec.MemberByNodeID(p.NodeID)
	if rec.Revoked && existing == nil {
		return nil, ErrTokenRevoked
	}

	// 3. 校验 join proof（节点身份绑定到 token 对应的会话）。
	if err := auth.VerifyJoinProof(p.PublicKey, p.NodeID, rec.TokenHash, p.Nonce, p.Signature); err != nil {
		return nil, ErrUnauthorized
	}

	// 4. 原子写入（容量/限额/审批策略在此判定，策略取自会话记录）。
	out, err := s.store.Join(ctx, rec.ID, storage.JoinRequest{
		NodeID:       p.NodeID,
		PublicKey:    hexEncode(p.PublicKey),
		ConnectionID: p.ConnectionID,
		Capabilities: p.Capabilities,
		Candidates:   p.Candidates,
		Now:          now,
	})
	if err != nil {
		return nil, mapStoreErr(err)
	}

	// 5. 签发连接票据。
	ticket := s.tickets.Issue(out.Session.ID, out.Member.ID, p.ConnectionID, out.Member.Role, s.cfg.TicketTTL)

	return &JoinResult{
		Session:   out.Session,
		Member:    out.Member,
		Peers:     peersOf(out.Session, out.Member.ID),
		Rejoined:  out.Rejoined,
		IsOwner:   out.IsOwner,
		Ticket:    ticket,
		Status:    out.Member.Status,
		Heartbeat: 15 * time.Second,
	}, nil
}

// ---- 查询 ------------------------------------------------------------------

// Get 读取会话。
func (s *Service) Get(ctx context.Context, sessionID string) (*storage.SessionRecord, error) {
	rec, err := s.store.Get(ctx, sessionID)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	if rec.Expired(s.now()) {
		return nil, ErrSessionExpired
	}
	return rec, nil
}

// Members 返回会话成员列表（含离线成员与 pending 成员，便于展示）。
func (s *Service) Members(ctx context.Context, sessionID string) ([]*storage.MemberRecord, error) {
	rec, err := s.Get(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return rec.Members, nil
}

// Member 返回单个成员。
func (s *Service) Member(ctx context.Context, sessionID, memberID string) (*storage.MemberRecord, error) {
	rec, err := s.Get(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	m := rec.MemberByID(memberID)
	if m == nil {
		return nil, ErrMemberNotFound
	}
	return m, nil
}

// MemberIndexFor 返回会话内某成员的数据面下标，且要求其为 active 成员。
// relay 转发与控制面寻址都以它为「该成员是否属于本会话且已生效」的判定。
func (s *Service) MemberIndexFor(sessionID, memberID string) (uint16, bool) {
	rec, err := s.store.Get(context.Background(), sessionID)
	if err != nil {
		return 0, false
	}
	if rec.Expired(s.now()) || rec.Closed {
		return 0, false
	}
	m := rec.MemberByID(memberID)
	if m == nil || m.Status != protocol.MemberActive {
		return 0, false
	}
	return m.Index, true
}

// IsActiveMember 实现 relay.Membership：发送方是否为本会话活跃成员。
func (s *Service) IsActiveMember(sessionID, memberID string) bool {
	_, ok := s.MemberIndexFor(sessionID, memberID)
	return ok
}

// MemberIndex 实现 relay.Membership：成员的数据面下标。
func (s *Service) MemberIndex(sessionID, memberID string) (uint16, bool) {
	return s.MemberIndexFor(sessionID, memberID)
}

// peersOf 返回除 selfID 外的 active 成员——这就是客户端「自动发现」的数据源。
func peersOf(rec *storage.SessionRecord, selfID string) []*storage.MemberRecord {
	out := make([]*storage.MemberRecord, 0, len(rec.Members))
	for _, m := range rec.Members {
		if m.ID == selfID || m.Status != protocol.MemberActive {
			continue
		}
		cp := *m
		out = append(out, &cp)
	}
	return out
}

// ---- 状态变更 --------------------------------------------------------------

// UpdateCandidates 覆盖某成员上报的网络候选。
func (s *Service) UpdateCandidates(ctx context.Context, sessionID, memberID string, cands []protocol.Candidate) error {
	rec, err := s.store.Get(ctx, sessionID)
	if err != nil {
		return mapStoreErr(err)
	}
	m := rec.MemberByID(memberID)
	if m == nil {
		return ErrMemberNotFound
	}
	if len(cands) > protocol.DefaultMaxCandidates {
		return fmt.Errorf("%w: too many candidates", ErrBadParams)
	}
	m.Candidates = cands
	m.LastSeen = s.now()
	return mapStoreErr(s.store.UpdateMember(ctx, sessionID, m))
}

// Touch 更新成员心跳时间（仅在连接 ID 匹配时生效）。
func (s *Service) Touch(ctx context.Context, sessionID, memberID, connectionID string) error {
	rec, err := s.store.Get(ctx, sessionID)
	if err != nil {
		return mapStoreErr(err)
	}
	m := rec.MemberByID(memberID)
	if m == nil {
		return ErrMemberNotFound
	}
	if connectionID != "" && m.ConnectionID != connectionID {
		return ErrStaleConnection
	}
	m.LastSeen = s.now()
	return mapStoreErr(s.store.UpdateMember(ctx, sessionID, m))
}

// MarkOffline 标记成员离线（保留 MemberID/Index 供重连恢复）。
func (s *Service) MarkOffline(ctx context.Context, sessionID, memberID, connectionID string) error {
	return mapStoreErr(s.store.MarkOffline(ctx, sessionID, memberID, connectionID, s.now()))
}

// Leave 成员主动离开：立即移除记录（释放名额与下标）。
func (s *Service) Leave(ctx context.Context, sessionID, memberID string) error {
	return mapStoreErr(s.store.RemoveMember(ctx, sessionID, memberID))
}

// Approve 由 owner 批准一个 pending 成员。
func (s *Service) Approve(ctx context.Context, sessionID, ownerMemberID, targetMemberID string) (*storage.MemberRecord, error) {
	rec, err := s.store.Get(ctx, sessionID)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	owner := rec.MemberByID(ownerMemberID)
	if owner == nil || owner.Role != protocol.RoleOwner {
		return nil, ErrNotOwner
	}
	target := rec.MemberByID(targetMemberID)
	if target == nil {
		return nil, ErrMemberNotFound
	}
	if target.Status != protocol.MemberPending {
		return nil, fmt.Errorf("%w: member is not pending", ErrBadParams)
	}
	// 批准时仍要重新检查容量：pending 期间名额可能已被占满。
	if rec.MaxMembers > 0 && rec.ActiveMembers() >= rec.MaxMembers {
		return nil, ErrSessionFull
	}
	target.Status = protocol.MemberActive
	target.JoinedAt = s.now()
	if err := s.store.UpdateMember(ctx, sessionID, target); err != nil {
		return nil, mapStoreErr(err)
	}
	return target, nil
}

// Reject 由 owner 拒绝（或移除）一个成员。
func (s *Service) Reject(ctx context.Context, sessionID, ownerMemberID, targetMemberID string) error {
	rec, err := s.store.Get(ctx, sessionID)
	if err != nil {
		return mapStoreErr(err)
	}
	owner := rec.MemberByID(ownerMemberID)
	if owner == nil || owner.Role != protocol.RoleOwner {
		return ErrNotOwner
	}
	if ownerMemberID == targetMemberID {
		return fmt.Errorf("%w: owner cannot remove self", ErrBadParams)
	}
	target := rec.MemberByID(targetMemberID)
	if target == nil {
		return ErrMemberNotFound
	}
	target.Status = protocol.MemberDenied
	target.ConnectionID = ""
	return mapStoreErr(s.store.UpdateMember(ctx, sessionID, target))
}

// Revoke 撤销 token（不再接受新加入，已在线成员不受影响）。
func (s *Service) Revoke(ctx context.Context, sessionID string) error {
	if _, err := s.store.Get(ctx, sessionID); err != nil {
		return mapStoreErr(err)
	}
	return mapStoreErr(s.store.Revoke(ctx, sessionID, s.now()))
}

// Rotate 轮换 token：生成新 token 替换旧哈希并清除撤销标记，
// 同时把 join code 一并更换（旧 code 立即失效）。
// 返回的新 token 仅在此时可见一次。
//
// 用途：token 疑似泄露时无需解散会话即可立即失效旧的加入能力。
func (s *Service) Rotate(ctx context.Context, sessionID string) (*auth.JoinToken, error) {
	token, err := auth.NewJoinToken()
	if err != nil {
		return nil, err
	}
	if err := s.store.RotateToken(ctx, sessionID, token.Hash(), token.JoinCode(), s.now()); err != nil {
		return nil, mapStoreErr(err)
	}
	return token, nil
}

// Close 关闭会话。
func (s *Service) Close(ctx context.Context, sessionID string) error {
	if _, err := s.store.Get(ctx, sessionID); err != nil {
		return mapStoreErr(err)
	}
	return mapStoreErr(s.store.Close(ctx, sessionID))
}

// Delete 删除会话。
func (s *Service) Delete(ctx context.Context, sessionID string) error {
	return mapStoreErr(s.store.Delete(ctx, sessionID))
}

// ---- 清扫 ------------------------------------------------------------------

// PruneResult 是清扫统计。
type PruneResult struct {
	SessionsExpired int
	MembersExpired  int
}

// Prune 回收过期会话与超出宽限期的离线成员。应周期性调用。
func (s *Service) Prune(ctx context.Context) (PruneResult, error) {
	now := s.now()
	var res PruneResult

	recs, err := s.store.List(ctx, storage.ListFilter{})
	if err != nil {
		return res, err
	}
	for _, rec := range recs {
		if rec.Expired(now) || rec.Closed {
			if err := s.store.Delete(ctx, rec.ID); err == nil {
				res.SessionsExpired++
			}
			continue
		}

		// 全员离线超过 IdleTimeout → 整会话回收。
		if rec.IdleTimeout > 0 {
			if idle, since := allIdle(rec); idle && now.Sub(since) > rec.IdleTimeout {
				if err := s.store.Delete(ctx, rec.ID); err == nil {
					res.SessionsExpired++
				}
				continue
			}
		}

		for _, m := range rec.Members {
			if m.Online() || m.LeaveAt.IsZero() {
				continue
			}
			if now.Sub(m.LeaveAt) > s.cfg.MemberGrace {
				if err := s.store.RemoveMember(ctx, rec.ID, m.ID); err == nil {
					res.MembersExpired++
				}
			}
		}
	}
	return res, nil
}

// allIdle 判断会话是否全员离线，并返回最早离线时刻。
func allIdle(rec *storage.SessionRecord) (bool, time.Time) {
	var since time.Time
	found := false
	for _, m := range rec.Members {
		if m.Status != protocol.MemberActive {
			continue
		}
		found = true
		if m.Online() {
			return false, time.Time{}
		}
		if !m.LeaveAt.IsZero() && (since.IsZero() || m.LeaveAt.Before(since)) {
			since = m.LeaveAt
		}
	}
	if !found {
		// 没有 active 成员：以创建时间起算。
		return true, rec.CreatedAt
	}
	return true, since
}

// Stats 返回会话维度的统计（指标用）。
type Stats struct {
	Sessions int
	Members  int
	Online   int
	Pending  int
}

// Stats 汇总当前状态。
func (s *Service) Stats(ctx context.Context) (Stats, error) {
	recs, err := s.store.List(ctx, storage.ListFilter{})
	if err != nil {
		return Stats{}, err
	}
	var st Stats
	now := s.now()
	for _, rec := range recs {
		if rec.Expired(now) {
			continue
		}
		st.Sessions++
		for _, m := range rec.Members {
			st.Members++
			if m.Online() {
				st.Online++
			}
			if m.Status == protocol.MemberPending {
				st.Pending++
			}
		}
	}
	return st, nil
}

// SetNow 注入时钟（测试用）。
func (s *Service) SetNow(f func() time.Time) { s.now = f }
