// Package storage 定义 p2psession 的持久化抽象与其内存实现。
//
// 分层意图：session 包只依赖本包的接口，因此
//   - 单机部署用 Memory（默认，零依赖）；
//   - 多实例部署可换上 Redis/Postgres 实现（键为 sessionID，成员内嵌或用集合）。
//
// 关键约束：Join 必须由实现方保证「校验 + 写入」的原子性
// （容量/加入次数/过期/撤销的检查与成员落库不可被并发穿插），
// 否则并发加入会突破 max_members。接口据此把策略校验下推到存储层内部。
package storage

import (
	"context"
	"errors"
	"time"

	"p2psession/internal/protocol"
)

// 存储层错误。session 包把它们映射为对外错误码。
var (
	ErrNotFound      = errors.New("session not found")
	ErrExpired       = errors.New("session expired")
	ErrRevoked       = errors.New("token revoked")
	ErrClosed        = errors.New("session closed")
	ErrFull          = errors.New("session full")
	ErrJoinLimit     = errors.New("join limit reached")
	ErrMemberExists  = errors.New("member already exists")
	ErrMemberMissing = errors.New("member not found")
	ErrConflict      = errors.New("concurrent modification")
)

// MemberRecord 是持久化的成员状态。
type MemberRecord struct {
	// ID 为会话内稳定成员标识（重连不变，恢复 Session 的依据）。
	ID string `json:"id"`
	// SessionID 为所属会话（冗余保存，便于外部存储实现做反向索引与展示）。
	SessionID string `json:"session_id,omitempty"`
	// Index 为数据面寻址用的 16 位下标（会话内唯一，复用空闲下标）。
	Index uint16 `json:"index"`
	// NodeID 为节点长期身份指纹。
	NodeID string `json:"node_id"`
	// PublicKey 为 Ed25519 公钥（hex）。
	PublicKey string `json:"public_key"`
	// Role 为 owner / member。
	Role string `json:"role"`
	// Status 为 active / pending / denied / left。
	Status string `json:"status"`
	// Capabilities 如 ["udp","tcp","quic","relay"]。
	Capabilities []string `json:"capabilities,omitempty"`
	// Candidates 为最近上报的网络候选。
	Candidates []protocol.Candidate `json:"candidates,omitempty"`
	// ConnectionID 为当前在线连接（离线为空）。
	ConnectionID string    `json:"connection_id,omitempty"`
	JoinedAt     time.Time `json:"joined_at"`
	LastSeen     time.Time `json:"last_seen"`
	// LeaveAt 为宽限期起始时刻（离线时记录，用于过期回收）。
	LeaveAt time.Time `json:"leave_at,omitempty"`
}

// Online 表示成员当前是否有活跃连接。
func (m *MemberRecord) Online() bool { return m.ConnectionID != "" }

// SessionRecord 是持久化的会话状态。
type SessionRecord struct {
	ID   string `json:"id"`
	Mode string `json:"mode"` // pair | group
	// JoinCode 是展示给用户的 P2P-XXXX 形式（便于 session join <code> 反查）。
	JoinCode string `json:"join_code"`
	// TokenHash 为 sha256(join token) 的 hex；原始 token 只在创建响应里出现一次。
	TokenHash string `json:"token_hash"`
	// JoinLimit 为最多允许的加入次数（0 = 不限，仅受 MaxMembers 约束）。
	JoinLimit int `json:"join_limit"`
	// JoinsUsed 为已消耗的加入次数（每次成功 join 递增）。
	JoinsUsed int `json:"joins_used"`
	// MaxMembers 为成员上限（pair 固定 2）。
	MaxMembers int `json:"max_members"`
	// RequireApproval 为 true 时新成员需 owner 审批（approve 前 status=pending）。
	RequireApproval bool `json:"require_approval"`
	// OwnerMemberID 为创建者成员 ID（首个 join 者）。
	OwnerMemberID string `json:"owner_member_id,omitempty"`
	// Revoked 为 true 表示 token 已被撤销，不再接受新加入。
	Revoked   bool      `json:"revoked"`
	Closed    bool      `json:"closed"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	// IdleTimeout 为「全员离线后自动过期」的空闲时长（0 = 不启用）。
	IdleTimeout time.Duration `json:"idle_timeout,omitempty"`

	Members []*MemberRecord `json:"members"`
}

// Expired 判断会话（按 now）是否已过期。
func (s *SessionRecord) Expired(now time.Time) bool {
	return !s.ExpiresAt.IsZero() && now.After(s.ExpiresAt)
}

// ActiveMembers 统计状态为 active 的成员数。
func (s *SessionRecord) ActiveMembers() int {
	n := 0
	for _, m := range s.Members {
		if m.Status == protocol.MemberActive {
			n++
		}
	}
	return n
}

// MemberByID 按 ID 查找成员（未找到返回 nil）。
func (s *SessionRecord) MemberByID(id string) *MemberRecord {
	for _, m := range s.Members {
		if m.ID == id {
			return m
		}
	}
	return nil
}

// MemberByNodeID 按 NodeID 查找成员（重连恢复的依据）。
func (s *SessionRecord) MemberByNodeID(nodeID string) *MemberRecord {
	for _, m := range s.Members {
		if m.NodeID == nodeID {
			return m
		}
	}
	return nil
}

// MemberByIndex 按数据面下标查找成员。
func (s *SessionRecord) MemberByIndex(idx uint16) *MemberRecord {
	for _, m := range s.Members {
		if m.Index == idx {
			return m
		}
	}
	return nil
}

// freeIndex 返回会话内最小可用的 16 位数据面下标（1 起，0 保留为“服务器”）。
// 成员离开后其下标可被复用，避免长时间会话内下标耗尽。
func (s *SessionRecord) freeIndex() uint16 {
	used := make(map[uint16]bool, len(s.Members))
	for _, m := range s.Members {
		used[m.Index] = true
	}
	for i := uint16(1); i != 0; i++ {
		if !used[i] {
			return i
		}
	}
	return 0
}

// JoinRequest 描述一次加入请求；由存储实现原子地完成校验与写入。
type JoinRequest struct {
	// NodeID/PublicKey 为已校验的节点身份。
	NodeID    string
	PublicKey string
	// ConnectionID 为本次加入建立的连接标识。
	ConnectionID string
	// Capabilities 为客户端声明的能力集。
	Capabilities []string
	// Candidates 为客户端首次上报的网络候选。
	Candidates []protocol.Candidate
	// Now 为当前时间（便于测试注入）。
	Now time.Time
}

// 注意：是否需要审批（SessionRecord.RequireApproval）由会话记录决定，
// 而不是由调用方通过 JoinRequest 传入——策略只能有一个权威来源，
// 否则并发加入时会出现「同一会话对某些请求要审批、对另一些不要」的分裂。

// JoinOutcome 是一次加入的结果。
type JoinOutcome struct {
	Session *SessionRecord
	Member  *MemberRecord
	// Rejoined 为 true 表示同一 NodeID 重新加入（复用原 MemberID/Index）。
	Rejoined bool
	// IsOwner 表示该成员是否成为 Session owner（首个加入者）。
	IsOwner bool
}

// ListFilter 用于枚举会话（后台清扫/管理接口）。
type ListFilter struct {
	// ExpiredBefore 非零时只返回在该时刻前过期的会话。
	ExpiredBefore time.Time
	// ClosedOnly 只返回已关闭的会话。
	ClosedOnly bool
	// Limit 上限（0 = 不限）。
	Limit int
}

// Store 是会话存储抽象。
//
// 实现必须保证所有方法并发安全；Join/Revoke/Close 等状态变更必须是原子的。
type Store interface {
	// Create 新建会话；ID 冲突返回 ErrConflict。
	Create(ctx context.Context, s *SessionRecord) error

	// Get 读取会话快照（含成员）。
	Get(ctx context.Context, sessionID string) (*SessionRecord, error)

	// GetByJoinCode 通过人类可读 join code 反查会话（session join P2P-… 用）。
	GetByJoinCode(ctx context.Context, code string) (*SessionRecord, error)

	// Join 原子地校验（过期/撤销/关闭/容量/加入次数）并写入成员。
	// 同一 NodeID 再次加入视为 rejoin：复用原成员及其 Index，并重置 LastSeen。
	Join(ctx context.Context, sessionID string, req JoinRequest) (*JoinOutcome, error)

	// UpdateMember 以传入记录整体替换同 ID 成员（用于审批、候选更新、状态变更）。
	UpdateMember(ctx context.Context, sessionID string, m *MemberRecord) error

	// RemoveMember 移除成员（离开或宽限期结束）。
	RemoveMember(ctx context.Context, sessionID, memberID string) error

	// MarkOffline 标记成员离线（保留记录与 MemberID，进入宽限期）。
	MarkOffline(ctx context.Context, sessionID, memberID, connectionID string, now time.Time) error

	// Revoke 撤销 token（不再接受新加入）。
	Revoke(ctx context.Context, sessionID string, now time.Time) error

	// RotateToken 用新的哈希与 join code 替换旧的（旧 token 与旧 code 立即失效），
	// 并清除 Revoked 标记。用于「疑似泄露但不想解散会话」的场景。
	RotateToken(ctx context.Context, sessionID, newHash, newCode string, now time.Time) error

	// Close 关闭会话（所有成员将被通知，连接断开）。
	Close(ctx context.Context, sessionID string) error

	// Delete 彻底删除会话（清扫）。
	Delete(ctx context.Context, sessionID string) error

	// List 枚举会话。
	List(ctx context.Context, f ListFilter) ([]*SessionRecord, error)
}
