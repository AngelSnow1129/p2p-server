package storage

import (
	"context"
	"crypto/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"p2psession/internal/protocol"
)

// idAlphabet 为 Crockford Base32（去掉易混字符 I/L/O/U）。
var idAlphabet = []byte("0123456789ABCDEFGHJKMNPQRSTVWXYZ")

// randomToken 生成 n 字节熵编码后的随机 ID 片段。
func randomToken(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand 失败属环境级故障；退化用时间戳避免产生空 ID。
		for i := range buf {
			buf[i] = byte(time.Now().UnixNano() >> (uint(i%8) * 8))
		}
	}
	out := make([]byte, n)
	for i, b := range buf {
		out[i] = idAlphabet[int(b)%len(idAlphabet)]
	}
	return string(out)
}

// MemoryStore 是单机部署的默认存储实现。
//
// 并发模型：单个 sync.RWMutex 保护全部会话。信令服务器的临界区极短
// （只做 map 查找与结构体替换），粗锁比细粒度锁更易证明正确；
// 数据面转发不经过这里（见 relay 包），因此不会成为吞吐瓶颈。
//
// 所有出参均为深拷贝，调用方可以安全地长期持有或修改快照，
// 不会与后续写入发生数据竞争。
type MemoryStore struct {
	mu sync.RWMutex
	// byID 为会话主索引。
	byID map[string]*SessionRecord
	// byCode 为 join code → sessionID 的二级索引（大小写不敏感，已归一化）。
	byCode map[string]string
}

// NewMemory 创建内存存储。
func NewMemory() *MemoryStore {
	return &MemoryStore{
		byID:   make(map[string]*SessionRecord),
		byCode: make(map[string]string),
	}
}

// ---- 深拷贝辅助 ------------------------------------------------------------

func cloneMember(m *MemberRecord) *MemberRecord {
	if m == nil {
		return nil
	}
	cp := *m
	if m.Capabilities != nil {
		cp.Capabilities = append([]string(nil), m.Capabilities...)
	}
	if m.Candidates != nil {
		cp.Candidates = append([]protocol.Candidate(nil), m.Candidates...)
	}
	return &cp
}

func cloneSession(s *SessionRecord) *SessionRecord {
	if s == nil {
		return nil
	}
	cp := *s
	cp.Members = make([]*MemberRecord, 0, len(s.Members))
	for _, m := range s.Members {
		cp.Members = append(cp.Members, cloneMember(m))
	}
	return &cp
}

// normalizeCode 归一化 join code 以便大小写/连字符不敏感地反查。
func normalizeCode(code string) string {
	c := strings.ToUpper(strings.TrimSpace(code))
	c = strings.ReplaceAll(c, "-", "")
	c = strings.ReplaceAll(c, " ", "")
	return c
}

// ---- Store 实现 ------------------------------------------------------------

// Create 新建会话。
func (s *MemoryStore) Create(ctx context.Context, rec *SessionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byID[rec.ID]; exists {
		return ErrConflict
	}
	if rec.JoinCode != "" {
		if _, exists := s.byCode[normalizeCode(rec.JoinCode)]; exists {
			return ErrConflict
		}
		s.byCode[normalizeCode(rec.JoinCode)] = rec.ID
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	s.byID[rec.ID] = cloneSession(rec)
	return nil
}

// Get 读取会话快照。
func (s *MemoryStore) Get(ctx context.Context, sessionID string) (*SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.byID[sessionID]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneSession(rec), nil
}

// GetByJoinCode 通过 join code 反查。
func (s *MemoryStore) GetByJoinCode(ctx context.Context, code string) (*SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byCode[normalizeCode(code)]
	if !ok {
		return nil, ErrNotFound
	}
	rec, ok := s.byID[id]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneSession(rec), nil
}

// Join 原子地完成校验与成员写入。
func (s *MemoryStore) Join(ctx context.Context, sessionID string, req JoinRequest) (*JoinOutcome, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.byID[sessionID]
	if !ok {
		return nil, ErrNotFound
	}
	if rec.Closed {
		return nil, ErrClosed
	}
	if rec.Expired(now) {
		return nil, ErrExpired
	}

	// 同一 NodeID 重新加入：复用原成员（MemberID/Index 不变），
	// 这是「掉线再上线自动恢复到原 Session」的关键路径，不受撤销/限额影响
	// （该成员此前已被授权加入）。
	if existing := rec.MemberByNodeID(req.NodeID); existing != nil {
		existing.ConnectionID = req.ConnectionID
		existing.LastSeen = now
		existing.LeaveAt = time.Time{}
		if len(req.Capabilities) > 0 {
			existing.Capabilities = append([]string(nil), req.Capabilities...)
		}
		if len(req.Candidates) > 0 {
			existing.Candidates = append([]protocol.Candidate(nil), req.Candidates...)
		}
		// 曾被拒绝/已离开的成员重新加入时，回到 pending 或 active。
		switch existing.Status {
		case protocol.MemberActive:
			// 保持
		default:
			if rec.RequireApproval {
				existing.Status = protocol.MemberPending
			} else {
				existing.Status = protocol.MemberActive
			}
		}
		return &JoinOutcome{
			Session:  cloneSession(rec),
			Member:   cloneMember(existing),
			Rejoined: true,
			IsOwner:  rec.OwnerMemberID == existing.ID,
		}, nil
	}

	// 新成员：先过撤销与限额。
	if rec.Revoked {
		return nil, ErrRevoked
	}
	if rec.JoinLimit > 0 && rec.JoinsUsed >= rec.JoinLimit {
		return nil, ErrJoinLimit
	}

	// 容量检查只统计已生效成员：pending 成员不占名额，
	// 否则「需审批 + 满员」会造成无人能再加入、owner 也无从审批的死结。
	//
	// 首个加入者就是会话创建者（owner）：审批策略约束的是「他以外的人」，
	// 否则 owner 自己会卡在 pending 而无法批准任何人。
	isFirst := rec.OwnerMemberID == ""
	pending := rec.RequireApproval && !isFirst
	if !pending && rec.MaxMembers > 0 && rec.ActiveMembers() >= rec.MaxMembers {
		return nil, ErrFull
	}

	idx := rec.freeIndex()
	if idx == 0 {
		return nil, ErrFull // 下标空间耗尽
	}

	status := protocol.MemberActive
	if pending {
		status = protocol.MemberPending
	}
	m := &MemberRecord{
		ID:           newMemberID(rec),
		SessionID:    rec.ID,
		Index:        idx,
		NodeID:       req.NodeID,
		PublicKey:    req.PublicKey,
		Role:         protocol.RoleMember,
		Status:       status,
		Capabilities: append([]string(nil), req.Capabilities...),
		Candidates:   append([]protocol.Candidate(nil), req.Candidates...),
		ConnectionID: req.ConnectionID,
		JoinedAt:     now,
		LastSeen:     now,
	}
	// 首个加入者成为 owner（创建会话的人随后 join）。
	isOwner := rec.OwnerMemberID == ""
	if isOwner {
		m.Role = protocol.RoleOwner
		rec.OwnerMemberID = m.ID
	}
	rec.Members = append(rec.Members, m)
	rec.JoinsUsed++

	return &JoinOutcome{
		Session:  cloneSession(rec),
		Member:   cloneMember(m),
		Rejoined: false,
		IsOwner:  isOwner,
	}, nil
}

// newMemberID 生成会话内唯一的成员 ID（前缀便于日志识别）。
// 15 字节随机 → 24 个 Crockford Base32 字符，碰撞概率可忽略；
// 极端情况下重试，保证返回的 ID 在会话内确实未被占用。
func newMemberID(rec *SessionRecord) string {
	for i := 0; i < 8; i++ {
		id := "mbr_" + randomToken(15)
		if rec.MemberByID(id) == nil {
			return id
		}
	}
	// 理论上不可达：退化为带计数器的高熵 ID。
	base := "mbr_" + randomToken(14)
	for n := 0; ; n++ {
		id := base + strconv.Itoa(n)
		if rec.MemberByID(id) == nil {
			return id
		}
	}
}

// UpdateMember 整体替换同 ID 成员。
func (s *MemoryStore) UpdateMember(ctx context.Context, sessionID string, m *MemberRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || m.ID == "" {
		return ErrMemberMissing
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[sessionID]
	if !ok {
		return ErrNotFound
	}
	for i, cur := range rec.Members {
		if cur.ID == m.ID {
			rec.Members[i] = cloneMember(m)
			return nil
		}
	}
	return ErrMemberMissing
}

// RemoveMember 移除成员。
func (s *MemoryStore) RemoveMember(ctx context.Context, sessionID, memberID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[sessionID]
	if !ok {
		return ErrNotFound
	}
	for i, cur := range rec.Members {
		if cur.ID == memberID {
			rec.Members = append(rec.Members[:i], rec.Members[i+1:]...)
			return nil
		}
	}
	return ErrMemberMissing
}

// MarkOffline 标记成员离线。仅当连接 ID 匹配时才生效，
// 避免「旧连接的关闭事件把新连接标记为离线」。
func (s *MemoryStore) MarkOffline(ctx context.Context, sessionID, memberID, connectionID string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[sessionID]
	if !ok {
		return ErrNotFound
	}
	m := rec.MemberByID(memberID)
	if m == nil {
		return ErrMemberMissing
	}
	if connectionID != "" && m.ConnectionID != connectionID {
		return ErrConflict // 已被新连接取代
	}
	m.ConnectionID = ""
	m.LeaveAt = now
	return nil
}

// Revoke 撤销 token。
func (s *MemoryStore) Revoke(ctx context.Context, sessionID string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = now
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[sessionID]
	if !ok {
		return ErrNotFound
	}
	rec.Revoked = true
	return nil
}

// RotateToken 替换 token 哈希与 join code，并清除撤销标记。
func (s *MemoryStore) RotateToken(ctx context.Context, sessionID, newHash, newCode string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = now
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[sessionID]
	if !ok {
		return ErrNotFound
	}
	// 维护 code 二级索引：删除旧 code，冲突时回滚。
	oldKey := normalizeCode(rec.JoinCode)
	newKey := normalizeCode(newCode)
	if newKey != "" && newKey != oldKey {
		if _, dup := s.byCode[newKey]; dup {
			return ErrConflict
		}
		if oldKey != "" {
			delete(s.byCode, oldKey)
		}
		s.byCode[newKey] = sessionID
	}
	rec.JoinCode = newCode
	rec.TokenHash = newHash
	rec.Revoked = false
	return nil
}

// Close 关闭会话。
func (s *MemoryStore) Close(ctx context.Context, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[sessionID]
	if !ok {
		return ErrNotFound
	}
	rec.Closed = true
	return nil
}

// Delete 删除会话及其二级索引。
func (s *MemoryStore) Delete(ctx context.Context, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byID[sessionID]
	if !ok {
		return ErrNotFound
	}
	if rec.JoinCode != "" {
		delete(s.byCode, normalizeCode(rec.JoinCode))
	}
	delete(s.byID, sessionID)
	return nil
}

// List 枚举会话（快照，按创建时间升序）。
func (s *MemoryStore) List(ctx context.Context, f ListFilter) ([]*SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*SessionRecord, 0, len(s.byID))
	for _, rec := range s.byID {
		if f.ClosedOnly && !rec.Closed {
			continue
		}
		if !f.ExpiredBefore.IsZero() && !(!rec.ExpiresAt.IsZero() && rec.ExpiresAt.Before(f.ExpiredBefore)) {
			continue
		}
		out = append(out, cloneSession(rec))
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// Count 返回当前会话总数（指标/测试用）。
func (s *MemoryStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byID)
}
