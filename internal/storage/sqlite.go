// SQLite 存储实现：把会话状态持久化到单个数据库文件。
//
// 适用场景：单机自托管（docker run 挂一个卷即可）。
// 不适用：多实例共享——SQLite 是本地文件，多进程不能同时写同一份。
// 多实例请改用共享存储实现（接口见 store.go）。
//
// # 与 MemoryStore 的关系
//
// 本实现**刻意复用** store.go 里 SessionRecord 的策略方法
// （freeIndex / MemberByNodeID / ActiveMembers / Expired），
// 只在事务内做「载入 → 用同一套规则变更 → 写回」。
//
// 这样做的原因：会话容量、撤销、审批、下标复用这些规则只能有一个
// 权威实现。若在 SQL 里重写一遍 WHERE 条件，两条路径迟早会漂移
// （例如「pending 成员不占名额」这条规则很容易漏），
// 而存储层的行为不一致是最难排查的一类缺陷。
//
// # 并发模型
//
// WAL 日志 + busy_timeout + _txlock=immediate：
//   - WAL 让「多读 + 单写」不互相阻塞；
//   - immediate 让写事务在一开始就拿写锁，避免「读后升级写锁」时
//     两个事务同时失败（SQLITE_BUSY 的经典来源）；
//   - busy_timeout 让偶发竞争转为短暂等待而不是直接报错。
//
// 连接池固定为 1 条连接。这是刻意的：SQLite 的写锁是库级的，
// 多连接只会把锁竞争搬到 Go 的连接池里，换来的是需要处理的
// SQLITE_BUSY 分支；而本服务的信令写入极少（心跳**不写库**，
// 见 session 包），单连接不会成为瓶颈，却能让正确性一目了然。
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // 纯 Go 驱动：CGO_ENABLED=0 也能静态编译

	"p2psession/internal/protocol"
)

// sqliteSchema 是建表语句。
//
// 时间统一用「Unix 纳秒整数」存储：SQLite 没有原生时间类型，
// 存整数便于比较与索引，也避免时区/格式解析的歧义。
// 0 表示零值时间（如未设置过期）。
//
// 会话的标量字段与成员分表：成员变更（审批、候选更新、上下线）
// 只写一行 members，不必重写整个会话——这是写入放大的关键抑制点。
const sqliteSchema = `
CREATE TABLE IF NOT EXISTS sessions (
    id               TEXT    PRIMARY KEY,
    mode             TEXT    NOT NULL,
    join_code        TEXT    NOT NULL DEFAULT '',
    join_code_norm   TEXT    NOT NULL DEFAULT '',
    token_hash       TEXT    NOT NULL DEFAULT '',
    join_limit       INTEGER NOT NULL DEFAULT 0,
    joins_used       INTEGER NOT NULL DEFAULT 0,
    max_members      INTEGER NOT NULL DEFAULT 0,
    require_approval INTEGER NOT NULL DEFAULT 0,
    owner_member_id  TEXT    NOT NULL DEFAULT '',
    revoked          INTEGER NOT NULL DEFAULT 0,
    closed           INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL,
    expires_at       INTEGER NOT NULL DEFAULT 0,
    idle_timeout     INTEGER NOT NULL DEFAULT 0
);

-- join code 二级索引：只对非空 code 建唯一索引，让多条空 code 共存。
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_join_code
    ON sessions (join_code_norm) WHERE join_code_norm <> '';

CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions (expires_at);

CREATE TABLE IF NOT EXISTS members (
    session_id    TEXT    NOT NULL,
    id            TEXT    NOT NULL,
    member_index  INTEGER NOT NULL,
    node_id       TEXT    NOT NULL,
    public_key    TEXT    NOT NULL,
    role          TEXT    NOT NULL,
    status        TEXT    NOT NULL,
    capabilities  TEXT    NOT NULL DEFAULT '[]',
    candidates    TEXT    NOT NULL DEFAULT '[]',
    connection_id TEXT    NOT NULL DEFAULT '',
    joined_at     INTEGER NOT NULL,
    last_seen     INTEGER NOT NULL,
    leave_at      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, id),
    FOREIGN KEY (session_id) REFERENCES sessions (id) ON DELETE CASCADE
);

-- 重连恢复按 (session_id, node_id) 查成员。
CREATE INDEX IF NOT EXISTS idx_members_node ON members (session_id, node_id);

-- 数据面下标在会话内唯一（复用依赖应用层计算，这里只保证不重复写坏）。
CREATE UNIQUE INDEX IF NOT EXISTS idx_members_index ON members (session_id, member_index);
`

// SQLiteStore 是 storage.Store 的 SQLite 实现。
type SQLiteStore struct {
	db   *sql.DB
	path string
}

// 编译期断言：接口实现完整性。
var _ Store = (*SQLiteStore)(nil)

// NewSQLite 打开（必要时创建）数据库并建表。
//
// path 为数据库文件路径，特例：
//   - ":memory:" 或 "" → 内存库（测试用；进程结束即丢）
func NewSQLite(path string) (*SQLiteStore, error) {
	dsn, err := sqliteDSN(path)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// 见文件头「并发模型」：单连接消除锁竞争，代价是写入串行化。
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	// 文件库做一次完整性检查，尽早暴露损坏或权限问题；
	// 内存库没有检查的必要（刚建好）。
	if !isMemoryPath(path) {
		var res string
		if err := db.QueryRow(`PRAGMA quick_check`).Scan(&res); err != nil {
			db.Close()
			return nil, fmt.Errorf("integrity check: %w", err)
		}
		if res != "ok" {
			db.Close()
			return nil, fmt.Errorf("sqlite integrity check failed: %s", res)
		}
	}

	return &SQLiteStore{db: db, path: path}, nil
}

// isMemoryPath 判断是否为内存库。
func isMemoryPath(path string) bool {
	return path == "" || path == ":memory:" || strings.HasPrefix(path, "file::memory:")
}

// sqliteDSN 组装连接串（含 pragma 与事务模式）。
func sqliteDSN(path string) (string, error) {
	base := path
	if isMemoryPath(path) {
		// 共享缓存让同一进程内的多次连接看到同一个内存库。
		base = "file::memory:?cache=shared"
	} else {
		if strings.HasPrefix(path, "file:") {
			base = path
		} else {
			base = "file:" + path
		}
	}

	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}

	params := []string{
		// 崩溃安全与并发：WAL 允许读写并行，NORMAL 在 WAL 下已足够安全
		// （只在断电时可能丢最后若干事务，不会损坏库）。
		"_pragma=journal_mode(WAL)",
		"_pragma=synchronous(NORMAL)",
		// 外键级联删除依赖它，必须显式开启（SQLite 默认关闭）。
		"_pragma=foreign_keys(ON)",
		// 竞争时等待而不是立刻 SQLITE_BUSY。
		"_pragma=busy_timeout(5000)",
		// 写事务一开始就取写锁，避免「读→升级写锁」阶段的死锁式失败。
		"_txlock=immediate",
	}
	if isMemoryPath(path) {
		// 内存库不支持 WAL（无文件），去掉该 pragma 以免报错。
		params = params[1:]
	}
	return base + sep + strings.Join(params, "&"), nil
}

// CloseDB 关闭数据库连接并释放文件句柄。
//
// 刻意不叫 Close：Store 接口已有 Close(ctx, sessionID) 表示「关闭会话」，
// 同名会让实现无法满足接口，也是极易误用的命名。
func (s *SQLiteStore) CloseDB() error { return s.db.Close() }

// Path 返回数据库文件路径（供日志/诊断）。
func (s *SQLiteStore) Path() string { return s.path }

// ---- 时间与 JSON 编解码 ----------------------------------------------------

// timeToSQL 把时间转为 Unix 纳秒；零值转为 0。
func timeToSQL(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

// timeFromSQL 还原时间；0 返回零值时间。
func timeFromSQL(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// marshalStringList 序列化字符串切片（nil → "[]"，避免 NULL 比较的坑）。
func marshalStringList(v []string) (string, error) {
	if len(v) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// marshalCandidates 序列化候选地址列表。
func marshalCandidates(v []protocol.Candidate) (string, error) {
	if len(v) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// unmarshalStringList 反序列化字符串切片。
func unmarshalStringList(s string) ([]string, error) {
	if s == "" || s == "[]" {
		return nil, nil
	}
	var v []string
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	return v, nil
}

// unmarshalCandidates 反序列化候选地址列表。
func unmarshalCandidates(s string) ([]protocol.Candidate, error) {
	if s == "" || s == "[]" {
		return nil, nil
	}
	var v []protocol.Candidate
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, err
	}
	return v, nil
}

// ---- 事务辅助 --------------------------------------------------------------

// querier 抽象 *sql.DB 与 *sql.Tx 的公共能力，让读写逻辑在
// 「事务内」与「事务外」共用同一份代码。
//
// *sql.DB 与 *sql.Tx 都实现这三个方法，因此无需类型断言
// （早期版本用 q.(*sql.Tx) 取事务，一旦传入非事务实现就会 panic）。
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// inTx 在写事务中执行 fn，并在返回错误时回滚。
//
// 所有「读后写」的操作都必须走这里：否则并发加入会突破 max_members
// （store.go 接口注释要求的原子性）。
func (s *SQLiteStore) inTx(ctx context.Context, fn func(q querier) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // 提交成功后 Rollback 是无害的 no-op

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// ---- 载入 ------------------------------------------------------------------

// loadSession 载入会话及其全部成员（快照语义，与 MemoryStore 一致）。
func (s *SQLiteStore) loadSession(ctx context.Context, q querier, sessionID string) (*SessionRecord, error) {
	row := q.QueryRowContext(ctx, `
        SELECT id, mode, join_code, token_hash, join_limit, joins_used,
               max_members, require_approval, owner_member_id,
               revoked, closed, created_at, expires_at, idle_timeout
          FROM sessions WHERE id = ?`, sessionID)

	rec, err := scanSession(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	members, err := loadMembers(ctx, q, sessionID)
	if err != nil {
		return nil, err
	}
	rec.Members = members
	return rec, nil
}

// rowScanner 抽象 *sql.Row 与 *sql.Rows 的 Scan。
type rowScanner interface{ Scan(dest ...any) error }

// scanSession 从一行读取会话标量字段。
func scanSession(sc rowScanner) (*SessionRecord, error) {
	var (
		rec       SessionRecord
		revoked   int
		closed    int
		approval  int
		createdAt int64
		expiresAt int64
		idleNanos int64
	)
	err := sc.Scan(
		&rec.ID, &rec.Mode, &rec.JoinCode, &rec.TokenHash,
		&rec.JoinLimit, &rec.JoinsUsed, &rec.MaxMembers, &approval,
		&rec.OwnerMemberID, &revoked, &closed,
		&createdAt, &expiresAt, &idleNanos,
	)
	if err != nil {
		return nil, err
	}
	rec.Revoked = revoked != 0
	rec.Closed = closed != 0
	rec.RequireApproval = approval != 0
	rec.CreatedAt = timeFromSQL(createdAt)
	rec.ExpiresAt = timeFromSQL(expiresAt)
	rec.IdleTimeout = time.Duration(idleNanos)
	return &rec, nil
}

// loadMembers 读取会话的全部成员。
func loadMembers(ctx context.Context, q querier, sessionID string) ([]*MemberRecord, error) {
	rows, err := q.QueryContext(ctx, `
        SELECT id, member_index, node_id, public_key, role, status,
               capabilities, candidates, connection_id, joined_at, last_seen, leave_at
          FROM members WHERE session_id = ? ORDER BY member_index`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*MemberRecord
	for rows.Next() {
		m, err := scanMember(rows, sessionID)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// scanMember 从一行读取成员记录。
func scanMember(sc rowScanner, sessionID string) (*MemberRecord, error) {
	var (
		m        MemberRecord
		idx      int64
		caps     string
		cands    string
		joinedAt int64
		lastSeen int64
		leaveAt  int64
	)
	err := sc.Scan(
		&m.ID, &idx, &m.NodeID, &m.PublicKey, &m.Role, &m.Status,
		&caps, &cands, &m.ConnectionID, &joinedAt, &lastSeen, &leaveAt,
	)
	if err != nil {
		return nil, err
	}
	m.SessionID = sessionID
	m.Index = uint16(idx)
	if m.Capabilities, err = unmarshalStringList(caps); err != nil {
		return nil, fmt.Errorf("member %s capabilities: %w", m.ID, err)
	}
	if m.Candidates, err = unmarshalCandidates(cands); err != nil {
		return nil, fmt.Errorf("member %s candidates: %w", m.ID, err)
	}
	m.JoinedAt = timeFromSQL(joinedAt)
	m.LastSeen = timeFromSQL(lastSeen)
	m.LeaveAt = timeFromSQL(leaveAt)
	return &m, nil
}

// ---- Store 实现：创建与读取 ------------------------------------------------

// Create 新建会话。
func (s *SQLiteStore) Create(ctx context.Context, rec *SessionRecord) error {
	if rec == nil || rec.ID == "" {
		return ErrConflict
	}
	return s.inTx(ctx, func(q querier) error {
		// 先查重：给出明确的 ErrConflict，而不是把驱动的主键错误透出去。
		var exists int
		err := q.QueryRowContext(ctx, `SELECT 1 FROM sessions WHERE id = ?`, rec.ID).Scan(&exists)
		if err == nil {
			return ErrConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		norm := normalizeCode(rec.JoinCode)
		if norm != "" {
			err := q.QueryRowContext(ctx,
				`SELECT 1 FROM sessions WHERE join_code_norm = ?`, norm).Scan(&exists)
			if err == nil {
				return ErrConflict // join code 已被占用
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}

		_, err = q.(*sql.Tx).ExecContext(ctx, `
            INSERT INTO sessions (
                id, mode, join_code, join_code_norm, token_hash,
                join_limit, joins_used, max_members, require_approval,
                owner_member_id, revoked, closed, created_at, expires_at, idle_timeout
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			rec.ID, rec.Mode, rec.JoinCode, norm, rec.TokenHash,
			rec.JoinLimit, rec.JoinsUsed, rec.MaxMembers, boolToInt(rec.RequireApproval),
			rec.OwnerMemberID, boolToInt(rec.Revoked), boolToInt(rec.Closed),
			timeToSQL(rec.CreatedAt), timeToSQL(rec.ExpiresAt), int64(rec.IdleTimeout),
		)
		if err != nil {
			// 唯一索引竞争（并发创建同一 code）也归一为 ErrConflict。
			if isUniqueViolation(err) {
				return ErrConflict
			}
			return err
		}

		// 允许带成员一并创建（与 MemoryStore 行为一致）。
		for _, m := range rec.Members {
			if err := insertMember(ctx, q.(*sql.Tx), rec.ID, m); err != nil {
				return err
			}
		}
		return nil
	})
}

// boolToInt 把 bool 转为 SQLite 的 0/1。
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// isUniqueViolation 判断是否唯一约束冲突（驱动不导出具体错误类型，只能按文本）。
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "constraint failed") && strings.Contains(msg, "unique")
}

// insertMember 写入一条成员记录。
func insertMember(ctx context.Context, tx *sql.Tx, sessionID string, m *MemberRecord) error {
	caps, err := marshalStringList(m.Capabilities)
	if err != nil {
		return err
	}
	cands, err := marshalCandidates(m.Candidates)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
        INSERT INTO members (
            session_id, id, member_index, node_id, public_key, role, status,
            capabilities, candidates, connection_id, joined_at, last_seen, leave_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		sessionID, m.ID, int64(m.Index), m.NodeID, m.PublicKey, m.Role, m.Status,
		caps, cands, m.ConnectionID,
		timeToSQL(m.JoinedAt), timeToSQL(m.LastSeen), timeToSQL(m.LeaveAt),
	)
	if err != nil && isUniqueViolation(err) {
		// 下标或成员 ID 冲突：按容量类错误上报更贴近事实。
		return ErrConflict
	}
	return err
}

// Get 读取会话快照。
func (s *SQLiteStore) Get(ctx context.Context, sessionID string) (*SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.loadSession(ctx, s.db, sessionID)
}

// GetByJoinCode 按 join code 反查会话（大小写/连字符不敏感）。
func (s *SQLiteStore) GetByJoinCode(ctx context.Context, code string) (*SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	norm := normalizeCode(code)
	if norm == "" {
		return nil, ErrNotFound
	}
	var sessionID string
	err := s.db.QueryRowContext(ctx,
		`SELECT id FROM sessions WHERE join_code_norm = ?`, norm).Scan(&sessionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return s.loadSession(ctx, s.db, sessionID)
}

// ---- Store 实现：加入（原子）------------------------------------------------

// Join 原子地校验并写入成员。
//
// 策略判断与 MemoryStore 完全一致（复用 SessionRecord 的方法），
// 只有「读改写」被包在一个 immediate 写事务里，从而保证并发加入
// 不会突破 max_members —— 这是 store.go 接口注释要求的原子性。
func (s *SQLiteStore) Join(ctx context.Context, sessionID string, req JoinRequest) (*JoinOutcome, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}

	var out *JoinOutcome
	err := s.inTx(ctx, func(q querier) error {
		rec, err := s.loadSession(ctx, q, sessionID)
		if err != nil {
			return err
		}
		if rec.Closed {
			return ErrClosed
		}
		if rec.Expired(now) {
			return ErrExpired
		}

		// 同一 NodeID 重新加入：复用原成员（MemberID/Index 不变），
		// 不受撤销/限额影响（该成员此前已被授权）。
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
			if err := updateMemberRow(ctx, q, sessionID, existing); err != nil {
				return err
			}
			out = &JoinOutcome{
				Session:  rec,
				Member:   cloneMember(existing),
				Rejoined: true,
				IsOwner:  rec.OwnerMemberID == existing.ID,
			}
			return nil
		}

		// 新成员：先过撤销与限额。
		if rec.Revoked {
			return ErrRevoked
		}
		if rec.JoinLimit > 0 && rec.JoinsUsed >= rec.JoinLimit {
			return ErrJoinLimit
		}

		// 容量只统计已生效成员：pending 不占名额，否则「需审批 + 满员」
		// 会造成死结。首个加入者即 owner，不受审批约束。
		isFirst := rec.OwnerMemberID == ""
		pending := rec.RequireApproval && !isFirst
		if !pending && rec.MaxMembers > 0 && rec.ActiveMembers() >= rec.MaxMembers {
			return ErrFull
		}

		idx := rec.freeIndex()
		if idx == 0 {
			return ErrFull // 下标空间耗尽
		}

		status := protocol.MemberActive
		if pending {
			status = protocol.MemberPending
		}
		m := &MemberRecord{
			ID:           newMemberID(rec),
			SessionID:    sessionID,
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
		isOwner := rec.OwnerMemberID == ""
		if isOwner {
			m.Role = protocol.RoleOwner
			rec.OwnerMemberID = m.ID
		}

		if err := insertMemberQ(ctx, q, sessionID, m); err != nil {
			return err
		}
		if isOwner {
			// 只在成为 owner 时更新会话行，避免每次加入都写 sessions。
			if _, err := q.ExecContext(ctx, `
                UPDATE sessions SET owner_member_id = ?, joins_used = joins_used + 1
                 WHERE id = ?`, m.ID, sessionID); err != nil {
				return err
			}
		} else {
			if _, err := q.ExecContext(ctx,
				`UPDATE sessions SET joins_used = joins_used + 1 WHERE id = ?`,
				sessionID); err != nil {
				return err
			}
		}
		rec.JoinsUsed++
		rec.Members = append(rec.Members, m)

		out = &JoinOutcome{
			Session:  rec,
			Member:   cloneMember(m),
			Rejoined: false,
			IsOwner:  isOwner,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---- Store 实现：成员变更 ---------------------------------------------------

// UpdateMember 整体替换同 ID 成员。
func (s *SQLiteStore) UpdateMember(ctx context.Context, sessionID string, m *MemberRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || m.ID == "" {
		return ErrMemberMissing
	}
	return s.inTx(ctx, func(q querier) error {
		var exists int
		err := q.QueryRowContext(ctx,
			`SELECT 1 FROM sessions WHERE id = ?`, sessionID).Scan(&exists)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		return updateMemberRow(ctx, q, sessionID, m)
	})
}

// updateMemberRow 用传入记录覆盖 members 中的一行。
// 影响行数为 0 表示成员不存在。
func updateMemberRow(ctx context.Context, q querier, sessionID string, m *MemberRecord) error {
	caps, err := marshalStringList(m.Capabilities)
	if err != nil {
		return err
	}
	cands, err := marshalCandidates(m.Candidates)
	if err != nil {
		return err
	}
	res, err := q.ExecContext(ctx, `
        UPDATE members SET
            member_index = ?, node_id = ?, public_key = ?, role = ?, status = ?,
            capabilities = ?, candidates = ?, connection_id = ?,
            joined_at = ?, last_seen = ?, leave_at = ?
         WHERE session_id = ? AND id = ?`,
		int64(m.Index), m.NodeID, m.PublicKey, m.Role, m.Status,
		caps, cands, m.ConnectionID,
		timeToSQL(m.JoinedAt), timeToSQL(m.LastSeen), timeToSQL(m.LeaveAt),
		sessionID, m.ID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrConflict
		}
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		// 区分「会话不存在」与「成员不存在」，便于上层映射错误码。
		var one int
		if err := q.QueryRowContext(ctx,
			`SELECT 1 FROM sessions WHERE id = ?`, sessionID).Scan(&one); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		return ErrMemberMissing
	}
	return nil
}

// insertMemberQ 在给定 querier 上插入成员。
func insertMemberQ(ctx context.Context, q querier, sessionID string, m *MemberRecord) error {
	caps, err := marshalStringList(m.Capabilities)
	if err != nil {
		return err
	}
	cands, err := marshalCandidates(m.Candidates)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `
        INSERT INTO members (
            session_id, id, member_index, node_id, public_key, role, status,
            capabilities, candidates, connection_id, joined_at, last_seen, leave_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		sessionID, m.ID, int64(m.Index), m.NodeID, m.PublicKey, m.Role, m.Status,
		caps, cands, m.ConnectionID,
		timeToSQL(m.JoinedAt), timeToSQL(m.LastSeen), timeToSQL(m.LeaveAt),
	)
	if err != nil && isUniqueViolation(err) {
		return ErrConflict
	}
	return err
}

// RemoveMember 移除成员（离开或宽限期结束）。
func (s *SQLiteStore) RemoveMember(ctx context.Context, sessionID, memberID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.inTx(ctx, func(q querier) error {
		var one int
		err := q.QueryRowContext(ctx,
			`SELECT 1 FROM sessions WHERE id = ?`, sessionID).Scan(&one)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		res, err := q.ExecContext(ctx,
			`DELETE FROM members WHERE session_id = ? AND id = ?`, sessionID, memberID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrMemberMissing
		}
		return nil
	})
}

// MarkOffline 标记成员离线。仅当连接 ID 匹配时生效，
// 避免旧连接的关闭事件把新连接标记为离线。
func (s *SQLiteStore) MarkOffline(ctx context.Context, sessionID, memberID, connectionID string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now()
	}
	return s.inTx(ctx, func(q querier) error {
		var curConn string
		err := q.QueryRowContext(ctx,
			`SELECT connection_id FROM members WHERE session_id = ? AND id = ?`,
			sessionID, memberID).Scan(&curConn)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// 区分会话与成员不存在。
				var one int
				if e2 := q.QueryRowContext(ctx,
					`SELECT 1 FROM sessions WHERE id = ?`, sessionID).Scan(&one); e2 != nil {
					if errors.Is(e2, sql.ErrNoRows) {
						return ErrNotFound
					}
					return e2
				}
				return ErrMemberMissing
			}
			return err
		}
		if connectionID != "" && curConn != connectionID {
			return ErrConflict // 已被新连接取代
		}
		_, err = q.ExecContext(ctx, `
            UPDATE members SET connection_id = '', leave_at = ?
             WHERE session_id = ? AND id = ?`,
			timeToSQL(now), sessionID, memberID)
		return err
	})
}

// ---- Store 实现：会话级变更 ------------------------------------------------

// Revoke 撤销 token。
func (s *SQLiteStore) Revoke(ctx context.Context, sessionID string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = now
	return s.execSessionUpdate(ctx, `UPDATE sessions SET revoked = 1 WHERE id = ?`, sessionID)
}

// RotateToken 替换 token 哈希与 join code，并清除撤销标记。
func (s *SQLiteStore) RotateToken(ctx context.Context, sessionID, newHash, newCode string, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = now
	return s.inTx(ctx, func(q querier) error {
		var oldCode string
		err := q.QueryRowContext(ctx,
			`SELECT join_code FROM sessions WHERE id = ?`, sessionID).Scan(&oldCode)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}

		norm := normalizeCode(newCode)
		if norm != "" && norm != normalizeCode(oldCode) {
			// 新 code 若已被别的会话占用则拒绝（唯一索引也会拦，但这里
			// 给出明确的 ErrConflict，而不是透出驱动错误）。
			var other string
			err := q.QueryRowContext(ctx,
				`SELECT id FROM sessions WHERE join_code_norm = ?`, norm).Scan(&other)
			if err == nil {
				return ErrConflict
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}

		_, err = q.ExecContext(ctx, `
            UPDATE sessions SET join_code = ?, join_code_norm = ?, token_hash = ?, revoked = 0
             WHERE id = ?`, newCode, norm, newHash, sessionID)
		if err != nil {
			if isUniqueViolation(err) {
				return ErrConflict
			}
			return err
		}
		return nil
	})
}

// Close 关闭会话。
func (s *SQLiteStore) Close(ctx context.Context, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.execSessionUpdate(ctx, `UPDATE sessions SET closed = 1 WHERE id = ?`, sessionID)
}

// execSessionUpdate 执行一条会话级 UPDATE，行数为 0 时返回 ErrNotFound。
func (s *SQLiteStore) execSessionUpdate(ctx context.Context, query string, args ...any) error {
	res, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// Delete 彻底删除会话（成员由外键级联删除）。
func (s *SQLiteStore) Delete(ctx context.Context, sessionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 显式删成员：不依赖调用方是否开启 foreign_keys，
	// 也让「删会话」在任何配置下都不会留下孤儿成员行。
	return s.inTx(ctx, func(q querier) error {
		if _, err := q.ExecContext(ctx,
			`DELETE FROM members WHERE session_id = ?`, sessionID); err != nil {
			return err
		}
		res, err := q.ExecContext(ctx, `DELETE FROM sessions WHERE id = ?`, sessionID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// List 枚举会话（快照，按创建时间升序）。过滤与排序语义与 MemoryStore 一致。
func (s *SQLiteStore) List(ctx context.Context, f ListFilter) ([]*SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var (
		where []string
		args  []any
	)
	if f.ClosedOnly {
		where = append(where, "closed = 1")
	}
	if !f.ExpiredBefore.IsZero() {
		// 只返回「有到期时间且早于 ExpiredBefore」的会话。
		where = append(where, "expires_at <> 0 AND expires_at < ?")
		args = append(args, timeToSQL(f.ExpiredBefore))
	}

	q := `SELECT id, mode, join_code, token_hash, join_limit, joins_used,
                 max_members, require_approval, owner_member_id,
                 revoked, closed, created_at, expires_at, idle_timeout
            FROM sessions`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created_at"
	if f.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, f.Limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*SessionRecord
	for rows.Next() {
		rec, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// 成员单独按会话批量载入（避免 N+1 查询）。
	for _, rec := range out {
		members, err := loadMembers(ctx, s.db, rec.ID)
		if err != nil {
			return nil, err
		}
		rec.Members = members
	}
	return out, nil
}

// Count 返回会话总数（指标/测试用）。
func (s *SQLiteStore) Count() int {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&n); err != nil {
		return 0
	}
	return n
}
