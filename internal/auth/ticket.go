package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
)

// 连接票据（Connection Ticket）。
//
// join 成功后服务器为每个 (Session, Member, Connection) 签发一张短期 HMAC 票据。
// 客户端随后携带该票据升级 /ws/control 与 /ws/relay，从而：
//   - 把 HTTP 授权与 WebSocket 升级解耦（Token 不必再出现在 WS 上）；
//   - 让数据面只凭票取信，无需重复校验能力凭证；
//   - 天然表达“这条 WS 属于哪个 Session/Member/Connection”，支持多连接与顶替。
//
// 票据是无状态自校验的（HMAC-SHA256），因此服务器可水平扩展：
// 任意实例共享同一 ticket key 即可验证，无需查询共享存储。

// TicketKeyBytes 是 HMAC 密钥长度。
const TicketKeyBytes = 32

var (
	// ErrTicketInvalid 票据签名/格式非法。
	ErrTicketInvalid = errors.New("invalid connection ticket")
	// ErrTicketExpired 票据已过期。
	ErrTicketExpired = errors.New("connection ticket expired")
)

// TicketKey 是签发/校验票据的 HMAC 密钥。
type TicketKey struct {
	key []byte
}

// NewTicketKey 生成随机票据密钥（单实例默认）。
func NewTicketKey() (*TicketKey, error) {
	k := make([]byte, TicketKeyBytes)
	if _, err := rand.Read(k); err != nil {
		return nil, err
	}
	return &TicketKey{key: k}, nil
}

// TicketKeyFromString 从配置的 base64/原始字符串恢复密钥（多实例部署时共享）。
func TicketKeyFromString(s string) (*TicketKey, error) {
	if s == "" {
		return nil, errors.New("empty ticket key")
	}
	raw, err := decodeB64u(strings.TrimSpace(s))
	if err != nil || len(raw) != TicketKeyBytes {
		return nil, fmt.Errorf("ticket key must be %d bytes base64url", TicketKeyBytes)
	}
	return &TicketKey{key: raw}, nil
}

// String 返回可放入配置的 base64url 形式。
func (t *TicketKey) String() string { return b64u(t.key) }

// Ticket 是票据的声明内容。
type Ticket struct {
	SessionID    string
	MemberID     string
	ConnectionID string
	// Role 为 member / owner，用于服务端做权限判定。
	Role string
	// ExpiresAt 为到期时间（Unix 秒）。
	ExpiresAt int64
}

// Issue 为一条连接签发票据。
func (t *TicketKey) Issue(sessionID, memberID, connID, role string, ttl time.Duration) string {
	exp := time.Now().Add(ttl).Unix()
	body := strings.Join([]string{sessionID, memberID, connID, role, fmt.Sprint(exp)}, "\x1f")
	mac := hmac.New(sha256.New, t.key)
	mac.Write([]byte(body))
	return base64.RawURLEncoding.EncodeToString([]byte(body)) + "." +
		base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// Verify 校验票据并返回其声明。
func (t *TicketKey) Verify(ticket string) (*Ticket, error) {
	parts := strings.SplitN(ticket, ".", 2)
	if len(parts) != 2 {
		return nil, ErrTicketInvalid
	}
	bodyRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrTicketInvalid
	}
	sigRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrTicketInvalid
	}
	mac := hmac.New(sha256.New, t.key)
	mac.Write(bodyRaw)
	if !hmac.Equal(sigRaw, mac.Sum(nil)) {
		return nil, ErrTicketInvalid
	}
	fields := strings.Split(string(bodyRaw), "\x1f")
	if len(fields) != 5 {
		return nil, ErrTicketInvalid
	}
	var exp int64
	if _, err := fmt.Sscanf(fields[4], "%d", &exp); err != nil {
		return nil, ErrTicketInvalid
	}
	if time.Now().Unix() > exp {
		return nil, ErrTicketExpired
	}
	return &Ticket{
		SessionID:    fields[0],
		MemberID:     fields[1],
		ConnectionID: fields[2],
		Role:         fields[3],
		ExpiresAt:    exp,
	}, nil
}

// ---- 随机标识 --------------------------------------------------------------

var idAlphabet = []byte("0123456789ABCDEFGHJKMNPQRSTVWXYZ") // Crockford Base32，去易混字符

// RandomID 生成前缀 + 随机 base32 串（如 sess_01JX…、mbr_…、con_…）。
// n 为随机字节数（编码后约 1.6n 个字符）。
func RandomID(prefix string, n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand 失败属不可恢复环境错误；退化用时间戳避免返回空 ID。
		binary.BigEndian.PutUint64(buf, uint64(time.Now().UnixNano()))
	}
	var b strings.Builder
	b.WriteString(prefix)
	for _, by := range buf {
		b.WriteByte(idAlphabet[int(by)%len(idAlphabet)])
	}
	return b.String()
}
