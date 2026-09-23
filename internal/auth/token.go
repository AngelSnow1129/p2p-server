// Package auth 提供 p2psession 的全部密码学原语：
//
//   - Join Token：32 字节高熵能力凭证（capability token）。服务器只存其
//     SHA-256，拿到原始 token 即拥有加入能力；可过期/撤销/限额。
//   - Join Code：token 的人类可传形式 P2P-XXXX-XXXX-…，与底层 token 等熵，
//     不是 6 位数字那种可枚举的短码。
//   - Connection Ticket：join 成功后由服务器用 HMAC 签发的短期凭证，用于
//     /ws/control 与 /ws/relay 升级，证明“某 Member 的某条连接”。
//   - Node Identity：节点长期 Ed25519 身份与 join proof。
//
// Join Token 只用于授权，绝不是数据加密密钥；端到端数据密钥由客户端之间
// 用 X25519 临时密钥协商（见 e2e 子包/SDK），服务器不参与。
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
)

// TokenBytes 是 join token 的原始熵长度（256 位）。
const TokenBytes = 32

// b32 为无填充大写 Base32（RFC4648 字母表 A-Z2-7），适合人类转写。
var b32 = base32.StdEncoding.WithPadding(base32.NoPadding)

var (
	// ErrBadToken 表示 token 编码或长度非法。
	ErrBadToken = errors.New("invalid join token")
)

// JoinToken 封装原始 token 及其服务器侧存储形态。
type JoinToken struct {
	raw []byte
}

// NewJoinToken 生成 256 位随机 token。
func NewJoinToken() (*JoinToken, error) {
	raw := make([]byte, TokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	return &JoinToken{raw: raw}, nil
}

// FromRaw 从原始字节恢复 token。
func FromRaw(raw []byte) (*JoinToken, error) {
	if len(raw) != TokenBytes {
		return nil, ErrBadToken
	}
	cp := append([]byte(nil), raw...)
	return &JoinToken{raw: cp}, nil
}

// Raw 返回原始 32 字节（仅在创建/展示给创建者时使用）。
func (t *JoinToken) Raw() []byte { return append([]byte(nil), t.raw...) }

// TokenString 返回机器友好形式：Base64URL 无填充（43 字符），用于 --token。
func (t *JoinToken) TokenString() string {
	return b64u(t.raw)
}

// Hash 返回服务器应持久化的形态：SHA-256(raw) 的 hex。
// 即使数据库泄露，也无法直接拿哈希当 token 使用。
func (t *JoinToken) Hash() string {
	sum := sha256.Sum256(t.raw)
	return hex.EncodeToString(sum[:])
}

// JoinCode 返回人类可传形式 P2P-XXXX-XXXX-…（与 token 等熵）。
func (t *JoinToken) JoinCode() string { return EncodeJoinCode(t.raw) }

// HashTokenString 返回任意形态 token 字符串对应的存储哈希（hex）。
func HashTokenString(s string) (string, error) {
	raw, err := DecodeToken(s)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// EncodeJoinCode 把原始 token 编码为 P2P-XXXX-XXXX-… 形式（每 4 个 base32
// 字符一组）。32 字节 → 52 个 base32 字符 → 13 组。
func EncodeJoinCode(raw []byte) string {
	if len(raw) != TokenBytes {
		return ""
	}
	s := b32.EncodeToString(raw)
	var b strings.Builder
	b.WriteString("P2P-")
	for i := 0; i < len(s); i += 4 {
		if i > 0 {
			b.WriteByte('-')
		}
		end := i + 4
		if end > len(s) {
			end = len(s)
		}
		b.WriteString(s[i:end])
	}
	return b.String()
}

// DecodeToken 接受三种形态并返回原始 32 字节：
//
//	P2P-XXXX-XXXX-…  join code（base32，大小写与连字符不敏感）
//	Base32 裸串       同上，仅省略前缀与分组
//	Base64URL 串      机器形式（--token），区分大小写
//
// 判定顺序刻意明确，避免两种编码互相误判：
// 去掉前缀与连字符后，若全为 base32 字母表（A-Z2-7）则按 base32 解析，
// 否则按 base64url 解析——这正好覆盖我们自己的两种生成器。
func DecodeToken(in string) ([]byte, error) {
	s := strings.TrimSpace(in)
	if s == "" {
		return nil, ErrBadToken
	}

	// 带 P2P- 前缀即明确是 join code。
	if len(s) >= 4 && strings.EqualFold(s[:4], "P2P-") {
		body := strings.ToUpper(s[4:])
		body = strings.ReplaceAll(body, "-", "")
		body = strings.ReplaceAll(body, " ", "")
		if raw, err := b32.DecodeString(body); err == nil && len(raw) == TokenBytes {
			return raw, nil
		}
		return nil, ErrBadToken
	}

	// 无前缀：看字符集决定编码族。
	// 注意必须先判断再转换大小写——base64url 区分大小写，
	// 早期版本无条件 ToUpper 会把 token 解成完全不同的字节。
	compact := strings.ReplaceAll(s, "-", "")
	compact = strings.ReplaceAll(compact, "_", "")
	if isBase32Alphabet(compact) {
		if raw, err := b32.DecodeString(strings.ToUpper(compact)); err == nil && len(raw) == TokenBytes {
			return raw, nil
		}
	}
	// base64url（保留原始大小写）。
	if raw, err := decodeB64u(s); err == nil && len(raw) == TokenBytes {
		return raw, nil
	}
	// 最后兜底：允许大小写不敏感的 base32 裸串（人工转写场景）。
	if raw, err := b32.DecodeString(strings.ToUpper(compact)); err == nil && len(raw) == TokenBytes {
		return raw, nil
	}
	return nil, ErrBadToken
}

// isBase32Alphabet 判断字符串是否只含 base32 标准字母表（A-Z 与 2-7）。
func isBase32Alphabet(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= 'a' && r <= 'z':
		case r >= '2' && r <= '7':
		default:
			return false
		}
	}
	return true
}

// ConstantTimeEqualHex 做恒定时间的 hex 哈希比较（用于 token 哈希校验）。
func ConstantTimeEqualHex(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// ---- base64url（无填充）辅助 ------------------------------------------------

// b64u 编码为 Base64URL 无填充字符串。
func b64u(raw []byte) string {
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeB64u 解码 Base64URL（容忍有/无填充）。
func decodeB64u(s string) ([]byte, error) {
	if raw, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return raw, nil
	}
	return base64.URLEncoding.DecodeString(s)
}
