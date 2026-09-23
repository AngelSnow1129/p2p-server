package auth

import (
	"strings"
	"testing"
	"time"
)

func TestJoinTokenRoundTrip(t *testing.T) {
	tok, err := NewJoinToken()
	if err != nil {
		t.Fatal(err)
	}
	raw := tok.Raw()
	if len(raw) != TokenBytes {
		t.Fatalf("raw len = %d, want %d", len(raw), TokenBytes)
	}

	// 三种形态必须都能解析回同一条 token。
	cases := map[string]string{
		"token":     tok.TokenString(),
		"joincode":  tok.JoinCode(),
		"lowercode": strings.ToLower(tok.JoinCode()),
	}
	for name, s := range cases {
		got, err := DecodeToken(s)
		if err != nil {
			t.Fatalf("%s: decode %q: %v", name, s, err)
		}
		if string(got) != string(raw) {
			t.Fatalf("%s: decoded %x, want %x", name, got, raw)
		}
	}
}

func TestJoinCodeFormat(t *testing.T) {
	tok, _ := NewJoinToken()
	code := tok.JoinCode()
	if !strings.HasPrefix(code, "P2P-") {
		t.Fatalf("join code must start with P2P-: %q", code)
	}
	// 32 字节 → 52 个 base32 字符 → 13 组，含前缀共 12 个连字符分隔。
	body := strings.TrimPrefix(code, "P2P-")
	groups := strings.Split(body, "-")
	if len(groups) != 13 {
		t.Fatalf("expected 13 groups, got %d (%q)", len(groups), code)
	}
	for _, g := range groups {
		if len(g) > 4 {
			t.Fatalf("group %q longer than 4 chars", g)
		}
	}
	// 熵必须足够：52 个 base32 字符 ≈ 260 位，远高于可枚举的短码。
	if len(body) < 50 {
		t.Fatalf("join code entropy too low: %d chars", len(body))
	}
}

func TestDecodeTokenRejects(t *testing.T) {
	bad := []string{
		"",
		"123456",        // 6 位数字短码不可作为凭证
		"P2P-1234-5678", // 长度不足
		"not-a-token",   // 含非法字符且长度不足
		"P2P-!!!!-!!!!", // 非法 base32 字母表
		"P2P-0000-0000-0000-0000-0000-0000-0000-0000-0000-0000-0000-0000-0000",
		// ↑ 长度正好但不构成 32 字节的 base32（52 个字符才有 32 字节）
	}
	for _, s := range bad {
		if _, err := DecodeToken(s); err == nil {
			t.Fatalf("accepted invalid token %q", s)
		}
	}
}

func TestTokenHashIsStableAndNotRaw(t *testing.T) {
	tok, _ := NewJoinToken()
	h := tok.Hash()
	raw := tok.Raw()

	// 哈希不得等于原始 token（服务器只存哈希）。
	if h == string(raw) {
		t.Fatal("hash equals raw token")
	}
	if len(h) != 64 { // sha256 hex
		t.Fatalf("hash len = %d, want 64", len(h))
	}
	again, err := HashTokenString(tok.TokenString())
	if err != nil {
		t.Fatal(err)
	}
	if again != h {
		t.Fatalf("hash unstable: %s vs %s", again, h)
	}
	// 不同 token 必须有不同哈希。
	other, _ := NewJoinToken()
	if other.Hash() == h {
		t.Fatal("hash collision")
	}
}

func TestConstantTimeEqualHex(t *testing.T) {
	if !ConstantTimeEqualHex("abc", "abc") {
		t.Fatal("equal strings reported unequal")
	}
	if ConstantTimeEqualHex("abc", "abd") {
		t.Fatal("different strings reported equal")
	}
	if ConstantTimeEqualHex("abc", "abcd") {
		t.Fatal("prefix reported equal")
	}
}

// ---- 票据 ------------------------------------------------------------------

func TestTicketIssueVerify(t *testing.T) {
	key, err := NewTicketKey()
	if err != nil {
		t.Fatal(err)
	}
	ticket := key.Issue("sess_1", "mbr_a", "con_1", "owner", time.Minute)

	got, err := key.Verify(ticket)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got.SessionID != "sess_1" || got.MemberID != "mbr_a" ||
		got.ConnectionID != "con_1" || got.Role != "owner" {
		t.Fatalf("ticket mismatch: %+v", got)
	}
}

func TestTicketRejectsTamperedAndExpired(t *testing.T) {
	key, _ := NewTicketKey()
	ticket := key.Issue("sess_1", "mbr_a", "con_1", "member", time.Minute)

	// 篡改 body 必须失败。
	parts := strings.SplitN(ticket, ".", 2)
	tampered := parts[0][:len(parts[0])-2] + "AA" + "." + parts[1]
	if _, err := key.Verify(tampered); err == nil {
		t.Fatal("tampered ticket accepted")
	}
	// 另一个 key 签发的票据必须失败。
	other, _ := NewTicketKey()
	if _, err := other.Verify(ticket); err == nil {
		t.Fatal("ticket from another key accepted")
	}
	// 过期票据必须失败。
	expired := key.Issue("sess_1", "mbr_a", "con_1", "member", -time.Second)
	if _, err := key.Verify(expired); err != ErrTicketExpired {
		t.Fatalf("expired ticket err = %v", err)
	}
	// 结构非法的票据必须失败。
	for _, bad := range []string{"", "nodot", "a.b", "!!!.???"} {
		if _, err := key.Verify(bad); err == nil {
			t.Fatalf("malformed ticket accepted: %q", bad)
		}
	}
}

func TestTicketKeyFromString(t *testing.T) {
	key, _ := NewTicketKey()
	s := key.String()

	restored, err := TicketKeyFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	// 同一 key 的字符串必须能互相验证票据（多实例共享密钥的基础）。
	ticket := key.Issue("sess", "mbr", "con", "member", time.Minute)
	if _, err := restored.Verify(ticket); err != nil {
		t.Fatalf("restored key cannot verify: %v", err)
	}
	// 非法输入必须被拒。
	for _, bad := range []string{"", "short", strings.Repeat("A", 100)} {
		if _, err := TicketKeyFromString(bad); err == nil {
			t.Fatalf("accepted bad ticket key %q", bad)
		}
	}
}

// ---- 随机 ID -----------------------------------------------------------------

func TestRandomID(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id := RandomID("sess_", 12)
		if !strings.HasPrefix(id, "sess_") {
			t.Fatalf("prefix missing: %q", id)
		}
		if len(id) != len("sess_")+12 {
			t.Fatalf("unexpected length: %q", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id generated: %s", id)
		}
		seen[id] = true
	}
}
