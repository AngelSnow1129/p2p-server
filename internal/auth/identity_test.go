package auth

import (
	"crypto/ed25519"
	"os"
	"path/filepath"
	"testing"
)

func TestIdentityGenerateAndNodeID(t *testing.T) {
	id, err := GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	pub := id.Public()
	if len(pub) != ed25519.PublicKeySize {
		t.Fatalf("pub size = %d", len(pub))
	}
	nodeID := id.NodeID()
	// NodeID = sha256(pub)[:16] 的 hex = 32 字符。
	if len(nodeID) != 32 {
		t.Fatalf("node id len = %d, want 32: %q", len(nodeID), nodeID)
	}
	if got := NodeIDFromPublic(pub); got != nodeID {
		t.Fatalf("node id mismatch: %s vs %s", got, nodeID)
	}
	// 不同身份必须有不同的 NodeID。
	other, _ := GenerateIdentity()
	if other.NodeID() == nodeID {
		t.Fatal("node id collision")
	}
}

func TestJoinProof(t *testing.T) {
	id, _ := GenerateIdentity()
	tokenHash := "deadbeef"
	nonce := []byte("random-nonce-0123456789abcdef")

	sig := id.SignJoin(tokenHash, nonce)
	if err := VerifyJoinProof(id.Public(), id.NodeID(), tokenHash, nonce, sig); err != nil {
		t.Fatalf("valid proof rejected: %v", err)
	}

	// 换 tokenHash：证明必须失效（防止签名被用于加入其它会话）。
	if err := VerifyJoinProof(id.Public(), id.NodeID(), "otherhash", nonce, sig); err == nil {
		t.Fatal("proof accepted for different token hash")
	}
	// 换 nonce：必须失效。
	if err := VerifyJoinProof(id.Public(), id.NodeID(), tokenHash, []byte("other"), sig); err == nil {
		t.Fatal("proof accepted for different nonce")
	}
	// 冒充他人 NodeID：必须失效。
	other, _ := GenerateIdentity()
	if err := VerifyJoinProof(id.Public(), other.NodeID(), tokenHash, nonce, sig); err != ErrNodeMismatch {
		t.Fatalf("err = %v, want ErrNodeMismatch", err)
	}
	// 坏签名：必须失效。
	bad := make([]byte, ed25519.SignatureSize)
	if err := VerifyJoinProof(id.Public(), id.NodeID(), tokenHash, nonce, bad); err == nil {
		t.Fatal("zero signature accepted")
	}
	// 坏公钥长度：必须失效。
	if err := VerifyJoinProof([]byte{1, 2, 3}, id.NodeID(), tokenHash, nonce, sig); err == nil {
		t.Fatal("short public key accepted")
	}
}

func TestIdentityPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "identity.json")

	id, created, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first call should create identity")
	}

	// 文件权限必须是 0600（私钥）。
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("identity file permission = %o, want 600", perm)
	}

	// 二次加载：必须复用同一身份，而不是新建。
	id2, created2, err := LoadOrCreateIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	if created2 {
		t.Fatal("second call should load existing identity")
	}
	if id2.NodeID() != id.NodeID() {
		t.Fatalf("node id changed: %s vs %s", id2.NodeID(), id.NodeID())
	}

	// 恢复出的私钥必须仍能生成可验证的证明。
	nonce := []byte("n")
	sig := id2.SignJoin("hash", nonce)
	if err := VerifyJoinProof(id2.Public(), id2.NodeID(), "hash", nonce, sig); err != nil {
		t.Fatalf("restored key proof invalid: %v", err)
	}
}

func TestIdentityFromSeedRejectsBadSize(t *testing.T) {
	if _, err := IdentityFromSeed([]byte{1, 2, 3}); err == nil {
		t.Fatal("short seed accepted")
	}
}
