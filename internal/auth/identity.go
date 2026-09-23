package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// 节点长期身份（Node Identity）与 join proof。
//
// 职责划分：
//   - Join Token  = “我被允许加入哪个 Session”（能力凭证，授权面）
//   - Node Identity = “我是谁”（Ed25519 长期公钥，身份面）
//   Member 由 (Session, NodeID) 共同决定，而不是“一个 token 对应一个匿名 socket”。
//
// join proof 让服务器确认：调用 /join 的人确实持有该公钥对应的私钥，
// 从而杜绝“拿别人的 NodeID 冒充加入”与事后抵赖。

// DomainSepJoin 是 join proof 的域分隔前缀，防止签名被挪用到其它用途。
const DomainSepJoin = "p2psession:join:v1\n"

var (
	// ErrProofInvalid 签名与声明不符。
	ErrProofInvalid = errors.New("invalid join proof")
	// ErrNodeMismatch NodeID 与公钥不匹配。
	ErrNodeMismatch = errors.New("node id does not match public key")
)

// Identity 是节点长期 Ed25519 身份。
type Identity struct {
	priv ed25519.PrivateKey
}

// GenerateIdentity 生成新身份。
func GenerateIdentity() (*Identity, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Identity{priv: priv}, nil
}

// IdentityFromSeed 从 32 字节种子恢复。
func IdentityFromSeed(seed []byte) (*Identity, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("seed must be %d bytes", ed25519.SeedSize)
	}
	return &Identity{priv: ed25519.NewKeyFromSeed(seed)}, nil
}

// Public 返回 32 字节公钥。
func (id *Identity) Public() ed25519.PublicKey { return id.priv.Public().(ed25519.PublicKey) }

// NodeID 返回节点标识：sha256(pub)[:16] 的 hex（32 字符）。
func (id *Identity) NodeID() string { return NodeIDFromPublic(id.Public()) }

// NodeIDFromPublic 由公钥派生 NodeID。
func NodeIDFromPublic(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:16])
}

// SignJoin 对 (tokenHash, nonce) 签名，作为 join proof。
//
// 只绑定 tokenHash 而非 sessionID，是因为客户端在加入前未必知道 sessionID
// （用 join code 加入时只拿到 code）。tokenHash 已足以把证明钉死在「某个 token
// 对应的会话」上——不同会话的 tokenHash 不同，签名无法跨会话复用。
func (id *Identity) SignJoin(tokenHash string, nonce []byte) []byte {
	return ed25519.Sign(id.priv, JoinProofMessage(tokenHash, nonce))
}

// JoinProofMessage 返回 join proof 覆盖的规范字节串，客户端与服务器共用。
func JoinProofMessage(tokenHash string, nonce []byte) []byte {
	h := sha256.New()
	h.Write([]byte(DomainSepJoin))
	h.Write([]byte(tokenHash))
	h.Write([]byte{0})
	h.Write(nonce)
	return h.Sum(nil)
}

// VerifyJoinProof 校验 join proof。
func VerifyJoinProof(pub ed25519.PublicKey, nodeID, tokenHash string, nonce, sig []byte) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("public key must be %d bytes", ed25519.PublicKeySize)
	}
	if NodeIDFromPublic(pub) != nodeID {
		return ErrNodeMismatch
	}
	if !ed25519.Verify(pub, JoinProofMessage(tokenHash, nonce), sig) {
		return ErrProofInvalid
	}
	return nil
}

// identityFile 是磁盘持久化格式。
type identityFile struct {
	// Seed 为 hex 编码的 32 字节 Ed25519 种子。
	Seed string `json:"seed"`
	// NodeID 冗余保存，便于人工查看。
	NodeID string `json:"node_id"`
	// CreatedAt 为创建时间。
	CreatedAt time.Time `json:"created_at"`
}

// SaveIdentity 以 0600 权限写入 path。
func SaveIdentity(id *Identity, path string) error {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(identityFile{
		Seed:      hex.EncodeToString(id.priv.Seed()),
		NodeID:    id.NodeID(),
		CreatedAt: time.Now().UTC(),
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// LoadIdentity 读取身份文件。
func LoadIdentity(path string) (*Identity, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f identityFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	seed, err := hex.DecodeString(f.Seed)
	if err != nil {
		return nil, errors.New("bad seed hex in identity file")
	}
	return IdentityFromSeed(seed)
}

// LoadOrCreateIdentity 加载身份；文件不存在则创建并落盘。
// 返回的 bool 表示是否新建。
func LoadOrCreateIdentity(path string) (*Identity, bool, error) {
	id, err := LoadIdentity(path)
	if err == nil {
		return id, false, nil
	}
	if !os.IsNotExist(err) {
		return nil, false, err
	}
	id, err = GenerateIdentity()
	if err != nil {
		return nil, false, err
	}
	if err := SaveIdentity(id, path); err != nil {
		return nil, false, err
	}
	return id, true, nil
}
