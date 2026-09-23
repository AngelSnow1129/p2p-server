// Package sessionclient 是 p2psession 的 Go 客户端 SDK。
//
// 客户端的全部职责被刻意限制为「极简配对」：
//
//  1. 连接服务器（REST 加入 + 两条 WebSocket）
//  2. 使用 Token / Join Code 加入会话（无需手工填 IP / Peer ID / 公钥）
//  3. 上报自身候选（Candidate）
//  4. 接收服务器下发的成员列表，自动发现对端
//  5. 尝试 P2P（直连信令已就绪，NAT 穿透在后续阶段接入）
//  6. 直连失败则自动经服务器中继
//  7. 在会话内与其它成员双向通信（逻辑 Stream）
//
// 端到端加密由 SDK 自动完成：会话内任意两成员之间用 X25519 临时密钥协商出
// 一份「成对会话密钥」，业务数据用 AEAD 封装后才交给服务器转发，
// 因此服务器只能看到密文、长度与时间，无法解密内容。
package sessionclient

import (
	"context"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"p2psession/internal/auth"
)

// 加密参数。
const (
	// aeadKeyLen 为成对会话密钥长度（ChaCha20-Poly1305 用 32 字节）。
	aeadKeyLen = 32
	// nonceLen 为 AEAD 随机数长度。
	nonceLen = 12
	// keyInfoPrefix 是 HKDF 的 info 域前缀，用于把密钥绑定到具体用途。
	keyInfoPrefix = "p2psession/pairwise/v1"
)

var (
	// ErrNoSessionKey 尚未与该对端完成密钥协商。
	ErrNoSessionKey = errors.New("no pairwise session key for peer yet")
	// ErrDecrypt 解密失败（密钥不匹配或数据被篡改）。
	ErrDecrypt = errors.New("decrypt failed")
)

// keyPair 是本节点的一次性 X25519 密钥对（每次连接会话重新生成）。
type keyPair struct {
	priv *ecdh.PrivateKey
	pub  []byte
}

func newKeyPair() (*keyPair, error) {
	priv, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &keyPair{priv: priv, pub: priv.PublicKey().Bytes()}, nil
}

// peerKey 是与某对端协商出的成对密钥。
type peerKey struct {
	aead  aeadCipher
	peerX []byte // 对端 X25519 公钥
	ready bool
}

// cryptoState 管理本节点的临时密钥与全部对端的成对密钥。
type cryptoState struct {
	mu   sync.RWMutex
	self *keyPair
	// peers 以对端 MemberID 为键。
	peers map[string]*peerKey
	// selfNodeID 用于在密钥派生中绑定双方身份，避免密钥被换位复用。
	selfNodeID string
	// sessionID 一并绑入密钥（同一对节点在不同会话中密钥不同）。
	sessionID string
}

func newCryptoState(selfNodeID, sessionID string) (*cryptoState, error) {
	kp, err := newKeyPair()
	if err != nil {
		return nil, err
	}
	return &cryptoState{
		self:       kp,
		peers:      make(map[string]*peerKey),
		selfNodeID: selfNodeID,
		sessionID:  sessionID,
	}, nil
}

// PublicKey 返回本节点的 X25519 临时公钥（经控制面交换给对端）。
func (c *cryptoState) PublicKey() []byte { return append([]byte(nil), c.self.pub...) }

// ready 返回是否已与某对端就绪。
func (c *cryptoState) ready(peerID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pk := c.peers[peerID]
	return pk != nil && pk.ready
}

// derive 用对端临时公钥派生成对会话密钥。
//
// 派生过程：
//
//	shared = X25519(self_priv, peer_pub)
//	key    = HKDF-SHA256(ikm=shared, salt=sha256(sessionID),
//	                     info="p2psession/pairwise/v1" || ordered(nodeA) || ordered(nodeB))
//
// 其中 nodeA/nodeB 按字典序排列，保证双方独立计算出同一把密钥；
// 绑定 sessionID 与双方 NodeID 可防止跨会话/跨对端的密钥复用。
func (c *cryptoState) derive(peerID, peerNodeID string, peerPub []byte) error {
	if len(peerPub) != 32 {
		return fmt.Errorf("peer x25519 public key must be 32 bytes, got %d", len(peerPub))
	}
	remote, err := ecdh.X25519().NewPublicKey(peerPub)
	if err != nil {
		return fmt.Errorf("invalid peer x25519 public key: %w", err)
	}
	shared, err := c.self.priv.ECDH(remote)
	if err != nil {
		return err
	}

	// 双方 NodeID 排序后串接，保证两侧 info 一致。
	a, b := c.selfNodeID, peerNodeID
	if a > b {
		a, b = b, a
	}
	info := append([]byte(keyInfoPrefix), 0)
	info = append(info, []byte(a)...)
	info = append(info, 0)
	info = append(info, []byte(b)...)

	salt := sha256.Sum256([]byte(c.sessionID))
	key, err := hkdf.Key(sha256.New, shared, salt[:], string(info), aeadKeyLen)
	if err != nil {
		return err
	}
	cipher, err := newAEAD(key)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.peers[peerID] = &peerKey{aead: cipher, peerX: append([]byte(nil), peerPub...), ready: true}
	c.mu.Unlock()

	// 立即擦除共享密钥与派生密钥的本地副本。
	zero(shared)
	zero(key)
	return nil
}

// encrypt 用与某对端的成对密钥加密明文。
func (c *cryptoState) encrypt(peerID string, plaintext []byte) ([]byte, error) {
	c.mu.RLock()
	pk := c.peers[peerID]
	c.mu.RUnlock()
	if pk == nil || !pk.ready {
		return nil, fmt.Errorf("%w: %s", ErrNoSessionKey, peerID)
	}
	nonce := make([]byte, nonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	// 输出布局：nonce || ciphertext(+tag)
	out := make([]byte, 0, nonceLen+len(plaintext)+16)
	out = append(out, nonce...)
	return pk.aead.Seal(out, nonce, plaintext, nil), nil
}

// decrypt 解密来自某对端的密文。
func (c *cryptoState) decrypt(peerID string, data []byte) ([]byte, error) {
	c.mu.RLock()
	pk := c.peers[peerID]
	c.mu.RUnlock()
	if pk == nil || !pk.ready {
		return nil, fmt.Errorf("%w: %s", ErrNoSessionKey, peerID)
	}
	if len(data) < nonceLen+16 {
		return nil, ErrDecrypt
	}
	nonce, ct := data[:nonceLen], data[nonceLen:]
	pt, err := pk.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDecrypt, err)
	}
	return pt, nil
}

// peerPub 返回记录的对端临时公钥（调试/校验用）。
func (c *cryptoState) peerPub(peerID string) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if pk := c.peers[peerID]; pk != nil {
		return pk.peerX
	}
	return nil
}

// forget 丢弃与某对端的密钥（对端离开时调用）。
func (c *cryptoState) forget(peerID string) {
	c.mu.Lock()
	delete(c.peers, peerID)
	c.mu.Unlock()
}

// zero 擦除敏感字节。
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// NodeIDOf 由 Ed25519 公钥计算 NodeID（与服务器一致的派生规则）。
func NodeIDOf(pub []byte) string {
	if len(pub) != 32 {
		return ""
	}
	return auth.NodeIDFromPublic(pub)
}

// sortedKeys 返回排序后的键（稳定输出，便于测试与日志）。
func sortedKeys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// u32 大端编码（帧序号等）。
func u32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// ctxWithCancel 便捷包装（保留 context 语义）。
func ctxWithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(parent)
}
