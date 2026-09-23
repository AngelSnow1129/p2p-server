package sessionclient

import (
	"crypto/cipher"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

// aeadCipher 与标准库 crypto/cipher.AEAD 等价（此处用别名，便于将来替换算法）。
type aeadCipher = cipher.AEAD

// newAEAD 用 32 字节密钥构造 ChaCha20-Poly1305。
//
// 选择 ChaCha20-Poly1305 而非 AES-GCM 的原因：纯软件实现快且不依赖 AES-NI，
// 在各类边缘/容器环境下表现一致；nonce 12 字节，与协议帧布局契合。
func newAEAD(key []byte) (aeadCipher, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("aead key must be %d bytes, got %d", chacha20poly1305.KeySize, len(key))
	}
	return chacha20poly1305.New(key)
}
