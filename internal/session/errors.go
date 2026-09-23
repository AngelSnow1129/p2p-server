package session

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"p2psession/internal/storage"
)

// 服务层对外错误。server 包把它们映射为协议错误码与 HTTP 状态码。
var (
	// ErrBadParams 入参非法。
	ErrBadParams = errors.New("invalid parameters")
	// ErrUnauthorized token 错误、join proof 校验失败或身份不匹配。
	ErrUnauthorized = errors.New("unauthorized")
	// ErrSessionNotFound 会话不存在。
	ErrSessionNotFound = errors.New("session not found")
	// ErrSessionExpired 会话已过期。
	ErrSessionExpired = errors.New("session expired")
	// ErrSessionClosed 会话已关闭。
	ErrSessionClosed = errors.New("session closed")
	// ErrSessionFull 成员已满。
	ErrSessionFull = errors.New("session full")
	// ErrJoinLimit token 加入次数已用尽。
	ErrJoinLimit = errors.New("join limit reached")
	// ErrTokenRevoked token 已被撤销。
	ErrTokenRevoked = errors.New("token revoked")
	// ErrMemberNotFound 成员不存在。
	ErrMemberNotFound = errors.New("member not found")
	// ErrNotOwner 仅 Session owner 可执行该操作。
	ErrNotOwner = errors.New("caller is not session owner")
	// ErrStaleConnection 操作来自已被取代的旧连接。
	ErrStaleConnection = errors.New("stale connection")
	// ErrUnsupported 当前存储实现不支持该操作。
	ErrUnsupported = errors.New("operation not supported")
)

// mapStoreErr 把存储层错误归一化为服务层错误。
func mapStoreErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return ErrSessionNotFound
	case errors.Is(err, storage.ErrExpired):
		return ErrSessionExpired
	case errors.Is(err, storage.ErrClosed):
		return ErrSessionClosed
	case errors.Is(err, storage.ErrRevoked):
		return ErrTokenRevoked
	case errors.Is(err, storage.ErrFull):
		return ErrSessionFull
	case errors.Is(err, storage.ErrJoinLimit):
		return ErrJoinLimit
	case errors.Is(err, storage.ErrMemberMissing):
		return ErrMemberNotFound
	default:
		return err
	}
}

// hashTokenRaw 返回原始 token 字节的存储哈希（hex），与 auth.JoinToken.Hash 一致。
func hashTokenRaw(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// hexEncode 编码公钥等二进制字段。
func hexEncode(b []byte) string { return hex.EncodeToString(b) }
