package sessionclient

import (
	"context"
	"fmt"
	"time"

	"p2psession/internal/protocol"
)

// SendTo 向指定对端发送一条消息（内部开一条短生命周期流）。
//
// 这是最常用的「会话内双向通信」入口：调用方只需成员 ID，
// 无需关心 IP、端口、直连还是中继。
func (c *Client) SendTo(peerID string, payload []byte) error {
	if _, ok := c.Peer(peerID); !ok {
		return fmt.Errorf("%w: %s", ErrUnknownPeer, peerID)
	}
	if !c.crypto.ready(peerID) {
		return fmt.Errorf("%w: %s", ErrNoSessionKey, peerID)
	}
	return c.sendOneShot(peerID, payload)
}

// KexReady 返回是否已与某对端完成端到端密钥协商。
func (c *Client) KexReady(peerID string) bool { return c.crypto.ready(peerID) }

// pongCh 返回心跳应答通知通道。
func (c *Client) pongCh() <-chan struct{} { return c.pong }

// Ping 发送一次控制面心跳并等待 pong，用于探活。
func (c *Client) Ping(ctx context.Context) error {
	select {
	case <-c.closed:
		return ErrClosed
	default:
	}
	if err := c.sendControl(&protocol.Message{
		Type: protocol.MsgPing,
		TS:   time.Now().UnixMilli(),
	}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return fmt.Errorf("ping timeout")
	case <-c.closed:
		return ErrClosed
	case <-c.pongCh():
		return nil
	}
}
