package sessionclient

import (
	"fmt"
	"sort"
)

// Group 会话的发送辅助。
//
// 设计说明（重要）：本系统的数据加密是**成对密钥**（每对成员一把 X25519 派生的
// AEAD 密钥），因此「一份密文发给多人」在密码学上并不成立——同一明文对不同
// 接收方必须用各自的密钥分别加密。
//
// 于是：
//   - 客户端侧的 Broadcast / Multicast 实现为「逐对加密 + 单播投递」（N 次）；
//   - 服务器侧仍然支持 DstBroadcast / DstMulticast 帧类型（见 protocol 与 relay），
//     但那只在双方共享同一把会话密钥（例如应用层自己协商的 group key）时才有意义，
//     适合未来引入 group key 后一次性投递，省去 N 次上行。
//
// 这样既保持了「会话里每个成员都可与其它成员双向通信」，又没有偷偷把
// 成对密钥降级成会话共享密钥。

// Broadcast 向会话内所有对端各发送一条消息，返回成功送达的对端数。
//
// 每个对端使用各自成对密钥加密，因此是 N 次独立单播。
func (c *Client) Broadcast(payload []byte) (int, error) {
	peers := c.Peers()
	ids := make([]string, 0, len(peers))
	for _, p := range peers {
		ids = append(ids, p.MemberID)
	}
	return c.Multicast(ids, payload)
}

// Multicast 只向指定对端发送（等价于批量单播）。
//
// 返回成功数；若全部失败则返回第一个错误，便于调用方判断原因（如密钥未就绪）。
func (c *Client) Multicast(peerIDs []string, payload []byte) (int, error) {
	if len(peerIDs) == 0 {
		return 0, fmt.Errorf("%w: no recipients", ErrUnknownPeer)
	}
	// 去重并按字典序处理，保证行为可预测、日志稳定。
	uniq := make(map[string]bool, len(peerIDs))
	ordered := make([]string, 0, len(peerIDs))
	for _, id := range peerIDs {
		if id == "" || id == c.selfID() || uniq[id] {
			continue
		}
		uniq[id] = true
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)

	delivered := 0
	var firstErr error
	for _, id := range ordered {
		if err := c.sendOneShot(id, payload); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		delivered++
	}
	if delivered == 0 && firstErr != nil {
		return 0, firstErr
	}
	return delivered, nil
}

// sendOneShot 向单个对端开一条短生命周期流并发送一条消息。
//
// 「一条消息一条流」的语义简单：接收方读到内容后再读到流关闭，天然形成
// 消息边界，无需应用层再设计长度前缀。
func (c *Client) sendOneShot(peerID string, payload []byte) error {
	s, err := c.OpenStream(peerID)
	if err != nil {
		return err
	}
	if _, err := s.Write(payload); err != nil {
		_ = s.Reset("write failed")
		return err
	}
	return s.Close()
}
