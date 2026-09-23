package sessionclient

import (
	"fmt"
	"sync"
	"time"
)

// Stream 是一条「逻辑流」：同一条成员间连接上可复用出多条独立双向流。
//
// 语义：
//   - Write 发送一段数据（自动分片、加密、按 StreamID 复用连接）；
//   - Read 按序返回对端写入的数据（服务器保证不重排，SDK 另做 seq 去重与排序）；
//   - Close 半关闭并通知对端（发送带 FlagClose 的空帧）；
//   - Reset 异常中止（发送 FlagReset，立即丢弃本地缓冲）。
//
// 所有业务数据都经过端到端加密，服务器只见密文。
type Stream struct {
	c      *Client
	peerID string
	id     uint32

	// 发送序号（16 位回绕，足以在丢帧窗口内区分顺序）。
	sendMu  sync.Mutex
	sendSeq uint16

	// 接收侧：按序投递 + 乱序缓冲。
	recvMu   sync.Mutex
	recvNext uint16
	recvBuf  map[uint16][]byte

	in     chan []byte
	closed chan struct{}
	once   sync.Once

	// reset 标记本流因异常/对端重置而终止。
	resetFlag bool
	closeErr  error

	openedAt time.Time
}

func (c *Client) newStream(peerID string, id uint32) *Stream {
	return &Stream{
		c:        c,
		peerID:   peerID,
		id:       id,
		recvBuf:  make(map[uint16][]byte),
		in:       make(chan []byte, 64),
		closed:   make(chan struct{}),
		openedAt: time.Now(),
	}
}

// ID 返回流标识。
func (s *Stream) ID() uint32 { return s.id }

// PeerID 返回对端成员 ID。
func (s *Stream) PeerID() string { return s.peerID }

// Write 发送数据（自动分片并加密）。
func (s *Stream) Write(p []byte) (int, error) {
	select {
	case <-s.closed:
		return 0, fmt.Errorf("%w: stream %d closed", ErrStreamClosed, s.id)
	default:
	}
	if len(p) == 0 {
		return 0, nil
	}

	total := 0
	for off := 0; off < len(p); {
		end := off + maxStreamChunk
		if end > len(p) {
			end = len(p)
		}
		chunk := p[off:end]

		s.sendMu.Lock()
		seq := s.sendSeq
		s.sendSeq++
		s.sendMu.Unlock()

		if err := s.c.sendStreamData(s.peerID, s.id, seq, 0, chunk); err != nil {
			return total, err
		}
		total += len(chunk)
		off = end
	}
	return total, nil
}

// Read 读取对端发送的一段数据；流关闭时返回 ErrStreamClosed。
//
// 注意「先取数据、再判关闭」：数据帧与关闭帧可能几乎同时到达（发送方
// 写完立刻 Close），此时 s.in 与 s.closed 同时就绪，而 select 在多个就绪
// case 中随机选择——若先命中 closed，已到达的最后一条消息就被丢掉了。
func (s *Stream) Read(p []byte) (int, error) {
	// 优先消费已缓冲的数据。
	select {
	case data := <-s.in:
		n := copy(p, data)
		if n < len(data) {
			select {
			case s.in <- data[n:]:
			default:
				// 队列满时丢弃尾部，避免阻塞读循环；调用方应以足够大的缓冲读取。
			}
		}
		return n, nil
	default:
	}

	select {
	case data := <-s.in:
		n := copy(p, data)
		if n < len(data) {
			select {
			case s.in <- data[n:]:
			default:
			}
		}
		return n, nil
	case <-s.closed:
		// 关闭后仍可能有残留数据，排空后再报错。
		select {
		case data := <-s.in:
			n := copy(p, data)
			if n < len(data) {
				select {
				case s.in <- data[n:]:
				default:
				}
			}
			return n, nil
		default:
		}
		if s.closeErr != nil {
			return 0, s.closeErr
		}
		return 0, ErrStreamClosed
	}
}

// ReadMessage 读取一条完整消息（分片边界即消息边界）。
func (s *Stream) ReadMessage() ([]byte, error) {
	// 与 Read 同理：先消费缓冲数据，避免关闭竞态吞掉最后一条消息。
	select {
	case data := <-s.in:
		return data, nil
	default:
	}

	select {
	case data := <-s.in:
		return data, nil
	case <-s.closed:
		select {
		case data := <-s.in:
			return data, nil
		default:
		}
		if s.closeErr != nil {
			return nil, s.closeErr
		}
		return nil, ErrStreamClosed
	}
}

// Close 半关闭本流并通知对端。
func (s *Stream) Close() error {
	s.once.Do(func() {
		s.closeErr = ErrStreamClosed
		// 通知对端；失败也不影响本地关闭。
		_ = s.c.sendStreamClose(s.peerID, s.id, false)
		close(s.closed)
		s.c.dropStream(s.id)
	})
	return nil
}

// Reset 异常中止本流（对端会收到 Reset 标志）。
func (s *Stream) Reset(reason string) error {
	s.once.Do(func() {
		s.closeErr = fmt.Errorf("%w: %s", ErrStreamReset, reason)
		_ = s.c.sendStreamClose(s.peerID, s.id, true)
		close(s.closed)
		s.c.dropStream(s.id)
	})
	return nil
}

// Done 在流关闭/重置时关闭。
func (s *Stream) Done() <-chan struct{} { return s.closed }

// Closed 返回流是否已终止。
func (s *Stream) Closed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

// deliver 由接收泵调用：按 seq 排序后投递给应用。
func (s *Stream) deliver(seq uint16, payload []byte) {
	s.recvMu.Lock()
	if s.recvNext == seq {
		s.recvNext++
		// 复制一份：payload 可能复用底层读缓冲。
		cp := append([]byte(nil), payload...)
		s.recvMu.Unlock()
		s.push(cp)
		// 继续吐出后续已就绪的乱序包。
		for {
			s.recvMu.Lock()
			next, ok := s.recvBuf[s.recvNext]
			if !ok {
				s.recvMu.Unlock()
				return
			}
			delete(s.recvBuf, s.recvNext)
			s.recvNext++
			s.recvMu.Unlock()
			s.push(next)
		}
	}
	// 乱序包：缓存等待缺口补齐。缓存上限防止恶意对端耗尽内存。
	if len(s.recvBuf) >= maxReorderBuffer {
		// 丢弃最旧的乱序包，保证流不被拖死。
		var oldest uint16
		first := true
		for k := range s.recvBuf {
			if first || int16(k-s.recvNext) < int16(oldest-s.recvNext) {
				oldest, first = k, false
			}
		}
		delete(s.recvBuf, oldest)
	}
	s.recvBuf[seq] = append([]byte(nil), payload...)
	s.recvMu.Unlock()
}

// push 把数据交给应用（队列满则丢弃并标记错误，避免阻塞接收泵）。
func (s *Stream) push(data []byte) {
	select {
	case s.in <- data:
	default:
		s.recvMu.Lock()
		if s.closeErr == nil {
			s.closeErr = fmt.Errorf("%w: receive buffer overflow", ErrStreamReset)
		}
		s.recvMu.Unlock()
	}
}

// markRemoteClosed 处理对端关闭/重置。
func (s *Stream) markRemoteClosed(reset bool) {
	s.recvMu.Lock()
	if reset {
		s.closeErr = fmt.Errorf("%w: closed by peer", ErrStreamReset)
	} else if s.closeErr == nil {
		s.closeErr = ErrStreamClosed
	}
	s.recvMu.Unlock()

	s.once.Do(func() {
		close(s.closed)
		s.c.dropStream(s.id)
	})
}
