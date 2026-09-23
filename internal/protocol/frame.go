package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// 数据面（/ws/relay）二进制 DataFrame。
//
// 设计目标：服务器只做授权与转发，不解析 Payload（端到端加密对服务器不透明）。
// 寻址使用会话内紧凑的 16 位 MemberIndex，由控制面在 joined/member_list 中下发，
// 客户端绝不在数据面使用 IP。支持单播 / 组播 / 广播。
//
// 帧布局（所有整数大端）：
//
//	偏移  0         1         3         5         9          10        12
//	     +---------+---------+---------+---------+----------+---------+----------------+
//	     | magic   | flags   | src idx | dst type| streamID | seq(16) | payload ...    |
//	     | 1B      | 1B      | 2B      | 1B      | 4B       | 2B      |                |
//	     +---------+---------+---------+---------+----------+---------+----------------+
//
//	dst type = UNICAST(1)   ：其后紧跟 dst idx(2B)，共 14 字节头
//	dst type = MULTICAST(2)：其后紧跟 count(1B) + count*idx(2B)
//	dst type = BROADCAST(3)：无附加目标
//
// src idx 由服务器在转发时盖写为真实发送方（客户端上行填 0 即可），
// 因此客户端无法伪造来源。

const (
	frameMagic = 0x21 // '!' 的高半字节 0x2 + 版本 1 → 0x21

	DstUnicast   byte = 1
	DstMulticast byte = 2
	DstBroadcast byte = 3

	// FlagReliable 表示该帧要求可靠/有序（基于 StreamID 的 seq 排序）。
	FlagReliable byte = 1 << 0
	// FlagClose 表示逻辑 Stream 半关闭/关闭。
	FlagClose byte = 1 << 1
	// FlagReset 表示逻辑 Stream 异常重置。
	FlagReset byte = 1 << 2
	// FlagPing 为数据面心跳（payload 为空）。
	FlagPing byte = 1 << 3

	frameFixedHeader = 12
	maxMulticastN    = 255
)

// Frame 是解析后的 DataFrame（Payload 复用入参底层数组）。
type Frame struct {
	Flags    byte
	SrcIndex uint16
	DstType  byte
	Dst      []uint16 // 单播 1 个；组播 N 个；广播为空
	StreamID uint32
	Seq      uint16
	Payload  []byte
}

// EncodeFrame 编码一帧。上行时 SrcIndex 可填 0（服务器盖写）。
func EncodeFrame(f *Frame) ([]byte, error) {
	if f.DstType < DstUnicast || f.DstType > DstBroadcast {
		return nil, errors.New("invalid dst type")
	}
	extra := 0
	switch f.DstType {
	case DstUnicast:
		if len(f.Dst) != 1 {
			return nil, errors.New("unicast needs exactly 1 destination")
		}
		extra = 2
	case DstMulticast:
		if len(f.Dst) == 0 {
			return nil, errors.New("multicast needs at least 1 destination")
		}
		if len(f.Dst) > maxMulticastN {
			return nil, fmt.Errorf("multicast exceeds %d destinations", maxMulticastN)
		}
		extra = 1 + 2*len(f.Dst)
	}

	buf := make([]byte, frameFixedHeader+extra+len(f.Payload))
	buf[0] = frameMagic
	buf[1] = f.Flags
	binary.BigEndian.PutUint16(buf[2:4], f.SrcIndex)
	buf[4] = f.DstType
	binary.BigEndian.PutUint32(buf[5:9], f.StreamID)
	binary.BigEndian.PutUint16(buf[9:11], f.Seq)
	// buf[11] 保留对齐字节
	off := frameFixedHeader
	switch f.DstType {
	case DstUnicast:
		binary.BigEndian.PutUint16(buf[off:off+2], f.Dst[0])
		off += 2 // 必须前进，否则 payload 会覆盖目标下标字段
	case DstMulticast:
		buf[off] = byte(len(f.Dst))
		off++
		for _, idx := range f.Dst {
			binary.BigEndian.PutUint16(buf[off:off+2], idx)
			off += 2
		}
	}
	copy(buf[off:], f.Payload)
	return buf, nil
}

// ParseFrame 解析数据帧；仅校验头部结构，不触碰 Payload。
func ParseFrame(b []byte) (*Frame, error) {
	if len(b) < frameFixedHeader {
		return nil, errors.New("frame too short")
	}
	if b[0] != frameMagic {
		return nil, fmt.Errorf("bad magic 0x%02x", b[0])
	}
	f := &Frame{
		Flags:    b[1],
		SrcIndex: binary.BigEndian.Uint16(b[2:4]),
		DstType:  b[4],
		StreamID: binary.BigEndian.Uint32(b[5:9]),
		Seq:      binary.BigEndian.Uint16(b[9:11]),
	}
	off := frameFixedHeader
	switch f.DstType {
	case DstUnicast:
		if len(b) < off+2 {
			return nil, errors.New("truncated unicast dst")
		}
		f.Dst = []uint16{binary.BigEndian.Uint16(b[off : off+2])}
		off += 2
	case DstMulticast:
		if len(b) < off+1 {
			return nil, errors.New("truncated multicast count")
		}
		n := int(b[off])
		off++
		if n == 0 || n > maxMulticastN || len(b) < off+2*n {
			return nil, errors.New("bad multicast destination list")
		}
		f.Dst = make([]uint16, n)
		for i := 0; i < n; i++ {
			f.Dst[i] = binary.BigEndian.Uint16(b[off : off+2])
			off += 2
		}
	case DstBroadcast:
		// 无附加目标
	default:
		return nil, fmt.Errorf("unknown dst type %d", f.DstType)
	}
	f.Payload = b[off:]
	return f, nil
}

// HeaderLen 返回某帧头部长度（用于大小统计）。
func (f *Frame) HeaderLen() int {
	switch f.DstType {
	case DstUnicast:
		return frameFixedHeader + 2
	case DstMulticast:
		return frameFixedHeader + 1 + 2*len(f.Dst)
	default:
		return frameFixedHeader
	}
}
