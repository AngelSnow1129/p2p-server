package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Candidate 是一个网络可达性候选（ICE 风格）。第二阶段 NAT 穿透时由客户端
// 上报并经服务器在会话成员间透传；MVP 阶段服务器只存储/转发，不解释其语义。
type Candidate struct {
	// Type 为 host / srflx / relay / prflx 之一。
	Type string `json:"type"`
	// IP 与 Port 为候选地址（MVP 不据此建连，仅交换）。
	IP   string `json:"ip,omitempty"`
	Port int    `json:"port,omitempty"`
	// Proto 为 udp / tcp / quic。
	Proto string `json:"proto,omitempty"`
	// Priority 为 ICE 风格优先级，越大越优先（可选）。
	Priority uint32 `json:"priority,omitempty"`
}

// MemberInfo 是服务器对外暴露的成员描述。
type MemberInfo struct {
	// MemberID 是会话内稳定的逻辑成员标识（重连不变）。
	MemberID string `json:"member_id"`
	// Index 是会话内紧凑的 16 位数据面寻址下标。
	Index uint16 `json:"index"`
	// NodeID 是节点长期身份（Ed25519 公钥指纹）。
	NodeID string `json:"node_id"`
	// PublicKey 为 Ed25519 公钥（hex），供对端做端到端身份核验。
	PublicKey string `json:"public_key"`
	// Capabilities 如 ["udp","tcp","quic","relay"]。
	Capabilities []string `json:"capabilities,omitempty"`
	// Candidates 为该成员最近上报的网络候选（MVP 可空）。
	Candidates []Candidate `json:"candidates,omitempty"`
	// IsOwner 标记 Session 创建者。
	IsOwner bool `json:"is_owner,omitempty"`
	// Role 为 owner / member。
	Role string `json:"role,omitempty"`
	// Status 为 active / pending / denied / left；pending 表示等待 owner 批准。
	Status string `json:"status,omitempty"`
	// Online 为当前在线状态（member_list 快照使用，由在线连接中心判定）。
	Online bool `json:"online"`
	// JoinedAt / LastSeen 为 RFC3339 UTC 时间。
	JoinedAt string `json:"joined_at,omitempty"`
	LastSeen string `json:"last_seen,omitempty"`
}

// Message 是控制面（/ws/control）的统一 JSON 信封。
// 未使用字段省略；语义由 Type 决定。
type Message struct {
	Type string `json:"type"`

	// 会话/成员定位（通常由连接上下文隐含，消息内冗余仅用于事件说明）
	SessionID string `json:"session_id,omitempty"`
	MemberID  string `json:"member_id,omitempty"` // 事件主体或自己
	TargetID  string `json:"target_id,omitempty"` // 对端成员（客户端上行指定）
	// From 为服务器在转发时盖写的来源成员（客户端不得伪造）。
	From   string `json:"from,omitempty"`
	ConnID string `json:"connection_id,omitempty"`

	// 成员/列表事件
	Member  *MemberInfo  `json:"member,omitempty"`
	Members []MemberInfo `json:"members,omitempty"`

	// Candidate / 打洞 / 状态
	Candidates []Candidate `json:"candidates,omitempty"`
	Transport  string      `json:"transport,omitempty"` // direct | relay
	State      string      `json:"state,omitempty"`     // PeerState*
	Success    *bool       `json:"success,omitempty"`
	Detail     string      `json:"detail,omitempty"`

	// 流（控制面仅承载状态说明；实际字节走数据面）
	StreamID uint32 `json:"stream_id,omitempty"`

	// Payload 为对服务器不透明的 JSON 负载：
	// 用于候选交换的附加信息、端到端密钥协商材料（key_exchange）等。
	// 服务器只做成员校验与透传，不解析其内容。
	Payload json.RawMessage `json:"payload,omitempty"`

	// ping/pong
	TS int64 `json:"ts,omitempty"`

	// error
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// NewError 构造一条 error 消息。
func NewError(code, msg string) *Message {
	return &Message{Type: MsgError, Code: code, Message: msg}
}

// Encode 编码为 JSON 文本帧。
func (m *Message) Encode() ([]byte, error) { return json.Marshal(m) }

// Decode 解析一条 JSON 文本帧。
func Decode(data []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidJSON, err)
	}
	if m.Type == "" {
		return nil, errors.New("missing message type")
	}
	return &m, nil
}

var errInvalidJSON = errors.New("invalid json")
