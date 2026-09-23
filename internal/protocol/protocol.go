// Package protocol 定义 p2psession 的线协议（v1）。
//
// 系统被明确划分为两个平面，使用两条相互独立的 WebSocket：
//
//	控制面  /ws/control  —— JSON 文本帧（Message），负责 Session、成员发现、
//	                       Candidate 交换、连接状态协调、心跳。
//	数据面  /ws/relay    —— 二进制帧（DataFrame），只转发端到端加密的业务数据，
//	                       服务器不解析也无法解密 Payload。
//
// 数据面用会话内 16 位 MemberIndex（而非 IP）寻址，逻辑地址形如
// session://<session_id>/<member_id>。
package protocol

// Version 是协议版本字符串与二进制帧魔数的共同版本号。
const Version = "1.0"

// Session 模式。
const (
	ModePair  = "pair"  // 最多 2 名成员，第 2 人加入即 PAIR_READY
	ModeGroup = "group" // N 名成员（max_members 控制）
)

// 物理传输类型（逻辑拓扑与传输解耦）。
const (
	TransportDirect = "direct" // 端到端直连（P2P）
	TransportRelay  = "relay"  // 经服务器中继
)

// 成员间连接状态。
const (
	PeerStateNegotiating = "negotiating"
	PeerStateDirect      = "direct"
	PeerStateRelay       = "relay"
	PeerStateClosed      = "closed"
)

// 成员审批状态（可选的 Owner Approval 能力）。
const (
	ApprovalAuto    = "auto"    // Session 默认：加入即生效
	ApprovalPending = "pending" // 需 owner 批准
	ApprovalDenied  = "denied"
)

// 成员生命周期状态（持久化在 MemberRecord.Status）。
const (
	MemberActive  = "active"  // 已生效成员
	MemberPending = "pending" // 等待 owner 批准
	MemberDenied  = "denied"  // 被拒绝或被移除
	MemberLeft    = "left"    // 已主动离开
)

// 成员角色。
const (
	RoleOwner  = "owner"
	RoleMember = "member"
)

// 控制消息类型。
const (
	// 客户端 -> 服务器
	MsgLeave           = "leave"
	MsgPing            = "ping"
	MsgCandidateOffer  = "candidate_offer"
	MsgCandidateAnswer = "candidate_answer"
	MsgPunchResult     = "punch_result"
	MsgPeerState       = "peer_state"   // 客户端上报与某对端的最终传输状态
	MsgKeyExchange     = "key_exchange" // 透传端到端密钥协商材料（X25519 公钥）
	MsgStreamOpen      = "stream_open"  // 通知对端「我要向你开一条逻辑流」
	MsgStreamClose     = "stream_close" // 通知对端逻辑流关闭/重置

	// 服务器 -> 客户端
	MsgJoined         = "joined"
	MsgSessionReady   = "session_ready" // 会话成员已齐（pair 满员）或已达可用状态
	MsgSessionClosed  = "session_closed"
	MsgMemberJoined   = "member_joined"
	MsgMemberLeft     = "member_left"
	MsgMemberList     = "member_list"
	MsgPunchStart     = "punch_start"
	MsgCandidateRelay = "candidate_relay" // 服务器透传对端 candidate（offer/answer 共用）
	MsgPeerStateNote  = "peer_state_note"
	MsgPong           = "pong"
	MsgError          = "error"
)

// 错误码。
const (
	ErrBadRequest       = "bad_request"
	ErrUnauthorized     = "unauthorized"
	ErrSessionNotFound  = "session_not_found"
	ErrSessionExpired   = "session_expired"
	ErrSessionFull      = "session_full"
	ErrJoinLimit        = "join_limit_reached"
	ErrTokenRevoked     = "token_revoked"
	ErrPendingApproval  = "pending_approval"
	ErrApprovalDenied   = "approval_denied"
	ErrMemberNotFound   = "member_not_found"
	ErrNotSessionMember = "not_session_member"
	ErrRateLimited      = "rate_limited"
	ErrPayloadTooLarge  = "payload_too_large"
	ErrRelayDenied      = "relay_denied"
	ErrDuplicateConn    = "duplicate_connection"
	ErrUnsupported      = "unsupported"
	ErrServer           = "server_error"
)

// WebSocket 应用私有关闭码（4000-4999）。
const (
	CloseProtocol    = 4000
	CloseAuth        = 4001
	CloseSessionFull = 4002
	CloseDuplicate   = 4003
	CloseIdle        = 4004
	ClosePolicy      = 4005
	CloseSessionGone = 4006
)

// 默认大小限制。
const (
	DefaultMaxControlFrame = 1 << 16 // 64 KiB 单条控制 JSON
	DefaultMaxRelayFrame   = 1 << 16 // 64 KiB 单帧用户数据（不含帧头）
	DefaultMaxCandidates   = 16
)
