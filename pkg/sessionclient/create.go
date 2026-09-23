package sessionclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CreateParams 是创建会话的参数。
type CreateParams struct {
	// ServerURL 为服务器基址。
	ServerURL string
	// Mode 为 pair（默认）或 group。
	Mode string
	// MaxMembers 为成员上限（pair 忽略，恒为 2；group 默认 10）。
	MaxMembers int
	// JoinLimit 为最多加入次数（0 = 不限）。
	JoinLimit int
	// TTL 为会话存活时长（如 "30m"），留空用服务器默认。
	TTL time.Duration
	// RequireApproval 为 true 时新成员需 owner 批准。
	RequireApproval bool
	// IdleTimeout 为全员离线后自动过期（0 = 不启用）。
	IdleTimeout time.Duration
	// HTTPClient 可选。
	HTTPClient *http.Client
	// Header 可选，附加到请求。
	Header http.Header
}

// CreatedSession 是创建结果。
//
// JoinToken 与 JoinCode 是同一份能力的两种表示，仅在此刻可见一次，
// 服务器只保存其哈希。
type CreatedSession struct {
	SessionID       string    `json:"session_id"`
	JoinToken       string    `json:"join_token"`
	JoinCode        string    `json:"join_code"`
	Mode            string    `json:"mode"`
	MaxMembers      int       `json:"max_members"`
	JoinLimit       int       `json:"join_limit"`
	RequireApproval bool      `json:"require_approval"`
	ExpiresAt       time.Time `json:"expires_at"`
	CreatedAt       time.Time `json:"created_at"`
	JoinHint        string    `json:"join_hint"`
}

// Create 在服务器上创建一个会话（不需要身份，任何持有管理凭据的调用者都可创建）。
//
// 这是「服务端生成 Session + Token」的入口：用户只需执行 p2p-node create，
// 服务器返回 Session 与 Join Code，随后任何拿到该 Code 的节点都能加入。
func Create(ctx context.Context, p CreateParams) (*CreatedSession, error) {
	base, err := normalizeBase(p.ServerURL)
	if err != nil {
		return nil, err
	}
	httpc := p.HTTPClient
	if httpc == nil {
		httpc = &http.Client{Timeout: 30 * time.Second}
	}

	body := map[string]any{
		"mode":             p.Mode,
		"max_members":      p.MaxMembers,
		"join_limit":       p.JoinLimit,
		"require_approval": p.RequireApproval,
	}
	if p.TTL > 0 {
		body["ttl"] = p.TTL.String()
	}
	if p.IdleTimeout > 0 {
		body["idle_timeout"] = p.IdleTimeout.String()
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/v1/sessions", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	applyHeader(req.Header, p.Header)

	resp, err := httpc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("create session: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}

	// 服务器返回的时间是 RFC3339 字符串；这里用中间结构解析。
	var raw struct {
		SessionID       string `json:"session_id"`
		JoinToken       string `json:"join_token"`
		JoinCode        string `json:"join_code"`
		Mode            string `json:"mode"`
		MaxMembers      int    `json:"max_members"`
		JoinLimit       int    `json:"join_limit"`
		RequireApproval bool   `json:"require_approval"`
		ExpiresAt       string `json:"expires_at"`
		CreatedAt       string `json:"created_at"`
		JoinHint        string `json:"join_hint"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("create session: bad response: %w", err)
	}
	out := &CreatedSession{
		SessionID:       raw.SessionID,
		JoinToken:       raw.JoinToken,
		JoinCode:        raw.JoinCode,
		Mode:            raw.Mode,
		MaxMembers:      raw.MaxMembers,
		JoinLimit:       raw.JoinLimit,
		RequireApproval: raw.RequireApproval,
		JoinHint:        raw.JoinHint,
	}
	if t, err := time.Parse(time.RFC3339, raw.ExpiresAt); err == nil {
		out.ExpiresAt = t
	}
	if t, err := time.Parse(time.RFC3339, raw.CreatedAt); err == nil {
		out.CreatedAt = t
	}
	return out, nil
}

// ApproveMember 由 owner 批准一个 pending 成员。
func ApproveMember(ctx context.Context, serverURL, sessionID, ticket, memberID string) error {
	return memberAction(ctx, serverURL, sessionID, "approve", ticket, memberID)
}

// RejectMember 由 owner 拒绝/移除一个成员。
func RejectMember(ctx context.Context, serverURL, sessionID, ticket, memberID string) error {
	return memberAction(ctx, serverURL, sessionID, "reject", ticket, memberID)
}

func memberAction(ctx context.Context, serverURL, sessionID, action, ticket, memberID string) error {
	base, err := normalizeBase(serverURL)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"member_id":    memberID,
		"actor_ticket": ticket,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/v1/sessions/"+url.PathEscape(sessionID)+"/"+action, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: HTTP %d: %s", action, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
}
