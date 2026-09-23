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
)

// Members 拉取会话成员列表。
//
// 与 Client.Peers 的区别：Peers 只含「已生效且被广播过」的成员，
// 而本方法直接查服务器，因此能看到 pending（等待批准）成员——
// owner 需要它来发现待审批者。
func (c *Client) Members(ctx context.Context) ([]MemberInfo, error) {
	base, err := normalizeBase(c.cfg.ServerURL)
	if err != nil {
		return nil, err
	}
	httpc := c.httpc
	if httpc == nil {
		httpc = &http.Client{}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/v1/sessions/"+url.PathEscape(c.sessionID)+"/members", nil)
	if err != nil {
		return nil, err
	}
	applyHeader(req.Header, c.cfg.Header)

	resp, err := httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("members: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var out struct {
		Members []MemberInfo `json:"members"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("members: bad response: %w", err)
	}
	return out.Members, nil
}

// PendingMembers 返回等待 owner 批准的成员。
func (c *Client) PendingMembers(ctx context.Context) ([]MemberInfo, error) {
	all, err := c.Members(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]MemberInfo, 0, len(all))
	for _, m := range all {
		if m.Status == "pending" {
			out = append(out, m)
		}
	}
	return out, nil
}

// ApproveMember 批准一个 pending 成员（需要 owner 身份；票据由 SDK 内部持有）。
func (c *Client) ApproveMember(ctx context.Context, memberID string) error {
	return c.ownerAction(ctx, "approve", memberID)
}

// RejectMember 拒绝（或移除）一个成员（需要 owner 身份）。
func (c *Client) RejectMember(ctx context.Context, memberID string) error {
	return c.ownerAction(ctx, "reject", memberID)
}

// RevokeToken 撤销本会话的加入能力（已在线成员不受影响）。
func (c *Client) RevokeToken(ctx context.Context) error {
	return c.ownerAction(ctx, "revoke-token", "")
}

// rotateResp 是 token 轮换响应。
type rotateResp struct {
	JoinToken string `json:"join_token"`
	JoinCode  string `json:"join_code"`
}

// RotateToken 轮换加入凭证，返回新的机器形式 token 与人类可传 code。
//
// 典型用途：Join Code 疑似泄露时立即失效旧的加入能力，而无需解散会话。
func (c *Client) RotateToken(ctx context.Context) (token, code string, err error) {
	body, err := c.ownerActionRaw(ctx, "rotate-token", "")
	if err != nil {
		return "", "", err
	}
	var out rotateResp
	if err := json.Unmarshal(body, &out); err != nil {
		return "", "", fmt.Errorf("rotate-token: bad response: %w", err)
	}
	return out.JoinToken, out.JoinCode, nil
}

// ownerAction 执行一次 owner 操作并丢弃响应体。
func (c *Client) ownerAction(ctx context.Context, action, memberID string) error {
	_, err := c.ownerActionRaw(ctx, action, memberID)
	return err
}

// ownerActionRaw 执行 owner 操作并返回响应体。
func (c *Client) ownerActionRaw(ctx context.Context, action, memberID string) ([]byte, error) {
	base, err := normalizeBase(c.cfg.ServerURL)
	if err != nil {
		return nil, err
	}
	c.wsMu.Lock()
	ticket := c.ticket
	c.wsMu.Unlock()
	if ticket == "" {
		return nil, fmt.Errorf("%w: no ticket available", ErrNotConnected)
	}

	payload := map[string]any{"actor_ticket": ticket}
	if memberID != "" {
		payload["member_id"] = memberID
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	httpc := c.httpc
	if httpc == nil {
		httpc = &http.Client{}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/v1/sessions/"+url.PathEscape(c.sessionID)+"/"+action, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// 票据同时放进 Authorization 头：服务器的 owner 鉴权走统一的 ticketFrom，
	// 它读的是查询参数或 Bearer 头（body 里的 actor_ticket 仅供不需要头部的调用方）。
	req.Header.Set("Authorization", "Bearer "+ticket)
	applyHeader(req.Header, c.cfg.Header)

	resp, err := httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d: %s", action, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}
