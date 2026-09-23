// Package server 装配 p2psession 的对外接口：REST 控制面 + 两条 WebSocket。
//
// 路由：
//
//	POST   /v1/sessions                     创建会话（返回 session_id + join_token/code）
//	GET    /v1/sessions/{id}                读取会话
//	DELETE /v1/sessions/{id}                解散会话
//	POST   /v1/sessions/{id}/join           加入（也支持 /v1/session/join 走 join code）
//	POST   /v1/sessions/{id}/leave          离开
//	GET    /v1/sessions/{id}/members        成员列表
//	GET    /v1/sessions/{id}/members/{mid}  单个成员
//	POST   /v1/sessions/{id}/revoke-token   撤销 token
//	POST   /v1/sessions/{id}/rotate-token   轮换 token
//	POST   /v1/sessions/{id}/approve        审批通过（owner）
//	POST   /v1/sessions/{id}/reject         拒绝/移除成员（owner）
//	GET    /v1/health  /v1/stats  /metrics  健康、统计、Prometheus 指标
//	GET    /v1/relays                       Relay 列表（本机 + 可选远端）
//	GET    /ws/control                     控制面 WebSocket（票据鉴权）
//	GET    /ws/relay                       数据面 WebSocket（票据鉴权）
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"

	"p2psession/internal/auth"
	"p2psession/internal/config"
	"p2psession/internal/member"
	"p2psession/internal/metrics"
	"p2psession/internal/relay"
	"p2psession/internal/session"
)

// Server 持有全部共享依赖。
type Server struct {
	cfg     config.Config
	svc     *session.Service
	hub     *member.Hub
	relay   *relay.Forwarder
	metrics *metrics.Registry
	log     *slog.Logger
	tickets *auth.TicketKey

	upgrader *websocket.Upgrader

	// 每 IP 限流
	limiterMu sync.Mutex
	ipCreate  *bucket
	ipJoin    *bucket

	started time.Time
}

// New 创建服务器。
func New(cfg config.Config, svc *session.Service, hub *member.Hub, fwd *relay.Forwarder,
	reg *metrics.Registry, tickets *auth.TicketKey, log *slog.Logger) *Server {

	if log == nil {
		log = slog.Default()
	}
	if reg == nil {
		reg = metrics.New()
	}
	s := &Server{
		cfg:     cfg,
		svc:     svc,
		hub:     hub,
		relay:   fwd,
		metrics: reg,
		log:     log,
		tickets: tickets,
		started: time.Now(),
	}
	s.upgrader = &websocket.Upgrader{
		HandshakeTimeout: 10 * time.Second,
		ReadBufferSize:   4096,
		WriteBufferSize:  4096,
		CheckOrigin:      func(r *http.Request) bool { return checkOrigin(cfg, r) },
	}
	s.ipCreate = newBucket(cfg.SessionRatePerMin, time.Minute)
	s.ipJoin = newBucket(cfg.JoinRatePerMin, time.Minute)

	// 成员彻底离线 → 立刻标记（宽限期由 session.Prune 回收）。
	hub.SetOfflineHook(func(sessionID, memberID, connID string) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := svc.MarkOffline(ctx, sessionID, memberID, connID); err != nil {
			log.Debug("mark offline", "session", sessionID, "member", memberID, "err", err)
		}
	})
	return s
}

// Shutdown 优雅下线：先给所有在线连接发送带原因的 Close 帧（让客户端能区分
// 「服务端下线」与「网络故障」并据此重连），再等待 HTTP 服务器排空。
//
// 顺序很关键：必须先关 WS 连接，否则 http.Server.Shutdown 会一直等待
// 长连接自行结束（WebSocket 不响应 Shutdown）。
func (s *Server) Shutdown(ctx context.Context, httpSrv *http.Server) error {
	n := s.hub.CloseAll("server shutting down")
	s.log.Info("closing active connections", "count", n)

	// 给写泵一点时间把 Close 帧真正刷到网络。
	timer := time.NewTimer(300 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}

	if httpSrv != nil {
		return httpSrv.Shutdown(ctx)
	}
	return nil
}

// Handler 返回装配好的 http.Handler。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/sessions", s.handleCreateSession)
	mux.HandleFunc("GET /v1/sessions/{id}", s.handleGetSession)
	mux.HandleFunc("DELETE /v1/sessions/{id}", s.handleDeleteSession)
	mux.HandleFunc("POST /v1/sessions/{id}/join", s.handleJoin)
	mux.HandleFunc("POST /v1/session/join", s.handleJoin) // join code 形式
	mux.HandleFunc("POST /v1/sessions/{id}/leave", s.handleLeave)
	mux.HandleFunc("GET /v1/sessions/{id}/members", s.handleMembers)
	mux.HandleFunc("GET /v1/sessions/{id}/members/{mid}", s.handleMember)
	mux.HandleFunc("POST /v1/sessions/{id}/revoke-token", s.handleRevoke)
	mux.HandleFunc("POST /v1/sessions/{id}/rotate-token", s.handleRotate)
	mux.HandleFunc("POST /v1/sessions/{id}/approve", s.handleApprove)
	mux.HandleFunc("POST /v1/sessions/{id}/reject", s.handleReject)

	mux.HandleFunc("GET /v1/health", s.handleHealth)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /v1/relays", s.handleRelays)

	mux.HandleFunc("GET /ws/control", s.handleControlWS)
	mux.HandleFunc("GET /ws/relay", s.handleRelayWS)

	return s.withMiddleware(mux)
}

// withMiddleware 叠加日志与 recover。
//
// 注意：中间件必须透传 http.Hijacker，否则 WebSocket 升级会返回 500
// （gorilla 需要劫持底层连接）。
func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := &wrapWriter{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic in handler", "path", r.URL.Path, "panic", rec)
				if ww.status == http.StatusOK {
					http.Error(ww, "internal error", http.StatusInternalServerError)
				}
			}
		}()
		next.ServeHTTP(ww, r)
		if r.URL.Path == "/v1/health" || r.URL.Path == "/metrics" {
			return
		}
		s.log.Debug("http",
			"method", r.Method, "path", r.URL.Path,
			"status", ww.status, "ip", s.clientIP(r))
	})
}

// wrapWriter 记录状态码，并把 Hijack/Flush 透传给底层 ResponseWriter。
type wrapWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *wrapWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *wrapWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.wrote = true
	}
	return w.ResponseWriter.Write(b)
}

func (w *wrapWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying ResponseWriter does not implement http.Hijacker")
	}
	return h.Hijack()
}

func (w *wrapWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ---- 辅助 ------------------------------------------------------------------

func checkOrigin(cfg config.Config, r *http.Request) bool {
	if len(cfg.AllowedOrigins) == 0 || cfg.AllowedOrigins["*"] {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // 非浏览器客户端（CLI/SDK）
	}
	return cfg.AllowedOrigins[strings.TrimRight(origin, "/")]
}

// clientIP 提取调用方 IP（可选信任代理头）。
func (s *Server) clientIP(r *http.Request) string {
	host := r.RemoteAddr
	if s.cfg.TrustProxyHeaders {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			host = strings.TrimSpace(strings.SplitN(xff, ",", 2)[0])
		} else if cf := r.Header.Get("CF-Connecting-IP"); cf != "" {
			host = strings.TrimSpace(cf)
		}
	}
	if i := strings.LastIndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	return host
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// apiError 是统一错误响应体。
type apiError struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, apiError{Error: msg, Code: code})
}

// mapServiceErr 把服务层错误映射为 HTTP 状态码与协议错误码。
func mapServiceErr(err error) (int, string) {
	switch {
	case err == nil:
		return http.StatusOK, ""
	case errors.Is(err, session.ErrBadParams):
		return http.StatusBadRequest, "bad_request"
	case errors.Is(err, session.ErrUnauthorized):
		return http.StatusUnauthorized, "unauthorized"
	case errors.Is(err, session.ErrSessionNotFound):
		return http.StatusNotFound, "session_not_found"
	case errors.Is(err, session.ErrMemberNotFound):
		return http.StatusNotFound, "member_not_found"
	case errors.Is(err, session.ErrSessionExpired):
		return http.StatusGone, "session_expired"
	case errors.Is(err, session.ErrSessionClosed):
		return http.StatusGone, "session_closed"
	case errors.Is(err, session.ErrSessionFull):
		return http.StatusConflict, "session_full"
	case errors.Is(err, session.ErrJoinLimit):
		return http.StatusConflict, "join_limit_reached"
	case errors.Is(err, session.ErrTokenRevoked):
		return http.StatusForbidden, "token_revoked"
	case errors.Is(err, session.ErrNotOwner):
		return http.StatusForbidden, "not_owner"
	case errors.Is(err, session.ErrStaleConnection):
		return http.StatusConflict, "stale_connection"
	case errors.Is(err, session.ErrUnsupported):
		return http.StatusNotImplemented, "unsupported"
	default:
		return http.StatusInternalServerError, "server_error"
	}
}

// maxDuration 取两者较大值（心跳兜底用）。
func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// newLimiter 创建令牌桶。
func newLimiter(perSec float64, burst int) *rate.Limiter {
	if perSec <= 0 {
		perSec = 1
	}
	if burst <= 0 {
		burst = int(perSec)
		if burst <= 0 {
			burst = 1
		}
	}
	return rate.NewLimiter(rate.Limit(perSec), burst)
}

// ---- 每 IP 计数限流 --------------------------------------------------------

// bucket 是简单的固定窗口计数器。
type bucket struct {
	mu     sync.Mutex
	counts map[string]int
	reset  time.Time
	limit  int
	window time.Duration
}

func newBucket(limit int, window time.Duration) *bucket {
	return &bucket{counts: make(map[string]int), limit: limit, window: window}
}

// allow 返回该 key 是否仍在限额内。
func (b *bucket) allow(key string) bool {
	if b.limit <= 0 {
		return true
	}
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.After(b.reset) {
		b.counts = make(map[string]int)
		b.reset = now.Add(b.window)
	}
	b.counts[key]++
	return b.counts[key] <= b.limit
}

// sweep 清理过期计数（由后台任务调用）。
func (b *bucket) sweep() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if time.Now().After(b.reset) {
		b.counts = make(map[string]int)
		b.reset = time.Now().Add(b.window)
	}
}

// RunMaintenance 启动后台任务：清扫过期会话、刷新指标。返回停止函数。
func (s *Server) RunMaintenance(ctx context.Context) func() {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(s.cfg.PruneInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if res, err := s.svc.Prune(ctx); err != nil {
					s.log.Warn("prune failed", "err", err)
				} else if res.SessionsExpired > 0 || res.MembersExpired > 0 {
					s.log.Info("prune", "sessions", res.SessionsExpired, "members", res.MembersExpired)
					s.metrics.SessionsExpired.Add(uint64(res.SessionsExpired))
				}
				s.refreshGauges(ctx)
				s.limiterMu.Lock()
				s.ipCreate.sweep()
				s.ipJoin.sweep()
				s.limiterMu.Unlock()
			}
		}
	}()
	return cancel
}

// refreshGauges 采集瞬时指标。
func (s *Server) refreshGauges(ctx context.Context) {
	if st, err := s.svc.Stats(ctx); err == nil {
		s.metrics.SetGauge("sessions_current", float64(st.Sessions))
		s.metrics.SetGauge("members_current", float64(st.Members))
		s.metrics.SetGauge("members_pending_current", float64(st.Pending))
	}
	hs := s.hub.Stats()
	s.metrics.SetGauge("online_members_current", float64(hs.OnlineMembers))
	s.metrics.SetGauge("online_conns_current", float64(hs.OnlineConns))
	s.metrics.SetGauge("relay_rooms_current", float64(hs.Rooms))
}

// unauthorized 统一处理票据校验失败的响应。
func (s *Server) ticketFrom(r *http.Request) (*auth.Ticket, error) {
	tok := r.URL.Query().Get("ticket")
	if tok == "" {
		// 也允许 Authorization: Bearer <ticket>（非浏览器客户端）。
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			tok = strings.TrimPrefix(h, "Bearer ")
		}
	}
	if tok == "" {
		return nil, auth.ErrTicketInvalid
	}
	return s.tickets.Verify(tok)
}
