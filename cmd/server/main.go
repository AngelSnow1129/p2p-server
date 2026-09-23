// p2psession-server 是 Session 协调 + 中继服务器。
//
// 全部配置来自 P2PS_ 前缀环境变量；不传任何变量时可零配置启动（监听 :60000）。
//
//	go run ./cmd/server
//
// 架构要点（详见 DESIGN.md）：
//   - 控制面 /ws/control 与数据面 /ws/relay 分离；
//   - 服务器负责 Session / Token / 成员发现 / 授权 / 中继，看不到业务明文；
//   - 单实例内存存储；水平扩展时替换 storage.Store 实现并共享 P2PS_TICKET_KEY。
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"p2psession/internal/auth"
	"p2psession/internal/config"
	"p2psession/internal/member"
	"p2psession/internal/metrics"
	"p2psession/internal/relay"
	"p2psession/internal/server"
	"p2psession/internal/session"
	"p2psession/internal/storage"
	"p2psession/internal/version"
)

func main() {
	// 版本查询要在配置校验**之前**处理：`p2psession-server --version`
	// 常被用在安装脚本与升级检查里，此时环境变量可能还没准备好，
	// 若先跑 config.FromEnv() 会因配置错误而退出（没设 P2PS_* 也会用
	// 默认值，但那属于巧合而非保证）。
	if isVersionRequest(os.Args[1:]) {
		fmt.Println(version.Short())
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	logger.Info("p2psession starting", "version", version.Short())

	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(2)
	}

	// 票据密钥：配置了就用（多实例必须共享），否则启动时随机生成（仅单实例）。
	tickets, err := resolveTicketKey(cfg, logger)
	if err != nil {
		logger.Error("ticket key", "err", err)
		os.Exit(2)
	}

	store, closeStore, err := openStore(cfg, logger)
	if err != nil {
		logger.Error("storage", "err", err)
		os.Exit(2)
	}
	defer closeStore()

	svc := session.New(store, tickets, session.Config{
		TicketTTL:   cfg.TicketTTL,
		MemberGrace: cfg.MemberGrace,
		DefaultTTL:  cfg.DefaultTTL,
		MaxSessions: cfg.MaxSessions,
	})
	hub := member.NewHub()
	reg := metrics.New()
	fwd := relay.New(hub, svc, cfg.MaxRelayFrame)

	srv := server.New(cfg, svc, hub, fwd, reg, tickets, logger)

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// WebSocket 长连接：不能设置整体 ReadTimeout/WriteTimeout。
		IdleTimeout: 120 * time.Second,
	}

	stopMaint := srv.RunMaintenance(context.Background())
	defer stopMaint()

	go func() {
		logger.Info("p2psession server starting", "config", cfg.Redacted())
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutting down", "signal", sig.String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx, httpSrv); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
	logger.Info("stopped")
}

// isVersionRequest 判断是否请求版本信息。
//
// 同时接受 -v/--version：前者是 Go 工具的惯例，后者是 GNU 惯例，
// 两类用户都会下意识地试其中一个。
func isVersionRequest(args []string) bool {
	for _, a := range args {
		switch a {
		case "-v", "--version", "version":
			return true
		}
	}
	return false
}

// openStore 按配置创建会话存储，并返回关闭函数。
//
// memory 为默认（零依赖）；sqlite 持久化到单文件，重启后会话仍在。
func openStore(cfg config.Config, logger *slog.Logger) (storage.Store, func(), error) {
	switch cfg.StorageBackend {
	case config.StorageSQLite:
		st, err := storage.NewSQLite(cfg.SQLitePath)
		if err != nil {
			return nil, nil, err
		}
		logger.Info("using sqlite storage", "path", cfg.SQLitePath)
		return st, func() {
			if err := st.CloseDB(); err != nil {
				logger.Warn("close sqlite", "err", err)
			}
		}, nil
	case config.StorageMemory, "":
		logger.Info("using in-memory storage (sessions do not survive restart)")
		return storage.NewMemory(), func() {}, nil
	default:
		// 理论上 Validate 已拦截；这里兜底避免静默降级到内存。
		return nil, nil, fmt.Errorf("unknown storage backend %q", cfg.StorageBackend)
	}
}

// resolveTicketKey 解析或生成票据密钥。
func resolveTicketKey(cfg config.Config, logger *slog.Logger) (*auth.TicketKey, error) {
	if cfg.TicketKey != "" {
		k, err := auth.TicketKeyFromString(cfg.TicketKey)
		if err != nil {
			return nil, err
		}
		return k, nil
	}
	k, err := auth.NewTicketKey()
	if err != nil {
		return nil, err
	}
	// 提示运维把该值固化到配置，以便多实例/重启后旧票据仍有效。
	raw, err := base64.RawURLEncoding.DecodeString(k.String())
	if err == nil && len(raw) > 0 {
		logger.Warn("no P2PS_TICKET_KEY configured; generated an ephemeral key " +
			"(connections tickets will not survive restart and cannot work across replicas)")
	}
	return k, nil
}
