// Package config 定义 p2psession 服务器的运行配置。
//
// 全量 12-factor：所有配置来自 P2PS_ 前缀环境变量，容器零文件部署。
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 是服务器全部可调参数。
type Config struct {
	// Listen 为 HTTP/WebSocket 监听地址。
	Listen string

	// TicketKey 为连接票据的 HMAC 密钥（base64url，32 字节）。
	// 多实例部署必须共享同一密钥；留空则启动时随机生成（仅适合单实例）。
	TicketKey string

	// DefaultTTL 为会话默认存活时长。
	DefaultTTL time.Duration
	// MaxSessions 为单实例会话上限（0 = 不限）。
	MaxSessions int
	// MemberGrace 为成员离线后保留 MemberID 的宽限时长。
	MemberGrace time.Duration
	// TicketTTL 为连接票据有效期。
	TicketTTL time.Duration

	// Heartbeat 为要求客户端发送 ping 的间隔。
	Heartbeat time.Duration
	// IdleTimeout 为连接读空闲断开阈值。
	IdleTimeout time.Duration

	// MaxControlFrame 为单条控制 JSON 上限（字节）。
	MaxControlFrame int64
	// MaxRelayFrame 为单帧数据面用户数据上限（字节）。
	MaxRelayFrame int

	// MaxCandidates 为单成员候选数量上限。
	MaxCandidates int

	// MsgRatePerSec / MsgRateBurst 为每连接控制消息令牌桶。
	MsgRatePerSec float64
	MsgRateBurst  int

	// RelayBytesPerSec 为每成员数据面中继带宽（字节/秒，0 = 不限）。
	RelayBytesPerSec int

	// RelayEnabled 控制是否允许数据面转发（关闭则只做信令与发现）。
	RelayEnabled bool

	// AllowedOrigins 为允许的 Origin 白名单（空 = 不校验，"*" = 全放行）。
	AllowedOrigins map[string]bool

	// TrustProxyHeaders 为 true 时信任 X-Forwarded-For / CF-Connecting-IP。
	TrustProxyHeaders bool

	// AdminToken 非空时管理类接口需 Bearer 令牌。
	AdminToken string

	// StaticDir 非空时在 / 下挂载静态文件目录。
	StaticDir string

	// PruneInterval 为过期会话/离线成员清扫间隔。
	PruneInterval time.Duration

	// SessionRatePerMin 为每 IP 创建会话的频率上限（次/分钟）。
	SessionRatePerMin int
	// JoinRatePerMin 为每 IP 加入会话的频率上限（次/分钟）。
	JoinRatePerMin int

	// StorageBackend 选择会话存储实现："memory"（默认，重启即清空）
	// 或 "sqlite"（持久化到单个文件，重启后会话仍在）。
	StorageBackend string
	// SQLitePath 为 SQLite 数据库文件路径（StorageBackend=sqlite 时生效）。
	// 目录必须已存在且可写。
	SQLitePath string
}

// 存储后端取值。
const (
	StorageMemory = "memory"
	StorageSQLite = "sqlite"
)

// Default 返回带默认值的配置。
func Default() Config {
	return Config{
		// 使用高端口（>=49152 的动态/私有端口区间），避免与本机常见服务
		// 及低端口（需要 root/CAP_NET_BIND_SERVICE）冲突。
		Listen:            ":60000",
		DefaultTTL:        30 * time.Minute,
		MaxSessions:       0,
		MemberGrace:       5 * time.Minute,
		TicketTTL:         10 * time.Minute,
		Heartbeat:         15 * time.Second,
		IdleTimeout:       45 * time.Second,
		MaxControlFrame:   1 << 16,
		MaxRelayFrame:     1 << 16,
		MaxCandidates:     16,
		MsgRatePerSec:     25,
		MsgRateBurst:      50,
		RelayBytesPerSec:  512 * 1024,
		RelayEnabled:      true,
		PruneInterval:     60 * time.Second,
		SessionRatePerMin: 30,
		JoinRatePerMin:    120,
		// 默认内存存储：零依赖、零配置启动。持久化是显式选择。
		StorageBackend: StorageMemory,
		SQLitePath:     "/data/p2psession.db",
	}
}

// FromEnv 读取 P2PS_ 前缀环境变量并校验。
func FromEnv() (Config, error) {
	cfg := Default()
	var errs []string
	get := func(k string) string { return os.Getenv("P2PS_" + k) }

	if v := get("LISTEN"); v != "" {
		cfg.Listen = v
	}
	if v := get("TICKET_KEY"); v != "" {
		cfg.TicketKey = v
	}
	if v := get("ADMIN_TOKEN"); v != "" {
		cfg.AdminToken = v
	}
	if v := get("STATIC_DIR"); v != "" {
		cfg.StaticDir = v
	}
	if v := get("ALLOWED_ORIGINS"); v != "" {
		cfg.AllowedOrigins = map[string]bool{}
		for _, o := range strings.Split(v, ",") {
			if o = strings.TrimSpace(o); o != "" {
				cfg.AllowedOrigins[strings.TrimRight(o, "/")] = true
			}
		}
	}

	dur := func(key string, dst *time.Duration) {
		if v := get(key); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				errs = append(errs, "P2PS_"+key+": "+err.Error())
				return
			}
			*dst = d
		}
	}
	dur("DEFAULT_TTL", &cfg.DefaultTTL)
	dur("MEMBER_GRACE", &cfg.MemberGrace)
	dur("TICKET_TTL", &cfg.TicketTTL)
	dur("HEARTBEAT", &cfg.Heartbeat)
	dur("IDLE_TIMEOUT", &cfg.IdleTimeout)
	dur("PRUNE_INTERVAL", &cfg.PruneInterval)

	integer := func(key string, dst *int) {
		if v := get(key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				errs = append(errs, "P2PS_"+key+": "+err.Error())
				return
			}
			*dst = n
		}
	}
	integer("MAX_SESSIONS", &cfg.MaxSessions)
	integer("MAX_RELAY_FRAME", &cfg.MaxRelayFrame)
	integer("MAX_CANDIDATES", &cfg.MaxCandidates)
	integer("MSG_RATE_BURST", &cfg.MsgRateBurst)
	integer("RELAY_BYTES_PER_SEC", &cfg.RelayBytesPerSec)
	integer("SESSION_RATE_PER_MIN", &cfg.SessionRatePerMin)
	integer("JOIN_RATE_PER_MIN", &cfg.JoinRatePerMin)

	if v := get("MAX_CONTROL_FRAME"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			errs = append(errs, "P2PS_MAX_CONTROL_FRAME: "+err.Error())
		} else {
			cfg.MaxControlFrame = n
		}
	}
	if v := get("MSG_RATE_PER_SEC"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			errs = append(errs, "P2PS_MSG_RATE_PER_SEC: "+err.Error())
		} else {
			cfg.MsgRatePerSec = f
		}
	}
	boolVar := func(key string, dst *bool) {
		v := get(key)
		if v == "" {
			return
		}
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "1", "true", "yes", "on":
			*dst = true
		case "0", "false", "no", "off":
			*dst = false
		default:
			errs = append(errs, "P2PS_"+key+": invalid bool "+v)
		}
	}
	boolVar("RELAY_ENABLED", &cfg.RelayEnabled)
	boolVar("TRUST_PROXY_HEADERS", &cfg.TrustProxyHeaders)

	if v := get("STORAGE_BACKEND"); v != "" {
		cfg.StorageBackend = strings.ToLower(strings.TrimSpace(v))
	}
	if v := get("SQLITE_PATH"); v != "" {
		cfg.SQLitePath = v
	}

	if err := cfg.Validate(); err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return cfg, errors.New(strings.Join(errs, "; "))
	}
	return cfg, nil
}

// Validate 做跨字段校验。
func (c Config) Validate() error {
	if c.TicketTTL < time.Second {
		return errors.New("ticket ttl too small")
	}
	if c.IdleTimeout < c.Heartbeat {
		return errors.New("idle timeout must be >= heartbeat")
	}
	if c.DefaultTTL < time.Minute {
		return errors.New("default ttl too small")
	}
	if c.MaxControlFrame < 4096 {
		return errors.New("max control frame too small")
	}
	if c.MaxRelayFrame < 1024 {
		return errors.New("max relay frame too small")
	}
	switch c.StorageBackend {
	case StorageMemory:
		// 无额外要求
	case StorageSQLite:
		if strings.TrimSpace(c.SQLitePath) == "" {
			return errors.New("sqlite path is empty (set P2PS_SQLITE_PATH)")
		}
	default:
		return fmt.Errorf("unknown storage backend %q (want %q or %q)",
			c.StorageBackend, StorageMemory, StorageSQLite)
	}
	return nil
}

// Redacted 返回可安全写日志的描述。
func (c Config) Redacted() string {
	admin := "off"
	if c.AdminToken != "" {
		admin = fmt.Sprintf("set(%d chars)", len(c.AdminToken))
	}
	key := "ephemeral"
	if c.TicketKey != "" {
		key = "configured"
	}
	return fmt.Sprintf(
		"listen=%s ticket_key=%s relay=%v relay_bw=%dB/s ttl=%s grace=%s hb=%s idle=%s origins=%v admin=%s storage=%s",
		c.Listen, key, c.RelayEnabled, c.RelayBytesPerSec,
		c.DefaultTTL, c.MemberGrace, c.Heartbeat, c.IdleTimeout,
		c.AllowedOrigins != nil, admin, c.storageDesc(),
	)
}

// storageDesc 描述存储后端（不泄露敏感信息）。
func (c Config) storageDesc() string {
	if c.StorageBackend == StorageSQLite {
		return "sqlite:" + c.SQLitePath
	}
	return c.StorageBackend
}
