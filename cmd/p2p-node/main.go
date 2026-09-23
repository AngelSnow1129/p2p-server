// p2p-node 是 p2psession 的客户端 CLI，演示「极简配对」体验：
//
//	p2p-node init                                  生成本机身份（NodeID）
//	p2p-node create [--mode pair|group] [--max-members N]
//	                                               创建会话，打印 Join Code
//	p2p-node join P2P-XXXX-XXXX-…                  用 Join Code 加入并聊天
//	p2p-node join --token <TOKEN>                  用机器形式 token 加入
//	p2p-node chat --session sess_… --token <TOKEN> 同上（显式指定会话）
//
// 用户不需要填写 IP、端口、Peer ID、NAT 信息、Relay 地址或公钥：
// 全部由服务器在加入时下发与协调。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"p2psession/internal/auth"
	"p2psession/internal/version"
	"p2psession/pkg/sessionclient"
)

const (
	defaultIdentityPath = ".p2p-node/identity.json"
	defaultServer       = "http://127.0.0.1:60000"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "init":
		err = cmdInit(args)
	case "create":
		err = cmdCreate(args)
	case "join":
		err = cmdJoin(args)
	case "whoami":
		err = cmdWhoami(args)
	case "-v", "--version", "version":
		fmt.Println(version.Short())
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `p2p-node - P2P Session 节点

用法:
  p2p-node init                                   生成身份（NodeID）
  p2p-node whoami                                 显示本机身份
  p2p-node create [选项]                           创建会话并打印 Join Code
  p2p-node join <JOIN_CODE>                       用 Join Code 加入并聊天
  p2p-node join --token <TOKEN>                   用 token 加入并聊天
  p2p-node join --session <ID> --token <TOKEN>    显式指定会话加入

通用选项:
  -f <path>     身份文件（默认 ./.p2p-node/identity.json）
  -s <url>      服务器地址（默认 http://127.0.0.1:60000）

create 选项:
  --mode pair|group        会话模式（默认 pair）
  --max-members N          group 模式成员上限（默认 10）
  --join-limit N           最多加入次数（0 = 不限）
  --ttl 30m                会话存活时长
  --require-approval       新成员需 owner 批准
  --idle-timeout 10m       全员离线后自动过期

聊天命令（进入后输入）:
  <文本>                   发给当前默认对端（pair 自动选定）
  /to <member_id> <文本>   发给指定成员
  /all <文本>              广播给全房间（group）
  /list                    列出成员
  /stream                  与默认对端打开一条逻辑流
  /quit                    离开会话
`)
}

// ---- 通用 -------------------------------------------------------------------

func identityPath(v string) string {
	if v != "" {
		return v
	}
	if env := os.Getenv("P2P_NODE_IDENTITY"); env != "" {
		return env
	}
	return defaultIdentityPath
}

// loadOrCreateIdentity 加载身份；不存在则创建。
func loadOrCreateIdentity(path string) (*auth.Identity, bool, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, false, err
		}
	}
	return auth.LoadOrCreateIdentity(path)
}

// signalContext 返回在 Ctrl-C 时取消的上下文。
func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		fmt.Println("\n正在退出…")
		cancel()
	}()
	return ctx, cancel
}

// ---- init / whoami ---------------------------------------------------------

func cmdInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	keyPath := fs.String("f", "", "身份文件路径")
	force := fs.Bool("force", false, "覆盖已存在的身份")
	if err := fs.Parse(args); err != nil {
		return err
	}
	path := identityPath(*keyPath)

	if *force {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	id, created, err := loadOrCreateIdentity(path)
	if err != nil {
		return err
	}
	if created {
		fmt.Printf("已生成节点身份\n  文件:   %s\n  NodeID: %s\n", path, id.NodeID())
	} else {
		fmt.Printf("身份已存在（未修改）\n  文件:   %s\n  NodeID: %s\n", path, id.NodeID())
	}
	return nil
}

func cmdWhoami(args []string) error {
	fs := flag.NewFlagSet("whoami", flag.ContinueOnError)
	keyPath := fs.String("f", "", "身份文件路径")
	if err := fs.Parse(args); err != nil {
		return err
	}
	path := identityPath(*keyPath)
	id, err := auth.LoadIdentity(path)
	if err != nil {
		return fmt.Errorf("加载身份失败（先运行 p2p-node init）: %w", err)
	}
	fmt.Printf("NodeID: %s\n文件:   %s\n", id.NodeID(), path)
	return nil
}

// ---- create ----------------------------------------------------------------

func cmdCreate(args []string) error {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	server := fs.String("s", defaultServer, "服务器地址")
	mode := fs.String("mode", "pair", "会话模式 pair|group")
	maxMembers := fs.Int("max-members", 0, "group 模式成员上限")
	joinLimit := fs.Int("join-limit", 0, "最多加入次数（0 = 不限）")
	ttl := fs.Duration("ttl", 0, "会话存活时长（如 30m）")
	requireApproval := fs.Bool("require-approval", false, "新成员需 owner 批准")
	idleTimeout := fs.Duration("idle-timeout", 0, "全员离线后自动过期")
	asJSON := fs.Bool("json", false, "以 JSON 输出（便于脚本消费）")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := sessionclient.Create(ctx, sessionclient.CreateParams{
		ServerURL:       *server,
		Mode:            *mode,
		MaxMembers:      *maxMembers,
		JoinLimit:       *joinLimit,
		TTL:             *ttl,
		RequireApproval: *requireApproval,
		IdleTimeout:     *idleTimeout,
	})
	if err != nil {
		return err
	}

	if *asJSON {
		fmt.Printf("{\"session_id\":%q,\"join_token\":%q,\"join_code\":%q,\"expires_at\":%q}\n",
			res.SessionID, res.JoinToken, res.JoinCode, res.ExpiresAt.UTC().Format(time.RFC3339))
		return nil
	}

	fmt.Printf("Session created\n\n")
	fmt.Printf("  Session ID:  %s\n", res.SessionID)
	fmt.Printf("  Join Code:   %s\n", res.JoinCode)
	fmt.Printf("  Join Token:  %s\n", res.JoinToken)
	fmt.Printf("  Mode:        %s (max %d)\n", res.Mode, res.MaxMembers)
	if res.RequireApproval {
		fmt.Printf("  Approval:    owner 需批准新成员\n")
	}
	if res.JoinLimit > 0 {
		fmt.Printf("  Join Limit:  %d\n", res.JoinLimit)
	}
	fmt.Printf("  Expires:     %s\n", humanUntil(res.ExpiresAt))
	fmt.Printf("\n在另一台机器上执行:\n\n  p2p-node join %s\n\n", res.JoinCode)
	fmt.Printf("（Join Code 与 Join Token 等价，均只在此刻显示一次）\n")
	return nil
}

// humanUntil 返回「还有多久」的可读描述。
func humanUntil(t time.Time) string {
	d := time.Until(t)
	if d <= 0 {
		return t.UTC().Format(time.RFC3339) + "（已过期）"
	}
	return fmt.Sprintf("%s（%s 后）", t.UTC().Format(time.RFC3339), d.Truncate(time.Second))
}

// ---- join ------------------------------------------------------------------

// valueFlags 列出需要一个值的选项名；extractJoinCode 用它区分
// 「选项的值」与「真正的位置参数」。
var valueFlags = map[string]bool{
	"-f": true, "-s": true, "--session": true, "--token": true,
	"--name": true, "--capabilities": true,
}

// extractJoinCode 从参数列表中摘出 Join Code 位置参数。
//
// 为什么需要这一步：标准库 flag 在遇到第一个非选项参数后**停止解析**，
// 因此 `join P2P-XXXX -f a.key -s http://…` 里的 -f/-s 会被当成位置参数
// 静默忽略（用户会莫名其妙连到默认的 :60000）。而「先写 code 再写选项」
// 恰恰是最自然的用法，所以这里显式支持选项与位置参数混排。
func extractJoinCode(args []string) (string, []string) {
	rest := make([]string, 0, len(args))
	code := ""
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			rest = append(rest, a)
			// 该选项需要取值时，把紧随其后的参数一并保留（即使它不以 - 开头）。
			if valueFlags[a] && !strings.Contains(a, "=") && i+1 < len(args) {
				i++
				rest = append(rest, args[i])
			}
			continue
		}
		if code == "" {
			code = a
			continue
		}
		rest = append(rest, a) // 多余的未知位置参数交回 flag 报错
	}
	return code, rest
}

func cmdJoin(args []string) error {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	keyPath := fs.String("f", "", "身份文件路径")
	server := fs.String("s", defaultServer, "服务器地址")
	sessionID := fs.String("session", "", "会话 ID（与 --token 搭配）")
	token := fs.String("token", "", "机器形式 Join Token")
	name := fs.String("name", "", "显示名（仅本地使用）")
	noChat := fs.Bool("no-chat", false, "只加入不进入交互（用于自动化）")
	capabilities := fs.String("capabilities", "udp,tcp,relay", "本节点能力声明（逗号分隔）")

	// 先摘出 Join Code，再解析选项——支持二者任意顺序。
	code, rest := extractJoinCode(args)
	if err := fs.Parse(rest); err != nil {
		return err
	}

	if code == "" && *token == "" {
		return errors.New("需要 Join Code 位置参数或 --token")
	}
	if code != "" && *token != "" {
		return errors.New("Join Code 与 --token 只能二选一")
	}

	id, created, err := loadOrCreateIdentity(identityPath(*keyPath))
	if err != nil {
		return err
	}
	if created {
		fmt.Printf("已自动生成节点身份: %s\n", id.NodeID())
	}

	cfg := sessionclient.Config{
		ServerURL:    *server,
		Identity:     id,
		SessionID:    *sessionID,
		JoinToken:    *token,
		JoinCode:     code,
		Capabilities: splitCSV(*capabilities),
	}

	h := newCLIHandler(*name)
	ctx, cancel := signalContext()
	defer cancel()

	cl, err := sessionclient.Join(ctx, cfg, h)
	if err != nil {
		return err
	}
	h.attach(cl)
	defer cl.Close()

	fmt.Printf("\nJoined session\n\n")
	fmt.Printf("  Session:   %s\n", cl.SessionIDOf())
	fmt.Printf("  Member ID: %s\n", cl.Self().MemberID)
	if cl.Self().IsOwner {
		fmt.Printf("  Role:      owner\n")
	}
	if st := cl.Self().Status; st != "" && st != "active" {
		fmt.Printf("  Status:    %s（等待 owner 批准）\n", st)
	}

	if err := cl.WaitReady(ctx); err != nil {
		return err
	}
	fmt.Printf("  模式与成员已同步\n")

	if *noChat {
		fmt.Println("（--no-chat：保持连接 2 秒后退出）")
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
		}
		return nil
	}

	fmt.Println("\n已连接。使用 /list 查看成员，/all 广播，/to <id> 定向发送，/quit 退出。")
	return h.runConsole(ctx)
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
