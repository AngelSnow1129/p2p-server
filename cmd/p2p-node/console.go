package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"p2psession/pkg/sessionclient"
)

// cliHandler 是 CLI 的事件处理器：把会话事件打印出来，并维护聊天状态。
type cliHandler struct {
	sessionclient.BaseHandler

	name string

	mu    sync.Mutex
	cl    *sessionclient.Client
	peers map[string]sessionclient.MemberInfo
	// defaultPeer 为 pair 模式自动选定的默认对端。
	defaultPeer string
	// grantedStreams 为等待读取的入站流。
	grantedStreams chan *sessionclient.Stream
	// ready 在收到成员列表后关闭。
	ready chan struct{}
	// status 记录本端可见状态（审批等待）。
	status string
	// selfIndex 为本端数据面下标。
	selfIndex uint16
}

func newCLIHandler(name string) *cliHandler {
	return &cliHandler{
		name:           name,
		peers:          make(map[string]sessionclient.MemberInfo),
		grantedStreams: make(chan *sessionclient.Stream, 16),
		ready:          make(chan struct{}),
	}
}

func (h *cliHandler) attach(c *sessionclient.Client) {
	h.mu.Lock()
	h.cl = c
	h.selfIndex = c.Self().Index
	h.mu.Unlock()
}

// ---- Handler 实现 -----------------------------------------------------------

func (h *cliHandler) OnReady(_ *sessionclient.Client, self sessionclient.MemberInfo, peers []sessionclient.MemberInfo) {
	h.mu.Lock()
	h.selfIndex = self.Index
	h.status = self.Status
	h.peers = make(map[string]sessionclient.MemberInfo, len(peers))
	for _, p := range peers {
		h.peers[p.MemberID] = p
	}
	h.pickDefaultPeerLocked()
	h.mu.Unlock()

	select {
	case <-h.ready:
	default:
		close(h.ready)
	}

	fmt.Printf("\n[已就绪] 会话内对端: %d\n", len(peers))
	for _, p := range peers {
		fmt.Printf("  - %s   index=%d  node=%s\n", p.MemberID, p.Index, short(p.NodeID))
	}
	if len(peers) == 0 {
		fmt.Printf("  （等待对端加入：把 Join Code 给对方即可自动配对）\n")
	}
}

func (h *cliHandler) OnMemberJoined(_ *sessionclient.Client, m sessionclient.MemberInfo) {
	h.mu.Lock()
	if m.MemberID != "" {
		h.peers[m.MemberID] = m
	}
	h.pickDefaultPeerLocked()
	h.mu.Unlock()
	fmt.Printf("\n[成员加入] %s  index=%d\n", m.MemberID, m.Index)
}

func (h *cliHandler) OnMemberLeft(_ *sessionclient.Client, memberID string) {
	h.mu.Lock()
	delete(h.peers, memberID)
	h.mu.Unlock()
	fmt.Printf("\n[成员离开] %s\n", memberID)
}

func (h *cliHandler) OnStream(_ *sessionclient.Client, s *sessionclient.Stream) {
	fmt.Printf("\n[入站流] 来自 %s  stream=%d\n", s.PeerID(), s.ID())
	select {
	case h.grantedStreams <- s:
	default:
		_ = s.Reset("no reader")
	}
}

func (h *cliHandler) OnPeerState(_ *sessionclient.Client, peerID, state string) {
	// 状态噪声较大，仅在调试时打印。
	if os.Getenv("P2P_NODE_VERBOSE") != "" {
		fmt.Printf("\n[对端状态] %s -> %s\n", peerID, state)
	}
}

func (h *cliHandler) OnError(_ *sessionclient.Client, code, msg string) {
	fmt.Printf("\n[错误] %s: %s\n", code, msg)
}

func (h *cliHandler) OnClose(_ *sessionclient.Client, err error) {
	if err != nil {
		fmt.Printf("\n[连接关闭] %v\n", err)
	} else {
		fmt.Printf("\n[连接关闭]\n")
	}
}

// ---- 交互控制台 --------------------------------------------------------------

// runConsole 进入交互模式，直到 ctx 取消或用户输入 /quit。
func (h *cliHandler) runConsole(ctx context.Context) error {
	lines := make(chan string, 8)
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case s := <-h.grantedStreams:
			// 自动开始读取入站流。
			go h.pumpStream(s)
		case line, ok := <-lines:
			if !ok {
				return nil
			}
			if quit := h.handleLine(strings.TrimSpace(line)); quit {
				return nil
			}
		}
	}
}

// pumpStream 持续打印入站流的数据。
func (h *cliHandler) pumpStream(s *sessionclient.Stream) {
	for {
		data, err := s.ReadMessage()
		if err != nil {
			fmt.Printf("\n[流关闭] %s stream=%d (%v)\n", s.PeerID(), s.ID(), err)
			return
		}
		fmt.Printf("\n[流 %s] %s\n", short(s.PeerID()), string(data))
	}
}

// handleLine 处理一行输入；返回 true 表示应退出。
func (h *cliHandler) handleLine(line string) bool {
	if line == "" {
		return false
	}
	if strings.HasPrefix(line, "/") {
		return h.handleCommand(line)
	}
	// 普通文本：发给默认对端。
	target := h.currentPeer()
	if target == "" {
		fmt.Println("（房间内还没有对端；等待对方加入后再发送）")
		return false
	}
	if err := h.sendTo(target, line); err != nil {
		fmt.Printf("发送失败: %v\n", err)
	}
	return false
}

func (h *cliHandler) handleCommand(line string) bool {
	fields := strings.Fields(line)
	cmd := fields[0]
	arg := func(i int) string {
		if i < len(fields) {
			return fields[i]
		}
		return ""
	}

	switch cmd {
	case "/quit", "/exit":
		fmt.Println("离开会话…")
		return true

	case "/help":
		fmt.Println("  <文本>              发给默认对端")
		fmt.Println("  /to <member> <文本> 定向发送")
		fmt.Println("  /all <文本>         广播（group）")
		fmt.Println("  /list               列出成员")
		fmt.Println("  /stream             与默认对端打开逻辑流")
		fmt.Println("  /quit               退出")

	case "/list":
		h.mu.Lock()
		ids := make([]string, 0, len(h.peers))
		for id := range h.peers {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		def := h.defaultPeer
		h.mu.Unlock()
		if len(ids) == 0 {
			fmt.Println("（暂无对端）")
			return false
		}
		fmt.Printf("会话内对端（%d）:\n", len(ids))
		for _, id := range ids {
			mark := " "
			if id == def {
				mark = "*"
			}
			fmt.Printf("  %s %s\n", mark, id)
		}

	case "/stream":
		target := h.currentPeer()
		if target == "" {
			fmt.Println("（暂无默认对端）")
			return false
		}
		s, err := h.openStream(target)
		if err != nil {
			fmt.Printf("打开流失败: %v\n", err)
			return false
		}
		fmt.Printf("已打开流 stream=%d -> %s；此后输入将经该流发送\n", s.ID(), short(target))
		go h.pumpStream(s)

	case "/to":
		target := arg(1)
		text := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(line, cmd), " "+target))
		if target == "" || text == "" {
			fmt.Println("用法: /to <member_id> <文本>")
			return false
		}
		if err := h.sendTo(target, text); err != nil {
			fmt.Printf("发送失败: %v\n", err)
		}

	case "/all":
		text := strings.TrimSpace(strings.TrimPrefix(line, cmd))
		if text == "" {
			fmt.Println("用法: /all <文本>")
			return false
		}
		if err := h.broadcast(text); err != nil {
			fmt.Printf("广播失败: %v\n", err)
		}

	default:
		fmt.Printf("未知命令 %s（/help 查看帮助）\n", cmd)
	}
	return false
}

// ---- 发送辅助 ----------------------------------------------------------------

// sendTo 向指定成员发送文本（内部打开临时流）。
func (h *cliHandler) sendTo(memberID, text string) error {
	h.mu.Lock()
	cl := h.cl
	h.mu.Unlock()
	if cl == nil {
		return fmt.Errorf("尚未连接")
	}
	if _, ok := cl.Peer(memberID); !ok {
		return fmt.Errorf("成员不存在: %s", memberID)
	}
	s, err := cl.OpenStream(memberID)
	if err != nil {
		return err
	}
	if _, err := s.Write([]byte(text)); err != nil {
		return err
	}
	fmt.Printf("[-> %s] %s\n", short(memberID), text)
	// 发送完成即关闭流：接收端会在读完数据后看到关闭。
	return s.Close()
}

// broadcast 向全房间广播（group 模式）。
func (h *cliHandler) broadcast(text string) error {
	h.mu.Lock()
	cl := h.cl
	peers := make([]sessionclient.MemberInfo, 0, len(h.peers))
	for _, p := range h.peers {
		peers = append(peers, p)
	}
	h.mu.Unlock()
	if cl == nil {
		return fmt.Errorf("尚未连接")
	}
	if len(peers) == 0 {
		return fmt.Errorf("房间内没有对端")
	}
	for _, p := range peers {
		if err := h.sendTo(p.MemberID, text); err != nil {
			fmt.Printf("发给 %s 失败: %v\n", short(p.MemberID), err)
		}
	}
	return nil
}

// openStream 打开一条长期流。
func (h *cliHandler) openStream(memberID string) (*sessionclient.Stream, error) {
	h.mu.Lock()
	cl := h.cl
	h.mu.Unlock()
	if cl == nil {
		return nil, fmt.Errorf("尚未连接")
	}
	return cl.OpenStream(memberID)
}

// currentPeer 返回当前默认对端（pair 模式唯一对端；group 模式取字典序首个）。
func (h *cliHandler) currentPeer() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.defaultPeer
}

// pickDefaultPeerLocked 选定默认对端（调用方需持有锁）。
func (h *cliHandler) pickDefaultPeerLocked() {
	if _, ok := h.peers[h.defaultPeer]; ok {
		return
	}
	ids := make([]string, 0, len(h.peers))
	for id := range h.peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		h.defaultPeer = ""
		return
	}
	h.defaultPeer = ids[0]
}

// short 截断 ID 便于显示。
func short(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12]
}
