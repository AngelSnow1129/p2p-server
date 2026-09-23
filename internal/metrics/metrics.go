// Package metrics 提供轻量的运行指标：进程内计数器 + Prometheus 文本输出。
//
// 刻意不引入 prometheus/client_golang：本服务指标维度少，手写文本格式更省依赖
// 且便于审计。计数器用 atomic，采集时聚合，不阻塞数据面。
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"p2psession/internal/version"
)

// escapeLabel 转义 Prometheus 标签值中的特殊字符。
//
// 版本号/commit 通常安全，但 build_date 可能被替换成含引号的字符串，
// 未转义会生成非法 exposition 格式，导致抓取端整页解析失败
// ——一个字段坏掉会拖垮全部指标。
func escapeLabel(s string) string {
	if !strings.ContainsAny(s, `\"`+"\n") {
		return s
	}
	r := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return r.Replace(s)
}

// Registry 汇总全部指标。
type Registry struct {
	start time.Time

	// 控制面
	ControlConnects    atomic.Uint64
	ControlDisconnects atomic.Uint64
	AuthFailures       atomic.Uint64
	RateLimited        atomic.Uint64

	// 会话
	SessionsCreated atomic.Uint64
	SessionsExpired atomic.Uint64
	JoinsTotal      atomic.Uint64
	JoinsRejected   atomic.Uint64
	Rejoins         atomic.Uint64

	// 数据面
	RelayFramesIn  atomic.Uint64
	RelayFramesOut atomic.Uint64
	RelayBytesIn   atomic.Uint64
	RelayBytesOut  atomic.Uint64
	RelayDenied    atomic.Uint64

	// 当前值（由采集时注入，非累加）
	mu       sync.Mutex
	gauges   map[string]float64
	gaugesMu sync.RWMutex
}

// New 创建指标注册表。
func New() *Registry {
	return &Registry{
		start:  time.Now(),
		gauges: make(map[string]float64),
	}
}

// SetGauge 设置一个瞬时值（如在线成员数）。
func (r *Registry) SetGauge(name string, v float64) {
	r.gaugesMu.Lock()
	r.gauges[name] = v
	r.gaugesMu.Unlock()
}

// Uptime 返回进程运行时长。
func (r *Registry) Uptime() time.Duration { return time.Since(r.start) }

// Render 输出 Prometheus 文本格式。
func (r *Registry) Render() string {
	var b strings.Builder
	w := func(name, help, typ string, v float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, v)
	}
	const ns = "p2psession_"

	w(ns+"uptime_seconds", "进程运行时长（秒）", "gauge", r.Uptime().Seconds())
	w(ns+"control_connects_total", "控制面连接建立总数", "counter", float64(r.ControlConnects.Load()))
	w(ns+"control_disconnects_total", "控制面连接断开总数", "counter", float64(r.ControlDisconnects.Load()))
	w(ns+"auth_failures_total", "鉴权失败总数", "counter", float64(r.AuthFailures.Load()))
	w(ns+"rate_limited_total", "被限流次数", "counter", float64(r.RateLimited.Load()))
	w(ns+"sessions_created_total", "创建会话总数", "counter", float64(r.SessionsCreated.Load()))
	w(ns+"sessions_expired_total", "过期回收会话总数", "counter", float64(r.SessionsExpired.Load()))
	w(ns+"joins_total", "成功加入次数", "counter", float64(r.JoinsTotal.Load()))
	w(ns+"joins_rejected_total", "被拒绝的加入次数", "counter", float64(r.JoinsRejected.Load()))
	w(ns+"rejoins_total", "同一节点重连复用成员次数", "counter", float64(r.Rejoins.Load()))
	w(ns+"relay_frames_in_total", "数据面接收帧数", "counter", float64(r.RelayFramesIn.Load()))
	w(ns+"relay_frames_out_total", "数据面投递帧数", "counter", float64(r.RelayFramesOut.Load()))
	w(ns+"relay_bytes_in_total", "数据面接收字节数", "counter", float64(r.RelayBytesIn.Load()))
	w(ns+"relay_bytes_out_total", "数据面投递字节数", "counter", float64(r.RelayBytesOut.Load()))
	w(ns+"relay_denied_total", "数据面被拒绝帧数", "counter", float64(r.RelayDenied.Load()))

	// build_info 是 Prometheus 的惯例：值恒为 1，版本信息全在标签里。
	// 这样可按版本做查询/聚合（如 sum by (version)(build_info)），
	// 且新增标签不会破坏既有时间序列语义。
	//
	// 放在 /metrics（受 P2PS_ADMIN_TOKEN 保护）而非公开的 /v1/health，
	// 是为了不向匿名请求者暴露完整构建信息（commit 可反推未修补状态）。
	{
		fields := version.Fields()
		keys := make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		labels := make([]string, 0, len(keys))
		for _, k := range keys {
			labels = append(labels, fmt.Sprintf("%s=%q", k, escapeLabel(fields[k])))
		}
		fmt.Fprintf(&b, "# HELP %sbuild_info 构建信息（值恒为 1）\n", ns)
		fmt.Fprintf(&b, "# TYPE %sbuild_info gauge\n", ns)
		fmt.Fprintf(&b, "%sbuild_info{%s} 1\n", ns, strings.Join(labels, ","))
	}

	r.gaugesMu.RLock()
	names := make([]string, 0, len(r.gauges))
	for n := range r.gauges {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		w(ns+n, "当前值", "gauge", r.gauges[n])
	}
	r.gaugesMu.RUnlock()
	return b.String()
}
