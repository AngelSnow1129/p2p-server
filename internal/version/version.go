// Package version 暴露构建期注入的版本信息。
//
// 为什么单独一个包：版本号必须能被**所有**入口读到（服务器启动日志、
// /v1/health、Prometheus 的 build_info 指标、CLI 的 --version），
// 而它们分属不同包。集中一处避免各自复制一份变量与解析逻辑。
//
// 注入方式（ldflags，见 Makefile）：
//
//	go build -ldflags "\
//	  -X p2psession/internal/version.Version=1.2.3 \
//	  -X p2psession/internal/version.Commit=abc1234 \
//	  -X p2psession/internal/version.BuildDate=2026-09-23T00:00:00Z"
//
// 未注入时（如 `go run ./cmd/server`）退化为合理的默认值，
// 而不是空字符串——空版本号会让日志与指标难以解读。
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

// 这三个变量由 ldflags 注入。刻意保持 var 而非 const：
// 只有 var 才能被 -X 覆盖。
var (
	// Version 为语义化版本号（如 "1.0.0" 或 "v1.0.0"）。
	Version = "dev"
	// Commit 为构建时的 git 短哈希。
	Commit = "unknown"
	// BuildDate 为构建时间（RFC3339）。
	BuildDate = "unknown"
)

// buildInfoOnce 保证从二进制元数据回填只做一次。
var (
	buildInfoOnce sync.Once
	buildInfoDone bool
)

// resolve 在可能的范围内从 Go 二进制内嵌的构建信息回填。
//
// 用途：`go install ...@latest` 或 `go build` 直接产出的二进制
// 没有走我们的 Makefile，因此没有 ldflags；但 Go 会把模块版本与
// VCS 信息嵌进二进制。回填后这类二进制也能报出可读版本，
// 而不是笼统的 "dev"。
//
// 只在 ldflags **未**提供对应值时回填，避免覆盖显式注入。
func resolve() {
	buildInfoOnce.Do(func() {
		info, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}

		// 模块版本：go install 时为 vX.Y.Z 或 (devel)。
		if Version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			Version = info.Main.Version
		}

		// VCS 信息：本地 go build 且处于 git 仓库时存在。
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if Commit == "unknown" && s.Value != "" {
					Commit = s.Value
					if len(Commit) > 12 {
						Commit = Commit[:12]
					}
				}
			case "vcs.time":
				if BuildDate == "unknown" && s.Value != "" {
					BuildDate = s.Value
				}
			case "vcs.modified":
				// 工作区有未提交改动时标记，避免把本地改动误认为发布版本。
				if s.Value == "true" && !strings.HasSuffix(Version, "-dirty") {
					Version += "-dirty"
				}
			}
		}
		buildInfoDone = true
	})
}

// Short 返回单行摘要，适合放进启动日志。
//
//	1.0.0 (abc1234, 2026-09-23T00:00:00Z, go1.25.12, linux/amd64)
func Short() string {
	resolve()
	return fmt.Sprintf("%s (%s, %s, %s, %s/%s)",
		Version, Commit, BuildDate, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

// String 返回纯版本号。
func String() string {
	resolve()
	return Version
}

// Fields 返回版本信息的结构化形式（供 metrics / JSON 输出）。
func Fields() map[string]string {
	resolve()
	return map[string]string{
		"version":    Version,
		"commit":     Commit,
		"build_date": BuildDate,
		"go_version": runtime.Version(),
		"goos":       runtime.GOOS,
		"goarch":     runtime.GOARCH,
	}
}
