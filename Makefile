# p2psession — 构建、打包与安装
#
# 二进制优先：本文件是「不用容器」部署路径的唯一入口。
#   make build           本机二进制（bin/）
#   make dist            全平台归档 + 校验和（dist/）
#   sudo make install    安装到 /usr/local/bin 并配置 systemd
#
# 设计取舍：
#   - 版本号来自 VERSION 文件（本仓库不是 git 仓库，无法依赖 git describe），
#     但若在 git 仓库内编译，会自动改用最近 tag + 提交哈希，两者都对；
#   - 所有目标都不依赖网络（不用 curl/wget 下载工具），便于离线构建；
#   - `install` 只做「放文件 + 装 unit」，不启动服务（见 INSTALL.md）。

# ---- 基本设置 --------------------------------------------------------------

SHELL := /bin/bash

# 二进制与包名。
SERVER_BIN := p2psession-server
CLIENT_BIN := p2p-node
# Go 主包路径。
SERVER_PKG := ./cmd/server
CLIENT_PKG := ./cmd/p2p-node

# 输出目录。
BINDIR   := bin
DISTDIR  := dist
# 安装前缀（make install PREFIX=/opt/p2p 可覆盖）。
PREFIX   ?= /usr/local
DESTDIR  ?=

# ---- 版本信息 --------------------------------------------------------------

# 版本来源优先级：
#   1. 命令行 VERSION=... （发布流水线显式指定）
#   2. VERSION 文件（本仓库的权威来源）
#   3. 最近 git tag（若确实处于 git 仓库）
#   4. dev
VERSION_FILE := VERSION
GIT := $(shell command -v git 2>/dev/null)
IN_GIT := $(shell test -d .git && echo yes)
ifeq ($(IN_GIT),yes)
  GIT_VERSION := $(shell $(GIT) describe --tags --always --dirty 2>/dev/null)
endif

VERSION ?= $(if $(wildcard $(VERSION_FILE)),$(shell cat $(VERSION_FILE)),$(if $(GIT_VERSION),$(GIT_VERSION),dev))
# 提交哈希：git 仓库内取短哈希，否则 unknown。
COMMIT  ?= $(if $(GIT_VERSION),$(shell $(GIT) rev-parse --short=12 HEAD 2>/dev/null),unknown)
# 构建时间：固定为 UTC，避免不同时区产出不同二进制而破坏可复现性。
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

# 注入到 internal/version 的变量（见 internal/version/version.go）。
VERSION_PKG := p2psession/internal/version
LDFLAGS := -s -w \
	-X $(VERSION_PKG).Version=$(VERSION) \
	-X $(VERSION_PKG).Commit=$(COMMIT) \
	-X $(VERSION_PKG).BuildDate=$(BUILD_DATE)

# CGO 必须关闭：SQLite 驱动选用纯 Go 实现（modernc.org/sqlite）正是为此，
# 关闭后产出静态链接二进制，可在 alpine/scratch 上直接跑。
export CGO_ENABLED := 0

GO ?= go
GOFLAGS ?= -trimpath

# 发布平台列表。用 `linux/amd64` 形式，便于循环里拆分。
PLATFORMS ?= linux/amd64 linux/arm64 linux/arm/v7 darwin/amd64 darwin/arm64 windows/amd64

# ---- 默认目标 --------------------------------------------------------------

.DEFAULT_GOAL := help

.PHONY: help
help: ## 显示本帮助
	@printf '\np2psession 构建目标：\n\n'
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@printf '\n当前版本: %s (commit %s)\n\n' '$(VERSION)' '$(COMMIT)'

.PHONY: version
version: ## 打印将写入二进制的版本信息
	@echo "version    = $(VERSION)"
	@echo "commit     = $(COMMIT)"
	@echo "build_date = $(BUILD_DATE)"
	@echo "ldflags    = $(LDFLAGS)"

# ---- 构建 ------------------------------------------------------------------

.PHONY: build
build: $(BINDIR)/$(SERVER_BIN) $(BINDIR)/$(CLIENT_BIN) ## 构建本机二进制到 bin/

# 两个二进制都注入同一份版本信息。
$(BINDIR)/$(SERVER_BIN): $(shell find . -name '*.go' -not -path './.git/*' 2>/dev/null) go.mod go.sum
	@mkdir -p $(BINDIR)
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $@ $(SERVER_PKG)
	@echo "  → $@ ($(VERSION))"

$(BINDIR)/$(CLIENT_BIN): $(shell find . -name '*.go' -not -path './.git/*' 2>/dev/null) go.mod go.sum
	@mkdir -p $(BINDIR)
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $@ $(CLIENT_PKG)
	@echo "  → $@ ($(VERSION))"

.PHONY: run
run: ## 本机启动服务器（内存存储，前台运行）
	$(GO) run $(SERVER_PKG)

# ---- 打包 ------------------------------------------------------------------

.PHONY: dist
dist: ## 构建全平台归档 + SHA256 校验和到 dist/
	@rm -rf $(DISTDIR)
	@mkdir -p $(DISTDIR)
	@for p in $(PLATFORMS); do \
		os=$${p%%/*}; rest=$${p#*/}; arch=$${rest%%/*}; variant=$${rest#*/}; \
		if [ "$$variant" = "$$arch" ]; then variant=""; fi; \
		goarm=""; [ "$$variant" = "v7" ] && goarm=7; \
		ext=""; [ "$$os" = "windows" ] && ext=".exe"; \
		name="p2psession-$(VERSION)-$$os-$$arch$${variant:+$$variant}"; \
		stage="$(DISTDIR)/$$name"; \
		mkdir -p "$$stage"; \
		printf '  %-28s ' "$$os/$$arch$${variant:+/$$variant}"; \
		if GOOS=$$os GOARCH=$$arch GOARM=$$goarm $(GO) build $(GOFLAGS) \
			-ldflags '$(LDFLAGS)' -o "$$stage/$(SERVER_BIN)$$ext" $(SERVER_PKG) 2>$(DISTDIR)/.err && \
		   GOOS=$$os GOARCH=$$arch GOARM=$$goarm $(GO) build $(GOFLAGS) \
			-ldflags '$(LDFLAGS)' -o "$$stage/$(CLIENT_BIN)$$ext" $(CLIENT_PKG) 2>>$(DISTDIR)/.err; then \
			cp README.md "$$stage/" 2>/dev/null || true; \
			cp VERSION "$$stage/" 2>/dev/null || true; \
			if [ "$$os" = "windows" ]; then \
				(cd $(DISTDIR) && zip -qr "$$name.zip" "$$name"); \
				rm -rf "$$stage"; \
			else \
				tar -czf "$(DISTDIR)/$$name.tar.gz" -C $(DISTDIR) "$$name"; \
				rm -rf "$$stage"; \
			fi; \
			echo "✓"; \
		else \
			echo "✗ (跳过)"; cat $(DISTDIR)/.err | head -3 | sed 's/^/      /'; \
			rm -rf "$$stage"; \
		fi; \
	done
	@rm -f $(DISTDIR)/.err
	@$(MAKE) --no-print-directory checksums
	@echo
	@echo "产物位于 $(DISTDIR)/："
	@ls -1 $(DISTDIR)/ | sed 's/^/  /'

.PHONY: checksums
checksums: ## 为 dist/ 中的归档生成 SHA256SUMS
	@cd $(DISTDIR) && sha256sum *.tar.gz *.zip 2>/dev/null > SHA256SUMS || true
	@test -s $(DISTDIR)/SHA256SUMS && { echo "  → $(DISTDIR)/SHA256SUMS"; cat $(DISTDIR)/SHA256SUMS | sed 's/^/      /'; } || echo "  （无归档，跳过校验和）"

.PHONY: clean
clean: ## 删除 bin/ 与 dist/
	@rm -rf $(BINDIR) $(DISTDIR)
	@echo "  ✓ 已清理 $(BINDIR)/ $(DISTDIR)/"

# ---- 质量检查 --------------------------------------------------------------

.PHONY: test
test: ## 运行全部测试（含端到端）
	$(GO) test ./...

.PHONY: test-race
test-race: ## 运行测试并开启竞态检测
	$(GO) test -race -count=1 ./...

.PHONY: vet
vet: ## go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## 用 gofmt 格式化（仅本仓库文件）
	@out=$$(gofmt -l . | grep -v '^$$'); \
	if [ -n "$$out" ]; then echo "以下文件已格式化："; echo "$$out" | sed 's/^/  /'; gofmt -w $$out; \
	else echo "  ✓ 无需格式化"; fi

.PHONY: fmt-check
fmt-check: ## 检查格式（CI 用，不修改文件）
	@out=$$(gofmt -l . | grep -v '^$$'); \
	if [ -n "$$out" ]; then echo "✗ 以下文件未格式化（运行 make fmt）："; echo "$$out" | sed 's/^/  /'; exit 1; \
	else echo "  ✓ 格式一致"; fi

.PHONY: check
check: fmt-check vet test ## 提交前检查：格式 + vet + 测试

.PHONY: tidy
tidy: ## go mod tidy
	$(GO) mod tidy

# ---- 安装（二进制 + systemd）-----------------------------------------------

.PHONY: install
install: build ## 安装到 $(PREFIX)/bin 并配置 systemd（需 root）
	@if [ "$$(id -u)" != "0" ]; then \
		echo "✗ make install 需要 root（会写入 $(PREFIX)/bin 与 /etc/systemd/system）"; \
		echo "  请用：sudo make install  或  sudo make install PREFIX=/opt/p2psession"; \
		exit 1; \
	fi
	@echo "== 1/5 创建系统用户与目录 =="
	@id -u p2psession >/dev/null 2>&1 || \
		useradd --system --no-create-home --shell /usr/sbin/nologin p2psession
	@install -d -o p2psession -g p2psession -m 0750 /var/lib/p2psession
	@install -d -m 0755 /etc/p2psession
	@echo "  ✓ 用户 p2psession、/var/lib/p2psession、/etc/p2psession"
	@echo "== 2/5 安装二进制 =="
	@install -m 0755 $(BINDIR)/$(SERVER_BIN) $(DESTDIR)$(PREFIX)/bin/$(SERVER_BIN)
	@install -m 0755 $(BINDIR)/$(CLIENT_BIN) $(DESTDIR)$(PREFIX)/bin/$(CLIENT_BIN)
	@echo "  ✓ $(DESTDIR)$(PREFIX)/bin/$(SERVER_BIN)"
	@echo "  ✓ $(DESTDIR)$(PREFIX)/bin/$(CLIENT_BIN)"
	@echo "== 3/5 安装 systemd unit =="
	@install -m 0644 deploy/systemd/p2psession.service $(DESTDIR)/etc/systemd/system/p2psession.service
	@echo "  ✓ /etc/systemd/system/p2psession.service"
	@echo "== 4/5 生成环境文件（若不存在）=="
	@if [ ! -f /etc/p2psession/p2psession.env ]; then \
		install -m 0640 -o root -g p2psession deploy/systemd/p2psession.env.example /etc/p2psession/p2psession.env; \
		KEY=$$(head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '='); \
		sed -i "s|^P2PS_TICKET_KEY=.*|P2PS_TICKET_KEY=$$KEY|" /etc/p2psession/p2psession.env; \
		echo "  ✓ 已生成并写入随机 P2PS_TICKET_KEY"; \
	else \
		echo "  · 已存在，保持不变（未覆盖现有配置）"; \
	fi
	@echo "== 5/5 重新加载 systemd =="
	@systemctl daemon-reload 2>/dev/null && echo "  ✓ systemctl daemon-reload" || echo "  · systemd 不可用，已跳过"
	@echo
	@echo "下一步："
	@echo "  sudo systemctl enable --now p2psession"
	@echo "  curl http://127.0.0.1:60000/v1/health"
	@echo "  查看日志：journalctl -u p2psession -f"

.PHONY: uninstall
uninstall: ## 卸载 systemd 服务（保留数据与配置）
	@if [ "$$(id -u)" != "0" ]; then echo "✗ 需要 root"; exit 1; fi
	@systemctl disable --now p2psession 2>/dev/null || true
	@rm -f /etc/systemd/system/p2psession.service
	@systemctl daemon-reload 2>/dev/null || true
	@rm -f $(DESTDIR)$(PREFIX)/bin/$(SERVER_BIN) $(DESTDIR)$(PREFIX)/bin/$(CLIENT_BIN)
	@echo "  ✓ 已卸载服务与二进制"
	@echo "  · 保留：/var/lib/p2psession（数据）、/etc/p2psession（配置）"
	@echo "    如需彻底删除：rm -rf /var/lib/p2psession /etc/p2psession && userdel p2psession"

.PHONY: upgrade
upgrade: build ## 仅替换二进制并重启服务（保留配置与数据）
	@if [ "$$(id -u)" != "0" ]; then echo "✗ 需要 root"; exit 1; fi
	@install -m 0755 $(BINDIR)/$(SERVER_BIN) $(DESTDIR)$(PREFIX)/bin/$(SERVER_BIN)
	@install -m 0755 $(BINDIR)/$(CLIENT_BIN) $(DESTDIR)$(PREFIX)/bin/$(CLIENT_BIN)
	@systemctl restart p2psession 2>/dev/null && echo "  ✓ 已升级并重启" || echo "  ✓ 二进制已替换（systemd 未运行）"
