#!/usr/bin/env bash
#
# p2psession 一键安装 / 升级 / 卸载脚本（网络引导型）
#
# 这是唯一的安装入口，既可以从仓库本地运行，也可以直接管道执行：
#
#   安装（最新版）：
#     curl -fsSL https://raw.githubusercontent.com/AngelSnow1129/p2p-server/main/deploy/install.sh | sudo bash
#   指定版本：
#     curl -fsSL .../install.sh | sudo bash -s -- --version v1.0.0
#   升级（保留配置与数据）：
#     curl -fsSL .../install.sh | sudo bash -s -- --upgrade
#   卸载（保留数据）：
#     curl -fsSL .../install.sh | sudo bash -s -- --uninstall
#   彻底卸载（连数据/配置/用户一起删）：
#     curl -fsSL .../install.sh | sudo bash -s -- --uninstall --purge
#
# 本地/离线用法（在仓库内）：
#   sudo ./deploy/install.sh                       # 优先用本地 dist/ 归档
#   sudo ./deploy/install.sh --from /path/x.tar.gz
#   ./deploy/install.sh --check                    # 只校验，不改动系统（免 root）
#
# 设计原则（按重要性排序）：
#   1. 校验和失败即中止；拿不到 SHA256SUMS 也拒绝安装（除非显式跳过并警告）。
#   2. 失败不留半装状态：二进制替换前备份，启动/自检失败自动回滚。
#   3. 幂等：可重复执行；既有配置（尤其 P2PS_TICKET_KEY）绝不被覆盖。
#
set -euo pipefail

# ---- 常量 ------------------------------------------------------------------

readonly SERVICE_NAME="p2psession"
readonly SERVER_BIN="p2psession-server"
readonly CLIENT_BIN="p2p-node"
readonly BIN_DIR="/usr/local/bin"
readonly UNIT_DIR="/etc/systemd/system"
readonly CONF_DIR="/etc/p2psession"
readonly CONF_FILE="${CONF_DIR}/p2psession.env"
readonly DATA_DIR="/var/lib/p2psession"
readonly SVC_USER="p2psession"
readonly HEALTH_URL="http://127.0.0.1:60000/v1/health"

# GitHub 发布信息。归档与校验和都从 Releases 下载。
readonly GH_OWNER="AngelSnow1129"
readonly GH_REPO="p2p-server"
# latest 重定向到最新 release 的 tag 页（仅取 HTTP 头，不下载页面体）。
readonly GH_LATEST_API="https://api.github.com/repos/${GH_OWNER}/${GH_REPO}/releases/latest"
readonly GH_DOWNLOAD_BASE="https://github.com/${GH_OWNER}/${GH_REPO}/releases/download"

# 脚本自身可能来自管道（/dev/stdin），此时没有「脚本目录」可言；
# 因此 unit / env 样例一律以「归档内自带」为权威来源，仓库路径只作本地回退。
#
# 这里不用任何 `A && B || C`（包括命令替换内部）：管道执行时 cd 会失败，
# 该模式在「A 真但 B 失败」时会错误地走到 C（SC2015）。用函数 + if 显式处理。
#
# 关键：必须用「能否 cd 进去」来判定目录有效，而不是 `[ -d ]`。
_resolve_dir() {
    local target="$1" out
    if out="$(cd "$target" 2>/dev/null && pwd)"; then
        printf '%s\n' "$out"
    fi
}

# 仅当脚本确实从一个**真实文件**运行时才解析目录。
#
# 管道执行（curl | bash）时 $0 是字面量 "bash"，dirname 是 "."（当前工作目录），
# 若不加判断会把 cwd 误当成脚本目录。因此必须先确认 ${BASH_SOURCE[0]}
# 指向一个真实存在的常规文件（不是 "bash"、不是 /dev/fd/*）。
SCRIPT_DIR=""
REPO_ROOT=""
_b0="${BASH_SOURCE[0]:-}"
if [ -f "$_b0" ] && [ "$_b0" != "bash" ]; then
    SCRIPT_DIR="$(_resolve_dir "$(dirname "$_b0")")"
    # 仅当脚本从真实文件运行时才推导仓库根；管道场景下两者都为空，
    # 后续 locate_asset 只用归档内文件，不依赖 REPO_ROOT。
    if [ -n "$SCRIPT_DIR" ]; then
        REPO_ROOT="$(_resolve_dir "${SCRIPT_DIR}/..")"
    fi
fi

# ---- 输出 ------------------------------------------------------------------

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    C_RESET=$'\033[0m'; C_BOLD=$'\033[1m'; C_DIM=$'\033[2m'
    C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_RED=$'\033[31m'
else
    C_RESET=''; C_BOLD=''; C_DIM=''; C_GREEN=''; C_YELLOW=''; C_RED=''
fi

step() { printf '\n%s== %s ==%s\n' "$C_BOLD" "$1" "$C_RESET"; }
ok()   { printf '  %s✓%s %s\n' "$C_GREEN" "$C_RESET" "$1"; }
info() { printf '  %s·%s %s\n' "$C_DIM" "$C_RESET" "$1"; }
warn() { printf '  %s!%s %s\n' "$C_YELLOW" "$C_RESET" "$1" >&2; }
die()  { printf '\n  %s✗ %s%s\n' "$C_RED" "$1" "$C_RESET" >&2; shift || true; for l in "$@"; do printf '    %s\n' "$l" >&2; done; exit 1; }

# ---- 参数 ------------------------------------------------------------------

MODE="install"          # install | upgrade | uninstall | check
FROM_ARCHIVE=""
PIN_VERSION=""
SKIP_VERIFY=0
PURGE=0
NO_START=0

usage() { sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0; }

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --from)      FROM_ARCHIVE="${2:?--from 需要路径}"; shift 2 ;;
            --version)   PIN_VERSION="${2:?--version 需要版本号，如 v1.0.0}"; shift 2 ;;
            --upgrade)   MODE="upgrade"; shift ;;
            --uninstall) MODE="uninstall"; shift ;;
            --purge)     PURGE=1; shift ;;
            --check)     MODE="check"; shift ;;
            --no-start)  NO_START=1; shift ;;
            --insecure-skip-verify) SKIP_VERIFY=1; shift ;;
            -h|--help)   usage ;;
            *)           die "未知参数: $1" "用 --help 查看用法。" ;;
        esac
    done
}

# ---- 前置检查 --------------------------------------------------------------

require_root() {
    if [ "$(id -u)" != "0" ]; then
        die "需要 root 权限" \
            "本脚本会写入 ${BIN_DIR}、${UNIT_DIR}、${CONF_DIR} 并配置 systemd。" \
            "请在命令前加 sudo（管道方式：curl ... | sudo bash -s -- <参数>）。"
    fi
}

# detect_platform 输出 "<os> <arch> [variant]"，与 Makefile 的归档命名一致。
detect_platform() {
    local os arch variant=""
    case "$(uname -s)" in
        Linux)  os="linux" ;;
        Darwin) os="darwin" ;;
        *)      die "不支持的系统: $(uname -s)" "仅提供 Linux 与 macOS 预编译包。" ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64) arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
        armv7l|armv7)  arch="arm"; variant="v7" ;;
        *)             die "不支持的架构: $(uname -m)" ;;
    esac
    printf '%s %s %s\n' "$os" "$arch" "$variant"
}

have_systemd() { command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; }

# ---- 临时目录与下载 ---------------------------------------------------------

WORK_DIR=""
cleanup() {
    # 显式 return 0：EXIT trap 的返回值会覆盖脚本退出码。
    # 若以 `[ -n "$X" ] && ...` 结尾，卸载分支（不设 WORK_DIR）会返回 1，
    # 使成功的卸载在调用方看起来失败（实测踩到过）。
    if [ -n "${WORK_DIR:-}" ] && [ -d "${WORK_DIR:-}" ]; then
        rm -rf "$WORK_DIR"
    fi
    return 0
}
trap cleanup EXIT

http_get() { # $1=url $2=输出文件（缺省输出到 stdout）
    local url="$1" out="${2:-}"
    if command -v curl >/dev/null 2>&1; then
        if [ -n "$out" ]; then
            curl -fsSL --retry 3 --connect-timeout 15 -o "$out" "$url"
        else
            curl -fsSL --retry 3 --connect-timeout 15 "$url"
        fi
    elif command -v wget >/dev/null 2>&1; then
        if [ -n "$out" ]; then wget -q -O "$out" "$url"; else wget -qO- "$url"; fi
    else
        die "需要 curl 或 wget 来下载"
    fi
}

# resolve_latest_tag 取最新 release 的 tag_name（如 v1.2.3）。
resolve_latest_tag() {
    local json tag
    json="$(http_get "$GH_LATEST_API" 2>/dev/null)" \
        || die "无法获取最新版本信息" "检查网络，或用 --version vX.Y.Z 显式指定。"
    # 不依赖 jq：用 grep/sed 提取 "tag_name": "..."，足够稳健。
    tag="$(printf '%s' "$json" | grep -oE '"tag_name"[[:space:]]*:[[:space:]]*"[^"]+"' | head -n1 | sed -E 's/.*"([^"]+)"$/\1/')"
    [ -n "$tag" ] || die "最新版本解析失败" "GitHub API 返回中未找到 tag_name。"
    printf '%s\n' "$tag"
}

# ---- 取归档 -----------------------------------------------------------------

ARCHIVE_PATH=""
CHECKSUMS_PATH=""

obtain_archive() {
    step "获取安装包"
    WORK_DIR="$(mktemp -d)"
    local os arch variant tag asset base
    read -r os arch variant <<<"$(detect_platform)"
    tag="${os}-${arch}${variant:+${variant}}"

    # 1) 显式本地归档（离线/调试）。
    if [ -n "$FROM_ARCHIVE" ]; then
        [ -f "$FROM_ARCHIVE" ] || die "归档不存在: $FROM_ARCHIVE"
        ARCHIVE_PATH="$(cd "$(dirname "$FROM_ARCHIVE")" && pwd)/$(basename "$FROM_ARCHIVE")"
        local d; d="$(dirname "$ARCHIVE_PATH")"
        if [ -f "${d}/SHA256SUMS" ]; then CHECKSUMS_PATH="${d}/SHA256SUMS"; fi
        ok "使用本地归档: $(basename "$ARCHIVE_PATH")"
        return
    fi

    # 2) 仓库本地 dist/（开发者在本机执行时优先，省一次下载）。
    if [ -n "$REPO_ROOT" ] && [ -d "${REPO_ROOT}/dist" ]; then
        local found
        found="$(find "${REPO_ROOT}/dist" -maxdepth 1 -type f \
            \( -name "*${tag}.tar.gz" -o -name "*${tag}.zip" \) 2>/dev/null | sort | tail -n1)"
        if [ -n "$found" ]; then
            ARCHIVE_PATH="$found"
            if [ -f "${REPO_ROOT}/dist/SHA256SUMS" ]; then CHECKSUMS_PATH="${REPO_ROOT}/dist/SHA256SUMS"; fi
            ok "使用本地构建产物: $(basename "$ARCHIVE_PATH")"
            return
        fi
    fi

    # 3) 网络：从 GitHub Releases 下载（curl | bash 的主路径）。
    local ver="$PIN_VERSION"
    if [ -z "$ver" ]; then
        ver="$(resolve_latest_tag)"
    fi
    asset="p2psession-${ver}-${tag}.tar.gz"
    base="${GH_DOWNLOAD_BASE}/${ver}"
    info "版本: ${ver}   平台: ${tag}"

    ARCHIVE_PATH="${WORK_DIR}/${asset}"
    info "下载归档: ${asset}"
    http_get "${base}/${asset}" "$ARCHIVE_PATH" || die "下载归档失败: ${base}/${asset}"
    ok "已下载 ${asset}"

    info "下载 SHA256SUMS"
    if http_get "${base}/SHA256SUMS" "${WORK_DIR}/SHA256SUMS"; then
        CHECKSUMS_PATH="${WORK_DIR}/SHA256SUMS"
        ok "已获取 SHA256SUMS"
    else
        # 不立即 die：留给 verify_archive 统一按 SKIP_VERIFY 决策，
        # 但默认（不跳过校验）会在那里中止。
        warn "未获取到 SHA256SUMS（该 release 可能未附带校验和）"
    fi
}

# ---- 校验 ------------------------------------------------------------------

verify_archive() {
    step "校验安装包"
    if [ "$SKIP_VERIFY" = "1" ]; then
        warn "已跳过校验（--insecure-skip-verify）——仅在你明确信任来源时使用"
        return
    fi
    if [ -z "$CHECKSUMS_PATH" ] || [ ! -f "$CHECKSUMS_PATH" ]; then
        die "没有可用的 SHA256SUMS，拒绝安装" \
            "无法验证下载内容是否完整、是否被篡改。" \
            "确认该 release 附带了 SHA256SUMS，或显式 --insecure-skip-verify（风险自负）。"
    fi

    local want have name
    name="$(basename "$ARCHIVE_PATH")"
    want="$(awk -v n="$name" '$2 == n {print $1}' "$CHECKSUMS_PATH" | head -n1)"
    [ -n "$want" ] || die "SHA256SUMS 中没有 ${name} 的记录" "归档与校验和可能不是同一次构建。"

    if command -v sha256sum >/dev/null 2>&1; then
        have="$(sha256sum "$ARCHIVE_PATH" | awk '{print $1}')"
    elif command -v shasum >/dev/null 2>&1; then
        have="$(shasum -a 256 "$ARCHIVE_PATH" | awk '{print $1}')"
    else
        die "找不到 sha256sum / shasum，无法校验"
    fi

    [ "$want" = "$have" ] || die "校验和不匹配，已中止安装" \
        "期望: ${want}" "实际: ${have}" "归档可能损坏或被篡改，请重新下载。"
    ok "SHA256 校验通过 (${have:0:16}…)"
}

# ---- 解包 ------------------------------------------------------------------

SRC_DIR=""
extract_archive() {
    step "解包"
    local d="${WORK_DIR}/x"
    mkdir -p "$d"
    case "$ARCHIVE_PATH" in
        *.zip)
            command -v unzip >/dev/null 2>&1 || die "需要 unzip 来解压 .zip"
            unzip -q "$ARCHIVE_PATH" -d "$d" ;;
        *.tar.gz|*.tgz) tar -xzf "$ARCHIVE_PATH" -C "$d" ;;
        *) die "不认识的归档格式: $ARCHIVE_PATH" ;;
    esac

    # 归档内有一层目录，定位含 server 二进制的那层。
    SRC_DIR="$(find "$d" -maxdepth 2 -type f -name "${SERVER_BIN}" \
        -exec dirname {} \; 2>/dev/null | head -n1)"
    # 用 if 而非 `[ ... ] && [ ... ] || die`：后者在「第一个条件为真、第二个为假」
    # 时仍会触发 die（SC2015：A && B || C 不是 if-then-else）。
    if [ -z "$SRC_DIR" ] || [ ! -f "${SRC_DIR}/${SERVER_BIN}" ]; then
        die "归档内未找到 ${SERVER_BIN}" "归档结构不符合预期。"
    fi
    ok "已解包，含 ${SERVER_BIN} / ${CLIENT_BIN}"
}

# ---- 用户与目录 ------------------------------------------------------------

ensure_user_and_dirs() {
    step "系统用户与目录"
    if id -u "$SVC_USER" >/dev/null 2>&1; then
        info "用户 ${SVC_USER} 已存在（保持不变）"
    else
        local sh="/usr/sbin/nologin"; [ -x "$sh" ] || sh="/bin/false"
        useradd --system --no-create-home --shell "$sh" "$SVC_USER" \
            || die "创建用户 ${SVC_USER} 失败"
        ok "已创建系统用户 ${SVC_USER}"
    fi
    install -d -o "$SVC_USER" -g "$SVC_USER" -m 0750 "$DATA_DIR"
    install -d -m 0755 "$CONF_DIR"
    ok "数据目录 ${DATA_DIR}（属主 ${SVC_USER}，0750）"
    ok "配置目录 ${CONF_DIR}"
}

# ---- 安装二进制（带回滚）----------------------------------------------------

BACKUP_DIR=""
install_binaries() {
    step "安装二进制"
    BACKUP_DIR="$(mktemp -d)"
    local b
    for b in "$SERVER_BIN" "$CLIENT_BIN"; do
        if [ -f "${BIN_DIR}/${b}" ] && [ "$MODE" = "install" ]; then
            cp -p "${BIN_DIR}/${b}" "${BACKUP_DIR}/${b}" && info "已备份原 ${b}"
        fi
    done

    install -m 0755 "${SRC_DIR}/${SERVER_BIN}" "${BIN_DIR}/${SERVER_BIN}" \
        || die "安装 ${SERVER_BIN} 失败"
    ok "${BIN_DIR}/${SERVER_BIN}"
    if [ -f "${SRC_DIR}/${CLIENT_BIN}" ]; then
        install -m 0755 "${SRC_DIR}/${CLIENT_BIN}" "${BIN_DIR}/${CLIENT_BIN}"
        ok "${BIN_DIR}/${CLIENT_BIN}"
    else
        info "归档中无 ${CLIENT_BIN}，跳过"
    fi

    # 立即验证可执行——架构下错时在这里就失败，而不是等服务起不来。
    local ver
    if ! ver="$("${BIN_DIR}/${SERVER_BIN}" --version 2>/dev/null)"; then
        rollback_binaries
        die "新二进制无法执行（--version 失败），已回滚" "常见原因：下载了错误平台的归档。"
    fi
    ok "版本: ${ver}"
}

rollback_binaries() {
    [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ] || return 0
    local b
    for b in "$SERVER_BIN" "$CLIENT_BIN"; do
        [ -f "${BACKUP_DIR}/${b}" ] && install -m 0755 "${BACKUP_DIR}/${b}" "${BIN_DIR}/${b}" && warn "已回滚 ${b}"
    done
}

# ---- systemd 单元与配置 ----------------------------------------------------

UNIT_INSTALLED=0

# locate_asset 在归档目录里找文件；找不到再回退仓库路径（本地开发用）。
locate_asset() { # $1=相对路径
    local cand="${SRC_DIR}/$1"
    [ -f "$cand" ] && { printf '%s\n' "$cand"; return 0; }
    cand="${SRC_DIR}/$(basename "$1")"
    [ -f "$cand" ] && { printf '%s\n' "$cand"; return 0; }
    [ -n "$REPO_ROOT" ] && [ -f "${REPO_ROOT}/$1" ] && { printf '%s\n' "${REPO_ROOT}/$1"; return 0; }
    return 1
}

gen_key() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -base64 32 | tr '+/' '-_' | tr -d '='
    else
        head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '='
    fi
}

install_unit() {
    step "systemd 单元与配置"
    if ! have_systemd; then
        warn "未检测到 systemd（或不在 systemd 下运行）"
        info "二进制已安装，可手动前台运行：${BIN_DIR}/${SERVER_BIN}"
        info "如需 systemd，请在有 systemd 的主机上重跑，或参考仓库 deploy/systemd/。"
        return
    fi

    local unit_src
    unit_src="$(locate_asset "deploy/systemd/${SERVICE_NAME}.service" 2>/dev/null || true)"
    [ -n "$unit_src" ] || die "归档内缺少 ${SERVICE_NAME}.service" "请确认下载的是完整 release 归档。"
    install -m 0644 "$unit_src" "${UNIT_DIR}/${SERVICE_NAME}.service"
    UNIT_INSTALLED=1
    ok "${UNIT_DIR}/${SERVICE_NAME}.service"

    # 环境文件只在首次安装生成，绝不覆盖既有配置
    # （升级覆盖会抹掉线上 P2PS_TICKET_KEY，使所有在途票据失效）。
    if [ -f "$CONF_FILE" ]; then
        info "已存在 ${CONF_FILE}，保持不变（未覆盖）"
        if ! grep -qE '^P2PS_TICKET_KEY=.+' "$CONF_FILE"; then
            local k; k="$(gen_key)"
            sed -i "s|^P2PS_TICKET_KEY=.*|P2PS_TICKET_KEY=${k}|" "$CONF_FILE" \
                || printf '\nP2PS_TICKET_KEY=%s\n' "$k" >>"$CONF_FILE"
            warn "原配置缺少 P2PS_TICKET_KEY，已生成并写入"
        fi
    else
        local env_src
        env_src="$(locate_asset "deploy/systemd/${SERVICE_NAME}.env.example" 2>/dev/null || true)"
        if [ -n "$env_src" ]; then
            install -m 0640 -o root -g "$SVC_USER" "$env_src" "$CONF_FILE"
        else
            printf 'P2PS_TICKET_KEY=%s\n' "$(gen_key)" >"$CONF_FILE"
            chown root:"$SVC_USER" "$CONF_FILE"; chmod 0640 "$CONF_FILE"
        fi
        local k; k="$(gen_key)"
        sed -i "s|^P2PS_TICKET_KEY=.*|P2PS_TICKET_KEY=${k}|" "$CONF_FILE"
        ok "已生成 ${CONF_FILE}（含随机 P2PS_TICKET_KEY，0640）"
    fi
    chown root:"$SVC_USER" "$CONF_FILE" 2>/dev/null || true
    chmod 0640 "$CONF_FILE" 2>/dev/null || true

    systemctl daemon-reload
    ok "systemctl daemon-reload"
}

# ---- 启动与验证 ------------------------------------------------------------

start_and_verify() {
    step "启动与验证"
    if [ "$UNIT_INSTALLED" = "0" ]; then
        info "无 systemd，跳过启动（二进制已就绪）"
        return 0
    fi
    if [ "$NO_START" = "1" ]; then
        info "已指定 --no-start，跳过启动；手动：systemctl enable --now ${SERVICE_NAME}"
        return 0
    fi

    systemctl enable "$SERVICE_NAME" >/dev/null 2>&1 || true
    if ! systemctl restart "$SERVICE_NAME"; then
        journalctl -u "$SERVICE_NAME" -n 20 --no-pager 2>/dev/null | sed 's/^/      /' >&2 || true
        rollback_binaries
        die "服务启动失败（已回滚二进制）" "journalctl -u ${SERVICE_NAME} -n 50 查看详情。"
    fi

    local out=""
    for _ in $(seq 1 30); do
        out="$(curl -s --max-time 2 "$HEALTH_URL" 2>/dev/null || true)"
        [ "$out" = "ok" ] && break
        sleep 0.5
    done
    if [ "$out" = "ok" ]; then
        ok "服务已启动，健康检查通过 (${HEALTH_URL})"
    else
        journalctl -u "$SERVICE_NAME" -n 15 --no-pager 2>/dev/null | sed 's/^/      /' >&2 || true
        die "安装未完成：服务未就绪（二进制已保留）" "常见原因：60000 端口被占用、${CONF_FILE} 配置有误。"
    fi
}

# ---- 卸载 ------------------------------------------------------------------

do_uninstall() {
    step "卸载 p2psession"
    if have_systemd; then
        systemctl disable --now "$SERVICE_NAME" >/dev/null 2>&1 || true
        rm -f "${UNIT_DIR}/${SERVICE_NAME}.service"
        systemctl daemon-reload >/dev/null 2>&1 || true
        ok "已停止并移除 systemd 服务"
    else
        info "无 systemd，跳过服务移除"
    fi
    local b
    for b in "$SERVER_BIN" "$CLIENT_BIN"; do
        [ -f "${BIN_DIR}/${b}" ] && rm -f "${BIN_DIR}/${b}" && ok "已删除 ${BIN_DIR}/${b}"
    done
    if [ "$PURGE" = "1" ]; then
        warn "purge：正在删除数据与配置（不可恢复）"
        rm -rf "$DATA_DIR" "$CONF_DIR"
        # userdel 在「用户不存在」时返回非零；这是预期情况，不该让该行报错。
        # 用 if 显式区分，避免 `cmd && ok || true` 的 SC2015 歧义。
        if userdel "$SVC_USER" 2>/dev/null; then
            ok "已删除用户 ${SVC_USER}"
        fi
        ok "已删除 ${DATA_DIR} 与 ${CONF_DIR}"
    else
        info "已保留：${DATA_DIR}（数据）、${CONF_DIR}（配置）"
        info "彻底删除：加 --purge"
    fi
    printf '\n  卸载完成。\n\n'
}

# ---- main ------------------------------------------------------------------

main() {
    parse_args "$@"
    printf '\n%sp2psession 安装程序%s  %s\n' "$C_BOLD" "$C_RESET" "$C_DIM(${MODE})${C_RESET}"
    printf '%s\n' "────────────────────────────────────────────────"

    if [ "$MODE" = "uninstall" ]; then
        require_root
        do_uninstall
        return 0
    fi
    # --check 不改动系统，免 root。
    if [ "$MODE" != "check" ]; then require_root; fi

    if [ "$MODE" = "upgrade" ]; then
        step "升级模式：保留 ${CONF_FILE} 与 ${DATA_DIR}"
    fi

    obtain_archive
    verify_archive
    extract_archive

    if [ "$MODE" = "check" ]; then
        step "检查完成"
        ok "安装包可获取、校验通过、可解包"
        info "未安装任何文件（--check 不改动系统）"
        printf '\n'
        return 0
    fi

    ensure_user_and_dirs
    install_binaries
    install_unit
    start_and_verify

    printf '\n%s安装完成%s\n' "$C_BOLD$C_GREEN" "$C_RESET"
    printf '%s\n' "────────────────────────────────────────────────"
    printf '  健康检查   curl %s\n' "$HEALTH_URL"
    printf '  日志       journalctl -u %s -f\n' "$SERVICE_NAME"
    printf '  配置文件   %s\n' "$CONF_FILE"
    printf '  数据目录   %s\n' "$DATA_DIR"
    printf '  升级       重新运行本脚本（或加 --version 固定版本）\n'
    printf '  卸载       本脚本加 --uninstall（彻底删除再加 --purge）\n'
    printf '\n'
}

main "$@"
