#!/usr/bin/env bash
#
# p2psession 一键安装 / 升级 / 卸载脚本
#
# 二进制优先部署路径：不依赖 Docker，直接用 systemd 托管静态二进制。
#
# 用法：
#   sudo ./install.sh                       # 从本地 dist/ 归档安装（自动识别平台）
#   sudo ./install.sh --from <归档路径>      # 指定归档
#   sudo ./install.sh --url <归档URL>        # 下载并安装（默认校验 SHA256）
#   sudo ./install.sh --check               # 只做前置检查与校验，不安装
#   sudo ./install.sh --upgrade             # 升级：只换二进制并重启，保留配置与数据
#   sudo ./install.sh --uninstall           # 卸载服务与二进制（保留数据）
#   sudo ./install.sh --uninstall --purge   # 连带删除数据与配置
#
# 设计原则（按重要性排序）：
#   1. **校验和失败即中止**。绝不安装校验不通过或无法校验的产物
#      ——除非显式 --insecure-skip-verify（会大声警告）。
#   2. **失败不留半装状态**。二进制替换前先备份，装完验证失败则回滚。
#   3. **幂等**。可重复执行；已存在的配置文件绝不被静默覆盖。
#
set -euo pipefail

# ---- 常量 ------------------------------------------------------------------

readonly SCRIPT_NAME="${0##*/}"
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

# 脚本所在目录（用于定位随包提供的 unit 与环境文件样例）。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 仓库根（deploy/install.sh → 上一级）。
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

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
FROM_URL=""
CHECKSUMS_URL=""
VERSION=""
SKIP_VERIFY=0
PURGE=0
NO_START=0

usage() {
    sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 0
}

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --from)      FROM_ARCHIVE="${2:?--from 需要路径}"; shift 2 ;;
            --url)       FROM_URL="${2:?--url 需要 URL}"; shift 2 ;;
            --checksums) CHECKSUMS_URL="${2:?--checksums 需要 URL}"; shift 2 ;;
            --version)   VERSION="${2:?--version 需要版本号}"; shift 2 ;;
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
            "请用：sudo ${SCRIPT_NAME} $*"
    fi
}

# detect_platform 输出 <os> <arch>[ <variant>]，与 Makefile 的 PLATFORMS 命名一致。
detect_platform() {
    local os arch variant=""
    case "$(uname -s)" in
        Linux)  os="linux" ;;
        Darwin) os="darwin" ;;
        *)      die "不支持的系统: $(uname -s)" "仅支持 Linux 与 macOS。" ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64) arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
        armv7l|armv7)  arch="arm"; variant="v7" ;;
        *)             die "不支持的架构: $(uname -m)" ;;
    esac
    printf '%s %s %s\n' "$os" "$arch" "$variant"
}

detect_version() {
    if [ -n "$VERSION" ]; then echo "$VERSION"; return; fi
    if [ -f "${REPO_ROOT}/VERSION" ]; then
        head -n1 "${REPO_ROOT}/VERSION" | tr -d '[:space:]'
    else
        echo "unknown"
    fi
}

have_systemd() { command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; }

# ---- 取归档 -----------------------------------------------------------------

WORK_DIR=""
ARCHIVE_PATH=""
CHECKSUMS_PATH=""

cleanup() {
    # 只在脚本正常/异常退出时清理临时目录，不动用户目录。
    #
    # 显式 `return 0` 不可省略：EXIT trap 的返回值会**覆盖脚本的退出码**。
    # 若这里以 `[ -n "$WORK_DIR" ] && ...` 结尾，未设 WORK_DIR 的分支
    # （如卸载流程）会返回 1，使成功的卸载在调用方看来是失败
    # ——实测踩到过：所有卸载动作都成功，退出码却是 1。
    if [ -n "${WORK_DIR:-}" ] && [ -d "${WORK_DIR:-}" ]; then
        rm -rf "$WORK_DIR"
    fi
    return 0
}
trap cleanup EXIT

download() {
    local url="$1" dest="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -fSL --retry 3 --connect-timeout 15 -o "$dest" "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$dest" "$url"
    else
        die "需要 curl 或 wget 来下载" "或改用 --from <本地归档路径>。"
    fi
}

obtain_archive() {
    step "1/7 获取安装包"
    WORK_DIR="$(mktemp -d)"
    local os arch variant
    read -r os arch variant <<<"$(detect_platform)"
    local tag="${os}-${arch}${variant:+${variant}}"
    local ver; ver="$(detect_version)"
    info "平台: ${tag}   版本: ${ver}"

    if [ -n "$FROM_ARCHIVE" ]; then
        [ -f "$FROM_ARCHIVE" ] || die "归档不存在: ${FROM_ARCHIVE}"
        ARCHIVE_PATH="$(cd "$(dirname "$FROM_ARCHIVE")" && pwd)/$(basename "$FROM_ARCHIVE")"
        ok "使用本地归档: $(basename "$ARCHIVE_PATH")"
        # 尝试在同一目录找 SHA256SUMS。
        #
        # 必须用 if 而不是 `[ -f x ] && Y=z`：后者作为分支的最后一条命令时，
        # 条件为假会让整个分支返回非零，`set -e` 随即**静默退出脚本**，
        # 用户看不到任何错误信息（实测踩到过）。
        local d; d="$(dirname "$ARCHIVE_PATH")"
        if [ -f "${d}/SHA256SUMS" ]; then
            CHECKSUMS_PATH="${d}/SHA256SUMS"
        fi
    elif [ -n "$FROM_URL" ]; then
        ARCHIVE_PATH="${WORK_DIR}/$(basename "${FROM_URL%%\?*}")"
        info "下载: $FROM_URL"
        download "$FROM_URL" "$ARCHIVE_PATH" || die "下载失败: $FROM_URL"
        ok "已下载 $(basename "$ARCHIVE_PATH")"

        if [ -n "$CHECKSUMS_URL" ]; then
            CHECKSUMS_PATH="${WORK_DIR}/SHA256SUMS"
            download "$CHECKSUMS_URL" "$CHECKSUMS_PATH" || die "校验和下载失败: $CHECKSUMS_URL"
            ok "已下载 SHA256SUMS"
        else
            # 约定：校验和与归档同目录（Makefile 的 dist/ 就是这样）。
            local base="${FROM_URL%/*}"
            local cand=""
            case "$FROM_URL" in
                https://github.com/*/releases/download/*) cand="${base}/SHA256SUMS" ;;
                http*://*) cand="${base}/SHA256SUMS" ;;
            esac
            if [ -n "$cand" ]; then
                info "尝试获取校验和: $cand"
                if download "$cand" "${WORK_DIR}/SHA256SUMS" 2>/dev/null; then
                    CHECKSUMS_PATH="${WORK_DIR}/SHA256SUMS"
                    ok "已获取 SHA256SUMS"
                else
                    info "未找到 SHA256SUMS"
                fi
            fi
        fi
    else
        # 默认：在 dist/ 里按当前平台找归档。
        local found=""
        if [ -d "${REPO_ROOT}/dist" ]; then
            found="$(find "${REPO_ROOT}/dist" -maxdepth 1 -type f \
                \( -name "*${tag}.tar.gz" -o -name "*${tag}.zip" \) 2>/dev/null | sort | tail -n1)"
        fi
        if [ -z "$found" ]; then
            die "未找到适用于 ${tag} 的安装包" \
                "请先执行：make dist        （在仓库根目录）" \
                "或指定：sudo ${SCRIPT_NAME} --from <归档路径>" \
                "或下载：sudo ${SCRIPT_NAME} --url <归档URL>"
        fi
        ARCHIVE_PATH="$found"
        # 同上：用 if 而非 `&&`，避免 set -e 静默退出。
        if [ -f "${REPO_ROOT}/dist/SHA256SUMS" ]; then
            CHECKSUMS_PATH="${REPO_ROOT}/dist/SHA256SUMS"
        fi
        ok "使用本地构建产物: $(basename "$ARCHIVE_PATH")"
    fi
}

# ---- 校验 ------------------------------------------------------------------

verify_archive() {
    step "2/7 校验安装包"

    if [ "$SKIP_VERIFY" = "1" ]; then
        warn "已跳过校验（--insecure-skip-verify）——仅在你自己刚构建、且明知来源时使用"
        return
    fi

    if [ -z "$CHECKSUMS_PATH" ] || [ ! -f "$CHECKSUMS_PATH" ]; then
        die "没有可用的 SHA256SUMS，拒绝安装" \
            "无法验证下载内容是否完整、是否被篡改。" \
            "请用 --checksums <URL> 显式提供校验和文件，" \
            "或确认 SHA256SUMS 与归档在同一目录。" \
            "（确实要跳过：--insecure-skip-verify，风险自负）"
    fi

    local want have name
    name="$(basename "$ARCHIVE_PATH")"
    want="$(awk -v n="$name" '$2 == n {print $1}' "$CHECKSUMS_PATH" | head -n1)"

    if [ -z "$want" ]; then
        die "SHA256SUMS 中没有 ${name} 的记录" \
            "归档与校验和文件可能不是同一次构建的产物。"
    fi

    # macOS 用 shasum；Linux 用 sha256sum。
    if command -v sha256sum >/dev/null 2>&1; then
        have="$(sha256sum "$ARCHIVE_PATH" | awk '{print $1}')"
    elif command -v shasum >/dev/null 2>&1; then
        have="$(shasum -a 256 "$ARCHIVE_PATH" | awk '{print $1}')"
    else
        die "找不到 sha256sum 或 shasum，无法校验" "请安装 coreutils，或显式 --insecure-skip-verify。"
    fi

    if [ "$want" != "$have" ]; then
        die "校验和不匹配，已中止安装" \
            "期望: ${want}" \
            "实际: ${have}" \
            "归档可能损坏或被篡改，请重新下载。"
    fi
    ok "SHA256 校验通过 (${have:0:16}…)"
}

# ---- 解包 ------------------------------------------------------------------

SRC_DIR=""
extract_archive() {
    step "3/7 解包"
    local d="${WORK_DIR:-$(mktemp -d)}"
    mkdir -p "$d"

    case "$ARCHIVE_PATH" in
        *.zip)
            command -v unzip >/dev/null 2>&1 || die "需要 unzip 来解压 .zip"
            unzip -q "$ARCHIVE_PATH" -d "$d"
            ;;
        *.tar.gz|*.tgz)
            tar -xzf "$ARCHIVE_PATH" -C "$d"
            ;;
        *)  die "不认识的归档格式: $ARCHIVE_PATH" ;;
    esac

    # 归档内有一层目录（p2psession-<ver>-<os>-<arch>/），定位含 server 二进制的那层。
    #
    # 用 -exec dirname {} \; 而不是 `| xargs dirname`：后者对含空格/换行的
    # 路径会拆分错位（shellcheck SC2038），而解包目录来自用户提供的路径，
    # 不该假设它安全。
    SRC_DIR="$(find "$d" -maxdepth 2 -type f -name "${SERVER_BIN}" \
        -exec dirname {} \; 2>/dev/null | head -n1)"
    [ -n "$SRC_DIR" ] && [ -f "${SRC_DIR}/${SERVER_BIN}" ] \
        || die "归档内未找到 ${SERVER_BIN}" "归档结构不符合预期。"

    ok "已解包，含 ${SERVER_BIN} / ${CLIENT_BIN}"
}

# ---- 用户与目录 ------------------------------------------------------------

ensure_user_and_dirs() {
    step "4/7 系统用户与目录"

    if id -u "$SVC_USER" >/dev/null 2>&1; then
        info "用户 ${SVC_USER} 已存在（保持不变）"
    else
        local shell="/usr/sbin/nologin"
        [ -x "$shell" ] || shell="/bin/false"
        useradd --system --no-create-home --shell "$shell" "$SVC_USER" \
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
    step "5/7 安装二进制"

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

    # 立即验证二进制可执行且版本可读——比等到服务起不来才发现要好。
    local ver
    if ! ver="$("${BIN_DIR}/${SERVER_BIN}" --version 2>/dev/null)"; then
        rollback_binaries
        die "新二进制无法执行（--version 失败），已回滚" \
            "常见原因：架构不匹配（下载了错误平台的归档）。"
    fi
    ok "版本: ${ver}"
}

rollback_binaries() {
    [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ] || return 0
    local b
    for b in "$SERVER_BIN" "$CLIENT_BIN"; do
        if [ -f "${BACKUP_DIR}/${b}" ]; then
            install -m 0755 "${BACKUP_DIR}/${b}" "${BIN_DIR}/${b}" && warn "已回滚 ${b}"
        fi
    done
}

# ---- systemd 单元与配置 ----------------------------------------------------

UNIT_INSTALLED=0
install_unit() {
    step "6/7 systemd 单元与配置"

    if ! have_systemd; then
        warn "未检测到 systemd（或不在 systemd 下运行）"
        info "二进制已安装，可手动前台运行：${BIN_DIR}/${SERVER_BIN}"
        info "如需 systemd，请在有 systemd 的主机上重跑，或参考 deploy/systemd/ 自行配置。"
        return
    fi

    local unit_src="${SCRIPT_DIR}/systemd/${SERVICE_NAME}.service"
    [ -f "$unit_src" ] || unit_src="${REPO_ROOT}/deploy/systemd/${SERVICE_NAME}.service"
    [ -f "$unit_src" ] || die "找不到 unit 文件" "期望位置：deploy/systemd/${SERVICE_NAME}.service"

    install -m 0644 "$unit_src" "${UNIT_DIR}/${SERVICE_NAME}.service"
    UNIT_INSTALLED=1
    ok "${UNIT_DIR}/${SERVICE_NAME}.service"

    # 环境文件：只在首次安装时生成，**绝不覆盖**既有配置
    # （升级时覆盖会把线上 P2PS_TICKET_KEY 抹掉，导致所有在途票据失效）。
    if [ -f "$CONF_FILE" ]; then
        info "已存在 ${CONF_FILE}，保持不变（未覆盖）"
        if ! grep -qE '^P2PS_TICKET_KEY=.+' "$CONF_FILE"; then
            local key; key="$(gen_key)"
            sed -i "s|^P2PS_TICKET_KEY=.*|P2PS_TICKET_KEY=${key}|" "$CONF_FILE" \
                || printf '\nP2PS_TICKET_KEY=%s\n' "$key" >>"$CONF_FILE"
            warn "原配置缺少 P2PS_TICKET_KEY，已生成并写入"
        fi
    else
        local env_src="${SCRIPT_DIR}/systemd/${SERVICE_NAME}.env.example"
        [ -f "$env_src" ] || env_src="${REPO_ROOT}/deploy/systemd/${SERVICE_NAME}.env.example"
        if [ -f "$env_src" ]; then
            install -m 0640 -o root -g "$SVC_USER" "$env_src" "$CONF_FILE"
        else
            printf 'P2PS_TICKET_KEY=%s\n' "$(gen_key)" >"$CONF_FILE"
            chown root:"$SVC_USER" "$CONF_FILE"; chmod 0640 "$CONF_FILE"
        fi
        local key; key="$(gen_key)"
        sed -i "s|^P2PS_TICKET_KEY=.*|P2PS_TICKET_KEY=${key}|" "$CONF_FILE"
        ok "已生成 ${CONF_FILE}（含随机 P2PS_TICKET_KEY，权限 0640）"
    fi

    chown root:"$SVC_USER" "$CONF_FILE" 2>/dev/null || true
    chmod 0640 "$CONF_FILE" 2>/dev/null || true

    systemctl daemon-reload
    ok "systemctl daemon-reload"
}

gen_key() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -base64 32 | tr '+/' '-_' | tr -d '='
    else
        head -c 32 /dev/urandom | base64 | tr '+/' '-_' | tr -d '='
    fi
}

# ---- 启动与验证 ------------------------------------------------------------

start_and_verify() {
    step "7/7 启动与验证"

    if [ "$UNIT_INSTALLED" = "0" ]; then
        info "无 systemd，跳过启动（二进制已就绪）"
        return 0
    fi

    if [ "$NO_START" = "1" ]; then
        info "已指定 --no-start，跳过启动"
        info "手动启动：systemctl enable --now ${SERVICE_NAME}"
        return 0
    fi

    systemctl enable "$SERVICE_NAME" >/dev/null 2>&1 || true
    if ! systemctl restart "$SERVICE_NAME"; then
        warn "服务启动命令失败，最近日志："
        journalctl -u "$SERVICE_NAME" -n 20 --no-pager 2>/dev/null | sed 's/^/      /' >&2 || true
        rollback_binaries
        die "服务启动失败（已回滚二进制）" \
            "用 journalctl -u ${SERVICE_NAME} -n 50 查看详细原因。"
    fi

    # 等待就绪：健康检查通过才算安装成功。
    # 最多等 15 秒（30 × 0.5s）——本地启动通常 1 秒内就绪，
    # 留足余量应对首次创建数据库文件的情况。
    local out=""
    for _ in $(seq 1 30); do
        out="$(curl -s --max-time 2 "$HEALTH_URL" 2>/dev/null || true)"
        [ "$out" = "ok" ] && break
        sleep 0.5
    done

    if [ "$out" = "ok" ]; then
        ok "服务已启动，健康检查通过"
    else
        warn "健康检查未通过（响应: ${out:-<无>}）"
        journalctl -u "$SERVICE_NAME" -n 15 --no-pager 2>/dev/null | sed 's/^/      /' >&2 || true
        die "安装未完成：服务未就绪（二进制已保留，日志见上）" \
            "常见原因：60000 端口被占用、${CONF_FILE} 配置有误。"
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
        if [ -f "${BIN_DIR}/${b}" ]; then
            rm -f "${BIN_DIR}/${b}" && ok "已删除 ${BIN_DIR}/${b}"
        fi
    done

    if [ "$PURGE" = "1" ]; then
        warn "purge：正在删除数据与配置（不可恢复）"
        rm -rf "$DATA_DIR" "$CONF_DIR"
        userdel "$SVC_USER" 2>/dev/null && ok "已删除用户 ${SVC_USER}" || true
        ok "已删除 ${DATA_DIR} 与 ${CONF_DIR}"
    else
        info "已保留：${DATA_DIR}（数据）、${CONF_DIR}（配置）"
        info "彻底删除：sudo ${SCRIPT_NAME} --uninstall --purge"
    fi
    printf '\n  卸载完成。\n\n'
}

# ---- main ------------------------------------------------------------------

main() {
    parse_args "$@"

    printf '\n%sp2psession 安装程序%s  %s\n' "$C_BOLD" "$C_RESET" "$C_DIM(${MODE})${C_RESET}"
    printf '%s\n' "────────────────────────────────────────────────"

    if [ "$MODE" = "uninstall" ]; then
        require_root --uninstall
        do_uninstall
        return 0
    fi

    # --check 明确不改动系统，因此不要求 root：
    # 它只负责获取归档、校验 SHA256、解包定位，全部在临时目录内完成。
    # 这样运维可以先用普通用户确认「包能拿到、校验通过」再提权安装。
    if [ "$MODE" != "check" ]; then
        require_root "$@"
    fi

    if [ "$MODE" = "upgrade" ]; then
        step "升级模式：将保留 ${CONF_FILE} 与 ${DATA_DIR}"
    fi

    obtain_archive
    verify_archive
    extract_archive

    if [ "$MODE" = "check" ]; then
        step "检查完成"
        ok "安装包可获取、校验通过、可解包"
        info "未安装任何文件（--check 不会改动系统）"
        printf '\n'
        return 0
    fi

    ensure_user_and_dirs
    install_binaries
    install_unit
    start_and_verify

    printf '\n%s安装完成%s\n' "$C_BOLD$C_GREEN" "$C_RESET"
    printf '%s\n' "────────────────────────────────────────────────"
    printf '  健康检查   %s\n' "curl ${HEALTH_URL}"
    printf '  查看日志   %s\n' "journalctl -u ${SERVICE_NAME} -f"
    printf '  查看状态   %s\n' "systemctl status ${SERVICE_NAME}"
    printf '  配置文件   %s\n' "${CONF_FILE}"
    printf '  数据目录   %s\n' "${DATA_DIR}"
    printf '  客户端     %s\n' "${CLIENT_BIN} --help"
    printf '  卸载       %s\n' "sudo ${SCRIPT_NAME} --uninstall"
    printf '\n'
}

main "$@"
