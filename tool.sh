#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Codex CLI / bubblewrap 安全修复脚本
#
# 目标：
# 1. 保持 AppArmor 对 unprivileged user namespace 的全局限制开启
# 2. 只给 bubblewrap，也就是 bwrap，配置受控例外
# 3. 让 Codex CLI 可以继续使用 bwrap 创建本地沙箱
# 4. 避免简单关闭安全限制带来的额外攻击面
# ============================================================

echo "[1/6] 安装必要软件包..."

# apparmor-profiles：提供官方 bwrap-userns-restrict profile
# apparmor-utils：提供 aa-status 等 AppArmor 管理工具
# bubblewrap：提供 bwrap 沙箱程序
# uidmap：提供 newuidmap/newgidmap，供 user namespace 映射使用
sudo apt update
sudo apt install -y apparmor-profiles apparmor-utils bubblewrap uidmap

echo "[2/6] 检查 bwrap 的 AppArmor profile 是否存在..."

# 官方 profile 的源路径
PROFILE_SRC="/usr/share/apparmor/extra-profiles/bwrap-userns-restrict"

# 加载到系统 AppArmor 配置目录后的目标路径
PROFILE_DST="/etc/apparmor.d/bwrap-userns-restrict"

# 如果系统包里没有这个 profile，直接退出，避免写入错误配置
if [ ! -f "$PROFILE_SRC" ]; then
  echo "ERROR: 找不到 profile: $PROFILE_SRC"
  echo "请检查 apparmor-profiles 包是否包含 bwrap-userns-restrict。"
  exit 1
fi

echo "[3/6] 安装并加载 bwrap 的 AppArmor profile..."

# 将官方 profile 安装到 /etc/apparmor.d
sudo install -m 0644 "$PROFILE_SRC" "$PROFILE_DST"

# 重新加载该 profile，让它立即生效
sudo apparmor_parser -r "$PROFILE_DST"

echo "[4/6] 开启并持久化 AppArmor userns 安全限制..."

# 写入持久化 sysctl 配置
# apparmor_restrict_unprivileged_userns=1：
#   开启 AppArmor 对未特权 user namespace 的限制
#
# apparmor_restrict_unprivileged_unconfined=1：
#   未被 AppArmor profile 约束的进程不能随意创建 user namespace
#
# 注意：
#   这里不是全局放开限制，而是保持限制开启；
#   bwrap 能工作是因为前面加载了专门的 AppArmor profile。
sudo tee /etc/sysctl.d/99-codex-apparmor-hardening.conf >/dev/null <<'EOF'
kernel.apparmor_restrict_unprivileged_userns=1
kernel.apparmor_restrict_unprivileged_unconfined=1
EOF

# 立即应用所有 sysctl 配置
sudo sysctl --system >/dev/null

echo "[5/6] 验证内核安全配置..."

# 读取当前内核参数
USERNS_VALUE="$(cat /proc/sys/kernel/apparmor_restrict_unprivileged_userns)"
UNCONFINED_VALUE="$(cat /proc/sys/kernel/apparmor_restrict_unprivileged_unconfined)"

# 确认 userns 限制已经开启
if [ "$USERNS_VALUE" != "1" ]; then
  echo "ERROR: apparmor_restrict_unprivileged_userns=$USERNS_VALUE，期望值为 1"
  exit 1
fi

# 确认 unconfined 进程限制已经开启
if [ "$UNCONFINED_VALUE" != "1" ]; then
  echo "ERROR: apparmor_restrict_unprivileged_unconfined=$UNCONFINED_VALUE，期望值为 1"
  exit 1
fi

echo "[6/6] 验证 AppArmor profile 和 bwrap 是否可用..."

# 检查 bwrap profile 是否已经加载
if ! sudo aa-status | grep -qE '(^|[[:space:]])bwrap($|[[:space:]])'; then
  echo "ERROR: AppArmor profile 'bwrap' 未加载"
  sudo aa-status | grep -E 'bwrap|unpriv_bwrap' || true
  exit 1
fi

# 检查 unpriv_bwrap profile 是否已经加载
# bwrap 创建出来的子进程会进入这个更受限的 profile
if ! sudo aa-status | grep -qE '(^|[[:space:]])unpriv_bwrap($|[[:space:]])'; then
  echo "ERROR: AppArmor profile 'unpriv_bwrap' 未加载"
  sudo aa-status | grep -E 'bwrap|unpriv_bwrap' || true
  exit 1
fi

# 测试 bwrap 是否可以正常创建最小沙箱
# 如果这里成功，说明 bwrap 的受控例外已经生效
bwrap --ro-bind / / /bin/true

echo
echo "OK: 安全的 bwrap 配置已经生效。"
echo
echo "当前期望状态："
echo "  kernel.apparmor_restrict_unprivileged_userns=1"
echo "  kernel.apparmor_restrict_unprivileged_unconfined=1"
echo "  AppArmor profiles 已加载: bwrap, unpriv_bwrap"
echo
echo "接下来请重启 Codex，然后在 Codex 中测试："
echo "  printf 'codex sandbox test\n' >> tmp.txt"
