#!/usr/bin/env bash
# 按仓库分别扫描 logs/<owner>_<repo>/,每个仓库产出独立报告
# reports/<owner>_<repo>.json
#
# 完整命令(每个子目录一次):
#   cd logs/<owner>_<repo> && \
#   $GITLEAKS_BIN dir . \
#     --config $GITLEAKS_CONFIG \
#     --max-archive-depth 100 --max-decode-depth 100 \
#     -vv -r reports/<owner>_<repo>.json
#
# 用法:
#   bash scripts/scan.sh                      # 扫所有仓库
#   bash scripts/scan.sh logs/Ascend_pytorch  # 扫单个目录
#   GITLEAKS_BIN=... GITLEAKS_CONFIG=... bash scripts/scan.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

GITLEAKS_BIN="${GITLEAKS_BIN:-${HOME}/linux-security-audit/bin/gitleaks-linux-amd64}"
CONFIG="${GITLEAKS_CONFIG:-${ROOT}/gitleaks.toml}"
LOGS_DIR="${ROOT}/logs"
REPORTS_DIR="${ROOT}/reports"

mkdir -p "${REPORTS_DIR}"

if [[ ! -x "${GITLEAKS_BIN}" ]]; then
  if command -v gitleaks >/dev/null 2>&1; then
    GITLEAKS_BIN="gitleaks"
  else
    echo "ERROR: gitleaks 未找到。设置 GITLEAKS_BIN 或安装 gitleaks。" >&2
    echo "  当前查找路径: ${GITLEAKS_BIN}" >&2
    exit 127
  fi
fi

CONFIG_ARG=()
if [[ -f "${CONFIG}" ]]; then
  CONFIG_ARG=(-c "${CONFIG}")
fi

scan_one() {
  local target="$1"
  local name
  name="$(basename "${target}")"
  local report="${REPORTS_DIR}/${name}.json"

  echo
  echo "─── 扫描 ${name} ───"
  echo "  目录: ${target}"
  echo "  报告: ${report}"

  local rc=0
  ( cd "${target}" && \
    "${GITLEAKS_BIN}" dir . \
      "${CONFIG_ARG[@]}" \
      --max-archive-depth 100 \
      --max-decode-depth 100 \
      -vv \
      -r "${report}" \
  ) || rc=$?

  if [[ "${rc}" -eq 0 ]]; then
    echo "  结果: 未发现敏感信息"
  else
    local count
    count=$(python3 -c "import json,sys; print(len(json.load(open('${report}'))))" 2>/dev/null || echo "?")
    echo "  结果: 发现 ${count} 处敏感信息 (退出码 ${rc})"
  fi
  return 0
}

echo "gitleaks: ${GITLEAKS_BIN}"
[[ -n "${CONFIG_ARG[*]:-}" ]] && echo "规则配置: ${CONFIG}" || echo "规则配置: (使用默认规则)"

# 单目标 / 全目录扫描
if [[ $# -gt 0 ]]; then
  scan_one "$1"
else
  if [[ ! -d "${LOGS_DIR}" ]]; then
    echo "ERROR: ${LOGS_DIR} 不存在" >&2
    exit 1
  fi
  shopt -s nullglob
  any=0
  for d in "${LOGS_DIR}"/*/; do
    scan_one "${d%/}"
    any=1
  done
  if [[ "${any}" -eq 0 ]]; then
    echo "${LOGS_DIR} 下无仓库子目录,请先运行 fetch_logs.py" >&2
    exit 1
  fi

  echo
  echo "─── 汇总 ───"
  for r in "${REPORTS_DIR}"/*.json; do
    [[ -f "$r" ]] || continue
    name=$(basename "$r" .json)
    count=$(python3 -c "import json; print(len(json.load(open('$r'))))" 2>/dev/null || echo "?")
    printf "  %-40s findings=%s\n" "${name}" "${count}"
  done
fi
