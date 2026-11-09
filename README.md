# codearts-log-scanner

拉取 gitcode.com 上指定仓库 PR 的 CodeArts CI 门禁日志,并用 `gitleaks` 做敏感信息扫描。

## 工作流

```
repos.txt  (一行一个仓库)
   │
   ▼
gitcode v5 API: PR 列表 (近 N 天) + PR 评论
   ├─ 评论 → 正则提取 ascend-robot 贴的 openlibing 流水线 URL
   │         (含 pipeline_id / pipeline_run_id / job_run_id)
   └─ web-api: 取 PR 流水线 → pipeline_detail (含 host / project_id)

   ↓ 用 GITCODE_TOKEN 鉴权 (web 会话 token)

web-api POST /pipeline-check/.../jobs/{job_id}/logs
   → 分页拉完整日志,直到 has_more=false
   → 写入 logs/{owner}_{repo}/{pr}/{idx}_{job_short}.log

   ↓ scan.sh 按仓库分别扫描

gitleaks dir . --config gitleaks.toml \
        --max-archive-depth 100 --max-decode-depth 100 -vv \
        -r reports/{owner}_{repo}.json
```

## 快速开始

```bash
# 1. 配置 token
cp .env.example .env
# 编辑 .env 写入 GITCODE_TOKEN

# 2. 配置要扫描的仓库
vi repos.txt   # 一行一个

# 3. 拉取日志
python3 scripts/fetch_logs.py --days 30

# 4. 扫描敏感信息
bash scripts/scan.sh
```

## 仓库配置 (`repos.txt`)

```
# 一行一个,以 # 开头的行被忽略
Ascend/pytorch
Ascend/MindSpeed-LLM
https://gitcode.com/kunpengcompute/hpckit-community

# 整个组织: 运行时调 v5 API 自动拉取该组织全部仓库
@BoostKit
```

`@org` 仓库列表缓存在项目根 `repos_cache.json`（7 天过期，API 失败时自动用过期缓存兜底），用 `--refresh-orgs` 强制刷新。

`fetch_logs.py` 默认从项目根的 `repos.txt` 读取,也可以用 `--repos-file other.txt` 指定其他文件。

## 获取 Token

CodeArts 门禁日志需登录态访问:

1. 浏览器登录 https://gitcode.com
2. F12 → Application → Cookies → `https://gitcode.com`
3. 复制 `GITCODE_ACCESS_TOKEN` 的值
4. 写入 `.env` 的 `GITCODE_TOKEN`

注意: web 会话 token 过期会失败,需要重新获取。

## 用法

### 拉取日志

```bash
# 多仓库 (从 repos.txt)
python3 scripts/fetch_logs.py --days 30

# 整个组织 (自动拉取仓库列表,--refresh-orgs 强制刷新缓存)
python3 scripts/fetch_logs.py --repo @BoostKit --days 30
python3 scripts/fetch_logs.py --repo @BoostKit --refresh-orgs --incremental

# 单仓库
python3 scripts/fetch_logs.py --repo Ascend/pytorch
python3 scripts/fetch_logs.py --repo https://gitcode.com/Ascend/pytorch

# 限制每仓库 PR 数 (调试用)
python3 scripts/fetch_logs.py --limit 5

# 自定义配置文件
python3 scripts/fetch_logs.py --repos-file my-repos.txt

# 交互输入
python3 scripts/fetch_logs.py -i
```

### 扫描

```bash
# 扫描 logs/ 下所有仓库,按仓库出独立报告
bash scripts/scan.sh

# 扫描单个仓库
bash scripts/scan.sh logs/Ascend_pytorch

# 自定义 gitleaks 路径或规则
GITLEAKS_BIN=/path/to/gitleaks GITLEAKS_CONFIG=/path/to/rules.toml bash scripts/scan.sh
```

#### 扫描的完整命令(等价)

```bash
cd logs/<owner>_<repo>
$GITLEAKS_BIN dir . \
    --config $GITLEAKS_CONFIG \
    --max-archive-depth 100 \
    --max-decode-depth 100 \
    -vv \
    -r reports/<owner>_<repo>.json
```

## 输出结构

```
logs/
└── <owner>_<repo>/
    ├── _index.json              # 该仓库所有 PR 概要
    └── <pr_number>/
        ├── _meta.json           # PR + 所有 jobs 元数据
        ├── 000_<job_id8>.log    # 每个 job 的完整日志
        └── ...

reports/
├── <owner>_<repo>.json          # 该仓库的 gitleaks 报告
└── ...
```

`_meta.json` 含每个 job 的 `pipeline_id` / `pipeline_run_id` / `job_run_id` / `source_url`,可追溯到原始流水线页面。

## 依赖

- Python 3.8+ (仅标准库)
- gitleaks >= 8.18 (需要 `--max-archive-depth` / `--max-decode-depth`)

`scan.sh` 默认使用 `~/linux-security-audit/bin/gitleaks-linux-amd64`,规则文件为项目根的 `gitleaks.toml`(从 `linux-security-audit/skills/sensitive-info-scan/config/gitleaks-custom.toml` 拷贝并适配)。

## 设计说明

**两套 API**: gitcode 同域下有两套:
- `api.gitcode.com/api/v5` 用于公开数据(PR 列表/评论),公开仓库免 token
- `web-api.gitcode.com` 用于前端 SPA(流水线/日志),**需要 web 会话 token**

脚本仅一个 `GITCODE_TOKEN` 配置,作为 web token 使用;v5 端点不传 token。

**为什么从评论里挖 jobRunId**: web-api 的 pipelines 端点只到 pipeline 级,不返回 jobRunId。`ascend-robot` 在每条评论里附带的 openlibing URL 含 `jobRunId`/`stepRunId`,可以反向提取。

**关于"无日志"job**: 同一 step 的多个 jobRunId 通常是矩阵任务的并行实例(或重跑取代旧实例),其中部分 API 返回 `log: null` 是正常现象,浏览器 UI 上同样为空。

**日志分页**: 单次最多 5000 行,自动按 `end_offset` 翻页直到 `has_more=false`。
