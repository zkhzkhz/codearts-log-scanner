#!/usr/bin/env python3
"""
Fetch PR gate (CodeArts CI) logs from gitcode.com repositories.

两套认证体系并存:
  - v5 API (api.gitcode.com/api/v5): 不带 token 即可读公开 PR 列表/评论
  - web-api (web-api.gitcode.com): 需要浏览器登录 token (GITCODE_ACCESS_TOKEN cookie)
                                    用于拉取门禁流水线日志

数据流:
  1. v5 API 列出近 N 天 PR
  2. v5 API 取每个 PR 的评论,从评论里提取 ascend-robot 贴的 openlibing 流水线 URL
  3. web-api 按 (pipeline_id, run_id, job_run_id) 拉每个 job 的完整日志,分页直到 has_more=False

输出:
  logs/<owner>_<repo>/<pr_number>/<idx>_<job_short>.log
  logs/<owner>_<repo>/<pr_number>/_meta.json
  logs/<owner>_<repo>/_index.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

class GitcodeError(RuntimeError):
    pass

def _request(url: str, *, method="GET", headers: dict | None = None,
             data: dict | None = None, timeout: int = 30,
             retries: int = 3) -> tuple[int, bytes]:
    base_headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/149 Safari/537.36",
    }
    if headers:
        base_headers.update(headers)
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        base_headers.setdefault("Content-Type", "application/json")

    last_exc: Exception | None = None
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=base_headers, method=method, data=body)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            content = e.read() if hasattr(e, "read") else b""
            if e.code in (429, 502, 503, 504) and attempt + 1 < retries:
                time.sleep(2 ** attempt)
                last_exc = e
                continue
            return e.code, content
        except urllib.error.URLError as e:
            last_exc = e
            if attempt + 1 < retries:
                time.sleep(2 ** attempt)
                continue
            raise GitcodeError(f"network error fetching {url}: {e}") from e
    raise GitcodeError(f"giving up on {url}: {last_exc}")

# ---------------------------------------------------------------------------
# v5 API — public PR list / comments
# ---------------------------------------------------------------------------

V5_BASE = "https://api.gitcode.com/api/v5"

def v5_get(url: str, v5_token: str = "") -> Any:
    headers = {}
    if v5_token:
        headers["PRIVATE-TOKEN"] = v5_token
    status, body = _request(url, headers=headers)
    if status >= 400:
        raise GitcodeError(f"HTTP {status} for {url}: {body[:300]!r}")
    return json.loads(body.decode("utf-8")) if body else None

def list_pulls(owner: str, repo: str, since: datetime, *, v5_token: str = "",
               limit: int = 0) -> list[dict]:
    pulls: list[dict] = []
    page = 1
    per_page = 50
    while True:
        qs = urllib.parse.urlencode({
            "state": "all", "sort": "updated", "direction": "desc",
            "page": page, "per_page": per_page,
        })
        url = f"{V5_BASE}/repos/{owner}/{repo}/pulls?{qs}"
        print(f"-> listing PRs page {page}", file=sys.stderr)
        data = v5_get(url, v5_token)
        if not data or not isinstance(data, list):
            break
        stop = False
        for pr in data:
            updated_raw = pr.get("updated_at") or pr.get("created_at")
            if not updated_raw:
                continue
            try:
                updated = datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
            except ValueError:
                continue
            if updated < since:
                stop = True
                break
            pulls.append(pr)
        if stop or len(data) < per_page:
            break
        if limit and len(pulls) >= limit:
            break
        page += 1
        if page > 200:
            print("! pagination safety limit", file=sys.stderr)
            break
    return pulls[:limit] if limit else pulls

def list_org_repos(org: str, v5_token: str = "") -> list[str]:
    """v5 API 翻页拉取组织全部仓库,返回 repo path 列表(不含 owner)。"""
    repos: list[str] = []
    page = 1
    while True:
        qs = urllib.parse.urlencode({"per_page": 100, "page": page})
        url = f"{V5_BASE}/orgs/{urllib.parse.quote(org, safe='')}/repos?{qs}"
        data = v5_get(url, v5_token)
        if not data or not isinstance(data, list):
            break
        for r in data:
            path = r.get("path")
            if path:
                repos.append(path)
        if len(data) < 100:
            break
        page += 1
        if page > 150:
            print("! org repos pagination safety limit", file=sys.stderr)
            break
    return repos

def list_pr_comments(owner: str, repo: str, pr_number: int, v5_token: str = "") -> list[dict]:
    comments: list[dict] = []
    page = 1
    while True:
        qs = urllib.parse.urlencode({"page": page, "per_page": 100})
        url = f"{V5_BASE}/repos/{owner}/{repo}/pulls/{pr_number}/comments?{qs}"
        try:
            data = v5_get(url, v5_token)
        except GitcodeError:
            break
        if not data or not isinstance(data, list):
            break
        comments.extend(data)
        if len(data) < 100:
            break
        page += 1
        if page > 20:
            break
    return comments

# ---------------------------------------------------------------------------
# Pipeline info extraction from comments
# ---------------------------------------------------------------------------

BOT_LOGIN_HINTS = (
    "ascend-robot", "ascend-ds-bot", "kunpeng-bot", "codearts",
    "ci-bot", "huawei-ci", "ascend-ci", "openubmc-ci-bot", "cann-robot",
    "mindspore-bot", "openlibing",
)

OPENLIBING_RE = re.compile(
    r"https?://(?:www\.)?openlibing\.com/apps/pipelineDetail\?[^\s<>\"')]+",
    re.IGNORECASE,
)

ACTIONS_RUN_RE = re.compile(
    r"https?://(?:www\.)?gitcode\.com/([^/\s<>\"')]+)/([^/\s<>\"')]+)/actions/runs/([0-9a-f]{16,})",
    re.IGNORECASE,
)

def comment_is_from_bot(c: dict) -> bool:
    user = c.get("user") or c.get("author") or {}
    login = (user.get("login") or user.get("username") or user.get("name") or "").lower()
    return any(h in login for h in BOT_LOGIN_HINTS)

def extract_pipeline_jobs(comments: list[dict]) -> list[dict]:
    """从评论里提取所有独立的 (pipeline_id, run_id, job_run_id) 三元组及其 pipeline_detail。"""
    seen = set()
    jobs: list[dict] = []
    for c in comments:
        if not comment_is_from_bot(c):
            continue
        body = c.get("body") or ""
        for m in OPENLIBING_RE.finditer(body):
            url = m.group(0).rstrip(".,);'\"")
            try:
                params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            except ValueError:
                continue
            pipeline_id = (params.get("pipelineId") or [""])[0]
            run_id = (params.get("pipelineRunId") or [""])[0]
            job_run_id = (params.get("jobRunId") or [""])[0]
            step_id = (params.get("stepId") or [""])[0]
            step_run_id = (params.get("stepRunId") or [""])[0]
            if not (pipeline_id and run_id and job_run_id):
                continue
            key = (pipeline_id, run_id, job_run_id)
            if key in seen:
                continue
            seen.add(key)
            jobs.append({
                "pipeline_id": pipeline_id,
                "pipeline_run_id": run_id,
                "job_run_id": job_run_id,
                "step_id": step_id,
                "step_run_id": step_run_id,
                "source_url": url,
                "comment_id": c.get("id"),
                "comment_user": (c.get("user") or {}).get("login"),
            })
    return jobs

def extract_action_runs(comments: list[dict]) -> list[dict]:
    """从评论里提取 gitcode Actions run URL (openubmc-ci-bot / cann-robot 风格)。

    返回: [{"run_id": ..., "comment_id": ..., "comment_user": ...}, ...]
    run 的 owner/repo 以评论所属 PR 的仓库为准,这里只提取 run_id。
    """
    seen: set[str] = set()
    runs: list[dict] = []
    for c in comments:
        body = c.get("body") or ""
        for m in ACTIONS_RUN_RE.finditer(body):
            run_id = m.group(3)
            if run_id in seen:
                continue
            seen.add(run_id)
            runs.append({
                "owner": m.group(1),
                "repo": m.group(2),
                "run_id": run_id,
                "source_url": m.group(0),
                "comment_id": c.get("id"),
                "comment_user": (c.get("user") or {}).get("login"),
            })
    return runs

# ---------------------------------------------------------------------------
# web-api — pipeline + log fetching
# ---------------------------------------------------------------------------

WEB_BASE = "https://web-api.gitcode.com/api/v2"

DEFAULT_PIPELINE_DETAIL = {
    "hook_id": "49841",
    "hook_type": "project",
    "job_log_url": None,
    "job_run_time": None,
    "pipeline_total_time": None,
    "project_id": "a9b188a62f334fc0963bcfb523426d17",
    "pipeline_host": "https://cloudpipeline-ext.cn-north-4.myhuaweicloud.com",
}

def _web_headers(web_token: str, owner: str, repo: str, pr: int | None = None) -> dict:
    referer = f"https://gitcode.com/{owner}/{repo}/pull/{pr}/check" if pr else f"https://gitcode.com/{owner}/{repo}"
    return {
        "Authorization": f"Bearer {web_token}",
        "Origin": "https://gitcode.com",
        "Referer": referer,
        "X-App-Channel": "gitcode-fe",
        "X-App-Version": "0",
        "X-Platform": "web",
    }

def fetch_pr_pipelines(owner: str, repo: str, pr_number: int, web_token: str) -> list[dict]:
    """通过 web-api 获取 PR 的所有流水线列表(含 pipeline_detail)。"""
    encoded = urllib.parse.quote(f"{owner}/{repo}", safe="")
    url = f"{WEB_BASE}/projects/pipeline-check/{encoded}/merge_requests/{pr_number}/pipelines"
    status, body = _request(url, headers=_web_headers(web_token, owner, repo, pr_number))
    if status != 200:
        return []
    try:
        d = json.loads(body)
    except json.JSONDecodeError:
        return []
    return d.get("content", []) if isinstance(d, dict) else []

def fetch_pipeline_jobs(owner: str, repo: str, web_token: str, *,
                        pipeline_id: str, pipeline_run_id: str,
                        pipeline_detail: dict | str | None = None,
                        pr_number: int | None = None) -> list[dict]:
    """通过 pipeline-runs/detail 端点取一个 run 的完整结构,提取所有 job_run_id。

    返回: [{"job_run_id": ..., "name": ..., "stage": ..., "status": ...}, ...]
    """
    encoded = urllib.parse.quote(f"{owner}/{repo}", safe="")
    url = f"{WEB_BASE}/projects/pipeline-check/{encoded}/pipelines/{pipeline_id}/pipeline-runs/detail"
    detail = pipeline_detail or DEFAULT_PIPELINE_DETAIL
    payload = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_detail": json.dumps(detail) if isinstance(detail, dict) else detail,
    }
    status, body = _request(url, method="POST", data=payload,
                            headers=_web_headers(web_token, owner, repo, pr_number),
                            timeout=30)
    if status != 200 or not body:
        return []
    try:
        d = json.loads(body)
    except json.JSONDecodeError:
        return []

    jobs: list[dict] = []
    seen: set[str] = set()
    for stage in (d.get("stages") or []):
        stage_name = stage.get("name", "")
        # pre / jobs / post 都可能有 job_run_id
        for slot in ("pre", "jobs", "post"):
            for j in (stage.get(slot) or []):
                # pre/post 直接有 job_run_id;jobs[] 里 id 就是 job_run_id
                jrid = j.get("job_run_id") or j.get("id")
                if not jrid or jrid in seen:
                    continue
                seen.add(jrid)
                jobs.append({
                    "job_run_id": jrid,
                    "name": j.get("name") or j.get("task") or "",
                    "stage": stage_name,
                    "slot": slot,
                    "status": j.get("status"),
                })
    return jobs

def fetch_job_log(owner: str, repo: str, web_token: str, *,
                  pipeline_id: str, pipeline_run_id: str, job_run_id: str,
                  pipeline_detail: dict | None = None,
                  pr_number: int | None = None,
                  page_limit: int = 50) -> str | None:
    """分页拉完整日志,直到 has_more=False。返回完整日志字符串(失败返回 None)。"""
    encoded = urllib.parse.quote(f"{owner}/{repo}", safe="")
    url = f"{WEB_BASE}/projects/pipeline-check/{encoded}/pipelines/{pipeline_id}/pipeline-runs/{pipeline_run_id}/jobs/{job_run_id}/logs"
    detail = pipeline_detail or DEFAULT_PIPELINE_DETAIL

    chunks: list[str] = []
    end_offset = 0
    for _ in range(page_limit):
        payload = {
            "pipeline_id": pipeline_id,
            "pipeline_run_id": pipeline_run_id,
            "job_run_id": job_run_id,
            "pipeline_detail": json.dumps(detail) if isinstance(detail, dict) else detail,
            "start_offset": end_offset,
            "end_offset": 0,
            "limit": 5000,
            "sort": "asc",
        }
        status, body = _request(url, method="POST", data=payload,
                                headers=_web_headers(web_token, owner, repo, pr_number),
                                timeout=60)
        if status != 200 or not body:
            return "".join(chunks) if chunks else None
        try:
            d = json.loads(body)
        except json.JSONDecodeError:
            return "".join(chunks) if chunks else None

        log_part = d.get("log", "")
        if isinstance(log_part, list):
            log_part = "\n".join(str(x) for x in log_part)
        if log_part:
            chunks.append(log_part)

        try:
            new_offset = int(d.get("end_offset", 0))
        except (TypeError, ValueError):
            break
        has_more = bool(d.get("has_more", False))
        if not has_more or new_offset <= end_offset:
            break
        end_offset = new_offset
        time.sleep(0.1)
    result = "".join(chunks) if chunks else None
    # API 在 job 还没有日志时会返回字面字符串 "has no log yet" — 视为无日志
    if result and result.strip() == "has no log yet":
        return None
    return result

# ---------------------------------------------------------------------------
# gitcode Actions (openubmc-ci-bot / cann-robot 风格) — AtomGit OpenAPI v8
# ---------------------------------------------------------------------------

V8_BASE = "https://api.gitcode.com/api/v8"

def _is_web_jwt(token: str) -> bool:
    """web 会话 token 是三段式 JWT; AtomGit PAT 是不透明短串。"""
    return token.count(".") == 2 and len(token) > 100

def _actions_auth(token: str) -> tuple[str, str]:
    """返回 (auth_header_name, auth_header_value)。

    PAT → PRIVATE-TOKEN;web JWT → Bearer。空 token 返回空串(匿名,元数据仍可读)。
    """
    if not token:
        return "", ""
    if _is_web_jwt(token):
        return "Authorization", f"Bearer {token}"
    return "PRIVATE-TOKEN", token

def fetch_action_run_detail(owner: str, repo: str, run_id: str,
                            token: str = "") -> dict | None:
    """取一个 Actions run 的完整结构 (stages→jobs→steps)。

    v8 run/jobs 元数据官方允许匿名访问,token 缺失/过期不影响结构解析。
    """
    url = f"{V8_BASE}/repos/{owner}/{repo}/actions/runs/{run_id}"
    name, value = _actions_auth(token)
    headers = {name: value} if name else {}
    status, body = _request(url, headers=headers)
    if status != 200 or not body:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None

def fetch_action_job_log(owner: str, repo: str, token: str, *,
                         run_id: str, job_id: str, step_run_id: str,
                         page_limit: int = 500) -> str | None:
    """分页拉一个 Actions step 的日志 (v8 端点,需 AtomGit PAT)。

    端点: POST /api/v8/repos/{owner}/{repo}/actions/runs/{run}/jobs/{job}/logs
    body: {"step_id": ..., "offset": N, "limit": 100, "sort": "asc"}
    响应: {"has_more": bool, "start_offset": N, "end_offset": N, "log": "..."}
    官方文档: 不允许匿名访问,仅认 AtomGit 用户访问令牌(PAT)。
    """
    url = f"{V8_BASE}/repos/{owner}/{repo}/actions/runs/{run_id}/jobs/{job_id}/logs"
    name, value = _actions_auth(token)
    headers = {name: value} if name else {}

    if not token or _is_web_jwt(token):
        raise GitcodeError(
            "Actions 日志仅支持 AtomGit PAT (atomgit.com 个人访问令牌);"
            "web JWT/匿名均 401 — 请在 .env 配置 ATOMGIT_TOKEN")

    chunks: list[str] = []
    offset = 0
    for _ in range(page_limit):
        status, body = _request(url, method="POST",
                                data={"step_id": step_run_id, "offset": offset,
                                      "limit": 100, "sort": "asc"},
                                headers=headers, timeout=60)
        if status == 401:
            raise GitcodeError("Actions 日志鉴权失败 (HTTP 401) — AtomGit PAT 无效或权限不足")
        if status == 403:
            raise GitcodeError("Actions 日志无权限 (HTTP 403) — PAT 权限范围不足或非该仓库可见日志")
        if status != 200 or not body:
            break
        try:
            d = json.loads(body)
        except json.JSONDecodeError:
            break

        part = d.get("log") if isinstance(d, dict) else ""
        if isinstance(part, list):
            part = "\n".join(str(x) for x in part)
        if not part:
            break
        chunks.append(str(part))

        try:
            new_offset = int(d.get("end_offset", 0))
        except (TypeError, ValueError):
            break
        has_more = bool(d.get("has_more", False))
        if not has_more or new_offset <= offset:
            break
        offset = new_offset
        time.sleep(0.1)
    result = "".join(chunks) if chunks else None
    if result and result.strip() == "has no log yet":
        return None
    return result

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_repo_arg(arg: str) -> tuple[str, str]:
    arg = arg.strip()
    if arg.startswith("@"):
        org = arg[1:].strip()
        if not org or "/" in org:
            raise ValueError(f"无效组织名: {arg}")
        return "@org", org
    m = re.match(r"https?://(?:www\.)?gitcode\.com/([^/]+)/([^/]+?)(?:\.git)?/?(?:[?#].*)?$", arg)
    if m:
        return m.group(1), m.group(2)
    if "/" in arg and not arg.startswith("/"):
        a, b = arg.split("/", 1)
        return a, b.split("/")[0].split("?")[0].split("#")[0]
    raise ValueError(f"无法解析: {arg}")

def prompt_for_repo() -> tuple[str, str]:
    print("\n请输入 gitcode 仓库 (owner/repo 或 https://gitcode.com/owner/repo):")
    while True:
        try:
            raw = input("仓库: ").strip()
            if raw:
                return parse_repo_arg(raw)
        except ValueError as e:
            print(f"  {e}")
        except EOFError:
            sys.exit(1)

def load_repos_file(path: Path) -> list[tuple[str, str]]:
    """读取 repos.txt,返回 [(owner, repo), ...]。

    行格式: owner/repo、https://gitcode.com/owner/repo 或 @org(整个组织,运行时展开)。
    空行和 # 注释跳过。
    """
    repos: list[tuple[str, str]] = []
    if not path.exists():
        return repos
    for ln, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        try:
            repos.append(parse_repo_arg(line))
        except ValueError as e:
            print(f"WARN: {path}:{ln} 跳过 ({e})", file=sys.stderr)
    return repos

# ---------------------------------------------------------------------------
# Org repos cache
# ---------------------------------------------------------------------------

ORG_CACHE_FILE = Path(__file__).resolve().parent.parent / "repos_cache.json"
ORG_CACHE_TTL = 7 * 86400

def _read_org_cache() -> dict:
    if not ORG_CACHE_FILE.exists():
        return {}
    try:
        data = json.loads(ORG_CACHE_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}

def _write_org_cache(cache: dict) -> None:
    ORG_CACHE_FILE.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def get_org_repos(org: str, *, refresh: bool = False, v5_token: str = "") -> list[str]:
    """返回 org 的仓库列表。优先用缓存(7 天 TTL),过期或 refresh=True 时重新拉取。

    API 失败时若有旧缓存则兜底使用。
    """
    cache = _read_org_cache()
    orgs = cache.get("orgs") or {}
    cached: list[str] | None = orgs.get(org)
    fresh = False
    if cached is not None and not refresh:
        fetched_at = cache.get("fetched_at", "")
        try:
            age = datetime.now(timezone.utc) - datetime.fromisoformat(
                fetched_at.replace("Z", "+00:00"))
            fresh = age.total_seconds() < ORG_CACHE_TTL
        except ValueError:
            fresh = False
        if fresh:
            print(f"从缓存读取组织 {org} 的 {len(cached)} 个仓库 "
                  f"({ORG_CACHE_FILE.name}, fetched_at {fetched_at})", file=sys.stderr)
            return cached
    try:
        repos = list_org_repos(org, v5_token)
    except GitcodeError as e:
        if cached is not None:
            print(f"WARN: 刷新组织 {org} 仓库列表失败 ({e}),使用过期缓存 "
                  f"({len(cached)} 个仓库)", file=sys.stderr)
            return cached
        raise
    print(f"从 API 拉取组织 {org} 的 {len(repos)} 个仓库", file=sys.stderr)
    cache["fetched_at"] = datetime.now(timezone.utc).isoformat()
    cache.setdefault("orgs", {})[org] = repos
    _write_org_cache(cache)
    return repos

def expand_repo_entries(entries: list[tuple[str, str]], *, refresh: bool = False,
                        v5_token: str = "") -> list[tuple[str, str]]:
    """把 ('@org', org) 条目展开为具体仓库,普通条目原样返回。"""
    out: list[tuple[str, str]] = []
    for owner, repo in entries:
        if owner == "@org":
            out.extend((repo, path) for path in get_org_repos(repo, refresh=refresh,
                                                              v5_token=v5_token))
        else:
            out.append((owner, repo))
    return out

def _process_one_pr(owner: str, repo: str, pr: dict, *, token: str,
                    repo_dir: Path, sleep: float, job_concurrency: int = 4,
                    incremental: bool = False, action_token: str = "") -> dict:
    """处理单个 PR:取评论、解析 jobs、并发拉日志。线程安全(只写自己的子目录)。

    incremental=True 时: 若 _meta.json 已存在且含日志,跳过拉取。
    """
    num = pr.get("number") or pr.get("iid")
    if not num:
        return {"skip": True}
    title = pr.get("title", "")
    pr_dir = repo_dir / str(num)

    # 增量模式: 若已有 _meta.json 且有日志,跳过
    if incremental:
        meta_path = pr_dir / "_meta.json"
        if meta_path.exists():
            try:
                existing = json.loads(meta_path.read_text(encoding="utf-8"))
                logs_list = existing.get("logs", [])
                if any(l.get("file") for l in logs_list if isinstance(l, dict)):
                    return {"number": num, "title": title, "log_count": 0,
                            "incremental_skip": True, "cached": True}
            except (json.JSONDecodeError, OSError):
                pass

    pr_dir.mkdir(parents=True, exist_ok=True)

    try:
        comments = list_pr_comments(owner, repo, num)
    except GitcodeError as e:
        return {"number": num, "title": title, "log_count": 0, "error": str(e)}

    # 路径 A: 评论里贴 jobRunId (Ascend/cann 风格)
    jobs = extract_pipeline_jobs(comments)

    # 路径 C: 评论里贴 gitcode Actions run URL (openubmc-ci-bot / cann-robot 风格)
    # 一个 run 含几十个 job、每个 job 又分多个 step,日志按 step 粒度拉取
    action_runs = extract_action_runs(comments)

    # 取 PR 的 pipelines (拿真实的 pipeline_detail)
    pipelines: list[dict] = []
    pipeline_details: dict[tuple[str, str], dict] = {}
    if token:
        try:
            pipelines = fetch_pr_pipelines(owner, repo, num, token)
            for p in pipelines:
                pid = p.get("pipeline_id")
                rid = p.get("pipeline_run_id")
                detail_str = p.get("pipeline_detail")
                if pid and rid and detail_str:
                    try:
                        pipeline_details[(pid, rid)] = json.loads(detail_str)
                    except (json.JSONDecodeError, TypeError):
                        pass
        except GitcodeError:
            pass

    # 路径 B: 评论里没 jobRunId (openubmc 风格)
    # 用 pipeline-runs/detail 端点拉每个 run 的所有 jobs
    if not jobs and token and pipelines:
        seen_jobs = set()
        for p in pipelines:
            pid = p.get("pipeline_id")
            rid = p.get("pipeline_run_id")
            if not (pid and rid):
                continue
            detail = pipeline_details.get((pid, rid))
            try:
                run_jobs = fetch_pipeline_jobs(
                    owner, repo, token,
                    pipeline_id=pid, pipeline_run_id=rid,
                    pipeline_detail=detail, pr_number=num,
                )
            except GitcodeError:
                run_jobs = []
            for rj in run_jobs:
                jrid = rj["job_run_id"]
                if jrid in seen_jobs:
                    continue
                seen_jobs.add(jrid)
                jobs.append({
                    "pipeline_id": pid,
                    "pipeline_run_id": rid,
                    "job_run_id": jrid,
                    "step_id": "",
                    "step_run_id": "",
                    "name": rj.get("name"),
                    "stage": rj.get("stage"),
                    "slot": rj.get("slot"),
                    "status": rj.get("status"),
                    "source": "pipeline-runs/detail",
                })

    def _one_job(i_job: tuple[int, dict]) -> dict:
        i, job = i_job
        short = job["job_run_id"][:8]
        fname = f"{i:03d}_{short}.log"
        dest = pr_dir / fname
        if dest.exists() and dest.stat().st_size > 0:
            return {"file": fname, "cached": True, **job}
        if not token:
            return {"file": None, "skipped": "no token", **job}
        detail = pipeline_details.get((job["pipeline_id"], job["pipeline_run_id"]))
        try:
            log = fetch_job_log(owner, repo, token,
                                pipeline_id=job["pipeline_id"],
                                pipeline_run_id=job["pipeline_run_id"],
                                job_run_id=job["job_run_id"],
                                pipeline_detail=detail,
                                pr_number=num)
        except GitcodeError:
            log = None
        if log:
            dest.write_text(log, encoding="utf-8")
            return {"file": fname, "size": len(log), **job}
        return {"file": None, "error": "no_log", **job}

    def _one_action_job(i_job: tuple[int, dict]) -> dict:
        i, job = i_job
        short = job["job_id"][:8]
        fname = f"a{i:03d}_{short}.log"
        dest = pr_dir / fname
        if dest.exists() and dest.stat().st_size > 0:
            return {"file": fname, "cached": True, **job}
        atok = action_token or token
        if not atok:
            return {"file": None, "skipped": "no token", **job}
        try:
            log = fetch_action_job_log(owner, repo, atok,
                                       run_id=job["run_id"],
                                       job_id=job["job_id"],
                                       step_run_id=job["step_run_id"])
        except GitcodeError as e:
            return {"file": None, "error": str(e), **job}
        if log:
            dest.write_text(log, encoding="utf-8")
            return {"file": fname, "size": len(log), **job}
        return {"file": None, "error": "no_log", **job}

    # 路径 C 展开: 每个 run → 每个(有日志可拉且非 IGNORED 的)job → 每个 step
    # run 详情匿名可读,无需 token;日志本体在 _one_action_job 里要求 PAT
    action_jobs: list[dict] = []
    action_detail: dict | None = None
    for ar in action_runs:
        try:
            action_detail = fetch_action_run_detail(owner, repo, ar["run_id"])
        except GitcodeError:
            action_detail = None
        if not action_detail:
            continue
        for stage in (action_detail.get("stages") or []):
            for slot in ("pre", "jobs", "post"):
                for j in (stage.get(slot) or []):
                    jid = j.get("id")
                    if not jid or j.get("status") == "IGNORED":
                        continue
                    for st in (j.get("steps") or []):
                        sid = st.get("id")
                        if not sid:
                            continue
                        action_jobs.append({
                            "run_id": ar["run_id"],
                            "job_id": jid,
                            "job_name": j.get("name"),
                            "job_status": j.get("status"),
                            "step_run_id": sid,
                            "step_name": st.get("name"),
                            "step_status": st.get("status"),
                            "source": "actions",
                        })

    downloaded: list[dict] = [None] * len(jobs)  # type: ignore
    log_count = 0
    if jobs:
        with ThreadPoolExecutor(max_workers=max(1, job_concurrency)) as jex:
            futures = {jex.submit(_one_job, (i, j)): i for i, j in enumerate(jobs)}
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    downloaded[idx] = fut.result()
                    if downloaded[idx].get("file"):
                        log_count += 1
                except Exception as e:
                    downloaded[idx] = {"file": None, "error": str(e), **jobs[idx]}
                if sleep:
                    time.sleep(sleep)

    # Actions 日志同样并发拉取,结果并入 downloaded
    action_downloaded: list[dict] = [None] * len(action_jobs)  # type: ignore
    if action_jobs:
        with ThreadPoolExecutor(max_workers=max(1, job_concurrency)) as jex:
            futures = {jex.submit(_one_action_job, (i, j)): i for i, j in enumerate(action_jobs)}
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    action_downloaded[idx] = fut.result()
                    if action_downloaded[idx].get("file"):
                        log_count += 1
                except Exception as e:
                    action_downloaded[idx] = {"file": None, "error": str(e), **action_jobs[idx]}
                if sleep:
                    time.sleep(sleep)
    downloaded.extend(a for a in action_downloaded if a is not None)

    (pr_dir / "_meta.json").write_text(json.dumps({
        "number": num, "title": title, "state": pr.get("state"),
        "html_url": pr.get("html_url"),
        "created_at": pr.get("created_at"),
        "updated_at": pr.get("updated_at"),
        "logs": downloaded,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"number": num, "title": title, "log_count": log_count,
            "comments": len(comments), "jobs": len(jobs)}

def get_last_scanned_update(repo_dir: Path) -> datetime | None:
    """读取 _index.json,返回已扫描 PR 的最新 updated_at 时间。"""
    idx_path = repo_dir / "_index.json"
    if not idx_path.exists():
        return None
    try:
        data = json.loads(idx_path.read_text(encoding="utf-8"))
        if not isinstance(data, list) or not data:
            return None
        latest = None
        for pr in data:
            upd_raw = pr.get("updated_at")
            if not upd_raw:
                continue
            try:
                upd = datetime.fromisoformat(upd_raw.replace("Z", "+00:00"))
            except ValueError:
                continue
            if latest is None or upd > latest:
                latest = upd
        return latest
    except (json.JSONDecodeError, OSError):
        return None

def get_scanned_pr_numbers(repo_dir: Path) -> set[int]:
    """读取 _index.json,返回已扫描的 PR number 集合。"""
    idx_path = repo_dir / "_index.json"
    if not idx_path.exists():
        return set()
    try:
        data = json.loads(idx_path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            return set()
        return {pr.get("number") for pr in data if isinstance(pr, dict) and pr.get("number")}
    except (json.JSONDecodeError, OSError):
        return set()

def fetch_one_repo(owner: str, repo: str, *, token: str, since: datetime,
                   out_root: Path, limit: int = 0, sleep: float = 0.0,
                   concurrency: int = 8, job_concurrency: int = 4,
                   incremental: bool = False,
                   action_token: str = "") -> tuple[int, int]:
    """拉取单个仓库的日志。PR 之间用线程池并发,PR 内部 jobs 也并发。

    action_token: AtomGit PAT,Actions 路径 (路径 C) 日志专用;空则用 token 回退逻辑。
    incremental=True 时:
      - 若已有 _index.json,只拉 updated_at > 最新已扫描 PR 的更新时间的 PR
      - 同时跳过已有日志的 PR (通过 _meta.json 判断)
    """
    repo_dir = out_root / f"{owner}_{repo}"
    repo_dir.mkdir(parents=True, exist_ok=True)

    # 增量模式: 调整 since 时间并获取已扫描 PR 集合
    scanned_prs: set[int] = set()
    effective_since = since
    if incremental:
        last_upd = get_last_scanned_update(repo_dir)
        if last_upd and last_upd > since:
            effective_since = last_upd + timedelta(seconds=1)
            print(f"增量模式: 从 {effective_since.isoformat()} 开始扫描", file=sys.stderr)
        scanned_prs = get_scanned_pr_numbers(repo_dir)

    print(f"\n=== {owner}/{repo} ===", file=sys.stderr)
    pulls = list_pulls(owner, repo, effective_since, limit=limit)
    # 增量模式过滤已扫描的 PR
    if incremental and scanned_prs:
        before = len(pulls)
        pulls = [p for p in pulls if p.get("number") not in scanned_prs]
        if before != len(pulls):
            print(f"增量模式: 跳过 {before - len(pulls)} 个已扫描 PR", file=sys.stderr)
    print(f"找到 {len(pulls)} 个 PR (PR并发={concurrency}, job并发={job_concurrency})",
          file=sys.stderr)

    index: list[dict] = []
    total_logs = 0
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(_process_one_pr, owner, repo, pr,
                             token=token, repo_dir=repo_dir, sleep=sleep,
                             job_concurrency=job_concurrency,
                             incremental=incremental,
                             action_token=action_token): pr
                   for pr in pulls}
        for fut in as_completed(futures):
            try:
                result = fut.result()
            except Exception as e:
                print(f"  !! PR 处理异常: {e}", file=sys.stderr)
                continue
            if result.get("skip"):
                continue
            done += 1
            total_logs += result.get("log_count", 0)
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(pulls) - done) / rate if rate > 0 else 0
            if "error" in result:
                print(f"  [{done}/{len(pulls)}] #{result['number']} 失败: {result['error']}",
                      file=sys.stderr)
            else:
                print(f"  [{done}/{len(pulls)}] #{result['number']} "
                      f"jobs={result['jobs']} logs={result['log_count']} "
                      f"({rate:.2f} PR/s, ETA {eta/60:.1f}min)", file=sys.stderr)
            index.append(result)

    # 增量模式: 合并旧 index
    if incremental:
        old_idx_path = repo_dir / "_index.json"
        if old_idx_path.exists():
            try:
                old_idx = json.loads(old_idx_path.read_text(encoding="utf-8"))
                if isinstance(old_idx, list):
                    # 用 dict 去重,新结果覆盖旧的
                    merged = {pr.get("number"): pr for pr in old_idx if isinstance(pr, dict) and pr.get("number")}
                    for pr in index:
                        if pr.get("number"):
                            merged[pr["number"]] = pr
                    index = list(merged.values())
            except (json.JSONDecodeError, OSError):
                pass

    index.sort(key=lambda x: -(x.get("number") or 0))
    (repo_dir / "_index.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(index), total_logs

def main() -> int:
    ap = argparse.ArgumentParser(
        description="拉取 gitcode PR 门禁(CodeArts CI)日志",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 单仓库
  %(prog)s --repo Ascend/pytorch --days 30
  %(prog)s --repo https://gitcode.com/Ascend/pytorch --limit 5

  # 整个组织 (--refresh-orgs 强制刷新缓存)
  %(prog)s --repo @BoostKit --days 30
  %(prog)s --repo @BoostKit --refresh-orgs --incremental

  # 多仓库 (从 repos.txt 读取,支持 @org 行)
  %(prog)s --repos-file repos.txt --days 30
  %(prog)s                       # 默认读 ./repos.txt 或 ../repos.txt

  # 交互输入
  %(prog)s -i

Token 获取:
  浏览器登录 gitcode.com → F12 → Application → Cookies →
  复制 GITCODE_ACCESS_TOKEN 到 .env 的 GITCODE_TOKEN
""")
    ap.add_argument("--repo", dest="repo_arg", help="单仓库: owner/repo、URL 或 @org(整个组织)")
    ap.add_argument("--repos-file", dest="repos_file",
                    help="多仓库列表文件 (一行一个,支持 @org,默认 repos.txt)")
    ap.add_argument("--days", type=int, default=int(os.environ.get("DAYS", "30")))
    ap.add_argument("--token", default=os.environ.get("GITCODE_TOKEN", ""),
                    help="gitcode web token (从 cookie GITCODE_ACCESS_TOKEN)")
    ap.add_argument("--atomgit-token", default=os.environ.get("ATOMGIT_TOKEN", ""),
                    help="AtomGit PAT (Actions 日志用;未配置时回退 --token,须为 PAT 格式)")
    ap.add_argument("--out", default="logs")
    ap.add_argument("--limit", type=int, default=0, help="每仓库最多处理 N 个 PR")
    ap.add_argument("--sleep", type=float, default=0.05,
                    help="单次请求后 sleep (秒),并发模式建议 0-0.05")
    ap.add_argument("--concurrency", "-j", type=int, default=8,
                    help="并发处理 PR 数(默认 8)")
    ap.add_argument("--job-concurrency", type=int, default=4,
                    help="单 PR 内并发拉 job 日志数(默认 4)")
    ap.add_argument("-i", "--interactive", action="store_true")
    ap.add_argument("--incremental", action="store_true",
                    help="增量模式: 只扫描上次扫描后新增/更新的 PR")
    ap.add_argument("--refresh-orgs", dest="refresh_orgs", action="store_true",
                    help="强制刷新 @org 组织仓库缓存 (repos_cache.json)")
    args = ap.parse_args()

    # 决定要拉哪些仓库
    repos: list[tuple[str, str]] = []
    if args.interactive:
        repos = [prompt_for_repo()]
    elif args.repo_arg:
        try:
            repos = [parse_repo_arg(args.repo_arg)]
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 2
    else:
        # 从 repos-file 读
        candidates = []
        if args.repos_file:
            candidates.append(Path(args.repos_file))
        # 默认查找路径: 当前工作目录 / 项目根
        candidates.append(Path("repos.txt"))
        candidates.append(Path(__file__).resolve().parent.parent / "repos.txt")
        for p in candidates:
            if p.exists():
                repos = load_repos_file(p)
                print(f"从 {p} 读取 {len(repos)} 个仓库", file=sys.stderr)
                break
        if not repos:
            # 没找到列表文件,进入交互
            print("未提供 --repo / --repos-file,且未找到 repos.txt", file=sys.stderr)
            repos = [prompt_for_repo()]

    try:
        repos = expand_repo_entries(repos, refresh=args.refresh_orgs,
                                    v5_token=os.environ.get("GITCODE_V5_TOKEN", ""))
    except GitcodeError as e:
        print(f"ERROR: 展开组织仓库失败: {e}", file=sys.stderr)
        return 2
    print(f"共 {len(repos)} 个仓库待处理", file=sys.stderr)

    if not args.token:
        print("WARN: 未配置 GITCODE_TOKEN,只会列出 URL,不下载日志", file=sys.stderr)

    # Actions 日志 token: 优先 ATOMGIT_TOKEN (PAT);fallback 到 --token,但仅当它是 PAT 格式
    action_token = args.atomgit_token
    if not action_token and args.token and not _is_web_jwt(args.token):
        action_token = args.token
        print("INFO: 未配置 ATOMGIT_TOKEN,GITCODE_TOKEN 是 PAT 格式,Actions 日志复用它", file=sys.stderr)
    if args.atomgit_token:
        print("INFO: AtomGit PAT 已配置 (Actions 日志)", file=sys.stderr)

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    print(f"窗口: {since.date()} 起", file=sys.stderr)
    if args.incremental:
        print("增量模式: 已扫描的仓库只拉新 PR", file=sys.stderr)

    grand_pr = 0
    grand_log = 0
    for owner, repo in repos:
        try:
            n_pr, n_log = fetch_one_repo(owner, repo, token=args.token, since=since,
                                         out_root=out_root, limit=args.limit,
                                         sleep=args.sleep, concurrency=args.concurrency,
                                         job_concurrency=args.job_concurrency,
                                         incremental=args.incremental,
                                         action_token=action_token)
            grand_pr += n_pr
            grand_log += n_log
        except GitcodeError as e:
            print(f"!! {owner}/{repo} 失败: {e}", file=sys.stderr)
        except KeyboardInterrupt:
            print(f"\n!! 用户中断", file=sys.stderr)
            break

    print(f"\n全部完成: {len(repos)} 仓库,{grand_pr} PR,{grand_log} 日志 → {out_root}/", file=sys.stderr)
    return 0

if __name__ == "__main__":
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sys.exit(main())
