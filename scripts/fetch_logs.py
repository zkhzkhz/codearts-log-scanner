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
    "ci-bot", "huawei-ci", "ascend-ci",
)

OPENLIBING_RE = re.compile(
    r"https?://(?:www\.)?openlibing\.com/apps/pipelineDetail\?[^\s<>\"')]+",
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
    return "".join(chunks) if chunks else None

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_repo_arg(arg: str) -> tuple[str, str]:
    arg = arg.strip()
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
    """读取 repos.txt,返回 [(owner, repo), ...]。空行和 # 注释跳过。"""
    repos: list[tuple[str, str]] = []
    if not path.exists():
        return repos
    for ln, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            repos.append(parse_repo_arg(line))
        except ValueError as e:
            print(f"WARN: {path}:{ln} 跳过 ({e})", file=sys.stderr)
    return repos

def _process_one_pr(owner: str, repo: str, pr: dict, *, token: str,
                    repo_dir: Path, sleep: float, job_concurrency: int = 4) -> dict:
    """处理单个 PR:取评论、解析 jobs、并发拉日志。线程安全(只写自己的子目录)。"""
    num = pr.get("number") or pr.get("iid")
    if not num:
        return {"skip": True}
    title = pr.get("title", "")
    pr_dir = repo_dir / str(num)
    pr_dir.mkdir(parents=True, exist_ok=True)

    try:
        comments = list_pr_comments(owner, repo, num)
    except GitcodeError as e:
        return {"number": num, "title": title, "log_count": 0, "error": str(e)}

    jobs = extract_pipeline_jobs(comments)

    pipeline_details: dict[tuple[str, str], dict] = {}
    if token:
        try:
            for p in fetch_pr_pipelines(owner, repo, num, token):
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

    (pr_dir / "_meta.json").write_text(json.dumps({
        "number": num, "title": title, "state": pr.get("state"),
        "html_url": pr.get("html_url"),
        "created_at": pr.get("created_at"),
        "updated_at": pr.get("updated_at"),
        "logs": downloaded,
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"number": num, "title": title, "log_count": log_count,
            "comments": len(comments), "jobs": len(jobs)}

def fetch_one_repo(owner: str, repo: str, *, token: str, since: datetime,
                   out_root: Path, limit: int = 0, sleep: float = 0.0,
                   concurrency: int = 8, job_concurrency: int = 4) -> tuple[int, int]:
    """拉取单个仓库的日志。PR 之间用线程池并发,PR 内部 jobs 也并发。"""
    repo_dir = out_root / f"{owner}_{repo}"
    repo_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== {owner}/{repo} ===", file=sys.stderr)
    pulls = list_pulls(owner, repo, since, limit=limit)
    print(f"找到 {len(pulls)} 个 PR (PR并发={concurrency}, job并发={job_concurrency})",
          file=sys.stderr)

    index: list[dict] = []
    total_logs = 0
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(_process_one_pr, owner, repo, pr,
                             token=token, repo_dir=repo_dir, sleep=sleep,
                             job_concurrency=job_concurrency): pr
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

  # 多仓库 (从 repos.txt 读取)
  %(prog)s --repos-file repos.txt --days 30
  %(prog)s                       # 默认读 ./repos.txt 或 ../repos.txt

  # 交互输入
  %(prog)s -i

Token 获取:
  浏览器登录 gitcode.com → F12 → Application → Cookies →
  复制 GITCODE_ACCESS_TOKEN 到 .env 的 GITCODE_TOKEN
""")
    ap.add_argument("--repo", dest="repo_arg", help="单仓库: owner/repo 或 URL")
    ap.add_argument("--repos-file", dest="repos_file",
                    help="多仓库列表文件 (一行一个,默认 repos.txt)")
    ap.add_argument("--days", type=int, default=int(os.environ.get("DAYS", "30")))
    ap.add_argument("--token", default=os.environ.get("GITCODE_TOKEN", ""),
                    help="gitcode web token (从 cookie GITCODE_ACCESS_TOKEN)")
    ap.add_argument("--out", default="logs")
    ap.add_argument("--limit", type=int, default=0, help="每仓库最多处理 N 个 PR")
    ap.add_argument("--sleep", type=float, default=0.05,
                    help="单次请求后 sleep (秒),并发模式建议 0-0.05")
    ap.add_argument("--concurrency", "-j", type=int, default=8,
                    help="并发处理 PR 数(默认 8)")
    ap.add_argument("--job-concurrency", type=int, default=4,
                    help="单 PR 内并发拉 job 日志数(默认 4)")
    ap.add_argument("-i", "--interactive", action="store_true")
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

    if not args.token:
        print("WARN: 未配置 GITCODE_TOKEN,只会列出 URL,不下载日志", file=sys.stderr)

    since = datetime.now(timezone.utc) - timedelta(days=args.days)
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    print(f"窗口: {since.date()} 起", file=sys.stderr)

    grand_pr = 0
    grand_log = 0
    for owner, repo in repos:
        try:
            n_pr, n_log = fetch_one_repo(owner, repo, token=args.token, since=since,
                                         out_root=out_root, limit=args.limit,
                                         sleep=args.sleep, concurrency=args.concurrency,
                                         job_concurrency=args.job_concurrency)
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
