#!/usr/bin/env python3
"""Seed and drive the Orca build run for agb-trader tickets T01-T32 (issues #14-#45).

Subcommands:
  seed [--run RUN] [--apply] create the PRE, BUILD and MERGE tasks in an Orca run (dry run without --apply)
  next --run RUN [--slots N] rank the ready BUILD tasks and list the coordinator-owned ready tasks
  frontier                   GitHub cross-check: open tickets whose blockers are all closed

The plan this implements is docs/build/orca-build-plan.md. Stdlib only; shells out to `gh`
and to the Orca CLI named by ORCA_CLI (default `orca-ide`, because bare `orca` is the GNOME
screen reader on Linux hosts).
"""
import argparse
import json
import os
import re
import subprocess
import sys
from functools import lru_cache

REPO = "agb1986/agb-trader"
ORCA = os.environ.get("ORCA_CLI", "orca-ide")
WORKER_START = ("{orca} orchestration worker-start --task {id} --worktree new-top-level --name {name} "
                "--agent claude --model opus --effort high --setup run --json")

# ticket: (issue, worktree slug, lane, blocked by). The edges mirror the native GitHub
# blocked_by links; `seed` and `frontier` fail loudly if the two ever disagree.
TICKETS = {
    1: (14, "walking-skeleton", "infra", []),
    2: (15, "scheduler", "infra", [1]),
    3: (16, "server-delivery", "infra", [2]),
    4: (17, "sharadar-lake", "data", [2]),
    5: (18, "cost-model", "core", [1]),
    6: (19, "universe-snapshot", "data", [4]),
    7: (20, "proposed-basket", "pipeline", [5, 6]),
    8: (21, "pretrade-gate", "execution", [7]),
    9: (22, "paper-moc", "execution", [8]),
    10: (23, "execution-recovery", "execution", [9]),
    11: (24, "telegram-bot", "bot", [2]),
    12: (25, "bot-readonly", "bot", [9, 11]),
    13: (26, "approval-loop", "bot", [12]),
    14: (27, "pause-resume", "execution", [9, 11]),
    15: (28, "liquidate", "execution", [14]),
    16: (29, "features", "data", [6]),
    17: (30, "research-profile", "research", [16]),
    18: (31, "nautilus-backtest", "research", [7, 17]),
    19: (32, "corporate-events", "pipeline", [9, 18]),
    20: (33, "validation-gates", "research", [18]),
    21: (34, "model-registry", "pipeline", [8, 20]),
    22: (35, "uk-tax-diagnostic", "research", [18]),
    23: (36, "panel-retrieval", "panel", [7]),
    24: (37, "panel-debate", "panel", [23]),
    25: (38, "stage2-blend", "panel", [12, 24]),
    26: (39, "panel-preflight", "panel", [24]),
    27: (40, "ui-shell", "ui", [12]),
    28: (41, "ui-writes", "ui", [13, 15, 27]),
    29: (42, "ui-evidence", "ui", [21, 25, 27]),
    30: (43, "perf-alerts", "bot", [12, 21]),
    31: (44, "autonomy-ladder", "pipeline", [13, 15, 21]),
    32: (45, "memory-limits", "infra", [3, 25]),
}

# Operator prerequisites. Each gates the MERGE of the listed tickets, because their live check
# needs it; none gates a BUILD, because workers never receive real secrets.
PREREQS = {
    "server": ("SSH from the workstation to the server works and /mnt/data is writable", [3]),
    "sharadar": ("the Sharadar key (SEP, SF1, TICKERS) is in env/pipeline.env where the live check runs", [4]),
    "alpaca": ("the Alpaca paper key, secret and endpoint are in env/execution.env where the live check runs", [9]),
    "telegram": ("the Telegram bot token and chat id are in env/bot.env on the server", [11]),
    "vpn": ("the ProtonVPN WireGuard private key is in env/vpn.env on the server", [11]),
    "anthropic": ("the Anthropic API key is in env/pipeline.env where the live check runs", [23]),
}
# Live checks that run on or against the server also wait for server delivery to merge.
EXTRA_MERGE_DEPS = {11: [3], 17: [3]}
# Tickets whose live check touches the server: the coordinator runs it with the operator watching.
ATTENDED = {3, 11, 17}

BUILD_SPEC = """BUILD T{t:02d}: {title} (GitHub issue #{issue}, {repo})
Lane: {lane}. Critical-path height: {height}.

Implement exactly this one build ticket.

1. Read the ticket with `gh issue view {issue} --repo {repo}`, then every spec document it links under docs/architecture/, then CLAUDE.md and docs/build/orca-build-plan.md.
2. You are in a fresh worktree created from origin/main. Work on this worktree's branch. Never commit to or push main.
3. Build only this ticket's scope. Meet every acceptance criterion that can be met without real credentials, the deployment server or a live market session, using fakes, fixtures and the committed *.example env files. Where a criterion needs any of those, add an opt-in live check that is skipped by default, and describe how to run it under "Operator live check" in the PR.
4. Never contact the deployment server or use SSH. Never read, create or commit real env files; only env/*.example is committed.
5. Follow the conflict rules in docs/build/orca-build-plan.md: new timestamp-prefixed migrations only, parallel-safe tests (no fixed host ports, no shared Compose project name), and `uv lock` regenerated rather than hand-merged.
6. Run the full test suite with the repo's single test command. It must pass.
7. Commit, push the branch, and open a PR against main titled "T{t:02d}: {title}". The PR body starts with "Closes #{issue}", maps the work to each acceptance criterion, and has an "Operator live check" section (write "None" if there is none). Use your session's own attribution for commits and the PR. Do not merge. Do not edit any issue, and never touch issue #1.
8. If a decision blocks you, ask the coordinator with the lifecycle commands in your dispatch preamble rather than guessing.
9. Send worker_done once. Use outcome succeeded only if the PR is open and the suite passes, and put the PR URL on the first line of the body."""

MERGE_SPEC = """MERGE T{t:02d}: {title} (GitHub issue #{issue})

Coordinator-owned. Never dispatch this task to a worker. It completes only when the PR for #{issue} is merged, so tickets blocked by T{t:02d} build on code that is on main.

1. Review the PR against the ticket's acceptance criteria.{live}
2. Get the operator's explicit approval to merge.
3. If main has moved since the PR was opened, rebase the branch on main and rerun the suite.
4. Merge with `gh pr merge <pr> --merge --delete-branch` and confirm issue #{issue} closed.
5. Run `task-update --id <this task> --status completed --result '{{"pr": <pr>, "merge_sha": "<sha>"}}'`."""

PRE_SPEC = """PRE {key}: operator prerequisite

Coordinator-owned. Never dispatch this task to a worker. Complete it with task-update once the operator confirms {what}. Never paste a secret into Orca, GitHub or chat."""


def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode:
        sys.exit(f"{' '.join(cmd[:3])} failed: {(p.stderr or p.stdout).strip()}")
    return p.stdout


def gh_json(*args):
    return json.loads(run(["gh", *args]))


def orca(*args):
    out = json.loads(run([ORCA, "orchestration", *args, "--json"]))
    return out.get("result", out)


def build_title(t):
    return f"BUILD T{t:02d} #{TICKETS[t][0]}"


def merge_title(t):
    return f"MERGE T{t:02d} #{TICKETS[t][0]}"


def ticket_of(title):
    return int(title.split()[1][1:])


@lru_cache(None)
def height(t):
    """Length of the longest chain of tickets from t to the end of the build, t included."""
    kids = [k for k, v in TICKETS.items() if t in v[3]]
    return 1 + max((height(k) for k in kids), default=0)


def fetch_github():
    """Title, state, assignees and blockers of every ticket, read live from GitHub."""
    ticket_by_issue = {v[0]: t for t, v in TICKETS.items()}
    graph = {}
    for t, (issue, *_rest) in TICKETS.items():
        info = gh_json("api", f"repos/{REPO}/issues/{issue}")
        blockers = gh_json("api", f"repos/{REPO}/issues/{issue}/dependencies/blocked_by")
        unknown = [b["number"] for b in blockers if b["number"] not in ticket_by_issue]
        if unknown:
            sys.exit(f"#{issue} is blocked by issues outside the build: {unknown}")
        graph[t] = {
            "title": info["title"],
            "state": info["state"],
            "assignees": [a["login"] for a in info["assignees"]],
            "blocked_by": sorted(ticket_by_issue[b["number"]] for b in blockers),
            "open_blockers": sorted(ticket_by_issue[b["number"]] for b in blockers if b["state"] == "open"),
        }
    drift = [t for t in TICKETS if graph[t]["blocked_by"] != sorted(TICKETS[t][3])]
    if drift:
        sys.exit("GitHub blocked_by differs from TICKETS for " + ", ".join(f"T{t:02d}" for t in drift))
    return graph


def task_id(res):
    """The created task's id, searched for because Orca receipts nest it."""
    if isinstance(res, dict):
        if str(res.get("id", "")).startswith("task_"):
            return res["id"]
        for v in res.values():
            found = task_id(v)
            if found:
                return found
    return None


def seed(args):
    if args.apply and not args.run:
        sys.exit("--apply needs --run")
    graph = fetch_github()
    ids = {t["task_title"]: t["id"] for t in orca("task-list", "--run", args.run)["tasks"]} if args.run else {}
    prereqs_for = {}
    for key, (_, tickets) in PREREQS.items():
        for t in tickets:
            prereqs_for.setdefault(t, []).append(f"PRE {key}")

    def create(title, spec, deps, display, done=False):
        if title in ids:
            print(f"exists        {title}")
            return
        cmd = ["task-create", "--run", args.run, "--task-title", title, "--display-name", display, "--spec", spec]
        if deps:
            cmd += ["--deps", json.dumps([ids[d] for d in deps])]
        if not args.apply:
            ids[title] = f"<{title}>"
            print(f"would create  {title:<18} deps={deps}{'  (already merged)' if done else ''}")
            return
        new_id = task_id(orca(*cmd))
        if not new_id:
            sys.exit(f"could not find the task id in the task-create receipt for {title}")
        ids[title] = new_id
        if done:
            orca("task-update", "--run", args.run, "--id", new_id, "--status", "completed",
                 "--result", json.dumps({"provenance": "issue closed before seeding"}))
        print(f"created       {title:<18} {new_id}{'  (completed: already merged)' if done else ''}")

    for key, (what, _) in PREREQS.items():
        create(f"PRE {key}", PRE_SPEC.format(key=key, what=what), [], f"pre-{key}")
    for t, (issue, slug, lane, blockers) in TICKETS.items():
        g = graph[t]
        done = g["state"] == "closed"
        live = ""
        if t in prereqs_for or t in EXTRA_MERGE_DEPS:
            live = " Then run the PR's operator live check"
            live += " with the operator watching, since it touches the server." if t in ATTENDED else "."
        create(build_title(t),
               BUILD_SPEC.format(t=t, title=g["title"], issue=issue, repo=REPO, lane=lane, height=height(t)),
               [merge_title(b) for b in blockers], f"t{t:02d}-{slug}", done)
        create(merge_title(t),
               MERGE_SPEC.format(t=t, title=g["title"], issue=issue, live=live),
               [build_title(t)] + prereqs_for.get(t, []) + [merge_title(b) for b in EXTRA_MERGE_DEPS.get(t, [])],
               f"merge-t{t:02d}", done)
    if not args.apply:
        print("\nDry run. Re-run with --apply to create the tasks.")


def next_(args):
    ready = orca("task-list", "--run", args.run, "--ready")["tasks"]
    active = [t for t in orca("task-list", "--run", args.run, "--status", "dispatched")["tasks"]
              if t["task_title"].startswith("BUILD")]
    busy = {TICKETS[ticket_of(t["task_title"])][2] for t in active}
    print(f"Active BUILD workers: {len(active)}/{args.slots}  "
          + " ".join(f"{t['task_title']} [{TICKETS[ticket_of(t['task_title'])][2]}]" for t in active))

    builds = [t for t in ready if t["task_title"].startswith("BUILD")]
    ranked = []
    while builds:
        # Critical path first; among equals, prefer a lane no worker is in yet.
        best = min(builds, key=lambda t: (-height(ticket_of(t["task_title"])),
                                          TICKETS[ticket_of(t["task_title"])][2] in busy,
                                          ticket_of(t["task_title"])))
        builds.remove(best)
        ranked.append(best)
        busy.add(TICKETS[ticket_of(best["task_title"])][2])
    slots = max(args.slots - len(active), 0)
    print(f"\nStart now ({min(slots, len(ranked))} of {slots} free slots):")
    for i, t in enumerate(ranked):
        n = ticket_of(t["task_title"])
        if i == slots:
            print("\nReady, waiting for a slot:")
        flag = "  ATTENDED live check" if n in ATTENDED else ""
        print(f"  {t['task_title']:<16} {t['id']}  height {height(n)}  lane {TICKETS[n][2]}{flag}")
        if i < slots:
            print("    " + WORKER_START.format(orca=ORCA, id=t["id"], name=f"t{n:02d}-{TICKETS[n][1]}"))

    merges = sorted((t for t in ready if t["task_title"].startswith("MERGE")),
                    key=lambda t: (-height(ticket_of(t["task_title"])), ticket_of(t["task_title"])))
    print("\nMerge queue, one at a time, highest first:" if merges else "\nMerge queue: empty")
    for t in merges:
        print(f"  {t['task_title']:<16} {t['id']}")
    pres = [t for t in ready if t["task_title"].startswith("PRE")]
    if pres:
        print("\nOperator prerequisites still open:")
        for t in pres:
            print(f"  {t['task_title']:<16} {t['id']}")


def frontier(_args):
    graph = fetch_github()
    prs = {}
    for pr in gh_json("pr", "list", "--repo", REPO, "--state", "open", "--json", "number,body"):
        for issue in re.findall(r"(?i)\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?) #(\d+)", pr["body"]):
            prs[int(issue)] = pr["number"]
    closed = sum(g["state"] == "closed" for g in graph.values())
    print(f"{closed}/{len(graph)} tickets merged. Open tickets with every blocker closed:")
    rows = [t for t, g in graph.items() if g["state"] == "open" and not g["open_blockers"]]
    for t in sorted(rows, key=lambda t: (-height(t), t)):
        g = graph[t]
        claim = ", ".join(g["assignees"]) or "unclaimed"
        if TICKETS[t][0] in prs:
            claim += f" PR #{prs[TICKETS[t][0]]}"
        print(f"  T{t:02d} #{TICKETS[t][0]:<3} height {height(t):<2} {TICKETS[t][2]:<9} {claim:<10} {g['title']}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--run")
    s.add_argument("--apply", action="store_true")
    s.set_defaults(fn=seed)
    n = sub.add_parser("next")
    n.add_argument("--run", required=True)
    n.add_argument("--slots", type=int, default=3)
    n.set_defaults(fn=next_)
    sub.add_parser("frontier").set_defaults(fn=frontier)
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
