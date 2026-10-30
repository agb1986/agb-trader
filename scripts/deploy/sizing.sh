#!/usr/bin/env bash
# The sizing run T32 asks for: sample every container's memory on the server while a real
# rebalance runs, so the limits in compose.yaml are measured rather than guessed. See
# docs/build/measured-limits.md, which this wraps.
#
#   make sizing CMD=check     the stack can produce a measurement, and when the next one is due
#   make sizing CMD=start     ship the sampler to the server and leave it sampling
#   make sizing CMD=watch     is it still sampling, what is the pipeline doing, the table so far
#   make sizing CMD=stop      stop it and copy its table here
#
# AGB_DEPLOY_HOST (default casaos.local) and AGB_DEPLOY_DIR (default
# /mnt/data/AppData/agb-trader) say where, as they do for deploy.sh. AGB_SIZING_INTERVAL
# (default 5) is the seconds between readings.
#
# The sampler keeps the highest reading per service, so it has to outlive the heaviest stage
# of the run: it is started detached on the server, under its own session, and a dropped SSH
# connection does not take it with it.
set -euo pipefail

host=${AGB_DEPLOY_HOST:-casaos.local}
dir=${AGB_DEPLOY_DIR:-/mnt/data/AppData/agb-trader}
interval=${AGB_SIZING_INTERVAL:-5}
# The compose project the sampler watches. agb-stack runs compose with --project-directory,
# so the project is that directory's name, and the household stack's containers are not it.
project=$(basename "$dir")
remote_script="$dir/measure.py"
# The pattern `pgrep` and `pkill` are given below travels to the server inside the command line
# of the shell that runs them, and that shell is a process like any other, so a plain path
# matches it: `pgrep` then always finds a sampler, `start` always refuses to start one, and
# `pkill` aims at the shell doing the asking. Bracketing the first character separates the two.
# As a regex it still matches the sampler's own path; as text in that shell's command line it
# reads `[m]easure.py`, which the regex does not match.
sampler_pattern="$dir/[m]easure.py"
remote_table="$dir/peaks.md"
remote_log="$dir/measure.log"

die() {
    echo "sizing: $*" >&2
    exit 1
}
remote() { ssh -o BatchMode=yes -- "$host" "$@"; }
q() { printf '%q' "$1"; }
# One psql query against the server's database, as a single unaligned line per row.
query() {
    remote "$(q "$dir/agb-stack") exec -T db psql -U postgres -d agb -tA -c $(q "$1")"
}
# The sampler's process on the server, if it is running.
sampler() { remote "pgrep -f $(q "$sampler_pattern") || true"; }

cd "$(git rev-parse --show-toplevel)"

check() {
    echo "checking $host:$dir"
    remote "test -d $(q "$dir")" || die "$host:$dir does not exist"
    remote "test -x $(q "$dir/agb-stack")" ||
        die "nothing is deployed in $host:$dir yet; run make deploy first"

    local up
    up=$(remote "$(q "$dir/agb-stack") ps --services --filter status=running" | sort | tr '\n' ' ')
    [ -n "${up// /}" ] || die "nothing is running in $host:$dir; see \`agb-stack logs\`"
    echo "running: ${up% }"
    case " $up " in
    *" pipeline "*) ;;
    *) die "the pipeline is not running, so no rebalance will be claimed" ;;
    esac

    # A rebalance with no promoted model fails before it proposes anything, and one with no
    # panel configuration runs stage 1 only, which leaves the heaviest stage unmeasured.
    local model configs lake
    model=$(query "SELECT model_id FROM agb.promoted_model")
    [ -n "$model" ] || die "no model is promoted, so a rebalance fails before it proposes a basket"
    echo "promoted model: $model"
    configs=$(remote "$(q "$dir/agb-stack") agb panel-config list" | grep -c . || true)
    [ "$configs" -gt 0 ] ||
        die "no panel_config, so the rebalance is stage-1 only and the panel is never measured"
    echo "panel configurations: $configs"
    lake=$(query "SELECT max(snapshot_date) FROM agb.lake_snapshot")
    echo "lake has data through: ${lake:-nothing}"

    echo "the sampler reads this server's docker:"
    remote "python3 - --project $(q "$project") --samples 2 --interval 1" < src/agb/measure.py ||
        die "the sampler could not read docker on $host"

    # The window the measurement has to happen in. Pure planning, so the workstation can say.
    uv run python - <<'PY' || echo "sizing: could not work out when the next rebalance is due" >&2
from datetime import date

from agb import schedule
from agb.nyse import NyseCalendar

today = date.today()
window = NyseCalendar(*schedule.calendar_window(today))
for planned in schedule.plan(window, today):
    if planned.kind == schedule.REBALANCE:
        print(
            f"next rebalance: {planned.period}, claimable from {planned.not_before:%Y-%m-%d %H:%M %Z}"
            f", filling at the close of {planned.session_date}"
        )
        break
PY
}

start() {
    local running
    running=$(sampler)
    [ -z "$running" ] || die "the sampler is already running on $host as pid ${running%%$'\n'*}; \`stop\` it first"
    remote "test -d $(q "$dir")" || die "$host:$dir does not exist"

    # Nothing outside the standard library, so the server needs no project environment.
    remote "cat > $(q "$remote_script")" < src/agb/measure.py
    # Its own session, so the sampler outlives this SSH connection and the terminal that ran it.
    #
    # The `exec` comes first because the redirections below are the sampler's, not the remote
    # shell's. That shell stays alive after it backgrounds the sampler, still holding the two
    # pipes ssh reads the command's output from, and ssh waits for them to close however long
    # that takes: `start` left the sampler running and then hung for good. Pointing the shell's
    # own output at /dev/null closes them, and ssh returns as soon as the command is launched.
    remote "exec > /dev/null 2>&1; cd $(q "$dir") && setsid nohup python3 $(q "$remote_script") \
        --project $(q "$project") --interval $(q "$interval") --out $(q "$remote_table") \
        >> $(q "$remote_log") 2>&1 < /dev/null &" || die "could not start the sampler on $host"
    sleep 2
    running=$(sampler)
    [ -n "$running" ] || die "the sampler did not stay up; see $host:$remote_log"
    echo "sampling $project on $host every ${interval}s as pid ${running%%$'\n'*}"
    echo "it writes $remote_table on every reading; \`watch\` shows it, \`stop\` ends the run"
}

watch() {
    local running
    running=$(sampler)
    if [ -n "$running" ]; then
        echo "sampling on $host as pid ${running%%$'\n'*}"
    else
        echo "the sampler is not running on $host" >&2
    fi

    # A container the kernel killed for exceeding its limit is the one thing that invalidates
    # the run, so it is worth seeing while there is still time to re-measure.
    local killed
    killed=$(remote "$(q "$dir/agb-stack") ps --all --format '{{.Service}} {{.Status}}'" |
        grep -i 'exited (137)' || true)
    [ -z "$killed" ] || echo "killed for memory during this run: $killed" >&2

    echo "--- pipeline, last 10 lines"
    remote "$(q "$dir/agb-stack") logs --tail 10 pipeline" || true
    echo "--- the table so far"
    remote "cat $(q "$remote_table") 2>/dev/null" || echo "no readings written yet"
}

stop() {
    local into=${1:-peaks.md} running
    running=$(sampler)
    if [ -n "$running" ]; then
        # The sampler writes its table and exits on the first SIGTERM.
        remote "pkill -TERM -f $(q "$sampler_pattern")"
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            [ -n "$(sampler)" ] || break
            sleep 1
        done
        [ -z "$(sampler)" ] || die "the sampler on $host did not stop; kill pid ${running%%$'\n'*} by hand"
        echo "stopped the sampler on $host"
    else
        echo "the sampler was not running; copying the table it left"
    fi
    remote "test -s $(q "$remote_table")" || die "$host:$remote_table is empty: no readings to copy"
    scp -q -- "$host:$remote_table" "$into"
    echo "wrote $into"
    cat "$into"
    echo "set each mem_limit in compose.yaml from the last column, then deploy and run a second"
    echo "rebalance under the new limits; docs/build/measured-limits.md has the arithmetic"
}

case ${1:-} in
check) check ;;
start) start ;;
watch) watch ;;
stop)
    shift
    [ $# -le 1 ] || die "usage: sizing.sh stop [<file>]"
    stop "$@"
    ;;
*)
    cat >&2 <<'EOF'
usage: sizing.sh <command>

  check          the stack can produce a measurement, and when the next rebalance is due
  start          ship the sampler to the server and leave it sampling, detached
  watch          whether it is still sampling, what the pipeline is doing, the table so far
  stop [<file>]  stop it and copy its table here (default peaks.md)
EOF
    exit 2
    ;;
esac
