#!/usr/bin/env bash
# The deliberate-corruption check T32 asks for: break one panel body on the server, let the
# nightly job find it, confirm the alert, put the body back. See docs/build/measured-limits.md,
# which this wraps.
#
#   make panel-check CMD=check       the store has bodies and the sample will read the one broken
#   make panel-check CMD=corrupt     back one body up, then overwrite it
#   make panel-check CMD=run         insert a drill audit job and wait for what it finds
#   make panel-check CMD=restore     put the body back from the backup
#   make panel-check CMD=status      what is broken right now, and the last audits
#
# The database is reached as the `cli` role, never as the `postgres` superuser. Reads run in a
# read-only session; the one write, `run`'s audit job, goes through agb.request_panel_audit,
# which `cli` may execute and which enqueues a panel_audit and nothing else. `run`'s period is
# checked against the pattern agb.panel_audit admits before anything is sent, and reaches SQL
# only as a psql variable, never as text in a statement.
#
# AGB_DEPLOY_HOST (default casaos.local) and AGB_DEPLOY_DIR (default
# /mnt/data/AppData/agb-trader) say where, as they do for deploy.sh. AGB_PANEL_PATH is the
# store's path on the server when it is not <dir>/panel.
#
# `corrupt` keeps its backup on the server, beside the store, and refuses to break a second
# body while one is still broken, so there is always exactly one body to put back and the
# `restore` is a copy rather than a reconstruction.
set -euo pipefail

host=${AGB_DEPLOY_HOST:-casaos.local}
dir=${AGB_DEPLOY_DIR:-/mnt/data/AppData/agb-trader}
store=${AGB_PANEL_PATH:-$dir/panel}
# Where the backup and the note of what was broken live. Not under the store: the audit reads
# everything the store holds, and a backup in there is a body nothing cites.
keep_dir="$dir/panel-check"
marker="$keep_dir/broken"
# The pipeline polls every AGB_HEARTBEAT_SECONDS, 30 by default, so a job inserted now is
# claimed within a minute. Five minutes is room for the claim, the read and the record.
wait_seconds=300
# The default period of `run`: the day, then the time, so each drill is its own period.
drill_format=%Y-%m-%d-drill-%H%M%S
# What agb.panel_audit's period check admits: a day, then an optional lowercase suffix. A bare
# day is the nightly job's; the suffix is a drill's. bash's =~ anchors at the end of the string,
# where a `$` in a Python or Postgres pattern also accepts a trailing newline.
period_pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}(-[a-z0-9][a-z0-9-]*)?$'
# The nightly cap on bodies read (panel_audit.SAMPLE), when the server does not ask for another.
default_sample=100000

die() {
    echo "panel-check: $*" >&2
    exit 1
}
remote() { ssh -o BatchMode=yes -- "$host" "$@"; }
q() { printf '%q' "$1"; }
# Whether $1 is a period panel_audit takes: the right shape, and a day that exists.
valid_period() {
    [[ $1 =~ $period_pattern ]] && [ "$(date -d "${1:0:10}" +%F 2>/dev/null)" = "${1:0:10}" ]
}

# One statement against the server's database as `cli`, run by psql on the server.
#
#   db <read|write> <psql flags> <statement> [name=value ...]
#
# The statement goes in on stdin, because psql substitutes its variables there and not in a
# -c string; each name=value becomes -v, which the statement reads as :'name', a quoted
# literal. `read` opens the session read-only, so a read cannot write whatever it is sent.
db() {
    local mode=$1 flags=$2 statement=$3 session="" vars="" pair
    shift 3
    if [ "$mode" = read ]; then
        session="-e $(q 'PGOPTIONS=-c default_transaction_read_only=on')"
    fi
    for pair in "$@"; do vars="$vars -v $(q "$pair")"; done
    printf '%s\n' "$statement" |
        remote "$(q "$dir/agb-stack") exec -T $session db psql -U cli -d agb $flags -v ON_ERROR_STOP=1$vars -f -"
}
# A read, as a single unaligned line per row.
query() { db read -tA "$@"; }
# The one write: the statement, as a single unaligned line per row.
write() { db write -tA "$@"; }

deployed() {
    remote "test -d $(q "$dir")" || die "$host:$dir does not exist"
    remote "test -x $(q "$dir/agb-stack")" ||
        die "nothing is deployed in $host:$dir yet; run make deploy first"
}

# Every hash the panel tables cite, as agb.panel_audit samples them.
referenced="
    WITH referenced AS (
        SELECT document_sha256 AS sha256 FROM agb.panel_filing
        UNION SELECT text_sha256 FROM agb.panel_filing
        UNION SELECT request_sha256 FROM agb.panel_model_call
        UNION SELECT response_sha256 FROM agb.panel_model_call
        UNION SELECT extract_sha256 FROM agb.panel_extract WHERE extract_sha256 IS NOT NULL
    )"

bodies() { query "$referenced SELECT count(*) FROM referenced"; }

# What the pipeline's own environment asks for, which is what the nightly job will use.
sample() {
    local set_to
    set_to=$(remote "grep -h '^AGB_PANEL_AUDIT_SAMPLE=' $(q "$dir/env/pipeline.env") 2>/dev/null |
        tail -1 | cut -d= -f2" || true)
    echo "${set_to:-$default_sample}"
}

# Whether the nightly sample is certain to read every body, and so certain to read the broken
# one. Once the store outgrows the cap it is a lottery of sample/bodies a night, which is no test.
certainty() {
    local bodies=$1 sample=$2
    if [ "$bodies" -le "$sample" ]; then
        echo "the cap of $sample covers all $bodies bodies, so the next audit reads the broken one"
        return 0
    fi
    cat >&2 <<EOF
panel-check: the store holds $bodies bodies and the cap is $sample, so one audit reads the
broken body with probability $sample/$bodies. For a check that is certain, raise the cap on
the server and restart the pipeline:

  ssh $host "echo AGB_PANEL_AUDIT_SAMPLE=$bodies >> $(q "$dir/env/pipeline.env")"
  ssh $host "$(q "$dir/agb-stack") up -d pipeline"

and remove that line when the check is done, so the nightly job goes back to the default
cap of $default_sample, which keeps it inside its grace period.
EOF
    return 1
}

latest_audits() {
    echo "--- the last 3 audits"
    db read "" "SELECT period, bodies, checked, sound, missing, corrupt
              FROM agb.panel_audit ORDER BY id DESC LIMIT 3" || true
}

check() {
    echo "checking $host:$dir"
    deployed

    local up
    up=$(remote "$(q "$dir/agb-stack") ps --services --filter status=running" | sort | tr '\n' ' ')
    case " $up " in
    *" pipeline "*) ;;
    *) die "the pipeline is not running, so no audit job will be claimed" ;;
    esac
    case " $up " in
    *" bot "*) ;;
    *) echo "panel-check: the bot is not running, so nothing will send the alert" >&2 ;;
    esac

    remote "test -d $(q "$store")" || die "$host:$store does not exist; is AGB_PANEL_PATH right?"
    local bodies sample
    bodies=$(bodies)
    [ "${bodies:-0}" -gt 0 ] ||
        die "the panel tables cite no bodies yet; this needs a rebalance that ran the panel"
    sample=$(sample)
    echo "the store holds $bodies cited bodies; the nightly cap is $sample"
    certainty "$bodies" "$sample" || true
    status
}

corrupt() {
    deployed
    ! remote "test -f $(q "$marker")" ||
        die "a body is already broken on $host; \`restore\` it before breaking another"

    local bodies sample
    bodies=$(bodies)
    [ "${bodies:-0}" -gt 0 ] ||
        die "the panel tables cite no bodies yet; this needs a rebalance that ran the panel"
    sample=$(sample)
    certainty "$bodies" "$sample" ||
        die "refusing to break a body the next audit probably will not read"

    # Any cited body will do; the first one keeps the choice reproducible.
    local sha path
    sha=$(query "$referenced SELECT sha256 FROM referenced ORDER BY sha256 LIMIT 1")
    [ -n "$sha" ] || die "could not read a cited hash from the database"
    path="$store/${sha:0:2}/$sha.zst"
    remote "test -f $(q "$path")" ||
        die "$host:$path does not exist, so the store is already missing that body; audit it as it is"

    # The backup first, and verified, before anything is overwritten: this is the only copy of
    # a body the database cannot rebuild.
    remote "mkdir -p $(q "$keep_dir") && cp -p $(q "$path") $(q "$keep_dir/$sha.zst.keep")"
    local before after
    before=$(remote "sha256sum < $(q "$path")" | cut -d' ' -f1)
    after=$(remote "sha256sum < $(q "$keep_dir/$sha.zst.keep")" | cut -d' ' -f1)
    if [ -z "$before" ] || [ "$before" != "$after" ]; then
        die "the backup of $sha does not match the body; nothing was overwritten"
    fi
    remote "printf '%s\n' $(q "$sha $before") > $(q "$marker")"

    remote "printf 'not a zstd frame' > $(q "$path")"
    echo "broke $sha: $path now holds bytes that do not decompress"
    echo "backed up as $host:$keep_dir/$sha.zst.keep; \`restore\` puts it back"
    echo "\`run\` inserts the audit job now, or wait for 04:00 New York"
}

run() {
    local period=${1:-}
    # Refused here, before anything reaches the server, and again where the SQL is.
    if [ -n "$period" ]; then
        valid_period "$period" ||
            die "'$period' is not a period; use a day with an optional suffix of lowercase letters, digits and hyphens, e.g. 2026-11-15-again"
    fi
    deployed
    if [ -z "$period" ]; then
        # A date plus a suffix, which the scheduler never plans. It plans a panel_audit for
        # every night 45 days ahead, so every bare date is taken ((kind, period) is unique),
        # tomorrow's included. The audit's own check admits the suffix.
        period=$(remote "date -u +$drill_format")
        valid_period "$period" || die "the server's clock gave '$period', which is not a period"
    fi
    case $period in
    [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[a-z0-9]*) ;;
    *) echo "panel-check: $period is a bare date, which the scheduler has probably planned" >&2 ;;
    esac
    local taken
    taken=$(query "SELECT state FROM agb.job_state
                   WHERE kind = 'panel_audit' AND period = :'period'" "period=$period")
    [ -z "$taken" ] ||
        die "panel_audit $period already exists on $host and is $taken; pass a period that has none"

    remote "test -f $(q "$marker")" ||
        echo "panel-check: no body is broken, so this audit should come back sound" >&2

    write "SELECT agb.request_panel_audit(:'period')" "period=$period" > /dev/null
    echo "inserted panel_audit $period; waiting up to ${wait_seconds}s for the pipeline"

    local waited=0 row=""
    while [ "$waited" -lt "$wait_seconds" ]; do
        row=$(query "SELECT checked, sound, coalesce(array_length(missing, 1), 0),
                            coalesce(array_length(corrupt, 1), 0)
                     FROM agb.panel_audit WHERE period = :'period'" "period=$period")
        [ -z "$row" ] || break
        sleep 10
        waited=$((waited + 10))
    done
    if [ -z "$row" ]; then
        echo "--- the job, as it stands, and everything that happened to it"
        query "SELECT s.state, e.event, e.actor, e.at, coalesce(e.detail, '')
               FROM agb.job_state s JOIN agb.job_event e ON e.job_id = s.id
               WHERE s.kind = 'panel_audit' AND s.period = :'period' ORDER BY e.id" "period=$period"
        die "no audit was recorded for $period within ${wait_seconds}s; see \`agb-stack logs pipeline\`"
    fi
    echo "audit $period recorded: checked/sound/missing/corrupt = ${row//|/ }"
    latest_audits

    # The row is the finding; the alert is what the criterion asks for, and the bot sends it
    # once on its next cycle.
    echo "--- bot, the lines naming the store"
    remote "$(q "$dir/agb-stack") logs --tail 200 bot" | grep -i 'panel store' ||
        echo "nothing yet; the bot alerts on its next poll, within a minute"
}

restore() {
    deployed
    remote "test -f $(q "$marker")" || die "nothing is recorded as broken on $host"
    local note sha want path
    note=$(remote "cat $(q "$marker")")
    sha=${note%% *}
    want=${note##* }
    path="$store/${sha:0:2}/$sha.zst"
    remote "test -f $(q "$keep_dir/$sha.zst.keep")" ||
        die "the backup $host:$keep_dir/$sha.zst.keep is gone; the body cannot be put back"

    remote "cp -p $(q "$keep_dir/$sha.zst.keep") $(q "$path")"
    local now
    now=$(remote "sha256sum < $(q "$path")" | cut -d' ' -f1)
    [ "$now" = "$want" ] ||
        die "$path reads back as $now, not the $want it was; leaving the backup in place"
    remote "rm -f $(q "$marker") $(q "$keep_dir/$sha.zst.keep")"
    echo "put $sha back, byte for byte"
    echo "run one more audit on a fresh period to see the store sound again"
}

status() {
    deployed
    if remote "test -f $(q "$marker")"; then
        echo "broken now: $(remote "cat $(q "$marker")" | cut -d' ' -f1)"
    else
        echo "no body is broken"
    fi
    latest_audits
}

case ${1:-} in
check) check ;;
corrupt) corrupt ;;
run)
    shift
    [ $# -le 1 ] || die "usage: panel-check.sh run [<period>]"
    run "$@"
    ;;
restore) restore ;;
status) status ;;
*)
    cat >&2 <<'EOF'
usage: panel-check.sh <command>

  check             the store has cited bodies and the sample will read the one broken
  corrupt           back one cited body up on the server, then overwrite it
  run [<period>]    insert the audit job now (default <today>-drill-<time>) and wait for what it finds
  restore           put the body back from the backup and verify it byte for byte
  status            whether a body is broken, and the last three audits
EOF
    exit 2
    ;;
esac
