#!/usr/bin/env bash
# The handoff T21 leaves to the operator: carry a certification from the workstation to the
# server, record it there, and make the model live. See docs/build/deploy.md, which this wraps.
#
#   make promote MODEL=lgbm-2026-10            ship, import and promote, skipping done steps
#   make promote MODEL=lgbm-2026-10 CMD=check  what the artifact and the server hold; no writes
#
# AGB_DEPLOY_HOST (default casaos.local) and AGB_DEPLOY_DIR (default
# /mnt/data/AppData/agb-trader) say where, as they do for deploy.sh. AGB_RESEARCH_DIR (default
# research) is where `certify` wrote the artifact.
#
# Both writes happen on the server, and neither can be done from the workstation's Makefile.
# `make research CMD="import ..."` cannot do the first: the research service runs with
# network_mode: none and reaches no database. `make agb CMD="promote ..."` cannot do the
# second: the cli service it runs talks to the workstation's own database, where the
# certification was never imported, so it refuses the model as uncertified.
#
# Each step checks whether it has already happened, because none of them can be repeated: a
# certification is never replaced, and a model that is already live is refused. So the whole
# run is safe to repeat after a failure part way through.
set -euo pipefail

host=${AGB_DEPLOY_HOST:-casaos.local}
dir=${AGB_DEPLOY_DIR:-/mnt/data/AppData/agb-trader}
research=${AGB_RESEARCH_DIR:-research}
# What compose.yaml mounts into the certification-import and cli services as /models.
models="$dir/models"

die() {
    echo "promote: $*" >&2
    exit 1
}
remote() { ssh -o BatchMode=yes -- "$host" "$@"; }
q() { printf '%q' "$1"; }
# One psql query against the server's database, as a single unaligned line per row.
query() {
    remote "$(q "$dir/agb-stack") exec -T db psql -U postgres -d agb -tA -c $(q "$1")"
}

usage() {
    cat >&2 <<'EOF'
usage: promote.sh <model id> [command]

  all    (default) ship the artifact, import its certification, promote the model
  check  what the artifact and the server already hold, without writing anything
EOF
    exit 2
}

cd "$(git rev-parse --show-toplevel)"

model=${1:-}
[ -n "$model" ] || usage
# The id becomes a path component on the server and a SQL literal below, so it is checked
# rather than quoted: certify accepts nothing wider than this either.
case $model in
*[!A-Za-z0-9._-]* | .*) die "'$model' is not a model id; use letters, digits, dot, dash and underscore" ;;
esac
artifact="$research/certifications/$model"

# The certification's own row on the server, and the model that is live there.
certified() { query "SELECT id FROM agb.model_certification WHERE model_id = '$model'"; }
live() { query "SELECT model_id FROM agb.promoted_model"; }

written() {
    [ -d "$artifact" ] ||
        die "$artifact does not exist; certify the model first with" \
            "\`make research CMD=\"certify --model-id $model\"\`"
    [ -f "$artifact/manifest.json" ] ||
        die "$artifact holds no manifest.json, so it is not a certification artifact"
}

deployed() {
    # Separately, so an unreachable host does not read as a missing deploy directory.
    remote true || die "cannot reach $host over ssh"
    remote "test -d $(q "$dir")" || die "$host:$dir does not exist"
    remote "test -x $(q "$dir/agb-stack")" ||
        die "nothing is deployed in $host:$dir yet; run make deploy first"
}

# The manifest's digest stands for the whole artifact: it lists every file's digest, and the
# import verifies each file against it, so two equal manifests mean the same artifact.
manifest_sha256() { sha256sum "$artifact/manifest.json" | cut -d' ' -f1; }

ship() {
    if remote "test -e $(q "$models/$model")"; then
        local there
        there=$(remote "sha256sum $(q "$models/$model/manifest.json") 2>/dev/null | cut -d' ' -f1")
        [ -n "$there" ] ||
            die "$host:$models/$model exists but holds no manifest.json; remove it and rerun"
        [ "$there" = "$(manifest_sha256)" ] ||
            die "$host:$models/$model is a different artifact under the same model id; an" \
                "imported certification is never replaced, so remove that copy by hand if it" \
                "is the stale one"
        echo "already shipped: $models/$model"
        return
    fi
    remote "mkdir -p $(q "$models")"
    # tar rather than scp: it keeps the mode and mtime of each file, and the artifact's
    # digests are over contents the import reads back.
    tar -cz -C "$research/certifications" -- "$model" | remote "tar -xzf - -C $(q "$models")"
    echo "shipped $artifact to $host:$models/$model"
}

record() {
    local id
    id=$(certified)
    if [ -n "$id" ]; then
        echo "already imported: model_certification $id"
        return
    fi
    remote "$(q "$dir/agb-stack") import $(q "$model")"
}

make_live() {
    local was
    was=$(live)
    if [ "$was" = "$model" ]; then
        echo "already live: $model"
        return
    fi
    remote "$(q "$dir/agb-stack") agb promote $(q "$model")"
}

check() {
    written
    echo "artifact: $artifact"
    deployed
    if remote "test -e $(q "$models/$model")"; then
        local there
        there=$(remote "sha256sum $(q "$models/$model/manifest.json") 2>/dev/null | cut -d' ' -f1")
        if [ "$there" = "$(manifest_sha256)" ]; then
            echo "shipped: $host:$models/$model, the same artifact"
        else
            echo "shipped: $host:$models/$model, a DIFFERENT artifact under this model id"
        fi
    else
        echo "not shipped yet: $host:$models/$model"
    fi
    local id voided was
    id=$(certified)
    if [ -n "$id" ]; then
        voided=$(query "SELECT coalesce(voided_at::text, '') FROM agb.model_certification
                        WHERE model_id = '$model'")
        echo "certified: model_certification $id${voided:+, voided $voided}"
    else
        echo "not certified yet on $host"
    fi
    was=$(live)
    echo "live model: ${was:-none}"
}

all() {
    written
    deployed
    ship
    record
    make_live
    echo "live model: $(live)"
}

case ${2:-all} in
all) all ;;
check) check ;;
*) usage ;;
esac
