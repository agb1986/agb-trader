#!/usr/bin/env bash
# Build the app image for this commit on the workstation, ship it to the server over SSH with
# no registry, and restart the server's stack on it. See docs/build/deploy.md.
#
#   make deploy          or: scripts/deploy/deploy.sh
#
# AGB_DEPLOY_HOST (default casaos.local) and AGB_DEPLOY_DIR (default
# /mnt/data/AppData/agb-trader) say where to. The server keeps the release this one
# replaces, and `make rollback` returns to it.
set -euo pipefail

host=${AGB_DEPLOY_HOST:-casaos.local}
dir=${AGB_DEPLOY_DIR:-/mnt/data/AppData/agb-trader}
image=agb-trader-app
# The env files the server's stack reads. Each must set every variable its example does.
server_env=(db pipeline execution bot vpn)

die() {
    echo "deploy: $*" >&2
    exit 1
}
remote() { ssh -o BatchMode=yes -- "$host" "$@"; }
q() { printf '%q' "$1"; }
# The variable names an env file sets.
names_expr='s/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p'

cd "$(git rev-parse --show-toplevel)"
git diff --quiet HEAD -- ||
    die "uncommitted changes: the image is built from the commit, so commit or stash them first"
tag=$(git rev-parse --short=12 HEAD)
rel="$dir/releases/$tag"

echo "checking $host:$dir"
remote "test -d $(q "$dir")" || die "$host:$dir does not exist"
for name in "${server_env[@]}"; do
    file="$dir/env/$name.env"
    remote "test -f $(q "$file")" || die "$host:$file does not exist; create it from env/$name.env.example"
    # The names are read on the server, so the values never leave it.
    missing=$(comm -23 <(git show "HEAD:env/$name.env.example" | sed -n "$names_expr" | sort -u) \
        <(remote "sed -n $(q "$names_expr") $(q "$file")" | sort -u))
    [ -z "$missing" ] || die "$host:$file does not set: $(echo $missing)"
done

echo "building $image:$tag"
# From the commit itself, not the working tree.
git archive --format=tar HEAD |
    docker build --platform linux/amd64 --target app --build-arg "AGB_VERSION=$tag" \
        --tag "$image:$tag" -

if remote "docker image inspect $(q "$image:$tag") >/dev/null 2>&1"; then
    echo "$host already has $image:$tag"
else
    echo "shipping $image:$tag to $host"
    docker save "$image:$tag" | gzip -1 | remote "gunzip | docker load"
fi

if ! remote "test -d $(q "$rel")"; then
    echo "installing release $tag"
    remote "rm -rf $(q "$rel.partial") && mkdir -p $(q "$rel.partial")"
    git show HEAD:compose.yaml | remote "cat > $(q "$rel.partial/compose.yaml")"
    git show HEAD:scripts/deploy/agb-stack |
        remote "cat > $(q "$rel.partial/agb-stack") && chmod +x $(q "$rel.partial/agb-stack")"
    remote "mv $(q "$rel.partial") $(q "$rel")"
fi

remote "$(q "$rel/agb-stack") activate $(q "$tag")"
remote "$(q "$dir/agb-stack") status"
echo "deployed $tag to $host"
