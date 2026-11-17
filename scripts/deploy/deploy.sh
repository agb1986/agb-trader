#!/usr/bin/env bash
# Build the app image for this commit on the workstation, ship it to the server over SSH with
# no registry, and restart the server's stack on it. See docs/build/deploy.md.
#
#   make deploy          or: scripts/deploy/deploy.sh [--allow-dirty]
#
# It refuses a working tree with uncommitted or untracked files, because the image is built
# from the commit and a deploy that leaves them out is not what was tested. --allow-dirty, or
# AGB_DEPLOY_ALLOW_DIRTY=1 (which `make deploy` passes through), deploys the commit anyway.
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

allow_dirty=${AGB_DEPLOY_ALLOW_DIRTY:-}
for arg in "$@"; do
    case $arg in
    --allow-dirty) allow_dirty=1 ;;
    *) die "usage: deploy.sh [--allow-dirty]" ;;
    esac
done

cd "$(git rev-parse --show-toplevel)"
dirty=$(git status --porcelain)
if [ -n "$dirty" ]; then
    if [ -z "$allow_dirty" ]; then
        die "the working tree is not clean, and the image is built from the commit, not the tree:
$dirty
commit or stash these, or pass --allow-dirty to deploy the commit without them"
    fi
    echo "deploy: the working tree is not clean; deploying the commit without these:" >&2
    echo "$dirty" >&2
fi
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
    [ -z "$missing" ] || die "$host:$file does not set: ${missing//$'\n'/ }"
done

echo "building $image:$tag"
# From the commit itself, not the working tree.
git archive --format=tar HEAD |
    docker build --platform linux/amd64 --target app --build-arg "AGB_VERSION=$tag" \
        --tag "$image:$tag" -

# What identifies the image that was built. A tag only names an image, and the server can hold
# the same tag for a different one (an earlier build of this commit, a partial load), so the
# image is compared, here and again after the load. Not by `.Id`: that is the config digest on
# Docker 20.10 but a manifest digest under the containerd image store (Docker 29), so the same
# image has two ids and a load always looked like a mismatch. The layer diff ids are the digests
# of the uncompressed layers, which both engines report identically for the same image.
layers_format='{{json .RootFS.Layers}}'
# Short and printable, for the messages: the first 12 hex digits of the layers' digest.
short() { printf '%s' "$1" | sha256sum | cut -c1-12; }
# The layers of the server's image under that tag, empty when it has none.
server_layers() { remote "docker image inspect --format '$layers_format' $(q "$image:$tag") 2>/dev/null || true"; }

built=$(docker image inspect --format "$layers_format" "$image:$tag")
case $built in "" | null | "[]") die "could not read the layers of the image just built" ;; esac

have=$(server_layers)
if [ "$have" = "$built" ]; then
    echo "$host already has $image:$tag (layers $(short "$built"))"
else
    if [ -n "$have" ]; then
        echo "$host has $image:$tag with layers $(short "$have"), not the built $(short "$built"); shipping it again"
    else
        echo "shipping $image:$tag to $host"
    fi
    docker save "$image:$tag" | gzip -1 | remote "gunzip | docker load"
    loaded=$(server_layers)
    if [ "$loaded" != "$built" ]; then
        found=none
        [ -z "$loaded" ] || found="layers $(short "$loaded")"
        die "$host loaded $image:$tag as $found, not the built $(short "$built"); nothing was activated"
    fi
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
