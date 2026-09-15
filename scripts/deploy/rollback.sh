#!/usr/bin/env bash
# Restart the server's stack on the release before the current one. Running it again returns
# to the release it rolled back from. See docs/build/deploy.md.
#
#   make rollback          or: scripts/deploy/rollback.sh
set -euo pipefail

host=${AGB_DEPLOY_HOST:-casaos.local}
dir=${AGB_DEPLOY_DIR:-/mnt/data/AppData/agb-trader}
stack=$(printf '%q' "$dir/agb-stack")

ssh -o BatchMode=yes -- "$host" "$stack rollback && $stack status"
