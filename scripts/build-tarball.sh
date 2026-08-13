#!/usr/bin/env bash
# Builds a self-contained publish of the Jepsen harness and packages it as the
# tarball that kommander.db uploads to each Jepsen node.
#
# The harness references Kommander by *project*, not by NuGet package, so what
# lands on the nodes is the code in your working tree — which is the entire
# point of running these tests.
#
# The tarball's root must contain:
#   KommanderJepsen.Harness   (executable)
#   *.so / runtime deps
#
# Usage:
#   scripts/build-tarball.sh [/path/to/kommander] [rid]
#
# `rid` must match the ARCHITECTURE OF THE JEPSEN NODE CONTAINERS, not your
# laptop: on Apple Silicon with default (arm64) containers use linux-arm64; if
# you run the nodes under amd64 emulation, or on a CI runner, use linux-x64.

set -euo pipefail

KOMMANDER_SRC="${1:-$HOME/kommander}"
RID="${2:-linux-arm64}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$ROOT/target"
STAGE="$(mktemp -d)"

if [ ! -f "$KOMMANDER_SRC/Kommander/Kommander.csproj" ]; then
  echo "error: $KOMMANDER_SRC does not look like the Kommander repository" >&2
  exit 1
fi

echo ">> publishing the harness against $KOMMANDER_SRC for $RID"
dotnet publish "$ROOT/harness/KommanderJepsen.Harness.csproj" \
  --configuration Release \
  --runtime "$RID" \
  --self-contained true \
  -p:KommanderPath="$KOMMANDER_SRC" \
  -o "$STAGE"

mkdir -p "$OUT_DIR"
tar -czf "$OUT_DIR/kommander-harness.tar.gz" -C "$STAGE" .
rm -rf "$STAGE"

echo ">> wrote $OUT_DIR/kommander-harness.tar.gz"
