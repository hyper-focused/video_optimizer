#!/usr/bin/env bash
# archive.sh — drive video_optimizer through scan/plan/apply for a preset.
#
# Encodes the top-N candidates under SCAN_PATH using the selected preset
# (hd-archive or uhd-archive), leaves new files in their original
# directories (mode=replace), and atomically moves originals into the
# NAS recycle bin preserving source hierarchy.
#
# Usage:  ./archive.sh [--preset hd|uhd] [--limit N] [--path DIR]
#                      [--dry-run] [--skip-scan] [--yes]

set -euo pipefail

# --- Defaults (edit here for site-specific paths) ---------------------------
SCAN_PATH="/mnt/nas/media"
SOURCE_ROOT="/mnt/nas/media"
RECYCLE_TO="/mnt/nas/media/@Recycle"
LIMIT=50
TARGET="av1+mkv"
PRESET="hd"

# --- CLI overrides ----------------------------------------------------------
DRY_RUN=0
SKIP_SCAN=0
YES=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --preset)    PRESET="$2";      shift 2 ;;
        --limit)     LIMIT="$2";       shift 2 ;;
        --path)      SCAN_PATH="$2";   shift 2 ;;
        --dry-run)   DRY_RUN=1;        shift ;;
        --skip-scan) SKIP_SCAN=1;      shift ;;
        --yes|-y)    YES=1;            shift ;;
        -h|--help)
            sed -n '2,10p' "$0" | sed 's/^# //; s/^#//'
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

case "$PRESET" in
    hd|uhd) ;;
    *) echo "error: --preset must be 'hd' or 'uhd' (got: $PRESET)" >&2; exit 2 ;;
esac
PRESET_CMD="${PRESET}-archive"

# --- Pre-flight -------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
TOOL="$SCRIPT_DIR/video_optimizer.py"

[[ -x "$TOOL" ]]      || { echo "error: $TOOL not executable" >&2; exit 1; }
[[ -d "$SCAN_PATH" ]] || { echo "error: scan path $SCAN_PATH not accessible (NAS down?)" >&2; exit 1; }
[[ -d "$RECYCLE_TO" ]] || { echo "error: recycle dir $RECYCLE_TO not found" >&2; exit 1; }
command -v ffmpeg  >/dev/null || { echo "error: ffmpeg not in PATH" >&2; exit 1; }
command -v ffprobe >/dev/null || { echo "error: ffprobe not in PATH" >&2; exit 1; }

cat <<EOF
==> archive.sh
    preset:        $PRESET_CMD
    scan path:     $SCAN_PATH
    source root:   $SOURCE_ROOT
    recycle to:    $RECYCLE_TO
    target codec:  $TARGET
    limit:         $LIMIT
    dry-run:       $DRY_RUN
    tool:          $TOOL

EOF

# --- [1/3] scan -------------------------------------------------------------
if (( SKIP_SCAN )); then
    echo "==> [1/3] scan: skipped (--skip-scan)"
else
    echo "==> [1/3] scan: probing $SCAN_PATH (cache hits skip ffprobe)..."
    "$TOOL" scan "$SCAN_PATH"
fi
echo

# --- [2/3] plan -------------------------------------------------------------
echo "==> [2/3] plan: evaluating rules against probe cache..."
"$TOOL" plan --target "$TARGET" >/dev/null
PENDING=$("$TOOL" status | awk -F': ' '/^pending decisions:/{print $2; exit}')
echo "    pending decisions in queue: ${PENDING:-0}"
if [[ "${PENDING:-0}" -eq 0 ]]; then
    echo "    nothing to apply. exiting."
    exit 0
fi
echo

# --- [3/3] apply ------------------------------------------------------------
echo "==> [3/3] apply: $PRESET_CMD will process the top $LIMIT pending candidates"
echo "    new files written alongside originals (mode=replace, codec rewrite ON)"
echo "    originals moved to: $RECYCLE_TO/<rel-path-from-source-root>"
(( DRY_RUN )) && echo "    DRY RUN — ffmpeg invocations printed, no encodes"

if (( ! YES )); then
    read -r -p "    Proceed? [y/N] " ans
    case "$ans" in
        [Yy]*) ;;
        *) echo "    aborted"; exit 0 ;;
    esac
fi

APPLY_ARGS=(
    "$PRESET_CMD"
    --auto
    --mode replace
    --source-root "$SOURCE_ROOT"
    --recycle-to "$RECYCLE_TO"
    --limit "$LIMIT"
)
(( DRY_RUN )) && APPLY_ARGS+=(--dry-run)

echo
echo "==> exec: $TOOL ${APPLY_ARGS[*]}"
echo
exec "$TOOL" "${APPLY_ARGS[@]}"
