#!/bin/bash

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "Usage: $0 <SBID> [-dryrun]"
    exit 1
fi

if ! [[ "$1" =~ ^[0-9]+$ ]]; then
    echo "Error: SBID must be an integer"
    exit 1
fi

SBID="SB$(printf '%06d' "$1")"

DRYRUN=false
if [ "$2" = "-dryrun" ]; then
    DRYRUN=true
fi

echo "SBID: $SBID"
echo "Dry run: $DRYRUN"

rclone ls "acacia:as116/${SBID}" --include "*.uvfits" |
while read -r size file; do
    # Only process the actual .uvfits file, not .uvfits.an_table etc.
    [[ "$file" =~ \.uvfits$ ]] || continue

    # Extract DATA_XX and everything after it
    data_dir=$(dirname "$file" | sed 's|.*/\(DATA_[^/]*/\)||')
    data_id=$(echo "$file" | sed -n 's|.*/\(DATA_[^/]*\)/.*|\1|p')

    # Remove DATA_XX from the path
    relative_path="${file%/${data_id}/*}/${file##*/}"

    dest="/CRACO/${data_id}/craco/${SBID}/scans/${relative_path}"

    mkdir -p "$(dirname "$dest")"

    echo "Downloading:"
    echo "  acacia:as116/${SBID}/${file}"
    echo "  -> $dest"

    if [ "$DRYRUN" = false ]; then
        rclone copyto \
            "acacia:as116/${SBID}/${file}" \
            "$dest" \
            -P --stats 1s
    fi
done