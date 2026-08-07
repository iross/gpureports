#!/bin/bash
set -euo pipefail

# emailer.sh — send GPU allocation reports via email
#
# Usage: bash emailer.sh <mode>
#
#   daily    24h allocation report → full recipient list (intended: 06:00 daily)
#   weekly   168h allocation report → full recipient list (intended: 06:00 Mondays)
#   monthly  monthly summary → full recipient list (intended: 06:00 on the 1st)
#   test     24h allocation report → iaross only (safe to run anytime)
#
# Runs as a k8s CronJob per mode (see OPERATIONS.md for the current deployment
# and PVC details -- the manifests themselves live outside this repo).

RECIPIENTS="chtc-reports@g-groups.wisc.edu,iaross@wisc.edu,gitter@biostat.wisc.edu"
TEST_RECIPIENT="iaross@wisc.edu"

MODE="${1:-}"

case "$MODE" in
    daily)
        uv run usage_stats.py \
            --exclude-hosts-yaml masked_hosts.yaml \
            --hours-back 24 \
            --group-by-device \
            --data-dir /data \
            --email-to "$RECIPIENTS"
        ;;
    weekly)
        uv run usage_stats.py \
            --exclude-hosts-yaml masked_hosts.yaml \
            --hours-back 168 \
            --group-by-device \
            --data-dir /data \
            --email-to "$RECIPIENTS"
        ;;
    monthly)
        uv run usage_stats.py \
            --exclude-hosts-yaml masked_hosts.yaml \
            --analysis-type monthly \
            --data-dir /data \
            --email-to "$RECIPIENTS"
        ;;
    test)
        uv run usage_stats.py \
            --exclude-hosts-yaml masked_hosts.yaml \
            --hours-back 24 \
            --group-by-device \
            --data-dir /data \
            --email-to "$TEST_RECIPIENT"
        ;;
    *)
        echo "Usage: bash emailer.sh <daily|weekly|monthly|test>" >&2
        exit 1
        ;;
esac
