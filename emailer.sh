#!/bin/bash
set -euo pipefail

# emailer.sh — send GPU allocation reports via email
#
# Usage: bash emailer.sh <mode>
#
#   daily    24h allocation report → full recipient list (runs 06:00 daily)
#   weekly   168h allocation report → full recipient list (runs 06:00 Mondays)
#   monthly  monthly summary → full recipient list (runs 06:00 on the 1st)
#   test     24h allocation report → iaross only (safe to run anytime)
#
# Crontab entries (on the production host):
#   0 6 * * *    bash /home/iaross/gpureports/emailer.sh daily  &> /tmp/gpu_emailer.log
#   0 6 * * 1    bash /home/iaross/gpureports/emailer.sh weekly &> /tmp/gpu_emailer_weekly.log
#   0 6 1 * *    bash /home/iaross/gpureports/emailer.sh monthly &> /tmp/gpu_emailer_monthly.log
#   41 12 * * *  bash /home/iaross/gpureports/emailer.sh test   &> /tmp/gpu_emailer.log

RECIPIENTS="chtc-reports@g-groups.wisc.edu,iaross@wisc.edu,gitter@biostat.wisc.edu"
TEST_RECIPIENT="iaross@wisc.edu"

MODE="${1:-}"

cd /home/iaross/gpureports
source .venv/bin/activate

case "$MODE" in
    daily)
        python usage_stats.py \
            --exclude-hosts-yaml masked_hosts.yaml \
            --hours-back 24 \
            --group-by-device \
            --email-to "$RECIPIENTS"
        ;;
    weekly)
        python usage_stats.py \
            --exclude-hosts-yaml masked_hosts.yaml \
            --hours-back 168 \
            --group-by-device \
            --email-to "$RECIPIENTS"
        ;;
    monthly)
        python usage_stats.py \
            --exclude-hosts-yaml masked_hosts.yaml \
            --analysis-type monthly \
            --email-to "$RECIPIENTS"
        ;;
    test)
        python usage_stats.py \
            --exclude-hosts-yaml masked_hosts.yaml \
            --hours-back 24 \
            --group-by-device \
            --email-to "$TEST_RECIPIENT"
        ;;
    *)
        echo "Usage: bash emailer.sh <daily|weekly|monthly|test>" >&2
        exit 1
        ;;
esac
