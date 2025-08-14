#!/bin/bash

today=`date +%F`

cd /home/iaross/gpureports
source .venv/bin/activate
today=$(date +%Y-%m-%e)


python usage_stats.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24 --group-by-device --email-to "iaross@wisc.edu" --smtp-server "postfix-mail" --smtp-port 587
