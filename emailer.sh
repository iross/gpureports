#!/bin/bash

today=`date +%F`

cd /home/iaross/gpureports
source .venv/bin/activate
today=$(date +%Y-%m-%e)


python usage_stats.py --output-format html --exclude-hosts-yaml masked_hosts.yaml --hours-back 24 --group-by-device | mailx -Smta=smtp://smtp.wiscmail.wisc.edu -Ssmtp-use-starttls \
    -s "CHTC GPU Utilization $today" -r "iaross@wisc.edu" iaross@wisc.edu
