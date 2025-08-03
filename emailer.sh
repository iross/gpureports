#!/bin/bash

today=`date +%F`

cd /home/iaross/gpureports
source .venv/bin/activate
today=$(date +%Y-%m-%e)


python usage_stats.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24 --group-by-device --email-to "chtc-reports@g-groups.wisc.edu,iaross@wisc.edu,gitter@biostat.wisc.edu"
#python usage_stats.py --exclude-hosts-yaml masked_hosts.yaml --hours-back 24 --group-by-device --email-to "iaross@wisc.edu,BBockelman@morgridge.org,ckoch5@wisc.edu"
