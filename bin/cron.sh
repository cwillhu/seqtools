#!/bin/bash

TYP=$(type -t module) || true
if [[ $TYP != "function" ]]; then
  source /etc/profile
fi

module --Silent load centos6/seqve_2.6.6
source activate
export DJANGO_SETTINGS_MODULE=seqstats.settings

/n/sw/www/seqtools/bin/cron.py ${@-}
