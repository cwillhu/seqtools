#!/bin/bash

TYP=$(type -t module) || true
if [[ $TYP != "function" ]]; then
  source /etc/profile
fi

. /n/sw/www/seqtools/bin/setup.sh
/n/sw/www/seqtools/bin/cron_seqmem.py ${@-}
