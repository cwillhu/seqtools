#!/bin/bash

TYP=$(type -t module) || true
if [[ $TYP != "function" ]]; then
  source /etc/profile
fi

. /n/sw/www/seqtools/setup.sh
/n/sw/www/seqtools/bin/cron_seqprep ${@-}
