export DJANGO_SETTINGS_MODULE=seqstats.settings

if [[ $(hostname) == "LSCI-CWILL-2.local" ]]; then
  export PYTHONPATH="$PYTHONPATH:/Users/williams03/d/django/seqtools:/Users/williams03/d/django/seqtools/seqstats/packages"
else
  export PYTHONPATH="$PYTHONPATH:/n/sw/www/seqtools:/n/sw/www/seqtools/seqstats/packages"
fi

module load centos6/seqve_2.6.6
source activate
