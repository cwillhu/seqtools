

from seqstats import addSeqRun
runName = '141125_D00365_0389_AHB6BCADXX'
myArgs = [runName, "--no-db-rewrite", "--no-hist-rewrite", "-d", "/n/informatics_external/seq/seqstats_bkup", "--verbose"]
addSeqRun.add(myArgs)


#-------------------
# For 141125_D00365_0389_AHB6BCADXX, qual_df 'lane' field ranges from 1 to 209, instead of 1 to 2 as we would expect. 
#
# Need updated format spec for QMetricsOut.bin from Rey? 
#
# Results of attempting to run seqstats on this run:
# Traceback (most recent call last):
#  File /n/sw/www/seqtools/bin/seqstats_cron.py, line 48, in seqstatsOnNew
#    addSeqRun.add(myArgs)
#  File /n/sw/www/seqtools/seqstats/addSeqRun.py, line 86, in add
#    lanes = stats.byLaneStats(tile.df, quality.df)
#  File /n/sw/www/seqtools/seqstats/stats.py, line 59, in byLaneStats
#    if q_below_thresh not in lanes[i].keys():
# KeyError: 3

import pandas
from illuminate import InteropDataset

runPath = '/n/informatics_external/seq/seqstats_bkup/141125_D00365_0389_AHB6BCADXX'

myDataset = InteropDataset(runPath)
tile = myDataset.TileMetrics()
quality = myDataset.QualityMetrics()
tile_df = tile.df
qual_df = quality.df



for index, row in qual_df.iterrows():
    i = int(row['lane'])
    print i


#-------------------

codeMap = { 100 : "cluster_density",
            101 : "cluster_density_pf",
            102 : "num_clusters",
            103 : "num_clusters_pf" }

lanes = dict()

    ##                                                                                                                                                          
    # Get density stats from tile data frame                                                                                                                    
    ##                                                                                                                                                          

for index, row in tile_df.iterrows():
    if row['code'] in codeMap.keys():
        i = int(row['lane'])
        metricType = codeMap[int(row['code'])]
        if i not in lanes.keys():
            lanes[i] = dict()
        if metricType not in lanes[i].keys():
            lanes[i][metricType] = list()
        lanes[i][metricType].append(row['value'])



#-----------------------------------------------
#fix links from lanes to seqruns:
from seqstats.models import SeqRun, Lane

allLanes = Lane.objects.all()

for myLane in allLanes:
    runName = myLane.run_name
    print "run name: %s" % runName
    run = SeqRun.objects.get(run_name__exact=runName)
    myLane.seqrun = run
    myLane.save()

