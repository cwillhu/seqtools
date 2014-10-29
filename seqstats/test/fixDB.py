#!/usr/bin/env python 
import sys, re, os
from seqstats.models import SeqRun, Lane

def fixDB():  
    allLanes = Lane.objects.all()

    print "Found " + str(len(allLanes)) + " lanes."

#    for myLane in allLanes:
#        myLane.percent_pf_clusters = 100.0 * myLane.percent_pf_clusters
#        myLane.save()

if __name__ == "__main__":
    fixDB()
