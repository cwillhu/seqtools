#!/usr/bin/env python 
from seqmem.util import scanDiskUsage, deleteOldRuns
from optparse import OptionParser
import sys

def main(argv):
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-v","--verbose", help="Verbose mode. Default: %default",
                      default=False, action="store_true", dest="verbose")
    (options, args) = parser.parse_args(argv)
    if len(args) != 0:
        parser.error("Expected 0 input arguments, " + str(len(args)) + " provided. Use -h to see usage.")

    scanDiskUsage( ['/n/ngsdata', '/n/seqcfs'], options.verbose )

    deleteOldRuns('/n/seqcfs/sequencing/analysis_in_progress', 120, options.verbose) #delete runs in analysis_in_progress after 120 days
    deleteOldRuns('/n/ngsdata', 60, options.verbose)                                 #delete runs in nsdata after 60 days

if __name__ == "__main__":
    main(sys.argv[1:])
