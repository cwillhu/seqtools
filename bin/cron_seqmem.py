#!/usr/bin/env python 
from seqmem.manageMem import reportDiskUsage, deleteOldRuns, moveOldPrimaryToArchive, capDirUsage
from optparse import OptionParser
import sys

def main(argv):
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-v","--verbose", help="Verbose mode. Default: %default",
                      default=False, action="store_true", dest="verbose")
    (options, args) = parser.parse_args(argv)
    if len(args) != 0:
        parser.error("Expected 0 input arguments, " + str(len(args)) + " provided. Use -h to see usage.")

    reportDiskUsage( ['/n/seqcfs', '/n/illumina01'] )

    capDirUsage('/n/ngsdata', 85, 30, copyInSEQCFS = True, verbose = options.verbose)  #delete oldest run folders until disk usage is below 85%, 
                                                                                       #while retaining runs for at least 30 days

    moveOldPrimaryToArchive(20, options.verbose)                                       #move runs from primary_data to archive/primary_data after 20 days

    deleteOldRuns('/n/illumina01/archive/primary_data', 60, copyInSEQCFS = True, verbose = options.verbose)  #delete runs in specified dir after n days
    deleteOldRuns('/n/seqcfs/sequencing/analysis_in_progress', 120, copyInSEQCFS = True, verbose=options.verbose) 


if __name__ == "__main__":
    main(sys.argv[1:])
