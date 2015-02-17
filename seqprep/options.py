from seqhub import hSettings
from seqprep import settings
from seqprep.dictOptParse import DictOptionParser
import subprocess, sys, os, select, re
import os.path as path

def parseOptions(argv):
    parser = DictOptionParser(usage="usage: %prog [options] <run_name>")

    #General options:
    parser.add_option("-p","--primary",help="Directory containing run directory <run_name>. Default: %default",
                      default=hSettings.PRIMARY_PARENT, action="store", type = "string", dest="primaryParent")
    parser.add_option("-s","--suffix", help="Suffix to add to run name to create output directory name. By default, no suffix is added.", 
                      default=None, action="store", type="string",dest="suffix")
    parser.add_option("-l","--lanes",help="A comma-separated list of lanes to process. HiSeq only. " + 
                      "By default, all lanes are processed. Example lane list: 2,3,4",
                      default=None, action="store", type="string", dest="lanesStr")
    parser.add_option("-d","--noDbStore",help="Suppress database updates. Default: %default",
                      default=settings.DB_STORE, action="store_false",  dest="dbStore")
    parser.add_option("-v","--verbose", help="Verbose mode. Default: %default",
                      default=False, action="store_true", dest="verbose")
    parser.add_option("-n","--numThreads",help="Number of threads. Default: %default",
                      default=settings.NUM_THREADS, action="store", type = "int", dest="numThreads")

    #bcl2fastq options:
    parser.add_option("-m","--mismatches",help="Number of mismatches allowed in index read. Default: %default",
                      default=settings.NUM_MISMATCHES, action="store", type = "int", dest="numMismatches")
    parser.add_option("-y","--customBasesMask",help="Custom bases mask. By default, mask is generated automatically from runinfo and samplesheet files.",
                      default=None, action="store", type = "string", dest="customBasesMask")
    parser.add_option("-w","--suppressAdapterTrimming",help="Suppress adapter trimming by removing adapters from samplesheet. Default: %default",
                      default=settings.SUPPRESS_TRIMMING, action="store_true", dest="suppressAdapterTrimming")
    parser.add_option("-b","--ignoreMissingBcl",help="Ignore missing BCL files. Default: %default",
                      default=settings.IGNORE_MISSING_BCL, action="store_true", dest="ignoreMissingBcl")
    parser.add_option("-c","--ignoreMissingControl",help="Ignore missing control files. Default: %default",
                      default=settings.IGNORE_MISSING_CONTROL, action="store_true", dest="ignoreMissingBcl")
    parser.add_option("-f","--withFailedReads",help="Include failed reads in demultiplexing results.. Default: %default",
                      default=settings.WITH_FAILED_READS, action="store_true", dest="withFailedReads")
    parser.add_option("-t","--tileRegex",help="Regular expression for tile selection. Default: %default",
                      default=settings.TILE_REGEX, action="store", type = "string", dest="tileRegex")
    parser.add_option("-k","--maskShortAdapterReads",help="Mask short adapter reads. NextSeq only. Default: 32 (Set by bcl2fastq)",
                      default=settings.NEXTSEQ["maskShortAdapterReads"], action="store", type = "int", dest="maskShortAdapterReads")
    parser.add_option("-x","--minTrimmedReadLength",help="Minimum trimmed read length.. NextSeq only. Default: 32 (Set by bcl2fastq)",
                      default=settings.NEXTSEQ["minTrimmedReadLength"], action="store", type = "int", dest="minTrimmedReadLength")

    #Slurm options:
    parser.add_option("-i","--time",help="Runtime to reserve on the cluster. Format: HH:MM:SS. Default: %default. (Unimplemented option)",
                      default=settings.SLURM["time"], action="store", type = "string", dest="time")
    parser.add_option("-r","--partition",help="Name of cluster partition to submit to. Default: %default. (Unimplemented option)",
                      default=settings.SLURM["partition"], action="store", type = "string", dest="partition")
    parser.add_option("-j","--jobName",help="Job name (unimplemented). Default: %default. (Unimplemented option)",
                      default=settings.SLURM["job-name"], action="store", type = "string", dest="jobName")
    parser.add_option("-u","--users",help="Comma-separated list of RC users to be sent SLURM notifications. Default: %default. (Unimplemented option)",
                      default=settings.USERS_STRING,action="store", type = "string", dest="usersString")
    (options, args) = parser.parse_args(argv)

    if not args:
        raise Exception('A run name must be provided. Use -h option to see usage details.')

    runName = args[0]
    if not re.match('[0-9]{6}_[0-9A-Za-z]+_', runName): #matches from beg.
        raise Exception("Expected run name as argument, got '" + runName + "'. Use -h option to see usage details.")

    return options, runName
