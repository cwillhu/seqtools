import os, stat, re
import os.path as path

SEQPREP_ROOT = "/n/sw/www/seqtools/seqprep"

##
# Main settings:
##

LOGDIR_PARENT = "/n/informatics_external/seq/seqprep_log"
LOGFILE = path.join(LOGDIR_PARENT, "log.txt")
PROCESSING_PARENT = "/n/seqcfs/sequencing/analysis_in_progress"
FINISHING_PARENT = "/n/seqcfs/sequencing/analysis_finished"
FINAL_PARENT = "/n/ngsdata"

WATCHERS_FILE = "/n/informatics/saved/seqprep_watchers_list.txt" # File containing comma-separated email addresses to be sent SeqPrep
with open(WATCHERS_FILE) as fh:                                  # completion summaries only. 
    watchers_string = fh.read().rstrip()                         # In order be sent all SeqPrep notifications and summaries, add email address
SEQPREP_WATCHERS_EMAILS = re.split("[,\s]+", watchers_string)    # to SEQTOOLS_USERS_EMAILS in seqhub.hSettings

##
# General bcl2fastq settings:
##

NUM_MISMATCHES = 0
NUM_THREADS = 8   
IGNORE_MISSING_BCL = True
IGNORE_MISSING_CONTROL = True
WITH_FAILED_READS = False
TILE_REGEX = None
DB_STORE = True 

##
# NextSeq bcl2fastq settings:
##
NEXTSEQ = {"suppressTrimming"      : False,
           "maskShortAdapterReads" : None,
           "minTrimmedReadLength"  : 0 }  #set to zero because of bcl2fastq bug. See seqprep README for details

##
# Slurm settings:
##

SLURM = {"nodes"     : "1",                # Number of nodes
         "ntasks"    : str(NUM_THREADS),   # Number of cores
         "time"      : "24:00:00",         # Runtime HH:MM:SS
         "mem"       : "10000",            # Memory in MB
         "partition" : "informatics-dev",  
         "job-name"  : "seqprep" }

SLURM_SCRIPT_DIR = path.join(LOGDIR_PARENT, "slurm_seqprep_log")
USERS_STRING = "cwill" #String containing comma-separated list of users to be sent ALL SLURM notifications 

##
# Misc. settings: 
##

VERBOSE = False

