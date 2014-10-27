#!/usr/bin/env python 
from optparse import OptionParser
from seqprep import slurmProcessRun, processRun
from seqstats import addSeqRun
from seqhub import hUtil, hSettings
import sys, os, re, time, traceback
import os.path as path
from seqhub.hUtil import intersect
from seqhub.hUtil import touch

def addAndProcessNew(argv):
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-d","--dir", help="Set directory searched for new sequencing runs. Default: %default",
                      default=hSettings.PRIMARY_PARENT, action="store", dest="primaryParent")
    parser.add_option("-s","--slurm", help="Process new runs on cluster instead of locally. Default: %default",
                      default=False, action="store_true", dest="slurm")
    parser.add_option("-v","--verbose", help="Verbose mode. Default: %default",
                      default=False, action="store_true", dest="verbose")
    (options, args) = parser.parse_args(argv)

    if len(args) != 0:
        parser.error("Expected 0 input arguments, " + str(len(args)) + " provided. Use -h to see usage.")

    cronLog = hSettings.SEQ_CRONLOG

    required = ["InterOp", "RunInfo.xml", "SampleSheet.csv", "RTAComplete.txt"]  #files that must be in run folder
    interopRequired = ["QMetricsOut.bin", "TileMetricsOut.bin"]  #files that must be in <run>/InterOp folder
    runParametersOptions = ["runParameters.xml", "RunParameters.xml"] #one of these must be in run folder
    for myDir in os.listdir(options.primaryParent):
        match = re.match('[0-9]{6}_[0-9A-Za-z]+_[0-9A-Za-z]+_[0-9A-Za-z]{10}$', myDir) #re.match matches from beginning of string
        if match:
            runName = myDir
            runPath = path.join(options.primaryParent,runName)
            items = os.listdir(runPath) #get list of directory contents
            #check that we haven't examined this run folder before, and then check that required files are present:
            if "seq_seen.txt" not in items \
                    and len(intersect(items, required)) == len(required) \
                    and len(intersect(items, runParametersOptions)) == 1 \
                    and len(intersect(os.listdir(path.join(runPath, "InterOp")), interopRequired)) == len(interopRequired):
                if options.verbose: print "\nNew run: " + runName
                touch(path.join(runPath, "seq_seen.txt"))  #mark run as seen

                #send notification email
                hUtil.email(hSettings.NOTIFY_EMAILS,"Found new run", "Processing:\n" + runName)

                try:                
                    #add run to database if not already present
                     myArgs = [runName, "--no-db-rewrite", "--no-hist-rewrite", "-d", options.primaryParent]
                     if options.verbose: myArgs.append("--verbose")
                     logMsg = time.strftime("%c") + "  Running addSeqRun.add() on " + runName + " with args: " + ' '.join(myArgs)
                     hUtil.append(logMsg, cronLog, echo = options.verbose)
                     addSeqRun.add(myArgs)  
                except:
                     excpStr = runName + "\n\n" + traceback.format_exc()
                     hUtil.append("Seqstats Exception\n" + excpStr, cronLog, echo = options.verbose)
                     hUtil.email(hSettings.NOTIFY_EMAILS,"Seqstats Exception", excpStr)
                     return

                #start seqprep for this run
                myArgs = [runName, "--primary", options.primaryParent]
                if options.verbose: myArgs.append("--verbose")
                if options.slurm:
                     logMsg = time.strftime("%c") + "  Running slurmProcessRun.slurmProcess() on " + runName + " with args: " + ' '.join(myArgs)
                     hUtil.append(logMsg, cronLog, echo = options.verbose)
                     slurmProcessRun.slurmProcess(myArgs)
                else:
                     logMsg = time.strftime("%c") + "  Running processRun.process() on " + runName + " with args: " + ' '.join(myArgs)
                     hUtil.append(logMsg, cronLog, echo = options.verbose)
                     processRun.process(myArgs)

if __name__ == "__main__":
    addAndProcessNew(sys.argv[1:])