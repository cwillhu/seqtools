from seqprep import nextSeqClass, hiSeqClass, settings
from seqprep import options as opts
from seqhub import hSettings
import sys, re

def processRun(argv): #process raw sequencing data
    options, runName = opts.parseOptions(argv)
    nameMatch = re.match('^[0-9]{6}_([0-9A-Za-z]+)_', runName)
    machine_id = nameMatch.group(1)
    machine_type = hSettings.MACHINE_TYPE[machine_id]
    if machine_type == "HiSeq":
        r = hiSeqClass.HiSeq(runName,  **options)
    elif machine_type == "NextSeq":
        r = nextSeqClass.NextSeq(runName, **options)
    r.processRun()

