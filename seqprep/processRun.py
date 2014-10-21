#!/usr/bin/env python 
from seqprep import nextSeqClass, hiSeqClass, settings, util
from seqhub import hSettings
import sys, re

def process(argv): #process raw sequencing data
    options, runName = util.parseOptions(argv)
    nameMatch = re.match('^[0-9]{6}_([0-9A-Za-z]+)_', runName)
    machine_id = nameMatch.group(1)
    machine_type = hSettings.MACHINE_TYPE[machine_id]
    if machine_type == "HiSeq":
        r = hiSeqClass.HiSeq(runName,  **options)
    elif machine_type == "NextSeq":
        r = nextSeqClass.NextSeq(runName, **options)
    r.processRun()

