from seqprep import options as opts
from seqprep import settings
from seqhub.runClasses import IlluminaNextGen
from seqhub import hSettings
import sys, re


def processRun(argv): #process raw sequencing data

    options, runName = opts.parseOptions(argv)

    r = IlluminaNextGen.getInstance(runName, options)
    r.processRun()

