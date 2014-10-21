#!/usr/bin/env python 
from seqprep import processRun
import sys

if __name__ == "__main__":
    processRun.process(sys.argv[1:])
