from seqhub.hUtil import copy
import sys, os, re
import shutil, errno

def copyRunFiles(runPath, dstDir):  #copy essential run files
    runName = os.path.basename(runPath)
    os.mkdir(os.path.join(dstDir,runName))
    for filename in ["SampleSheet.csv", "RunInfo.xml", "runParameters.xml", "RunParameters.xml", "InterOp"]:
        if os.path.isfile(os.path.join(runPath, filename)):
            copy(os.path.join(runPath,filename), os.path.join(dstDir,runName,filename))
