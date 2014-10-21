from seqhub import hUtil, hSettings
from seqprep import settings
import os, re, shutil, glob, fnmatch, errno, stat
from lxml import etree
from os import path
import hashlib

class IlluminaNextGen(object):

    def __init__(self, runName, **kwargs):
        self.runName = runName
        self.flowcell = runName[-9:]
        self.flowcellPosition = runName[-10]
        self.inSlurm = "SLURM_JOBID" in os.environ.keys()
        self.suffix = None
        self.primaryParent = hSettings.PRIMARY_PARENT
        self.numMismatches = settings.NUM_MISMATCHES
        self.ignoreMissingBcl = settings.IGNORE_MISSING_BCL
        self.ignoreMissingControl = settings.IGNORE_MISSING_CONTROL
        self.withFailedReads = settings.WITH_FAILED_READS
        self.tileRegex = settings.TILE_REGEX
        self.numThreads= settings.NUM_THREADS
        self.processingParent = settings.PROCESSING_PARENT
        self.finishingParent = settings.FINISHING_PARENT
        self.finalParent = settings.FINAL_PARENT
        self.dbStore = settings.DB_STORE
        self.verbose = settings.VERBOSE
        self.customBasesMask = None

        for key, value in kwargs.iteritems():  #get optional initialization parameters 
            if key == "suffix": self.suffix = value
            if key == "primaryParent": self.primaryParent = value
            if key == "numMismatches": self.numMismatches = value
            if key == "ignoreMissingBcl": self.ignoreMissingBcl = value
            if key == "ignoreMissingControl": self.ignoreMissingControl = value
            if key == "withFailedReads": self.withFailedReads = value
            if key == "tileRegex": self.tileRegex = value
            if key == "numThreads": self.numThreads = value
            if key == "dbStore": self.dbStore = value
            if key == "verbose": self.verbose = value
            if key == "customBasesMask": self.customBasesMask = value

        if self.suffix:
            self.runOutName = self.runName + self.suffix
        else:
            self.runOutName = self.runName
        self.logDir = path.join(settings.LOGDIR_PARENT, self.runOutName)
        self.logFile = path.join(self.logDir, "log.txt")
        self.primaryDir = path.join(self.primaryParent, self.runName)
        self.processingDir = path.join(self.processingParent, self.runOutName)
        self.finishingDir = path.join(self.finishingParent, self.runOutName)
        self.finalDir = path.join(self.finalParent, self.runOutName)
        self.samplesheetFile = path.join(self.primaryDir, "SampleSheet.csv")
        self.runinfoFile = path.join(self.primaryDir, "RunInfo.xml")

    def logOptions(self):
        optionsStr = "Base Parameters:\n" \
            + "runName:              " + self.runName                   + "\n" \
            + "runOutName:           " + self.runOutName                + "\n" \
            + "suffix:               " + str(self.suffix)               + "\n" \
            + "flowcell:             " + self.flowcell                  + "\n" \
            + "flowcellPosition:     " + self.flowcellPosition          + "\n" \
            + "inSlurm:              " + str(self.inSlurm)              + "\n" \
            + "numMismatches:        " + str(self.numMismatches)        + "\n" \
            + "ignoreMissingBcl:     " + str(self.ignoreMissingBcl)     + "\n" \
            + "ignoreMissingControl: " + str(self.ignoreMissingControl) + "\n" \
            + "withFailedReads:      " + str(self.withFailedReads)      + "\n" \
            + "numThreads:           " + str(self.numThreads)           + "\n" \
            + "primaryParent:        " + self.primaryParent             + "\n" \
            + "processingParent:     " + self.processingParent          + "\n" \
            + "finishingParent:      " + self.finishingParent           + "\n" \
            + "finalParent:          " + self.finalParent               + "\n" \
            + "dbStore:              " + str(self.dbStore)              + "\n" \
            + "logDir:               " + self.logDir                    + "\n" \
            + "logFile:              " + self.logFile                   + "\n" \
            + "primaryDir:           " + self.primaryDir                + "\n" \
            + "processingDir:        " + self.processingDir             + "\n" \
            + "finishingDir:         " + self.finishingDir              + "\n" \
            + "finalDir:             " + self.finalDir                  + "\n" \
            + "samplesheetFile:      " + self.samplesheetFile           + "\n" \
            + "runinfoFile:          " + self.runinfoFile               + "\n" \
            + "tileRegex:            " + str(self.tileRegex)            + "\n" \
            + "customBasesMask:      " + str(self.customBasesMask)      + "\n" 
        self.append(optionsStr, self.logFile)

    def parseRunInfo(self):
        with open (self.runinfoFile, 'r') as myfile:
            xmlstr=myfile.read().replace('\n', '')
        root = etree.fromstring(xmlstr)
        reads = root.find('Run/Reads').getchildren()
        r = dict()
        for read in reads:
            read_num = read.attrib['Number']
            r["Read" + read_num] = dict()
            r["Read" + read_num]['is_index'] = read.attrib['IsIndexedRead']
            r["Read" + read_num]['num_cycles'] = int(read.attrib['NumCycles'])
        return r

    def _hashfile(self, afile, hasher, blocksize=65536): #helper function for calculating checksums
        buf = afile.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(blocksize)
        return hasher.hexdigest()

    def md5sum(self,myDir):
        # Calculate md5sum checksums on any .fastq.gz files in myDir. Saves checksums to md5sum.txt in myDir
        # Writing md5sum.txt to the same directory as the input files avoids ambiguity that could arise if samples
        # with the same name are run in different lanes.
        outFile = file(path.join(myDir,"md5sum.txt"), "w")
        files = [f for f in glob.glob(path.join(myDir, '*.fastq.gz')) if path.isfile(f)]
        checksums = [(path.basename(fname), self._hashfile(open(fname, 'rb'), hashlib.md5())) for fname in files]
        for pair in checksums:
            outFile.write("%s\t%s\n" % pair)
        outFile.close()

    def copy(self, src, dst):  #copy a file or directory.
        self.deleteItem(dst)
        if not path.isdir(path.dirname(dst)):
            hUtil.mkdir_p(path.dirname(dst))
        try:  
            shutil.copytree(src, dst)
        except OSError as exc:
            if exc.errno == errno.ENOTDIR:
                shutil.copy(src, dst)
            else: raise

    def copyToFinal(self): #copy processing results to self.finalDir
        self.append("Copying data to " + self.finalDir + "...", self.logFile)
        self.copy(self.finishingDir, self.finalDir)
        self.append("Copy to " + self.finalDir + " finished.", self.logFile)

    def deleteItem(self, item):
        dirsOrString = settings.LOGDIR_PARENT+'|'+'|'+settings.PROCESSING_PARENT+'|'+settings.FINISHING_PARENT+'|'+settings.FINAL_PARENT
        match = re.match('^'+dirsOrString+'/[0-9A-Za-z_]+[/0-9A-Za-z_]*$', item)
        if not match:
            raise Exception("Deletion requested for unexpected directory or file: " + item)
        elif path.isdir(item):
            shutil.rmtree(item, ignore_errors=True)
        elif path.isfile(item):
            os.remove(item)

    def clearDir(self, item):
        if path.isdir(item) or path.isfile(item):
            self.deleteItem(item)
        hUtil.mkdir_p(item)

    def shell(self, command, outputFile, append=True):
        if append:
            mode = "a"
        else:
            mode = "w"
        fh = open(outputFile, mode)
        fh.write(command + "\n") #write command to file 
        for line in hUtil.runCmd(command):
            line = line.strip()
            if line != '': 
                fh.write(line + "\n") #write command output to file
                fh.flush()
            if self.verbose: print line
        fh.close()

    def setPermissions(self, item):
        filePermissions = stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IWGRP|stat.S_IROTH
        if path.isfile(item):
            os.chmod(item, filePermissions)
        elif path.isdir(item):
            dirPermissions = stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IRGRP|stat.S_IWGRP|stat.S_IXGRP|stat.S_IRUSR|stat.S_IROTH|stat.S_IXOTH
            hUtil.recursiveChmod(item, filePermissions, dirPermissions)

    def append(self, text, filename):
        if self.verbose: print text  #echo text to stdout
        if not path.isdir(path.dirname(filename)):
            hUtil.mkdir_p(path.dirname(filename))
        fh = open(filename, 'a')
        fh.write(text + "\n") #append text to file
        fh.close()

    def showSampleSheet(self):
        with open(self.samplesheetFile, 'r') as fin:
            print fin.read()

    def showRunInfo(self):
        with open(self.runinfoFile, 'r') as fin:
            print fin.read()

    def notify(self, subject, body):
        addresses = hSettings.NOTIFY_EMAILS.split(',')
        for address in addresses:
            self.shell("echo '" + body.rstrip() + """' |  mail -s '""" + subject.rstrip() + """' '""" + address + """' """, self.logFile)
        self.append('Notification:\n' + subject + '\n' + body + '\n\n', self.logFile)

    def formatTable(self, rows):
        lines = list()
        cols = zip(*rows)
        colWidths = [ max(len(elem) for elem in col) for col in cols ]
        rowFormat = '    ' + '  |  '.join(['%%%ds' % width for width in colWidths ]) + '\n'
        for row in rows:
            lines.append(rowFormat % tuple(row))
        lines.append('\n')
        return lines
