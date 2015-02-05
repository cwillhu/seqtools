from seqhub import hUtil, hSettings, sampleSheetClass
from seqprep import settings
import os, re, shutil, glob, fnmatch, errno, stat, gzip
from lxml import etree
from os import path
import hashlib
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict # for python 2.6 and earlier, use backport                                                                                                          
class IlluminaNextGen(object):

    def __init__(self, runName, **kwargs):
        self.runName = runName
        self.flowcell = runName[-9:]
        self.flowcellPosition = runName[-10]
        self.inSlurm = 'SLURM_JOBID' in os.environ.keys()
        self.suffix = None
        self.watcherEmails = settings.SEQPREP_WATCHERS_EMAILS
        self.primaryParent = hSettings.PRIMARY_PARENT
        self.numMismatches = settings.NUM_MISMATCHES
        self.ignoreMissingBcl = settings.IGNORE_MISSING_BCL
        self.ignoreMissingControl = settings.IGNORE_MISSING_CONTROL
        self.suppressAdapterTrimming = settings.SUPPRESS_TRIMMING
        self.withFailedReads = settings.WITH_FAILED_READS
        self.tileRegex = settings.TILE_REGEX
        self.numThreads= settings.NUM_THREADS
        self.processingParent = settings.PROCESSING_PARENT
        self.finishingParent = settings.FINISHING_PARENT
        self.finalParent = settings.FINAL_PARENT
        self.dbStore = settings.DB_STORE
        self.verbose = settings.VERBOSE
        self.customBasesMask = None
        self.selectedLanes = None

        for key, value in kwargs.iteritems():  #get optional initialization parameters 
            if key == 'suffix': self.suffix = value
            if key == 'watchersList': self.watcherEmails = value
            if key == 'primaryParent': self.primaryParent = value
            if key == 'numMismatches': self.numMismatches = value
            if key == 'ignoreMissingBcl': self.ignoreMissingBcl = value
            if key == 'ignoreMissingControl': self.ignoreMissingControl = value
            if key == 'suppressAdapterTrimming': self.suppressAdapterTrimming = value
            if key == 'withFailedReads': self.withFailedReads = value
            if key == 'tileRegex': self.tileRegex = value
            if key == 'numThreads': self.numThreads = value
            if key == 'dbStore': self.dbStore = value
            if key == 'verbose': self.verbose = value
            if key == 'customBasesMask': self.customBasesMask = value

        if self.suffix:
            self.runOutName = self.runName + self.suffix
        else:
            self.runOutName = self.runName
        self.logDir = path.join(settings.LOGDIR_PARENT, self.runOutName)
        self.logFile = path.join(self.logDir, 'log.txt')
        self.primaryDir = path.join(self.primaryParent, self.runName)
        self.processingDir = path.join(self.processingParent, self.runOutName)
        self.finishingDir = path.join(self.finishingParent, self.runOutName)
        self.finalDir = path.join(self.finalParent, self.runOutName)
        self.samplesheetFile = path.join(self.primaryDir, 'SampleSheet.csv')
        self.runinfoFile = path.join(self.primaryDir, 'RunInfo.xml')
        self.SampleSheet = None

        #touch status file to prevent re-processing of this run by cron job
        statusFile = path.join(self.primaryDir, 'seqprep_seen.txt')
        hUtil.touch(statusFile)
        hUtil.setPermissions(statusFile)

        self.initLogFile()
        self.logOptions()   #write options to log. (More options set and logged by child class)

    def initLogFile(self):
        hUtil.mkdir_p(self.logDir)
        k = 1
        if path.isfile(self.logFile):  #preserve any previous log files
            logBkup = self.logFile + str(k)
            while path.isfile(logBkup):
                k += 1
                logBkup = self.logFile + str(k)
            self.safeCopy(self.logFile, logBkup)
            hUtil.setPermissions(logBkup)
            self.safeDeleteItem(self.logFile)

    def logOptions(self):  
        optionsStr = 'Base Parameters:\n' \
            + 'runName:              ' + self.runName                   + '\n' \
            + 'runOutName:           ' + self.runOutName                + '\n' \
            + 'suffix:               ' + str(self.suffix)               + '\n' \
            + 'flowcell:             ' + self.flowcell                  + '\n' \
            + 'flowcellPosition:     ' + self.flowcellPosition          + '\n' \
            + 'inSlurm:              ' + str(self.inSlurm)              + '\n' \
            + 'numMismatches:        ' + str(self.numMismatches)        + '\n' \
            + 'ignoreMissingBcl:     ' + str(self.ignoreMissingBcl)     + '\n' \
            + 'ignoreMissingControl: ' + str(self.ignoreMissingControl) + '\n' \
            + 'withFailedReads:      ' + str(self.withFailedReads)      + '\n' \
            + 'numThreads:           ' + str(self.numThreads)           + '\n' \
            + 'primaryParent:        ' + self.primaryParent             + '\n' \
            + 'processingParent:     ' + self.processingParent          + '\n' \
            + 'finishingParent:      ' + self.finishingParent           + '\n' \
            + 'finalParent:          ' + self.finalParent               + '\n' \
            + 'dbStore:              ' + str(self.dbStore)              + '\n' \
            + 'logDir:               ' + self.logDir                    + '\n' \
            + 'logFile:              ' + self.logFile                   + '\n' \
            + 'primaryDir:           ' + self.primaryDir                + '\n' \
            + 'processingDir:        ' + self.processingDir             + '\n' \
            + 'finishingDir:         ' + self.finishingDir              + '\n' \
            + 'finalDir:             ' + self.finalDir                  + '\n' \
            + 'samplesheetFile:      ' + self.samplesheetFile           + '\n' \
            + 'runinfoFile:          ' + self.runinfoFile               + '\n' \
            + 'tileRegex:            ' + str(self.tileRegex)            + '\n' \
            + 'customBasesMask:      ' + str(self.customBasesMask)      + '\n' 
        self.append(optionsStr, self.logFile)

    def parseSamplesheet(self, write_validated=False, write_analysis_samplesheets=False):
        if not self.SampleSheet:
            self.SampleSheet = sampleSheetClass.SampleSheet(self.samplesheetFile, self.suppressAdapterTrimming, self.selectedLanes)

        if self.SampleSheet.warnings:
            self.notify('SeqPrep Warning',self.runOutName + '\n\n' + '\n'.join(self.SampleSheet.warnings))

        if write_validated:
            self.SampleSheet.write_validatedSamplesheet()

        if write_analysis_samplesheets:
            self.SampleSheet.write_analysisSamplesheets(self.processingDir)
            
    def makeBasesMask(self, index1Length, index2Length):  
        index1Length = int(index1Length)
        index2Length = int(index2Length)
        r, ignored = hUtil.parseRunInfo(self.runinfoFile) #example: {'Read1': {'num_cycles': 76, 'is_index': 'N'}, 'Read2': {'num_cycles': 7, 'is_index': 'Y'}}  

        basesMask = 'Y' + str( int(r['Read1']['num_cycles']) - 1 ) + 'N' 
        if 'Read2' in r.keys():
            if r['Read2']['is_index'] == 'Y': #then Read2 is an index
                if index1Length > 0:
                    basesMask += ',I' + str(index1Length) + 'N' * (int(r['Read2']['num_cycles']) - index1Length)
                else:
                    basesMask += ',' + 'N' * int(r['Read2']['num_cycles'])
            else: #then Read2 is not an index
                basesMask += ',Y' + str( int(r['Read2']['num_cycles']) - 1 ) + 'N'

            if 'Read3' in r.keys():
                if r['Read3']['is_index'] == 'Y': #then Read3 is an index
                    if index2Length > 0:
                        basesMask += ',I' + str( analDict['index2Length'] ) + 'N' * (int(r['Read3']['num_cycles']) - analDict['index2Length'])
                    else:
                        basesMask += ',' + 'N' * int(r['Read3']['num_cycles'])
                else: #then Read3 is not an index
                    basesMask += ',Y' + str( int(r['Read3']['num_cycles']) - 1 ) + 'N'
                if 'Read4' in r.keys(): #Read4 is never an index
                    basesMask += ',Y' + str( int(r['Read4']['num_cycles']) - 1 ) + 'N'
        return basesMask

    def DBupdate(self):
        if not self.dbStore: return
        if not hasattr(self, 'subIDs'): self.parseSamplesheet()
        command = 'php ' + path.join(settings.SEQPREP_ROOT, 'DBupdate.php')  + ' \\\n' \
                                         + '-r ' + self.runName          + ' \\\n' \
                                         + '-d ' + self.primaryParent    + ' \\\n' \
                                         + '-u ' + ','.join(self.SampleSheet.subIDs) + ' \\\n' \
                                         + '-s 1 \n'  #1 to store values in database
        self.shell(command, self.logFile)

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
        outFile = file(path.join(myDir,'md5sum.txt'), 'w')
        files = [f for f in glob.glob(path.join(myDir, '*.fastq.gz')) if path.isfile(f)]
        checksums = [(path.basename(fname), self._hashfile(open(fname, 'rb'), hashlib.md5())) for fname in files]
        for pair in checksums:
            outFile.write('%s\t%s\n' % pair)
        outFile.close()

    def copyToFinal(self): #copy processing results to self.finalDir
        self.append('Copying data to ' + self.finalDir + '...', self.logFile)
        self.safeCopy(self.finishingDir, self.finalDir)
        self.append('Copy to ' + self.finalDir + ' finished.', self.logFile)

    def clearDir(self, item):
        if path.isdir(item) or path.isfile(item):
            self.safeDeleteItem(item)
        hUtil.mkdir_p(item)

    def safeDeleteItem(self, item):
        self.checkDest(item)
        hUtil.deleteItem(item)

    def safeCopy(self, src, dst):  #copy a file or directory. 
        self.checkDest(dst)
        hUtil.deleteItem(dst)
        if not path.isdir(path.dirname(dst)):
            hUtil.mkdir_p(path.dirname(dst))
        hUtil.copy(src, dst)

    def checkDest(self, dest):  #make sure a parent directory is not a target
        dirsOrString = settings.LOGDIR_PARENT+'|'+'|'+settings.PROCESSING_PARENT+'|'+settings.FINISHING_PARENT+'|'+settings.FINAL_PARENT
        match = re.match('^'+dirsOrString+'/[0-9A-Za-z_]+[/0-9A-Za-z_]*$', dest)
        if not match:
            raise Exception('Unexpected item: ' + dest)

    def shell(self, command, outputFile, append=True):
        if append:
            mode = 'a'
        else:
            mode = 'w'
        fh = open(outputFile, mode)
        fh.write(command + '\n') #write command to file 
        for line in hUtil.runCmd(command):
            line = line.strip()
            if line != '': 
                fh.write(line + '\n') #write command output to file
                fh.flush()
            if self.verbose: print line
        fh.close()

    def append(self, text, filename):
        if self.verbose: print text  #echo text to stdout
        if not path.isdir(path.dirname(filename)):
            hUtil.mkdir_p(path.dirname(filename))
        fh = open(filename, 'a')
        fh.write(text + '\n') #append text to file
        fh.close()

    def showSampleSheet(self):
        with open(self.samplesheetFile, 'r') as fin:
            print fin.read()

    def showRunInfo(self):
        with open(self.runinfoFile, 'r') as fin:
            print fin.read()

    def notify(self, subject, body, includeWatchers = False):
        addresses = hSettings.SEQTOOLS_USERS_EMAILS  #users email list 
        if includeWatchers and self.watcherEmails:   #watchers email list
            addresses += self.watcherEmails
            addresses = hUtil.unique(addresses)
        for address in addresses:
            self.shell("echo '" + body.rstrip() + """' |  mail -s '""" + subject.rstrip() + """' '""" + address + """' """, outputFile = '/dev/null')
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

    def gzNotEmpty(self, fh):
        if not path.isfile(fh): 
            raise Exception('File %s not found.' % fh)
        if not re.search('.gz$', fh, flags=re.IGNORECASE):
            raise Exception('Attempt to decompress file not ending in .gz: %s' % fh)
        fh = gzip.open(fh, 'rb') 
        data = fh.read(100)
        fh.close()
        if data:
            return True #gz file contains data
        else:
            return False  
            
