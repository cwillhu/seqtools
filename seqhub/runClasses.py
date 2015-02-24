from seqhub import hUtil, hSettings
from seqhub.sampleSheetClasses import BaseSampleSheet
from seqprep import settings
import os, re, shutil, glob, fnmatch, errno, stat, gzip, traceback
from lxml import etree
from os import path
import hashlib
from abc import ABCMeta, abstractmethod

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict # for python 2.6 and earlier, use backport                                                                                                        

class IlluminaNextGen:

    __metaclass__ = ABCMeta


    @classmethod
    def getInstance(cls, runName, **options):
        
        machine_id = re.match('^[0-9]{6}_([0-9A-Za-z]+)_', runName).group(1)
        machine_type = hSettings.MACHINE_TYPE[machine_id]

        if machine_type == "HiSeq":
            return HiSeq(runName,  **options)
        elif machine_type == "NextSeq":
            return NextSeq(runName, **options)


    @abstractmethod
    def makeLetter(self):
        pass


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
        self.analyses = None

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

        self.log(optionsStr)

    def processRun(self):

        try:
            self.clearDir(self.processingDir)
            self.parseSamplesheet(write_validated=True, write_analysis_samplesheets=True)
            self.bcl2fastq()
            self.postProcess()

        except:
            if not path.isdir(path.dirname(self.logFile)): 
                hUtil.mkdir_p(path.dirname(self.logFile))

            self.notify('Seqprep Exception', 'Error in ' + self.runOutName + ':\n' + traceback.format_exc())
            return

    def postProcess(self):

        try:
            self.gatherFastq()
            self.countUndetIndices()
            self.fastQC()
            self.calcCheckSums()
            hUtil.setPermissions(self.finishingDir)

            self.copyToFinal()
            hUtil.setPermissions(self.finalDir)
            self.validateFinalDir()

            self.summarizeDemuxResults()
            self.DBupdate()

        except:
            self.notify('Seqprep Exception', 'Error in ' + self.runOutName + ':\n' + traceback.format_exc())
            return

    def parseSamplesheet(self, write_validated=False, write_analysis_samplesheets=False):

        if not self.SampleSheet:
            self.SampleSheet = BaseSampleSheet.getInstance(self)
            self.SampleSheet.parse()
            self.analyses = self.SampleSheet.analyses

        if self.SampleSheet.warnings:
            self.notify('SeqPrep Warning',self.runOutName + '\n\n' + '\n'.join(self.SampleSheet.warnings))

        if write_validated:
            self.SampleSheet.write_validatedSamplesheet()

        if write_analysis_samplesheets:
            for a in self.analyses:
                a.writeSamplesheet(self.processingDir)

            
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
                        basesMask += ',I' + str(index2Length) + 'N' * (int(r['Read3']['num_cycles']) - index2Length)
                    else:
                        basesMask += ',' + 'N' * int(r['Read3']['num_cycles'])
                else: #then Read3 is not an index
                    basesMask += ',Y' + str( int(r['Read3']['num_cycles']) - 1 ) + 'N'

                if 'Read4' in r.keys(): #Read4 is never an index
                    basesMask += ',Y' + str( int(r['Read4']['num_cycles']) - 1 ) + 'N'

        return basesMask


    def bcl2fastq(self):

        for a in self.analyses:
            a.bcl2fastq()


    def gatherFastq(self):
        if not self.analyses:
            self.parseSamplesheet()

        self.clearDir(self.finishingDir)

        # Copy the run's SampleSheet.csv, RunInfo.xml, RunParameters.xml files to self.finishingDir
        filesToCopy = [self.samplesheetFile, 
                       self.runinfoFile, 
                       self.runparametersFile ]

        for item in filesToCopy:
            newItem = path.join(self.finishingDir, path.basename(item))
            self.safeCopy(item, newItem)

        for a in self.analyses:
            a.gather_analysis_fastq()  # Merge analysis fastq files into one file per sample per lane, and write to finishingDir
            a.copy_reports_to_finishing()  #Copy analysis stats files to finishingDir

        hUtil.setPermissions(self.finishingDir)


    def countUndetIndices(self):
        for a in self.analyses:
            a.countUndetIndices()


    def fastQC(self):
        for a in self.analyses:
            a.fastQC()

                
    def calcCheckSums(self):
        for a in self.analyses:
            a.md5sum()


    def validateFinalDir(self):

        self.log('Checking for required files in finalDir ' + self.finalDir + ' ...')

        requiredItems = ['RunInfo.xml', 'SampleSheet.csv']

        if len(hUtil.intersect(os.listdir(self.finalDir), requiredItems)) < 2:
            raise Exception('One or more files missing from finalDir %s: %s' % (self.finalDir, ', '.join(requiredItems)))

        runParameters_filenames = ['runParameters.xml', 'RunParameters.xml']

        if len(hUtil.intersect(os.listdir(self.finalDir), runParameters_filenames)) < 1:
            raise Exception('RunParameters file missing from finalDir ' + self.finalDir )

        for a in self.analyses:
            a.validateFinalDir()


    def summarizeDemuxResults(self, writeFiles = True):
        if writeFiles: 
            self.log('Scanning log and summarizing demux statistics...')

        with open(self.logFile,'r') as fh:
            log = fh.readlines()

        summary = list()
        for i, line in enumerate(log):

            if re.search(r'error|exception|inconsistent| failed|failed |negative number of base', line, flags=re.IGNORECASE):

                summary.append('\n_____Error found in %s at line %s:_____\n' % (self.logFile, i))

                if i-2 >= 0: summary.append(log[i-2])
                if i-1 >= 0: summary.append(log[i-1])

                summary.append(log[i])

                if i+1 < len(log): summary.append(log[i+1])
                if i+2 < len(log): summary.append(log[i+2])

                summary[-1] = summary[-1] + '\n\n'

        for a in self.analyses:
            summary += a.summarizeDemuxResults()

        summary = ''.join(summary)

        letter = self.makeLetter()

        if self.verbose: print summary + '\n\n' + letter

        self.notify('Demultiplex Summary', self.runOutName + '\n\n' + summary + '\n\n' + letter, includeWatchers=True)


    def DBupdate(self):
        if not self.dbStore: return
        if not hasattr(self, 'subIDs'): self.parseSamplesheet()
        command = 'php ' + path.join(settings.SEQPREP_ROOT, 'DBupdate.php')  + ' \\\n' \
                                         + '-r ' + self.runName          + ' \\\n' \
                                         + '-d ' + self.primaryParent    + ' \\\n' \
                                         + '-u ' + ','.join(self.SampleSheet.subIDs) + ' \\\n' \
                                         + '-s 1 \n'  #1 to store values in database
        self.shell(command, self.logFile)


    def copyToFinal(self): #copy processing results to self.finalDir
        self.log('Copying data to ' + self.finalDir + '...')
        self.safeCopy(self.finishingDir, self.finalDir)
        self.log('Copy to ' + self.finalDir + ' finished.')


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


    def log(self, text):
        self.append(text, self.logFile)


    def notify(self, subject, body, includeWatchers = False):
        addresses = hSettings.SEQTOOLS_USERS_EMAILS  #users email list 

        if includeWatchers and self.watcherEmails:   #watchers email list
            addresses += self.watcherEmails
            addresses = hUtil.unique(addresses)

        for address in addresses:
            self.shell("echo '" + body.rstrip() + """' |  mail -s '""" + subject.rstrip() + """' '""" + address + """' """, outputFile = '/dev/null')

        self.log('Notification:\n' + subject + '\n' + body + '\n\n')


    def formatTable(self, rows):
        lines = list()
        cols = zip(*rows)
        colWidths = [ max(len(elem) for elem in col) for col in cols ]
        rowFormat = '    ' + '  |  '.join(['%%%ds' % width for width in colWidths ]) + '\n'
        for row in rows:
            lines.append(rowFormat % tuple(row))
        lines.append('\n')
        return lines



class NextSeq(IlluminaNextGen):

    def __init__(self, runName, **kwargs):

        IlluminaNextGen.__init__(self, runName, **kwargs)

        self.runparametersFile = path.join(self.primaryDir, 'RunParameters.xml')
        self.minTrimmedReadLength = settings.NEXTSEQ['minTrimmedReadLength']
        self.maskShortAdapterReads = settings.NEXTSEQ['maskShortAdapterReads']
        self.runType = 'NextSeq' 

        if kwargs:
            if 'maskShortAdapterReadsStr' in kwargs.keys():
                self.maskShortAdapterReads = int(kwargs['maskShortAdapterReads'])
            if 'minTrimmedReadLength' in kwargs.keys():
                self.minTrimmedReadLength = int(kwargs['minTrimmedReadLength'])

        optionsStr = 'NextSeq Parameters:\n' \
            + 'runparametersFile:       '  + self.runparametersFile            + '\n' \
            + 'minTrimmedReadLength:    '  + str(self.minTrimmedReadLength)    + '\n' \
            + 'maskShortAdapterReads:   '  + str(self.maskShortAdapterReads)   + '\n' 

        self.log(optionsStr)  #log NextSeq options


    def makeLetter(self):
        return ''.join(('\n\nHi all,\n\n',
                        'The fastq files with the read sequences of run ' + self.runName + ' are available at:\n\n',
                        'https://software.rc.fas.harvard.edu/ngsdata/' + self.runOutName + '\n\n',
                        'or under /n/ngsdata/' + self.runOutName + ' on the cluster.\n\n',
                        'Summary statistics can be found at:\n\n',
                        'https://software.rc.fas.harvard.edu/ngsdata/'+self.runOutName+'/Reports/html/'+self.flowcell+'/all/all/all/laneBarcode.html\n\n',
                        'Reads with indices not in SampleSheet.csv are in the fastq file labeled\n',
                        '\'Undetermined_S0.\' We encourage users to download a local copy of their\n',
                        'data, as run data will eventually be removed from the ngsdata server.\n\nBest,\nChris\n\n'))



class HiSeq(IlluminaNextGen):

    def __init__(self, runName, **kwargs):
        IlluminaNextGen.__init__(self, runName, **kwargs)

        self.runparametersFile = path.join(self.primaryDir, 'runParameters.xml')
        self.runType = 'HiSeq'

        if kwargs and 'lanesStr' in kwargs.keys() and kwargs['lanesStr']: 
            lanesStr = kwargs['lanesStr']
            self.selectedLanes = lanesStr.split(',')
        else:
            lanesStr = 'All'

        optionsStr = 'HiSeq Parameters:\n' \
            + 'runparametersFile:    ' + self.runparametersFile + '\n' \
            + 'lanes:                ' + lanesStr + '\n'

        self.log(optionsStr)  #log HiSeq options


    def makeLetter(self):
        return ''.join(('\n\nHi all,\n\n',
                        'The fastq files with the read sequences of run ' + self.runName + ' are available at:\n\n',
                        'https://software.rc.fas.harvard.edu/ngsdata/' + self.runOutName + '\n\n',
                        'or under /n/ngsdata/' + self.runOutName + ' on the cluster.\n\n',
                        'Summary statistics can be found in Basecall_Stats/Demultiplex_Stats.htm. Reads\n',
                        'with indices not in the SampleSheet are in the fastq file(s) labeled \'Undetermined.\'\n\n',
                        'We encourage users to download a local copy of their data, as run data will\n',
                        'eventually be removed from the ngsdata server.\n\nBest,\nChris\n\n'))

