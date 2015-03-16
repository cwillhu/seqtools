from seqhub import hUtil, hSettings
from seqhub.sampleSheetClasses import BaseSampleSheet
from seqprep import settings
import os, re, traceback
from lxml import etree
from os import path
from abc import ABCMeta, abstractmethod


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
            self.warn()

        except:
            self.notify('Seqprep Exception', 'Error in ' + self.runOutName + ':\n' + traceback.format_exc())
            return

        self.log('\nPost-processing complete.')


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
                a.writeSamplesheet()

            
    def bcl2fastq(self):
        if not self.analyses:  
            self.parseSamplesheet()

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
        if not self.analyses:  
            self.parseSamplesheet()

        for a in self.analyses:
            a.countUndetIndices()


    def fastQC(self):
        if not self.analyses:  
            self.parseSamplesheet()

        for a in self.analyses:
            a.fastQC()

                
    def calcCheckSums(self):
        if not self.analyses:  
            self.parseSamplesheet()

        for a in self.analyses:
            a.calcCheckSums()


    def validateFinalDir(self):
        if not self.analyses:  
            self.parseSamplesheet()

        self.log('Checking for required files in finalDir ' + self.finalDir + ' ...')

        requiredItems = ['RunInfo.xml', 'SampleSheet.csv']

        if len(hUtil.intersect(os.listdir(self.finalDir), requiredItems)) < 2:
            raise Exception('One or more files missing from finalDir %s: %s' % (self.finalDir, ', '.join(requiredItems)))

        runParameters_filenames = ['runParameters.xml', 'RunParameters.xml']

        if len(hUtil.intersect(os.listdir(self.finalDir), runParameters_filenames)) < 1:
            raise Exception('RunParameters file missing from finalDir ' + self.finalDir )

        warnings = list()
        for a in self.analyses:
            warnings += a.validateFinalDir()

        if warnings:
            self.notify('SeqPrep Warning', '\n'.join(warnings))


    def summarizeDemuxResults(self):  #base run
        if not self.analyses:  
            self.parseSamplesheet()

        self.log('Scanning bcl2fastq log and summarizing demux statistics...')                                                                        

        with open(self.logFile,'r') as fh:  #read in log file
            log = fh.readlines()

        errors = list()
        for i, line in enumerate(log):

            if re.search(r'error|exception|inconsistent| failed|failed |negative number of base', line, flags=re.IGNORECASE) \
                    and re.search(r'^((?!0 errors).)*$', line, flags=re.IGNORECASE):

                errors.append('\n_____Error found in %s at line %s:_____\n' % (self.logFile, i))

                if i-2 >= 0: errors.append(log[i-2])
                if i-1 >= 0: errors.append(log[i-1])

                errors.append(log[i])

                if i+1 < len(log): errors.append(log[i+1])
                if i+2 < len(log): errors.append(log[i+2])

                errors[-1] = errors[-1] + '\n'


        summary = list()

        ## Begin summary with runOutName

        summary.append('\n' + self.runOutName)
        summary.append('--------------------------------------------------\n')
        summary.append('Anaylses: %s\n' % len(self.analyses))

        ## Append any bcl2fastq errors to summary
        if errors:
            summary += errors

        ## Append analysis summaries
        for a in self.analyses:
            summary.append('\nAnalysis  ' + a.name + ':')
            summary.append('--------------------------------------------------\n')
            summary += a.summarizeDemuxResults()

        ## Append user letter
        summary.append('\n\nLetter:')
        summary.append('--------------------------------------------------\n')
        letter = self.makeLetter()
        summary += letter
        self.letter = '\n'.join(letter)

        ## Append SampleSheet
        summary.append('\nSampleSheet:')
        summary.append('--------------------------------------------------\n')
        summary += self.SampleSheet.ss
        summary.append('\n')

        self.summary = '\n'.join(summary)

        if self.verbose: 
            print self.summary

        self.notify('%s Demultiplex Summary' % self.runType, self.summary, includeWatchers=True)


    def DBupdate(self):
        if not self.dbStore: return
        if not hasattr(self, 'subIDs'): self.parseSamplesheet()
        command = 'php ' + path.join(settings.SEQPREP_ROOT, 'DBupdate.php')  + ' \\\n' \
                                         + '-r ' + self.runName          + ' \\\n' \
                                         + '-d ' + self.primaryParent    + ' \\\n' \
                                         + '-u ' + ','.join(self.SampleSheet.subIDs) + ' \\\n' \
                                         + '-s 1 \n'  #1 to store values in database
        self.shell(command, self.logFile)


    def warn(self):
        warnings = list()

        for a in self.analyses:
            warnings += a.warnings

        if warnings:
            self.notify('SeqPrep Warning', self.runOutName + '\n\n' + '\n'.join(warnings))


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


    def makeLetter(self):  #nextseq

        if not self.analyses:  
            self.parseSamplesheet()

        analysisStatsPages = ['https://software.rc.fas.harvard.edu/ngsdata/'+self.runOutName+'/'+x.name+'/Reports/html/'+self.flowcell+'/all/all/all/laneBarcode.html' \
                              for x in self.analyses]

        return ['Hi all,\n',
                'The fastq files with the read sequences of run ' + self.runName + ' are available at:\n',
                'https://software.rc.fas.harvard.edu/ngsdata/' + self.runOutName + '\n',
                'or under /n/ngsdata/' + self.runOutName + ' on the cluster.\n',
                'Summary statistics can be found at:\n',
                '\n'.join(analysisStatsPages),
                '\nReads with indices not in SampleSheet.csv are in the fastq file labeled',
                '\'Undetermined_S0.\' We encourage users to download a local copy of their',
                'data, as run data will eventually be removed from the ngsdata server.\n',
                'For more information, please see our FAQ page:\n',
                'http://informatics.fas.harvard.edu/faq\n\nBest,\nChris\n']



class HiSeq(IlluminaNextGen):

    def __init__(self, runName, **kwargs):
        IlluminaNextGen.__init__(self, runName, **kwargs)

        self.runparametersFile = path.join(self.primaryDir, 'runParameters.xml')
        self.runType = 'HiSeq'

        #set selected lanes
        if kwargs and \
                ('lanesStr' in kwargs.keys() and kwargs['lanesStr']) or \
                ('laneStr' in kwargs.keys() and kwargs['laneStr']) :
            if kwargs['lanesStr']: 
                lanesStr = kwargs['lanesStr']
            elif kwargs['laneStr']: 
                lanesStr = kwargs['lanesStr']
            self.selectedLanes = lanesStr.split(',')
        else:
            lanesStr = 'All'

        optionsStr = 'HiSeq Parameters:\n' \
            + 'runparametersFile:    ' + self.runparametersFile + '\n' \
            + 'lanes:                ' + lanesStr + '\n'

        self.log(optionsStr)  #log HiSeq options


    def makeLetter(self):  #hiseq
        return ['Hi all,\n',
                'The fastq files with the read sequences of run ' + self.runName + ' are available at:\n',
                'https://software.rc.fas.harvard.edu/ngsdata/' + self.runOutName + '\n',
                'or under /n/ngsdata/' + self.runOutName + ' on the cluster.\n',
                'Summary statistics can be found in Basecall_Stats/Demultiplex_Stats.htm. Reads',
                'with indices not in the SampleSheet are in the fastq file(s) labeled \'Undetermined.\'\n',
                'We encourage users to download a local copy of their data, as run data will',
                'eventually be removed from the ngsdata server.\n',
                'For more information, please see our FAQ page:\n',
                'http://informatics.fas.harvard.edu/faq\n\nBest,\nChris\n']

