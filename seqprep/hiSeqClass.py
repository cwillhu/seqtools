from seqprep import settings, countUndetInd
from seqprep.baseSeqClass import IlluminaNextGen
from seqhub import hUtil
import os, re, glob, traceback
from os import path
from lxml import html
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict # for python 2.6 and earlier, use backport

class HiSeq(IlluminaNextGen):
    def __init__(self, runName, **kwargs):
        IlluminaNextGen.__init__(self, runName, **kwargs)
        self.runparametersFile = path.join(self.primaryDir, 'runParameters.xml')
        if kwargs and 'lanesStr' in kwargs.keys() and kwargs['lanesStr']: 
            lanesStr = kwargs['lanesStr']
            self.selectedLanes = lanesStr.split(',')
        else:
            lanesStr = 'All'
        optionsStr = 'HiSeq Parameters:\n' \
            + 'runparametersFile:    ' + self.runparametersFile + '\n' \
            + 'lanes:                ' + lanesStr + '\n'

        self.append(optionsStr, self.logFile)  #log HiSeq options

    def bcl2fastq(self):  #run bcl2fastq on each child samplesheet. 
        command = 'module load centos6/bcl2fastq-1.8.3 \n' + \
                  'echo "Using configureBclToFastq.pl:"; which configureBclToFastq.pl\n\n' 
        for analysisName in self.SampleSheet.analyses:
            a = self.SampleSheet.analyses[analysisName]
            ssFile = a['ssFile']
            outDir = path.join(self.processingDir, analysisName)
            inDir = path.join(self.primaryDir, 'Data', 'Intensities', 'BaseCalls')
            if self.customBasesMask:
                basesMask = self.customBasesMask
            else:
                basesMask = self.makeBasesMask(a['index1Length'], a['index2Length'])
            command += 'configureBclToFastq.pl --input-dir ' + inDir                     + ' \\\n' \
                                           + ' --output-dir ' + outDir                   + ' \\\n' \
                                           + ' --sample-sheet ' + ssFile                 + ' \\\n' \
                                           + ' --use-bases-mask ' + basesMask            + ' \\\n' \
                                           + ' --mismatches ' + str(self.numMismatches)  + ' \\\n' \
                                           + ' --ignore-missing-stats'                   + ' \\\n' 
            if self.ignoreMissingBcl:
                command += ' --ignore-missing-bcl \\\n'
            if self.ignoreMissingControl:
                command += ' --ignore-missing-control \\\n'
            if self.withFailedReads:
                command += ' --with-failed-reads \\\n'
            if self.tileRegex:
                command += ' --tiles ' + self.tileRegex + ' \\\n'
            command += '\n'  #end line continuation
            command += 'cd ' + outDir + '; make -j ' + str(self.numThreads) + '\n'
        self.shell(command, self.logFile)

    def copyRunFilesToFinishing(self):
        # Copy SampleSheet.csv, RunInfo.xml, RunParameters.xml files to self.finishingDir
        if not hasattr(self, 'analyses'): self.parseSamplesheet()

        self.append('Copying analyses and stats files to finishing dir...', self.logFile)
        filesToCopy = [self.samplesheetFile, 
                       self.runinfoFile, 
                       self.runparametersFile ]
        for item in filesToCopy:
            newItem = path.join(self.finishingDir, path.basename(item))
            self.safeCopy(item, newItem)

        # For each analysis, copy Basecall_Stats dir and individual samplesheet
        for analysisName in self.SampleSheet.analyses:
            processingAnalysisDir = path.join(self.processingDir,analysisName)
            finishingAnalysisDir = path.join(self.finishingDir,analysisName)

            #find basecall_stats directory to copy
            items = glob.glob(path.join(processingAnalysisDir, 'Basecall_Stats_*'))
            if items:
                basecallStatsDir = items[0]
                for item in ['Demultiplex_Stats.htm', 'Plots', 'css', 'All.htm', 'IVC.htm']:
                    itemPath = path.join(processingAnalysisDir, basecallStatsDir, item)
                    if path.isfile(itemPath) or path.isdir(itemPath):
                        newItem = path.join(finishingAnalysisDir, 'Basecall_Stats', item)
                        self.safeCopy( itemPath, newItem )

            #copy samplesheet for this analysis
            ssFile = self.SampleSheet.analyses[analysisName]['ssFile']
            self.safeCopy( ssFile, path.join(finishingAnalysisDir, path.basename(ssFile)) )
        hUtil.setPermissions(self.finishingDir)

    def gatherFastq(self):
        # Merge fastq.gz files into one fastq.gz file per sample, per lane.
        if not hasattr(self, 'analyses'): self.parseSamplesheet()

        self.append('Concatenating fastq files...', self.logFile)
        self.clearDir(self.finishingDir)
        for analysisName in self.SampleSheet.analyses:
            #Example file set to concatenate: 
            #  SampleA_ACAGTG_L001_R1_001.fastq.gz
            #  SampleA_ACAGTG_L001_R1_002.fastq.gz
            #  SampleA_ACAGTG_L001_R1_003.fastq.gz
            projectDir = path.join(self.processingDir, analysisName, 'Project_Fastq_Files')
            sampDirs = glob.glob( path.join(projectDir, 'Sample_*') )
            undetDir = path.join(self.processingDir, analysisName, 'Undetermined_indices')
            undetSampDirs = glob.glob( path.join(undetDir, 'Sample_*') )

            outDir = path.join(self.finishingDir, analysisName, 'Fastq')
            hUtil.mkdir_p(outDir)
            for sampDir in sampDirs + undetSampDirs:
                for filename in os.listdir(sampDir):
                    for readNumStr in ['1', '2']:
                        labelMatch = re.match('(?P<fileLabel>(?P<sampLabel>[\S]+)_L[0-9]+_R'+readNumStr+')_001.fastq.gz', filename)
                        if labelMatch:
                            sampLabel = labelMatch.group('sampLabel')
                            fileLabel = labelMatch.group('fileLabel')
                            mergeFile = path.join(outDir, sampLabel + '.R' + readNumStr + '.fastq.gz')
                            componentFiles = sorted([path.join(projectDir, sampDir, f) for f in os.listdir(sampDir) if re.match(fileLabel + '_[0-9]+.fastq.gz', f)])
                            self.append('Concatenating ' + sampLabel + ' R'+readNumStr+' fastq component files:', self.logFile)
                            self.append('\n'.join(['  %s' % path.basename(x) for x in componentFiles]), self.logFile)
                            fout = file(mergeFile, 'wb')
                            for componentFile in componentFiles:
                                fin  = file(componentFile,'rb')
                                while True:
                                    data = fin.read(65536)
                                    if not data: break
                                    fout.write(data)
                                fin.close()
                            fout.close()
        self.copyRunFilesToFinishing()

    def countUndetIndices(self):
        if not hasattr(self, 'analyses'): self.parseSamplesheet()
        for analysisName in self.SampleSheet.analyses:
            undetFastq = glob.glob(path.join(self.finishingDir, analysisName, 'Fastq', '*Undetermined*.R1.fastq.gz'))
            if undetFastq:
                undetFastq = undetFastq[0]
                if path.isfile(undetFastq):
                    self.append('Tallying undetermined indices...', self.logFile)
                    countUndetInd.count(undetFastq, path.join(self.finishingDir, analysisName, 'QC'))
                else:
                    self.append('No undetermined indices found to count.', self.logFile)                    

    def fastQC(self):
        if not hasattr(self, 'analyses'): self.parseSamplesheet()
        self.append('Running FastQC...', self.logFile)
        for analysisName in self.SampleSheet.analyses:
            projectDir = path.join(self.finishingDir, analysisName, 'Fastq')
            outDir = path.join(self.finishingDir, analysisName, 'QC')
            if not path.isdir(outDir):
                hUtil.mkdir_p(outDir)
            for filename in os.listdir(projectDir):
                if re.match('\S+.fastq.gz', filename) and self.gzNotEmpty(path.join(projectDir,filename)):
                    command = 'echo "Analysis name: %s"; ' % analysisName
                    command += 'module load centos6/fastqc-0.10.1; fastqc -t 4 --noextract --nogroup -o ' + outDir + ' ' + path.join(projectDir,filename)
                    self.shell(command, self.logFile)
                
    def calcCheckSums(self):
        self.append('Calculating checksums...', self.logFile)
        for analysisName in self.SampleSheet.analyses:
            self.md5sum( path.join(self.finishingDir, analysisName, 'Fastq') )

    def validateFinalDir(self):
        if not hasattr(self, 'analyses'): self.parseSamplesheet()
        self.append('Checking for required files in finalDir ' + self.finalDir + ' ...', self.logFile)
        requiredItems = ['runParameters.xml', 'RunInfo.xml', 'SampleSheet.csv']
        if len(hUtil.intersect(os.listdir(self.finalDir), requiredItems)) < 3:
            raise Exception('One or more files missing from finalDir ' + self.finalDir + ': ' + ', '.join(requiredItems))
        for analysisName in self.SampleSheet.analyses:
            analysisDir = path.join(self.finalDir, analysisName)
            requiredItems = [path.join(analysisDir,x) for x in ['Fastq', 'QC', 'Basecall_Stats']]
            for item in requiredItems:
                if not path.isfile(item) and not path.isdir(item):
                    raise Exception('Item missing from ' + analysisDir + ': ' + item)
            if len(glob.glob(path.join(analysisDir, 'Fastq', '*.fastq.gz'))) == 0:
                raise Exception('No fastq.gz files found in ' + analysisDir)

    def summarizeDemuxResults(self, writeFiles = True):
        if not hasattr(self, 'analyses'): self.parseSamplesheet()
        if writeFiles: self.append('Scanning log and summarizing demux statistics...', self.logFile)
        with open(self.logFile,'r') as fh:
            log = fh.readlines()
        summary = list()
        for i, line in enumerate(log):
            if re.search(r'error|exception|inconsistent| failed|failed |negative number of base', line, flags=re.IGNORECASE):
                summary.append('\n_____Error found in '+self.logFile+' at line '+i+':_____\n')
                if i-2 >= 0: summary.append(log[i-2])
                if i-1 >= 0: summary.append(log[i-1])
                summary.append(log[i])
                if i+1 < len(log): summary.append(log[i+1])
                if i+2 < len(log): summary.append(log[i+2])
                summary[-1] = summary[-1] + '\n\n'
        for analysisName in self.SampleSheet.analyses:
            summary.append('\n\n  ' + analysisName + ':\n')
            statsFile = path.join(self.finalDir, analysisName, 'Basecall_Stats', 'Demultiplex_Stats.htm')
            if not path.isfile(statsFile):
                summary.append('    No Demultiplex_Stats.htm found in finalDir! Checking finishingDir...\n\n')
                statsFile = path.join(self.finishingDir, analysisName, 'Basecall_Stats', 'Demultiplex_Stats.htm')
                if not path.isfile(statsFile):
                    summary.append('    No Demultiplex_Stats.htm found in finishingDir either.\n\n')
                    continue
            with open(statsFile,'r') as fh:
                stats = fh.read()
            stats = re.sub(r'% of &gt;= Q30 Bases \(PF\)','% bases Q ge 30',stats)
            srows = list()
            tree = html.fromstring(stats)
            for tables in tree.xpath('//table')[0:2]:
                for row in tables.xpath('./tr'):
                    cols = row.xpath('./th | ./td');
                    samp = cols[1].text
                    reads = cols[9].text
                    perc = cols[13].text
                    srows.append([samp, reads, perc])
            scols = zip(*srows)
            colWidths = [ max( [len(elem) if elem else 0 for elem in col] ) for col in scols ]
            rowFormat = '    ' + '  |  '.join(['%%%ds' % width for width in colWidths ]) + '\n'
            for row in srows:
                summary.append(rowFormat % tuple(row))
        self.demuxSummary = ''.join(summary)
        self.letter = ''.join(('\n\nHi all,\n\n',
                               'The fastq files with the read sequences of run ' + self.runName + ' are available at:\n\n',
                               'https://software.rc.fas.harvard.edu/ngsdata/' + self.runOutName + '\n\n',
                               'or under /n/ngsdata/' + self.runOutName + ' on the cluster.\n\n',
                               'Summary statistics can be found in Basecall_Stats/Demultiplex_Stats.htm. Reads\n',
                               'with indices not in the SampleSheet are in the fastq file(s) labeled \'Undetermined.\'\n\n',
                               'We encourage users to download a local copy of their data, as run data will\n',
                               'eventually be removed from the ngsdata server.\n\nBest,\nChris\n\n'))
        if self.verbose: print self.demuxSummary + '\n\n' + self.letter
        self.notify('HiSeq Demultiplex Summary', self.runOutName + '\n\n' + self.demuxSummary + '\n\n' + self.letter, includeWatchers=True)

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
