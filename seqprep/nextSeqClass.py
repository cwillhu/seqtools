from seqprep import settings, countUndetInd
from seqprep.baseSeqClass import IlluminaNextGen
from seqhub import hUtil
import os, sys, re, fnmatch, glob, traceback
from lxml import etree, html
from os import path
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict # for python 2.6 and earlier, use backport                                                                                                 
import locale
ignored = locale.setlocale(locale.LC_ALL, '') # empty string for platform's default setting

class NextSeq(IlluminaNextGen):
    def __init__(self, runName, **kwargs):
        IlluminaNextGen.__init__(self, runName, **kwargs)
        self.runparametersFile = path.join(self.primaryDir, 'RunParameters.xml')
        self.minTrimmedReadLength = settings.NEXTSEQ['minTrimmedReadLength']
        self.maskShortAdapterReads = settings.NEXTSEQ['maskShortAdapterReads']

        if kwargs:
            if 'maskShortAdapterReadsStr' in kwargs.keys():
                self.maskShortAdapterReads = int(kwargs['maskShortAdapterReads'])
            if 'minTrimmedReadLength' in kwargs.keys():
                self.minTrimmedReadLength = int(kwargs['minTrimmedReadLength'])

        optionsStr = 'NextSeq Parameters:\n' \
            + 'runparametersFile:       '  + self.runparametersFile            + '\n' \
            + 'minTrimmedReadLength:    '  + str(self.minTrimmedReadLength)    + '\n' \
            + 'maskShortAdapterReads:   '  + str(self.maskShortAdapterReads)   + '\n' 

        self.append(optionsStr, self.logFile)  #log NextSeq options

    def bcl2fastq(self):
        #determine index1 and index2 lengths to use in basesMask (use maximum lengths from all analyses in samplesheet)
        index1Length = 0
        index2Length = 0
        for analName in self.SampleSheet.analyses:
            analysis = self.SampleSheet.analyses[analName]
            if analysis['index1Length'] > index1Length:
                index1Length = analysis['index1Length']
            if analysis['index2Length'] > index2Length:
                index2Length = analysis['index12ength']

        #build bcl2fastq command        
        command = 'source new-modules.sh; module load bcl2fastq2; echo "Using bcl2fastq: "; which bcl2fastq; '
        if self.customBasesMask:
            basesMask = self.customBasesMask
        else:
            basesMask = self.makeBasesMask(index1Length, index2Length)
        command += 'bcl2fastq --runfolder-dir '       + self.primaryDir \
                        + ' --barcode-mismatches ' + str(self.numMismatches) \
                        + ' --output-dir '         + self.processingDir \
                        + ' --use-bases-mask '     + basesMask
        if self.maskShortAdapterReads:
            command += ' --mask-short-adapter-reads ' + str(self.maskShortAdapterReads)
        if self.minTrimmedReadLength is not None:
            command += ' --minimum-trimmed-read-length ' + str(self.minTrimmedReadLength)  #see README for notes on this parameter
        command += '; '
        self.shell(command, self.logFile)

    def copyRunFilesToFinishing(self):
        # Copy SampleSheet.csv, RunInfo.xml, RunParameters.xml files, as well as Stats and Reports directories to self.finishingDir
        items = [self.samplesheetFile, 
                 self.runinfoFile, 
                 self.runparametersFile, 
                 path.join(self.processingDir,'Stats'), 
                 path.join(self.processingDir,'Reports')]
        for item in items:
            newItem = path.join(self.finishingDir, path.basename(item))
            self.safeCopy(item, newItem)
        hUtil.setPermissions(self.finishingDir)

    def gatherFastq(self):
        # Merge per-lane fastq.gz files into one fastq.gz file per sample. (Lanes here refer to NextSeq lanes 1-4)
        self.append('Concatenating fastq files...', self.logFile)
        self.clearDir(self.finishingDir)
        os.mkdir(path.join(self.finishingDir,'Fastq'))
        for filename in os.listdir(self.processingDir):
            for readNumStr in ['1', '2']:
                sampNameMatch = re.match('([\S]+)_L001_R'+readNumStr+'_001.fastq.gz', filename)  #example file name: 'mySample_L001_R1_001.fastq.gz'
                if sampNameMatch:
                    sampName = sampNameMatch.groups(1)[0]
                    self.append('Merging ' + sampName + ' R'+readNumStr+' fastq files...', self.logFile)
                    mergeFile = path.join(self.finishingDir,'Fastq',sampName + '.R' + readNumStr + '.fastq.gz')
                    self.safeCopy( path.join(self.processingDir, sampName + '_L001_R' + readNumStr + '_001.fastq.gz'), mergeFile )
                    fout = file(mergeFile, 'ab')
                    for laneNumStr in ['2','3','4']:
                        fin  = file(path.join(self.processingDir, sampName + '_L00' + laneNumStr + '_R' + readNumStr + '_001.fastq.gz'),'rb')
                        while True:
                            data = fin.read(65536)
                            if not data:
                                break
                            fout.write(data)
                        fin.close()
                    fout.close()
        self.copyRunFilesToFinishing()

    def fastQC(self):
        self.append('Running FastQC...', self.logFile)
        outDir = path.join(self.finishingDir,'QC')
        hUtil.mkdir_p(outDir)
        fastqDir = path.join(self.finishingDir,'Fastq')
        for filename in os.listdir(fastqDir):
            if re.match('\S+.fastq.gz', filename) and self.gzNotEmpty(path.join(fastqDir,filename)):
                command = 'module load centos6/fastqc-0.10.1; fastqc -t 4 --noextract --nogroup -o ' + outDir + ' ' + path.join(fastqDir,filename)
                self.shell(command, self.logFile)
                
    def countUndetIndices(self):
        self.append('Tallying undetermined indices...', self.logFile)
        undetFastq = path.join(self.finishingDir, 'Fastq', 'Undetermined_S0.R1.fastq.gz')   #index counts should be the same, whether tallied from R1 or R2
        if path.isfile(undetFastq):
            countUndetInd.count(undetFastq, path.join(self.finishingDir,'QC'))

    def calcCheckSums(self):
        self.append('Calculating checksums...', self.logFile)
        self.md5sum(path.join(self.finishingDir,'Fastq'))

    def validateFinalDir(self):
        self.append('Checking for required files in finalDir ' + self.finalDir + ' ...', self.logFile)
        requiredItems = [path.join(self.finalDir,x) for x in ['Fastq', 'QC', 'Reports', 'RunInfo.xml', 'RunParameters.xml', 'SampleSheet.csv', 'Stats']]
        for item in requiredItems:
            if not path.isfile(item) and not path.isdir(item):
                raise('Item missing from finalDir: ' + item)
        if len(glob.glob(path.join(self.finalDir, 'Fastq', '*.fastq.gz'))) == 0:
                raise('No fastq.gz files found in finalDir: ' + self.finalDir)

    def summarizeDemuxResults(self):
        self.append('Summarizing demux results...', self.logFile)
        summary = list()
        self.letter = ''
        self.demuxSummary = ''
        statsFile = path.join(self.finalDir, 'Reports', 'html', self.flowcell, 'all', 'all', 'all', 'laneBarcode.html')
        if not path.isfile(statsFile):
            summary.append('    No laneBarecode.html found in finalDir! Checking finishingDir...\n\n')
            statsFile = path.join(self.finishingDir,analysisName,'Basecall_Stats','Demultiplex_Stats.htm')
            if not path.isfile(statsFile):
                summary.append('    No laneBarecode.html found in finishingDir either.\n\n')
                self.demuxSummary = ''.join(summary)
                return

        with open(statsFile,'r') as fh: stats = fh.read()
        tree = html.fromstring(stats)
        table = tree.xpath('//table')[2]
        rows = table.xpath('./tr')
        nsamps = (len(rows) - 2)/4.0 #nsamps is number of samples per lane, including the 'unknown' sample of unassigned reads 
        laneStats = OrderedDict()
        sampStats = OrderedDict()
        #get per-lane and per-sample stats
        for i in range(2,len(rows)):
            row = rows[i]
            cols = row.xpath('./th | ./td');
            lane = cols[0].text
            samp = cols[2].text
            reads = cols[8].text
            perc = cols[11].text
            if not perc: perc = 0  
            if lane not in laneStats.keys():
                laneStats[lane] = {'reads': 0, 'perc': 0}
            laneStats[lane]['reads'] += int(re.sub(',','',reads))
            laneStats[lane]['perc'] += float(perc)/nsamps
            if samp not in sampStats.keys():
                sampStats[samp] = {'reads': 0, 'perc': 0}
            sampStats[samp]['reads'] += int(re.sub(',','',reads))
            sampStats[samp]['perc'] += float(perc)/4.0

        laneRows = [['Lane', 'Reads', '% Bases >= Q30']]  + [[lane, format(laneStats[lane]['reads'], 'n'), '%.2f' % laneStats[lane]['perc']] for lane in laneStats.keys()]
        sampRows = [['Sample', 'Reads', '% Bases >= Q30']] + [[samp, format(sampStats[samp]['reads'], 'n'), '%.2f' % sampStats[samp]['perc']] for samp in sampStats.keys()]
        
        summary.append('\nNumber of samples:  ' + str(int(nsamps - 1)) + '\n')  #subtract 1 to exclude the 'unknown' sample
        summary += self.formatTable(laneRows)
        summary += self.formatTable(sampRows)
        self.demuxSummary = ''.join(summary)
        self.letter = ''.join(('\n\nHi all,\n\n',
                       'The fastq files with the read sequences of run ' + self.runName + ' are available at:\n\n',
                       'https://software.rc.fas.harvard.edu/ngsdata/' + self.runOutName + '\n\n',
                       'or under /n/ngsdata/' + self.runOutName + ' on the cluster.\n\n',
                       'Summary statistics can be found at:\n\n',
                       'https://software.rc.fas.harvard.edu/ngsdata/'+self.runOutName+'/Reports/html/'+self.flowcell+'/all/all/all/laneBarcode.html\n\n',
                       'Reads with indices not in SampleSheet.csv are in the fastq file labeled\n', 
                       '\'Undetermined_S0.\' We encourage users to download a local copy of their\n',
                       'data, as run data will eventually be removed from the ngsdata server.\n\nBest,\nChris\n\n'))
        if self.verbose: print self.demuxSummary + '\n' + self.letter
        self.notify('NextSeq Demultiplex Summary',self.runOutName + '\n\n' + self.demuxSummary + '\n\n' + self.letter, includeWatchers=True)

    def processRun(self):
        try:
            self.clearDir(self.processingDir) 
            self.parseSamplesheet(write_validated=True, write_analysis_samplesheets=True)
            self.bcl2fastq()
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
            if not path.isdir(path.dirname(self.logFile)):
                hUtil.mkdir_p(path.dirname(self.logFile))
            errMsg = 'Error in ' + self.runOutName + ':\n' + traceback.format_exc()
            self.notify('Seqprep Exception', errMsg)
            return
