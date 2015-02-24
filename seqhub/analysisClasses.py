import os.path as path
from seqprep import countUndetInd
from seqhub import hUtil
import os, re, glob, traceback
from lxml import etree, html
from abc import ABCMeta, abstractmethod

class IlluminaNextGenAnalysis:

    __metaclass__ = ABCMeta


    @classmethod
    def getInstance(cls, analysisName, run):

        if run.runType == 'HiSeq':
            return HiSeqAnalysis(analysisName, run)
        elif run.runType == 'NextSeq':
            return NextSeqAnalysis(analysisName, run)
        else:
            raise Exception('Unrecognized run type: %s' % run.runType)


    def __init__(self, analysisName, run):
        self.name = analysisName
        self.Run = run

        self.index1Length = None
        self.index2Length = None

        self.subIDs = list()
        self.sampleIDs = list()

        self.ssSampleLines = list()
        self.ssLineIndices = list()

        self.processingDir = None
        self.finishingDir = None
        self.finalDir = None

        self.ssFile = None

        if self.Run.processingDir:
            self.processingDir = path.join(self.Run.processingDir, self.name)
            self.ssFile = path.join(self.Run.processingDir, 'SampleSheet.' + self.name + '.csv')

        if self.Run.finishingDir:
            self.finishingDir = path.join(self.Run.finishingDir, self.name)

        if self.Run.finalDir:
            self.finalDir = path.join(self.Run.finalDir, self.name)



    @abstractmethod
    def writeSamplesheet(self, outDir):
        pass


    @abstractmethod
    def bcl2fastq(self):
        pass


    @abstractmethod
    def gather_analysis_fastq(self):
        pass


    @abstractmethod
    def copy_reports_to_finishing(self):
        pass


    @abstractmethod
    def summarizeDemuxResults(self):
        pass


    def calcCheckSums(self):
        self.Run.log('Calculating checksums...')

        hUtil.md5sum( path.join(self.finishingDir, 'Fastq') )


    def countUndetIndices(self):
        undetFastq = glob.glob(path.join(self.finishingDir, 'Fastq', '*Undetermined*.R1.fastq.gz'))  #index counts should be the same whether tallied from R1 or R2

        if undetFastq:
            undetFastq = undetFastq[0]

            if path.isfile(undetFastq):
                self.Run.log('Tallying undetermined indices...')
                countUndetInd.count(undetFastq, path.join(self.finishingDir, 'QC'))
            else:
                self.Run.log('No undetermined indices found to count.')


    def fastQC(self):

        self.Run.log('Running FastQC...')

        outDir = path.join(self.finishingDir, 'QC')
        hUtil.mkdir_p(outDir)

        fastqDir = path.join(self.finishingDir, 'Fastq')

        for filename in os.listdir(fastqDir):
            if re.match('\S+.fastq.gz', filename) and hUtil.gzNotEmpty( path.join(fastqDir, filename) ):
                command = 'module load centos6/fastqc-0.10.1; fastqc -t 4 --noextract --nogroup -o ' + outDir + ' ' + path.join(fastqDir,filename)

                self.Run.shell(command, self.Run.logFile)


    def validateFinalDir(self):

        self.Run.log('Checking for required files in finalDir ' + self.finalDir + ' ...')

        for item in self.finalDir_requiredItems:
            if not path.isfile(item) and not path.isdir(item):
                raise Exception('Item missing from finalDir: ' + item)

        if len(glob.glob(path.join(self.finalDir, 'Fastq', '*.fastq.gz'))) == 0:
                raise Exception('No fastq.gz files found in finalDir: ' + self.finalDir)



class HiSeqAnalysis(IlluminaNextGenAnalysis):

    def __init__(self, analysisName, Run = None):

        IlluminaNextGenAnalysis.__init__(self, analysisName, Run)
        self.finalDir_requiredItems = ['Fastq', 'QC', 'Basecall_Stats']


    def writeSamplesheet(self, outDir):
        ssFile = path.join(outDir, 'SampleSheet.' + self.name + '.csv')

        with open(ssFile, 'w') as fh:
            fh.write(self.Run.SampleSheet.ss[0] + '\n')  #write out header line 
            fh.write('\n'.join( [self.Run.SampleSheet.ss[x] for x in self.ssLineIndices] ))  #write out lines corresponding to this analysis


    def bcl2fastq(self):  #run bcl2fastq v1.8.3 analysis 

        self.Run.log('Running bcl2fastq...')

        command = 'module load centos6/bcl2fastq-1.8.3 \n' + \
                  'echo "Using configureBclToFastq.pl:"; which configureBclToFastq.pl\n\n'

        outDir = path.join(self.Run.processingDir, self.name)
        inDir = path.join(self.Run.primaryDir, 'Data', 'Intensities', 'BaseCalls')

        if self.Run.customBasesMask:
            basesMask = self.customBasesMask
        else:
            basesMask = self.Run.makeBasesMask(self.index1Length, self.index2Length)

        command += 'configureBclToFastq.pl --input-dir ' + inDir                         + ' \\\n' \
                                       + ' --output-dir ' + outDir                       + ' \\\n' \
                                       + ' --sample-sheet ' + self.ssFile                + ' \\\n' \
                                       + ' --use-bases-mask ' + basesMask                + ' \\\n' \
                                       + ' --mismatches ' + str(self.Run.numMismatches)  + ' \\\n' \
                                       + ' --ignore-missing-stats'                       + ' \\\n'

        if self.Run.ignoreMissingBcl:
            command += ' --ignore-missing-bcl \\\n'

        if self.Run.ignoreMissingControl:
            command += ' --ignore-missing-control \\\n'

        if self.Run.withFailedReads:
            command += ' --with-failed-reads \\\n'

        if self.Run.tileRegex:
            command += ' --tiles ' + self.tileRegex + ' \\\n'

        command += '\n'  #end line continuation                                                                                                                                   
        command += 'cd ' + outDir + '; make -j ' + str(self.Run.numThreads) + '\n'

        self.Run.shell(command, self.Run.logFile)


    def gather_analysis_fastq(self):

        self.Run.log('Concatenating fastq files...')

        #Example file set to concatenate:
        #  SampleA_ACAGTG_L001_R1_001.fastq.gz
        #  SampleA_ACAGTG_L001_R1_002.fastq.gz 
        #  SampleA_ACAGTG_L001_R1_003.fastq.gz 

        projectDir = path.join(self.processingDir, 'Project_Fastq_Files')
        sampDirs = glob.glob( path.join(projectDir, 'Sample_*') )
        undetDir = path.join(self.processingDir, 'Undetermined_indices')
        undetSampDirs = glob.glob( path.join(undetDir, 'Sample_*') )

        outDir = path.join(self.finishingDir, 'Fastq')
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

                        self.Run.log('Concatenating ' + sampLabel + ' R'+readNumStr+' fastq component files:')
                        self.Run.log('\n'.join(['  %s' % path.basename(x) for x in componentFiles]))

                        fout = file(mergeFile, 'wb')

                        for componentFile in componentFiles:
                            fin  = file(componentFile,'rb')
                            while True:
                                data = fin.read(65536)
                                if not data: break
                                fout.write(data)
                            fin.close()

                        fout.close()


    def copy_reports_to_finishing(self):

        self.Run.log('Copying stats files to finishing dir...')

        #find basecall_stats directory to copy 
        items = glob.glob(path.join(self.processingDir, 'Basecall_Stats_*'))

        if items:
            basecallStatsDir = items[0]
            for item in ['Demultiplex_Stats.htm', 'Plots', 'css', 'All.htm', 'IVC.htm']:
                itemPath = path.join(self.processingDir, basecallStatsDir, item)

                if path.isfile(itemPath) or path.isdir(itemPath):
                    newItem = path.join(self.finishingDir, 'Basecall_Stats', item)
                    self.Run.safeCopy( itemPath, newItem )

        #copy analysis samplesheet
        self.Run.safeCopy( self.ssFile, path.join(self.finishingDir, path.basename(self.ssFile)) )

    
    def summarizeDemuxResults(self):
        summary = list()
        summary.append('\n\n  ' + self.name + ':\n')

        statsFile = path.join(self.finalDir, 'Basecall_Stats', 'Demultiplex_Stats.htm')

        if not path.isfile(statsFile):
            summary.append('    No Demultiplex_Stats.htm found in finalDir! Checking finishingDir...\n\n')
            statsFile = path.join(self.finishingDir, 'Basecall_Stats', 'Demultiplex_Stats.htm')

            if not path.isfile(statsFile):
                summary.append('    No Demultiplex_Stats.htm found in finishingDir.\n\n')

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

        return summary



class NextSeqAnalysis(IlluminaNextGenAnalysis):

    def __init__(self, analysisName, Run = None):

        IlluminaNextGenAnalysis.__init__(self, analysisName, Run)
        self.finalDir_requiredItems = ['Fastq', 'QC', 'Reports', 'Stats']

    def writeSamplesheet(self, outDir):
        ssFile = path.join(outDir, 'SampleSheet.' + self.name + '.csv')

        with open(ssFile, 'w') as fh:
            fh.write('\n'.join( self.Run.SampleSheet.ss[:self.Run.SampleSheet.colNamesLineIndex+1] ))  #write out header portion of samplesheet
            fh.write('\n'.join( [self.Run.SampleSheet.ss[x] for x in self.ssLineIndices] ))  #write out lines corresponding to this analysis


    def bcl2fastq(self):

        command = 'source new-modules.sh; module load bcl2fastq2; echo "Using bcl2fastq: "; which bcl2fastq; '

        if self.Run.customBasesMask:
            basesMask = self.Run.customBasesMask
        else:
            basesMask = self.Run.makeBasesMask(self.index1Length, self.index2Length)

        command += 'bcl2fastq --runfolder-dir '      + self.Run.primaryDir \
                          + ' --barcode-mismatches ' + str(self.Run.numMismatches) \
                          + ' --output-dir '         + self.processingDir \
                          + ' --use-bases-mask '     + basesMask

        if self.Run.maskShortAdapterReads:
            command += ' --mask-short-adapter-reads ' + str(self.Run.maskShortAdapterReads)

        if self.Run.minTrimmedReadLength is not None:
            command += ' --minimum-trimmed-read-length ' + str(self.Run.minTrimmedReadLength)  #see README for notes on this parameter                                      

        command += '; '

        self.Run.shell(command, self.Run.logFile)


    def gather_analysis_fastq(self):

        # Merge per-lane fastq.gz files into one fastq.gz file per sample. (Lanes here refer to NextSeq lanes 1-4)

        self.Run.log('Concatenating fastq files...')

        self.Run.clearDir(self.finishingDir)
        os.mkdir(path.join(self.finishingDir,'Fastq'))

        for filename in os.listdir(self.processingDir):
            for readNumStr in ['1', '2']:

                sampNameMatch = re.match('([\S]+)_L001_R'+readNumStr+'_001.fastq.gz', filename)  #example file name: 'mySample_L001_R1_001.fastq.gz'

                if sampNameMatch:
                    sampName = sampNameMatch.groups(1)[0]

                    self.Run.log('Merging ' + sampName + ' R'+readNumStr+' fastq files...')

                    mergeFile = path.join(self.finishingDir,'Fastq',sampName + '.R' + readNumStr + '.fastq.gz')
                    self.Run.safeCopy( path.join(self.processingDir, sampName + '_L001_R' + readNumStr + '_001.fastq.gz'), mergeFile )

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


    def copy_reports_to_finishing(self):

        self.Run.log('Copying stats files to finishing dir...')

        for dirname in ['Stats', 'Reports']:
            src = path.join(self.processingDir, dirname)
            dest = path.join(self.finishingDir, dirname)

            self.Run.safeCopy(src, dest)
                

    def summarizeDemuxResults(self):
        self.Run.log('Summarizing demux results...')

        summary = list()

        statsFile = path.join(self.finalDir, 'Reports', 'html', self.Run.flowcell, 'all', 'all', 'all', 'laneBarcode.html')

        if not path.isfile(statsFile):

            summary.append('    No laneBarecode.html found in finalDir! Checking finishingDir...\n\n')

            statsFile = path.join(self.finishingDir,'Basecall_Stats','Demultiplex_Stats.htm')
            if not path.isfile(statsFile):

                summary.append('    No laneBarecode.html found in finishingDir either.\n\n')

                return summary

        with open(statsFile,'r') as fh: stats = fh.read()
        tree = html.fromstring(stats)
        table = tree.xpath('//table')[2]
        rows = table.xpath('./tr')
        nsamps = (len(rows) - 2)/4.0 #nsamps is number of samples per lane, including the 'unknown' sample of unassigned reads 

        laneStats = OrderedDict()
        sampStats = OrderedDict()
        
        for i in range(2,len(rows)):  #get per-lane and per-sample stats
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
        
        summary.append('\nNumber of samples:  ' + str(int(nsamps - 1)) + '\n')  #subtract 1 to exclude the undetermined sample

        summary += self.formatTable(laneRows)
        summary += self.formatTable(sampRows)

        return summary

