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
        self.clearDir(self.logDir)
        self.runparametersFile = path.join(self.primaryDir, "RunParameters.xml")
        self.minTrimmedReadLength = settings.NEXTSEQ["minTrimmedReadLength"]
        self.maskShortAdapterReads = settings.NEXTSEQ["maskShortAdapterReads"]
        self.suppressAdapterTrimming = settings.NEXTSEQ["suppressTrimming"]

        if kwargs:
            if "maskShortAdapterReadsStr" in kwargs.keys():
                self.maskShortAdapterReads = int(kwargs["maskShortAdapterReads"])
            if "minTrimmedReadLength" in kwargs.keys():
                self.minTrimmedReadLength = int(kwargs["minTrimmedReadLength"])
            if "suppressAdapterTrimming" in kwargs.keys():
                self.suppressAdapterTrimming = kwargs["suppressAdapterTrimming"]

        optionsStr = "\nNextSeq Parameters:\n" \
            + "runparametersFile:       "  + self.runparametersFile            + "\n" \
            + "suppressAdapterTrimming: "  + str(self.suppressAdapterTrimming) + "\n" \
            + "minTrimmedReadLength:    "  + str(self.minTrimmedReadLength)    + "\n" \
            + "maskShortAdapterReads:   "  + str(self.maskShortAdapterReads)   + "\n" 

        #save previous logfiles
        hUtil.mkdir_p(self.logDir)
        k = 1
        if path.isfile(self.logFile):
            logBkup = self.logFile + str(k)
            while path.isfile(logBkup):
                k += 1
                logBkup = self.logFile + str(k)
            self.copy(self.logFile, logBkup)
            self.setPermissions(logBkup)
            self.deleteItem(self.logFile)
        self.append(optionsStr, self.logFile)

    def parseSampleSheet(self):
        with open(self.samplesheetFile,"r") as fh:
            ss = fh.readlines()
        d = dict()
        d['nonIndexRead1_numCycles'] = ''
        d['nonIndexRead2_numCycles'] = ''
        d['index1Length'] = ''
        d['index2Length'] = ''
        hasSecondIndexColumn = False
        inSampleData = False
        section = ''
        ss_orig = list()
        for i in range(0,len(ss)):
            blankLineMatch = re.match('[\s,]*$', ss[i]) #matches from beg. of line
            if blankLineMatch: continue
            ss_orig.append(ss[i][:])
            line = ss[i].rstrip()
            vals = line.split(',')
            if vals[0] == '[Header]':
                section = 'Header'
            if vals[0] == '[Reads]':
                section = 'Reads'
            elif vals[0] == '[Settings]':
                section = 'Settings'
            elif vals[0] == '[Data]':
                section = 'Data'
            elif section == 'Header' and vals[0] == 'Description':
                if vals[1]:
                    self.subID = vals[1]
                else:
                    self.subID = ''
                    self.notify("Seqprep Alert","Warning: Missing subID in samplesheet " + self.samplesheetFile + ",  Line " + str(i+1) + ": " + line)
            elif section == 'Reads':
                if vals[0].isdigit():
                    if d['nonIndexRead1_numCycles'] == '':
                        d['nonIndexRead1_numCycles'] = int(vals[0])
                    else:
                        d['nonIndexRead2_numCycles'] = int(vals[0])
            elif section == 'Settings':
                if vals[0] == 'Adapter' or vals[0] == 'AdapterRead2':
                    if self.suppressAdapterTrimming:
                        vals[1] = ''  #delete adapter sequence
                    else:
                        seqMatch = re.match('[AGCT]+$',vals[1])  #matches from beg.
                        if not seqMatch:
                            self.notify("Seqprep Alert","Warning: Missing adapter sequence in samplesheet " + self.samplesheetFile + ",  Line " + str(i+1) + ": " + line)
            elif section == 'Data' and vals[0] == "Sample_ID" and inSampleData == False:
                inSampleData = True
                if "index2" in vals:
                    hasSecondIndexColumn = True  #this column can be present without there being a second index
            elif inSampleData:
                for j in range(0,len(vals)):
                    vals[j] = re.sub(r'[ .)(@-]','_',vals[j]) #replace any illegal characters in sample data with underscores
                if len(vals[0]) == 0 and len(vals[1]) > 0:  #make sure both first and second cols contain the sample names
                    vals[0] == vals[1]
                elif len(vals[1]) == 0 and len(vals[0]) > 0:
                    vals[1] == vals[0]
                elif len(vals[0]) == 0 and len(vals[1]) == 0:
                    self.notify("Seqprep Alert","Warning: Missing sample names in samplesheet " + self.samplesheetFile + ",  Line " + str(i+1) + ": " + line)
                firstIndex = vals[5]
                if firstIndex == '':
                    self.notify("Seqprep Alert","Warning: No index in samplesheet " + self.samplesheetFile + ",  Line " + str(i+1) + ": " + line)
                if d['index1Length'] == '':
                    d['index1Length'] = len(firstIndex)
                elif len(firstIndex) != d['index1Length']:
                    raise Exception("Multiple index1 lengths found in samplesheet")
                if hasSecondIndexColumn:
                    secondIndex = vals[7]
                    if len(secondIndex) > 0 or d['index2Length'] != '':
                        if d['index2Length'] == '':
                            d['index2Length'] = len(secondIndex)
                        elif len(secondIndex) != d['index2Length']:
                            raise Exception("Multiple index2 lengths found in samplesheet")
            ss[i] = ','.join(vals)

        if ss != ss_orig: #if ss is different from original, move old samplesheet to backup file and write new one.
            ssBkupDir = path.join(path.dirname(self.samplesheetFile),"ss")
            hUtil.mkdir_p(ssBkupDir)
            ssBkupFile = path.join(ssBkupDir,"SampleSheet.csv.orig")
            k = 2
            while path.isfile(ssBkupFile):
                ssBkupFile = ssBkupFile + str(k)
                k += 1
            self.copy(self.samplesheetFile, ssBkupFile)
            self.setPermissions(ssBkupFile)
            self.deleteItem(self.samplesheetFile)  #cannot set permissions if someone else is owner. Therefore delete before openning to rewrite
            with open(self.samplesheetFile, "w") as fh:
                fh.write("\n".join(ss))
            self.setPermissions(self.samplesheetFile)
        return d

    def makeBasesMask(self):
        s = self.parseSampleSheet() #example: {'nonIndexRead2_numCycles': '', 'index1Length': 6, 'index2Length': '', 'nonIndexRead1_numCycles': 76}
        r = self.parseRunInfo()     #example: {'Read1': {'num_cycles': 76, 'is_index': 'N'}, 'Read2': {'num_cycles': 7, 'is_index': 'Y'}}
        basesMask = "Y" + str( s['nonIndexRead1_numCycles'] - 1 ) + "N"  #Read1 is never an index

        if "Read2" in r.keys():
            if r["Read2"]['is_index'] == 'Y': #then Read2 is an index
                if s["index1Length"] > 0:
                    basesMask += ',I' + str( s["index1Length"] ) + 'N' * (int(r["Read2"]['num_cycles']) - s["index1Length"])
                else:
                    basesMask += ',' + 'N' * int(r["Read2"]['num_cycles'])
            else: #then Read2 is not an index
                basesMask += ',Y' + str( int(r["Read2"]['num_cycles']) - 1 ) + 'N'
            
            if "Read3" in r.keys():
                if r["Read3"]['is_index'] == 'Y': #then Read3 is an index
                    if s["index2Length"] > 0:
                        basesMask += ',I' + str( s["index2Length"] ) + 'N' * (int(r["Read3"]['num_cycles']) - s["index2Length"])
                    else:
                        basesMask += ',' + 'N' * int(r["Read3"]['num_cycles'])
                else: #then Read3 is not an index
                    basesMask += ',Y' + str( int(r["Read3"]['num_cycles']) - 1 ) + 'N'

                if "Read4" in r.keys(): #Read 4 is never an index
                    basesMask += ',Y' + str( int(r["Read4"]['num_cycles']) - 1 ) + 'N'
        return basesMask

    def bcl2fastq(self):
        self.clearDir(self.processingDir)  #clear output directory
        if self.customBasesMask:
            basesMask = self.customBasesMask
        else:
            basesMask = self.makeBasesMask()
        command = "source new-modules.sh; module load bcl2fastq2; echo 'Using bcl2fastq:'; which bcl2fastq; " \
                         + "bcl2fastq --runfolder-dir " + self.primaryDir \
                         + " --barcode-mismatches " + str(self.numMismatches) \
                         + " --output-dir " + self.processingDir \
                         + " --use-bases-mask " + basesMask
        if self.maskShortAdapterReads:
            command += " --mask-short-adapter-reads " + str(self.maskShortAdapterReads)
        if self.minTrimmedReadLength is not None:
            command += " --minimum-trimmed-read-length " + str(self.minTrimmedReadLength)  #see README for notes on this parameter
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
            self.copy(item, newItem)
        self.setPermissions(self.finishingDir)

    def gatherFastq(self):
        # Merge per-lane fastq.gz files into one fastq.gz file per sample.
        self.append("Concatenating fastq files...", self.logFile)
        self.clearDir(self.finishingDir)
        os.mkdir(path.join(self.finishingDir,'Fastq'))
        for filename in os.listdir(self.processingDir):
            for readNumStr in ['1', '2']:
                sampNameMatch = re.match('([\S]+)_L001_R'+readNumStr+'_001.fastq.gz', filename)  #example file name: "mySample_L001_R1_001.fastq.gz"
                if sampNameMatch:
                    sampName = sampNameMatch.groups(1)[0]
                    self.append("Merging " + sampName + " R"+readNumStr+" fastq files...", self.logFile)
                    mergeFile = path.join(self.finishingDir,'Fastq',sampName + '.R' + readNumStr + '.fastq.gz')
                    self.copy( path.join(self.processingDir, sampName + '_L001_R' + readNumStr + '_001.fastq.gz'), mergeFile )
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
        self.append("Running FastQC...", self.logFile)
        outDir = path.join(self.finishingDir,'QC')
        hUtil.mkdir_p(outDir)
        fastqDir = path.join(self.finishingDir,'Fastq')
        for filename in os.listdir(fastqDir):
            if re.match('\S+.fastq.gz', filename) and self.gzNotEmpty(path.join(fastqDir,filename))::
                command = "module load centos6/fastqc-0.10.1; fastqc -t 4 --noextract --nogroup -o " + outDir + " " + path.join(fastqDir,filename)
                self.shell(command, self.logFile)
                
    def countUndetIndices(self):
        self.append("Tallying undetermined indices...", self.logFile)
        undetFastq = path.join(self.finishingDir, 'Fastq', 'Undetermined_S0.R1.fastq.gz')   #index counts should be the same, whether tallied from R1 or R2 file
        if path.isfile(undetFastq):
            countUndetInd.count(undetFastq, path.join(self.finishingDir,'QC'))

    def calcCheckSums(self):
        self.append("Calculating checksums...", self.logFile)
        self.md5sum(path.join(self.finishingDir,'Fastq'))

    def DBupdate(self):
        if not self.dbStore: return
        if not hasattr(self, 'subID'): self.parseSampleSheet()
        command = "php " + path.join(settings.SEQPREP_ROOT, "DBupdate.php")  + " \\\n" \
                                         + "-r " + self.runName          + " \\\n" \
                                         + "-d " + self.primaryParent    + " \\\n" \
                                         + "-u " + self.subID            + " \\\n" \
                                         + "-s 1 \n"  #1 to store values in database
        self.shell(command, self.logFile)

    def validateFinalDir(self):
        self.append("Checking for required files in finalDir " + self.finalDir + " ...", self.logFile)
        requiredItems = [path.join(self.finalDir,x) for x in ['Fastq', 'QC', 'Reports', 'RunInfo.xml', 'RunParameters.xml', 'SampleSheet.csv', 'Stats']]
        for item in requiredItems:
            if not path.isfile(item) and not path.isdir(item):
                raise("Item missing from finalDir: " + item)
        if len(glob.glob(path.join(self.finalDir, 'Fastq', '*.fastq.gz'))) == 0:
                raise("No fastq.gz files found in finalDir: " + self.finalDir)

    def summarizeDemuxResults(self, writeFiles = True):
        self.append("Summarizing demux results...", self.logFile)
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

        with open(statsFile,"r") as fh: stats = fh.read()
        tree = html.fromstring(stats)
        table = tree.xpath('//table')[2]
        rows = table.xpath('./tr')
        nsamps = (len(rows) - 2)/4.0 #nsamps is number of samples per lane, including the "unknown" sample of unassigned reads 
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
        
        summary.append('\nNumber of samples (including unknown): ' + str(nsamps) + '\n')
        summary += self.formatTable(laneRows)
        summary += self.formatTable(sampRows)
        self.demuxSummary = ''.join(summary)
        self.letter = ''.join(("\n\nHi all,\n\n",
                       "The fastq files with the read sequences of run " + self.runName + " are available at:\n\n",
                       "https://software.rc.fas.harvard.edu/ngsdata/" + self.runOutName + "\n\n",
                       "or under /n/ngsdata/" + self.runOutName + " on the cluster.\n\n",
                       "Summary statistics can be found at:\n\n",
                       "https://software.rc.fas.harvard.edu/ngsdata/"+self.runOutName+"/Reports/html/"+self.flowcell+"/all/all/all/laneBarcode.html\n\n",
                       "Reads with indices not in SampleSheet.csv are in the fastq file labeled\n", 
                       "\"Undetermined_S0.\" We encourage users to download a local copy of their\n",
                       "data, as run data will eventually be removed from the ngsdata server.\n\nBest,\nChris\n\n"))
        if self.verbose:
            print self.demuxSummary + "\n" + self.letter

    def processRun(self):
        try:
            self.logOptions()
            self.bcl2fastq()
            self.gatherFastq()
            self.countUndetIndices()
            self.fastQC()
            self.calcCheckSums()
            self.setPermissions(self.finishingDir)
            self.copyToFinal()
            self.setPermissions(self.finalDir)
            self.validateFinalDir()  
            self.summarizeDemuxResults()  
            self.DBupdate()
            self.notify('Seqprep terminated',self.runOutName + '\n\n' + self.demuxSummary + '\n\n' + self.letter)
        except:
            if not path.isdir(path.dirname(self.logFile)):
                hUtil.mkdir_p(path.dirname(self.logFile))
            errMsg = "Error in " + self.runOutName + ":\n" + traceback.format_exc()
            self.notify('Seqprep Exception', errMsg)
            return
