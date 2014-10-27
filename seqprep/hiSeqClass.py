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
        self.runparametersFile = path.join(self.primaryDir, "runParameters.xml")
        if kwargs and "lanesStr" in kwargs.keys() and kwargs["lanesStr"]: 
            lanesStr = kwargs["lanesStr"]
            self.lanes = lanesStr.split(',')
        else:
            lanesStr = 'All'
            self.lanes = []
        optionsStr = "\nHiSeq Parameters:\n" \
            + "runparametersFile:    " + self.runparametersFile + "\n" \
            + "lanes:                " + lanesStr + "\n"

        #log file setup
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

    def validateSampleSheet(self):
        with open(self.samplesheetFile,"r") as fh:
            lines = fh.readlines()
        origLines = lines[:]

        lines = [re.sub(' ', '',x) for x in lines] #delete spaces
        lines = [x for x in lines if x.rstrip()]  #delete blank lines

        if not re.match('FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,Operator,SampleProject$',lines[0].rstrip()): #matches from beginning
            raise Exception("Unexpected header line in samplesheet %s: %s" % (self.samplesheetFile, lines[0]))

        seenNames = list()
        seenIndices = list()
        for i in range(1,len(lines)): 
            vals = lines[i].rstrip().split(',')
            flowcell = vals[0]
            lane = vals[1]
            sampName = vals[2]
            index = vals[4]
            subID = vals[5]
            indexType = vals[7]

            if flowcell != self.flowcell:
                raise Exception("Flowcell in samplesheet %s, line %s, does not match flowcell in run name: %s" % (self.samplesheetFile, i+1, flowcell))
            if not re.match('[1-8]$', lane):
                raise Exception("Unexpected lane in samplesheet %s, line %s: %s" % (self.samplesheetFile, i+1, lane))
            if not re.match('[AGCT-]+$', index):
                raise Exception("Unexpected index in samplesheet %s, line %s: %s" % (self.samplesheetFile, i+1, index))
            if not re.match('[A-Za-z0-9]+$', subID):
                raise Exception("Unexpected subID in samplesheet %s, line %s: %s" % (self.samplesheetFile, i+1, subID))
            if not re.match('[0-9]+([-_][0-9]+)?$', indexType):
                raise Exception("Unexpected indexType in samplesheet %s, line %s: %s" % (self.samplesheetFile, i+1, indexType))

            if lane + index in seenIndices:
                raise Exception("Duplicate index in samplesheet %s, line %s: %s" % (self.samplesheetFile, i+1, index))
            else:
                seenIndices.append(lane + index)

            sampName = re.sub(r'[- .)(@]','_',sampName) #replace illegal characters in sample name with underscores
            if lane + sampName in seenNames:
                raise Exception("Duplicate sample name in samplesheet %s, line %s: %s" % (self.samplesheetFile, i+1, sampName))
            else:
                seenNames.append(lane + sampName)

            vals[2] = sampName
            lines[i] = ','.join(vals) + '\n'

        if lines != origLines:  #then move old samplesheet to backup file and write new one
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
                fh.write(''.join(lines))
            self.setPermissions(self.samplesheetFile)

    def parseSampleSheet(self, writeFiles = False): #Split samplesheet by lane and index length, and write new samplesheets to processingDir
        self.samplesheets = OrderedDict()
        self.subIDs = set()
        header = "FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,Operator,SampleProject\n"
        seen_index_types = dict()

        def adjustIndex(index, rlen):
            rlen = int(rlen)
            if len(index) < rlen: #then lengthen index
                index += 'A' * (rlen - len(index))
            elif len(index) > rlen: #then shorten index
                index = index1[:rlen]
            return index

        with open(self.samplesheetFile,"r") as fh:
            lines = fh.readlines()
        for i in range(1,len(lines)): #skip header line
            vals = lines[i].split(',')
            lane = vals[1]
            if self.lanes:
                if lane not in self.lanes:
                    continue
            index = vals[4]
            subID = vals[5]
            indexType = vals[7]  #examples: "6" for a 6-base index, "8_8" for two indices, each 8 bases.
            vals[9] = "Fastq_Files"  #reset project field, as this will create uniform output directory name

            if subID:
                self.subIDs.add(subID)

            #Get indices from index string
            iMatch = re.match('(?P<index1>[AGCTagct]+)(-(?P<index2>[AGCTagct]+))?$', index)
            if not iMatch or iMatch.group('index1') is None:
                raise Exception("Missing index in samplesheet " + self.samplesheetFile)
            index1 = iMatch.group('index1')
            index2 = iMatch.group('index2')
            if index2 is None:
                index2 = ''

            if not indexType:
                indexType = str(len(index1))
                if len(index2) > 0:
                    indexType += "_" + str(len(index2))
            else: #adjust index lengths according to specified indexType
                tMatch = re.match('(?P<rlen1>[0-9]+)((_|-)(?P<rlen2>[0-9]+))?$', indexType) # "rlen" denotes "real length"
                if tMatch:
                    rlen1 = tMatch.group("rlen1")  
                    rlen2 = tMatch.group("rlen2")
                    if rlen2 is None:
                        rlen2 = 0
                    index1 = adjustIndex(index1, rlen1)  #shorten or lengthen indices
                    index2 = adjustIndex(index2, rlen2)
                    vals[4] = index1
                    if index2:
                        vals[4] += "-" + index2

            analysisName = "Lane" + lane + ".indexlength_" + indexType
            ssFilename = path.join(self.processingDir, "SampleSheet." + analysisName + ".csv")
            if lane not in seen_index_types.keys() or indexType not in seen_index_types[lane]:
                if lane not in seen_index_types.keys():
                    seen_index_types[lane] = [indexType]
                elif indexType not in seen_index_types[lane]:
                    seen_index_types[lane].append(indexType)
                #start new analysis samplesheet:
                if writeFiles:
                    with open(ssFilename, "w") as myfile:  
                        myfile.write(header)
                self.samplesheets[analysisName] = dict()
                self.samplesheets[analysisName]['ssFile'] = ssFilename
                self.samplesheets[analysisName]['index1Length'] = len(index1)
                self.samplesheets[analysisName]['index2Length'] = len(index2)

            if writeFiles:                
                with open(ssFilename, "a") as myfile:
                    myfile.write(','.join(vals)+"\n")

    def makeBasesMask(self, ssdict):
        r = self.parseRunInfo()     #example: {'Read1': {'num_cycles': 76, 'is_index': 'N'}, 'Read2': {'num_cycles': 7, 'is_index': 'Y'}}  
        basesMask = 'Y' + str( int(r["Read1"]['num_cycles']) - 1 ) + 'N' #Read1 is never an index
        if "Read2" in r.keys():
            if r["Read2"]['is_index'] == 'Y': #then Read2 is an index
                if ssdict["index1Length"] > 0:
                    basesMask += ',I' + str( ssdict["index1Length"] ) + 'N' * (int(r["Read2"]['num_cycles']) - ssdict["index1Length"])
                else:
                    basesMask += ',' + 'N' * int(r["Read2"]['num_cycles'])
            else: #then Read2 is not an index
                basesMask += ',Y' + str( int(r["Read2"]['num_cycles']) - 1 ) + 'N'

            if "Read3" in r.keys():
                if r["Read3"]['is_index'] == 'Y': #then Read3 is an index
                    if ssdict["index2Length"] > 0:
                        basesMask += ',I' + str( ssdict["index2Length"] ) + 'N' * (int(r["Read3"]['num_cycles']) - ssdict["index2Length"])
                    else:
                        basesMask += ',' + 'N' * int(r["Read3"]['num_cycles'])
                else: #then Read3 is not an index
                    basesMask += ',Y' + str( int(r["Read3"]['num_cycles']) - 1 ) + 'N'
                if "Read4" in r.keys(): #Read4 is never an index
                    basesMask += ',Y' + str( int(r["Read4"]['num_cycles']) - 1 ) + 'N'
        return basesMask

    def bcl2fastq(self):
        self.clearDir(self.processingDir)  
        self.validateSampleSheet()
        self.parseSampleSheet(writeFiles = True)

        #run bcl2fastq on each child samplesheet
        command = "module load centos6/bcl2fastq-1.8.3 \n" + \
                  "echo 'Using configureBclToFastq.pl:'; which configureBclToFastq.pl\n\n" 
        for analysisName in self.samplesheets.keys():
            ssFile = self.samplesheets[analysisName]['ssFile']
            outDir = path.join(self.processingDir, analysisName)
            inDir = path.join(self.primaryDir, 'Data', 'Intensities', 'BaseCalls')
            if self.customBasesMask:
                basesMask = self.customBasesMask
            else:
                basesMask = self.makeBasesMask(self.samplesheets[analysisName])
            command += "configureBclToFastq.pl --input-dir " + inDir                     + " \\\n" \
                                           + " --output-dir " + outDir                   + " \\\n" \
                                           + " --sample-sheet " + ssFile                 + " \\\n" \
                                           + " --use-bases-mask " + basesMask            + " \\\n" \
                                           + " --mismatches " + str(self.numMismatches)  + " \\\n" \
                                           + " --ignore-missing-stats"                   + " \\\n" 
            if self.ignoreMissingBcl:
                command += " --ignore-missing-bcl \\\n"
            if self.ignoreMissingControl:
                command += " --ignore-missing-control \\\n"
            if self.withFailedReads:
                command += " --with-failed-reads \\\n"
            if self.tileRegex:
                command += " --tiles " + self.tileRegex + " \\\n"
            command += "\n"  #end line continuation
            command += "cd " + outDir + "; make -j " + str(self.numThreads) + "\n"
        self.shell(command, self.logFile)

    def copyRunFilesToFinishing(self):
        # Copy SampleSheet.csv, RunInfo.xml, RunParameters.xml files to self.finishingDir
        if not hasattr(self, 'samplesheets'): self.parseSampleSheet()

        self.append("Copying samplesheets and stats files to finishing dir...", self.logFile)
        filesToCopy = [self.samplesheetFile, 
                       self.runinfoFile, 
                       self.runparametersFile ]
        for item in filesToCopy:
            newItem = path.join(self.finishingDir, path.basename(item))
            self.copy(item, newItem)

        # For each analysis, copy Basecall_Stats dir and individual samplesheet
        for analysisName in self.samplesheets.keys():
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
                        self.copy( itemPath, newItem )

            #copy samplesheet for this analysis
            ssFile = self.samplesheets[analysisName]['ssFile']
            self.copy( ssFile, path.join(finishingAnalysisDir, path.basename(ssFile)) )
        self.setPermissions(self.finishingDir)

    def gatherFastq(self):
        # Merge fastq.gz files into one fastq.gz file per sample, per lane.
        if not hasattr(self, 'samplesheets'): self.parseSampleSheet()

        self.append("Concatenating fastq files...", self.logFile)
        self.clearDir(self.finishingDir)
        for analysisName in self.samplesheets.keys():
            #Example file set to concatenate: 
            #  SampleA_ACAGTG_L001_R1_001.fastq.gz
            #  SampleA_ACAGTG_L001_R1_002.fastq.gz
            #  SampleA_ACAGTG_L001_R1_003.fastq.gz
            projectDir = path.join(self.processingDir, analysisName, 'Project_Fastq_Files')
            sampDirs = glob.glob( path.join(projectDir, "Sample_*") )
            undetDir = path.join(self.processingDir, analysisName, 'Undetermined_indices')
            undetSampDirs = glob.glob( path.join(undetDir, "Sample_*") )

            outDir = path.join(self.finishingDir, analysisName, 'Fastq')
            hUtil.mkdir_p(outDir)
            for sampDir in sampDirs + undetSampDirs:
                for filename in os.listdir(sampDir):
                    for readNumStr in ['1', '2']:
                        labelMatch = re.match('(?P<fileLabel>(?P<sampLabel>[\S]+)_L[0-9]+_R'+readNumStr+')_001.fastq.gz', filename)
                        if labelMatch:
                            sampLabel = labelMatch.group('sampLabel')
                            fileLabel = labelMatch.group('fileLabel')
                            self.append("Concatenating " + sampLabel + " R"+readNumStr+" fastq files...", self.logFile)
                            mergeFile = path.join(outDir, sampLabel + '.R' + readNumStr + '.fastq.gz')
                            componentFiles = [path.join(projectDir, sampDir, f) for f in os.listdir(sampDir) if re.match(fileLabel + '_[0-9]+.fastq.gz', f)]
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
        if not hasattr(self, 'samplesheets'): self.parseSampleSheet()
        for analysisName in self.samplesheets.keys():
            undetFastq = glob.glob(path.join(self.finishingDir, analysisName, 'Fastq', '*Undetermined*.R1.fastq.gz'))
            if undetFastq:
                undetFastq = undetFastq[0]
                if path.isfile(undetFastq):
                    self.append("Tallying undetermined indices...", self.logFile)
                    countUndetInd.count(undetFastq, path.join(self.finishingDir, analysisName, 'QC'))
                else:
                    self.append("No undetermined indices found to count.", self.logFile)                    

    def fastQC(self):
        if not hasattr(self, 'samplesheets'): self.parseSampleSheet()
        self.append("Running FastQC...", self.logFile)
        for analysisName in self.samplesheets.keys():
            projectDir = path.join(self.finishingDir, analysisName, 'Fastq')
            outDir = path.join(self.finishingDir, analysisName, 'QC')
            if not path.isdir(outDir):
                hUtil.mkdir_p(outDir)
            for filename in os.listdir(projectDir):
                fastqMatch = re.match('\S+.fastq.gz', filename)
                if fastqMatch:
                    command = "module load centos6/fastqc-0.10.1; fastqc -t 4 --noextract --nogroup -o " + outDir + " " + path.join(projectDir,filename)
                    self.shell(command, self.logFile)
                
    def calcCheckSums(self):
        self.append("Calculating checksums...", self.logFile)
        for analysisName in self.samplesheets.keys():
            self.md5sum( path.join(self.finishingDir, analysisName, 'Fastq') )

    def DBupdate(self):
        if not self.dbStore: return
        if not hasattr(self, 'subIDs'): self.parseSampleSheet()
        command = "php " + path.join(settings.SEQPREP_ROOT, "DBupdate.php")  + " \\\n" \
                                         + "-r " + self.runName          + " \\\n" \
                                         + "-d " + self.primaryParent    + " \\\n" \
                                         + "-u " + ",".join(self.subIDs) + " \\\n" \
                                         + "-s 1 \n"  #1 to store values in database
        self.shell(command, self.logFile)

    def validateFinalDir(self):
        if not hasattr(self, 'samplesheets'): self.parseSampleSheet()
        self.append("Checking for required files in finalDir " + self.finalDir + " ...", self.logFile)
        requiredItems = ["runParameters.xml", "RunInfo.xml", "SampleSheet.csv"]
        if len(hUtil.intersect(os.listdir(self.finalDir), requiredItems)) < 3:
            raise Exception("One or more files missing from finalDir " + self.finalDir + ": " + ', '.join(requiredItems))
        for analysisName in self.samplesheets.keys():
            analysisDir = path.join(self.finalDir,analysisName)
            requiredItems = [path.join(analysisDir,x) for x in ['Fastq', 'QC', 'Basecall_Stats']]
            for item in requiredItems:
                if not path.isfile(item) and not path.isdir(item):
                    raise Exception("Item missing from " + analysisDir + ": " + item)
            if len(glob.glob(path.join(analysisDir, 'Fastq', '*.fastq.gz'))) == 0:
                raise Exception("No fastq.gz files found in " + analysisDir)

    def summarizeDemuxResults(self, writeFiles = True):
        if not hasattr(self, 'samplesheets'): self.parseSampleSheet()
        if writeFiles: self.append("Scanning log and summarizing demux statistics...", self.logFile)
        with open(self.logFile,"r") as fh:
            log = fh.readlines()
        summary = list()
        for i, line in enumerate(log):
            if re.search(r'error|exception|inconsistent| failed|failed |negative number of base', line, flags=re.IGNORECASE):
                summary.append("\n_____Error found in "+self.logFile+" at line "+i+":_____\n")
                if i-2 >= 0: summary.append(log[i-2])
                if i-1 >= 0: summary.append(log[i-1])
                summary.append(log[i])
                if i+1 < len(log): summary.append(log[i+1])
                if i+2 < len(log): summary.append(log[i+2])
                summary[-1] = summary[-1] + "\n\n"
        for analysisName in self.samplesheets.keys():
            summary.append('\n\n  ' + analysisName + ':\n')
            statsFile = path.join(self.finalDir,analysisName,'Basecall_Stats','Demultiplex_Stats.htm')
            if not path.isfile(statsFile):
                summary.append('    No Demultiplex_Stats.htm found in finalDir! Checking finishingDir...\n\n')
                statsFile = path.join(self.finishingDir,analysisName,'Basecall_Stats','Demultiplex_Stats.htm')
                if not path.isfile(statsFile):
                    summary.append('    No Demultiplex_Stats.htm found in finishingDir either.\n\n')
                    continue
            with open(statsFile,"r") as fh:
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
            colWidths = [ max(len(elem) for elem in col) for col in scols ]
            rowFormat = '    ' + '  |  '.join(['%%%ds' % width for width in colWidths ]) + '\n'
            for row in srows:
                summary.append(rowFormat % tuple(row))
            self.demuxSummary = ''.join(summary)
            self.letter = ''.join(("\n\nHi all,\n\n",
                           "The fastq files with the read sequences of run " + self.runName + " are available at:\n\n",
                           "https://software.rc.fas.harvard.edu/ngsdata/" + self.runOutName + "\n\n",
                           "or under /n/ngsdata/" + self.runOutName + " on the cluster.\n\n",
                           "Summary statistics can be found in Basecall_Stats/Demultiplex_Stats.htm. Reads\n",
                           "with indices not in the SampleSheet are in the fastq file(s) labeled \"Undetermined.\"\n\n",
                           "We encourage users to download a local copy of their data, as run data will\n",
                           "eventually be removed from the ngsdata server.\n\nBest,\nChris\n\n"))
            if self.verbose:
                print self.demuxSummary + "\n\n" + self.letter

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
            self.notify('Seqprep terminated',self.runOutName + "\n\n" + self.demuxSummary + "\n\n" + self.letter)
        except:
            if not path.isdir(path.dirname(self.logFile)): 
                hUtil.mkdir_p(path.dirname(self.logFile))
            errMsg = "Error in " + self.runOutName + ":\n" + traceback.format_exc()
            self.notify('Seqprep Exception', errMsg)
            return
