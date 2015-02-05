from seqhub import hUtil, hSettings
import os, re, shutil, glob, fnmatch, errno, stat, gzip
from os import path
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict # for python 2.6 and earlier, use backport                                                                                                          
class SampleSheet(object):
    def __init__(self, samplesheetFile, suppressAdapterTrimming = False, selectedLanes = None):
        self.file = samplesheetFile
        self.suppressAdapterTrimming = suppressAdapterTrimming
        self.selectedLanes = selectedLanes

        self.analyses = list()
        self.lanes = list()

        self.nonIndexRead1_numCycles = None  # Non-index read lengths are not provided in samplesheet format A.
        self.nonIndexRead2_numCycles = None

        self.warnings = list()

        with open(self.file,'r') as fh:
            ss = fh.readlines()

        ss = [ re.sub('\s', '', x) for x in ss if x.rstrip()]  #delete blank lines and other whitespace
        ss = [ re.sub(r'[)(]', '_', x) for x in ss ]                  #replace illegal characters with underscores
        self.ss = [ re.sub(r'(?<=[AGCTNagtcn]{2})-(?=[AGCTNagtcn]{2})', '_', x) for x in ss]  #replace dashes unless they may be in a dual index
        
        if re.match('FCID',self.ss[0]): #matches from beg.
            self.parseFormatA()
        else:
            self.parseFormatB()

        self.contentString = '\n'.join(self.ss)  #validated samplesheet contents, concatenated into a string

    def write_validatedSamplesheet(self):
        ssBkupDir = path.join(path.dirname(self.file),'ss')
        hUtil.mkdir_p(ssBkupDir)
        ssBkupBase = path.join(ssBkupDir,'SampleSheet.csv.orig')
        ssBkupFile = ssBkupBase
        k = 2
        while path.isfile(ssBkupFile):
            ssBkupFile = ssBkupBase + str(k)
            k += 1
        hUtil.copy(self.file, ssBkupFile)
        hUtil.setPermissions(ssBkupFile)
        hUtil.deleteItem(self.file)  #cannot set permissions if someone else is owner. Therefore delete before openning to rewrite
        with open(self.file, 'w') as fh:
            fh.write('\n'.join(self.ss))
        hUtil.setPermissions(self.file)

    def write_analysisSamplesheets(self, outDir):
        for analName in self.analyses: 
            analSampleSheet = path.join(outDir, 'SampleSheet.' + analName + '.csv')
            with open(analSampleSheet, 'w') as fh:
                if self.format == 'A':
                    fh.write(self.ss[0] + '\n')  #write out header line
                elif self.format == 'B':                    
                    fh.write('\n'.join( self.ss[:self.colNamesLineIndex+1] ))  #write out header portion of samplesheet

                fh.write('\n'.join( [self.ss[x] for x in self.analyses[analName]['lineIndices']] ))  #write out samplesheet lines corresponding to this analysis
            self.analyses[analName]['ssFile'] = analSampleSheet

    def parseFormatB(self):
        self.format = 'B'
        inSampleData = False
        section = ''
        subIDs = set()
        headerSubIDs = set()
        for i in range(len(self.ss)):
            line = self.ss[i]
            vals = line.split(',')
            vDict = None
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
                    for ID in re.split('[-_]', vals[1]):
                        self.validate_subIDChars(ID, i)
                        headerSubIDs.add(ID)
                else:
                    self.warnings.append("Expected subID in Header section's Description field (samplesheet %s, Line %s: %s)" % (self.file, i+1, line))
            elif section == 'Reads':
                if vals[0].isdigit():
                    if self.nonIndexRead1_numCycles is None:
                        self.nonIndexRead1_numCycles = int(vals[0])
                    else:
                        self.nonIndexRead2_numCycles = int(vals[0])
            elif section == 'Settings':
                if vals[0] == 'Adapter' or vals[0] == 'AdapterRead2':
                    if self.suppressAdapterTrimming:
                        vals[1] = ''  #delete adapter sequence
                    elif not re.match('[AGCT]+$',vals[1]):  #matches from beg.
                        self.warnings.append('No adapter sequence found in samplesheet %s, Line %s: %s' % (self.file, i+1, line))
            elif section == 'Data' and inSampleData == False:
                colNames = [x.lower() for x in vals] 
                self.colNamesLineIndex = i
                analyses = OrderedDict()  #store samplesheet info by analysis
                samplesheetLanes = OrderedDict()   #store samplesheet info by lane
                inSampleData = True

                #possible headerline for hiseq:
                #        Lane,Sample_ID,Sample_Plate,Sample_Name,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description
                #possible headerline for nextseq:
                #        Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description

            elif inSampleData:
                vDict = OrderedDict(zip(colNames, vals))  #convert list of values to dict where keys are column names

                if 'lane' in colNames:
                    lane = vDict['lane']
                    if self.selectedLanes and lane not in self.selectedLanes:
                        continue
                else:
                    lane = 'NoLane'

                if 'index' in colNames:
                    index1 = vDict['index']
                    self.validate_indexChars(index1, i)
                else:
                    index1 = ''
                    self.warnings.append('No index1 found in samplesheet %s, Line %s: %s' % (self.file, i+1, line))                        

                if 'index2' in colNames:
                    index2 = vDict['index2']
                    self.validate_indexChars(index2, i)
                else:
                    index2 = ''

                if lane == 'NoLane':
                    laneLabel = '1'
                else:
                    laneLabel = lane
                if laneLabel not in samplesheetLanes:
                    #start new lane dict
                    samplesheetLanes[laneLabel] = dict()
                    samplesheetLanes[laneLabel]['subIDs'] = set()
                    samplesheetLanes[laneLabel]['samples'] = list()
                    samplesheetLanes[laneLabel]['index1Length'] = None
                    samplesheetLanes[laneLabel]['index2Length'] = None

                rlen1 = len(index1)  #initialize "real" index lengths (i.e., without padding)
                rlen2 = len(index2)

                #get real index lengths and submission ID from description column, if present
                if 'description' in colNames and vDict['description']: 
                    elems = re.split('_|-', vDict['description'])
                    for j in range(len(elems)):
                        if re.match('SUB', elems[j], re.IGNORECASE):
                            subID = elems[j]
                            elems.remove(subID)
                            subID = subID.upper()
                            samplesheetLanes[lane]['subIDs'].add(subID)
                    
                    if len(elems) > 2:
                        self.warnings.append('Too many values in description field in samplesheet %s, Line %s: %s' % (self.file, i+1, line))
                    elif len(elems) > 0:
                        rlen1 = elems[0]
                        if len(elems) == 2:
                            rlen2 = elems[1]
                        else:
                            rlen2 = 0
                        
                        #shorten or lengthen indices according to rlen1 and rlen2
                        if rlen1 != len(index1):
                            index1 = self.adjustIndexLength(index1, rlen1)  
                        if rlen2 != len(index2):
                            index2 = self.adjustIndexLength(index2, rlen2)

                lane_index1Length = samplesheetLanes[laneLabel]['index1Length']  #set lane index1 length to maximum of index1 real lengths
                if not lane_index1Length or rlen1 > lane_index1Length:
                    samplesheetLanes[laneLabel]['index1Length'] = rlen1

                lane_index2Length = samplesheetLanes[laneLabel]['index2Length']  #set lane index2 length to maximum of index2 real lengths
                if not lane_index2Length or rlen2 > lane_index2Length:
                    samplesheetLanes[laneLabel]['index2Length'] = rlen2

                indexType = str(rlen1)  #make index type label
                if rlen2 > 0:
                    indexType += '_' + str(rlen2)

                if lane == 'NoLane':
                    analName = 'indexlength_' + indexType  
                else:
                    analName = 'Lane' + lane + '.indexlength_' + indexType  
                if analName not in analyses: 
                    #start new analysis dict
                    analyses[analName] = dict() 
                    analyses[analName]['index1Length'] = rlen1
                    analyses[analName]['index2Length'] = rlen2
                    analyses[analName]['samples'] = list()
                    analyses[analName]['lineIndices'] = list()
                
                #ensure both sample_id and sample_name are set
                if len(vDict['sample_id']) == 0 and len(vDict['sample_name']) > 0:  
                    vDict['sample_id'] == vDict['sample_name']
                elif len(vDict['sample_name']) == 0 and len(vDict['sample_id']) > 0:  
                    vDict['sample_name'] == vDict['sample_id']
                elif len(vDict['sample_name']) == 0 and len(vDict['sample_id']) == 0:
                    self.warnings.append('No Sample_Name or Sample_ID found in samplesheet %s, Line %s: %s' % (self.file, i+1, line))
                    vDict['sample_name'] = 'Sample_' + str(len(analyses[analName]['samples']))
                    vDict['sample_id'] = vDict['sample_name']

                #make sample_id's unique w.r.t. other samples in this analysis (required by bcl2fastq)
                seenIDs = [x['sample_id'] for x in analyses[analName]['samples']]
                if vDict['sample_id'] in seenIDs:
                    k = 2
                    while ('%s_%s' % (vDict['sample_id'], k) in seenIDs):
                        k += 1
                    newID = '%s_%s' % (vDict['sample_id'], k) 
                    vDict['sample_id'] = newID

                vDict['index1'] = index1     # put updated indices in vDict
                vDict['index2'] = index2

                vDict['sample_project'] = '' # suppress Sample_Project field  (changes output directory structure)

                samplesheetLanes[laneLabel]['samples'].append(vDict)  #assign sample to this lane
                analyses[analName]['samples'].append(vDict)           #assign sample to this analysis
                analyses[analName]['lineIndices'].append(i)           #record lines in samplesheet that correspond to this analysis

            # Build new samplesheet line
            if vDict:
                self.ss[i] = ','.join( [vDict[x] for x in colNames] )
            else:
                self.ss[i] = ','.join( vals )

            # Done parsing samplesheet.

        if self.nonIndexRead1_numCycles is None:  
            raise Exception('Non-index read 1 has length zero in samplesheet %s' % (self.file))
        if self.nonIndexRead2_numCycles is None:  
            self.nonIndexRead2_numCycles = 0

        subIDs = set()  #will contain the set of all subIDs in the samplesheet
        for laneLabel in samplesheetLanes:
            laneSubIDs = samplesheetLanes[laneLabel]['subIDs']
            if laneSubIDs:
                subIDs.add(laneSubIDs)
                samplesheetLanes[laneLabel]['subIDs'] = list(laneSubIDs)  #make the set a list
            else:
                samplesheetLanes[laneLabel]['subIDs'] = list(headerSubIDs)  #give the lane the header's subID(s) if no others were provided

        self.subIDs = list(subIDs.union(headerSubIDs))
        self.analyses = analyses
        self.lanes = samplesheetLanes

    def parseFormatA(self): 
        self.format = 'A'

        samplesheetLanes = OrderedDict()
        analyses = OrderedDict()
        subIDs = set()
        seenLaneIndices = list()
        seenLaneSamps = list()

        # Example header line: FCID,Lane,SampleID,SampleRef,Index,Description,Control,Recipe,Operator,SampleProject
        colNames = [x.lower() for x in self.ss[0].rstrip().split(',')] 

        for i in range(1,len(self.ss)): 
            line = self.ss[i].rstrip()
            vDict = OrderedDict(zip(colNames, line.split(',')))  #convert list of values to dict where keys are column names                                                                
            lane = vDict['lane']
            if self.selectedLanes and lane not in self.selectedLanes:
                continue
            sampName = vDict['sampleid']
            index = vDict['index']
            subID = vDict['description']
            indexType = vDict['recipe']            #examples: '6' for a 6-base index; '8_8' for two indices were each 8 is bases.

            vDict['sampleproject'] = 'Fastq_Files'               #reset project field, as this will create uniform output directory stucture for all runs
            vDict['sampleid'] = re.sub(r'[- .)(@]','_',sampName) #replace illegal characters in sample name with underscores                                                       

            # Error checking and validation
            self.validate_indexChars(index, i)
            self.validate_indexTypeChars(indexType, i)
            self.validate_subIDChars(subID, i)

            if not re.match('[1-8]$', lane): #matches from beg.
                raise Exception('Unexpected lane in samplesheet %s, line %s: %s' % (self.file, i+1, line))

            if lane + index in seenLaneIndices:
                raise Exception('Duplicate index in samplesheet %s, line %s: %s' % (self.file, i+1, line))
            else:
                seenLaneIndices.append(lane + index)

            if lane + sampName in seenLaneSamps:
                raise Exception('Duplicate sample in lane %s in samplesheet %s, line %s: %s' % (lane, self.file, i+1, line))
            else:
                seenLaneSamps.append(lane + sampName)

            if subID:
                self.validate_subIDChars(subID, i)
                subIDs.add(subID)

            # Extract index strings from index field
            iMatch = re.match('(?P<index1>[AGCTagct]+)(-(?P<index2>[AGCTagct]+))?$', index)
            if not iMatch or iMatch.group('index1') is None:
                self.warnings.append('No index found in samplesheet %s, line %s: %s' % (self.file, i+1, line))
            index1 = iMatch.group('index1')
            if index1 is None:
                index1 = ''
            index2 = iMatch.group('index2')
            if index2 is None:
                index2 = ''

            if not indexType:  #make indexType label if it was not specified 
                indexType = str(len(index1))
                if len(index2) > 0:
                    indexType += '_' + str(len(index2))
            else:  #adjust index lengths according to specified indexType
                tMatch = re.match('(?P<rlen1>[0-9]+)((_|-)(?P<rlen2>[0-9]+))?$', indexType) # 'rlen' denotes 'real length'
                if tMatch:
                    rlen1 = tMatch.group('rlen1')
                    rlen2 = tMatch.group('rlen2')
                    if rlen2 is None:
                        rlen2 = 0
                    index1 = self.adjustIndexLength(index1, rlen1)  #shorten or lengthen indices
                    index2 = self.adjustIndexLength(index2, rlen2)
                    vDict['index'] = index1  #update index in vDict
                    if index2:
                        vDict['index'] += '-' + index2
            
            # Lane Dict
            if lane not in samplesheetLanes:
                #start new lane dict
                samplesheetLanes[lane] = dict()
                samplesheetLanes[lane]['subIDs'] = set()
                samplesheetLanes[lane]['samples'] = list()
                samplesheetLanes[lane]['index1Length'] = None
                samplesheetLanes[lane]['index2Length'] = None

            lane_index1Length = samplesheetLanes[lane]['index1Length']  #set lane index1 length to maximum of index1 real lengths
            if not lane_index1Length or rlen1 > lane_index1Length:
                samplesheetLanes[lane]['index1Length'] = rlen1

            lane_index2Length = samplesheetLanes[lane]['index2Length']  #set lane index2 length to maximum of index2 real lengths
            if not lane_index2Length or rlen2 > lane_index2Length:
                samplesheetLanes[lane]['index2Length'] = rlen2

            samplesheetLanes[lane]['samples'].append(vDict)  #assign sample to this lane

            # Analysis Dict
            analName = 'Lane' + lane + '.indexlength_' + indexType  
            if analName not in analyses:
                #build new analysis dict
                analyses[analName] = dict()
                analyses[analName]['samples'] = list()
                analyses[analName]['index1Length'] = len(index1)
                analyses[analName]['index2Length'] = len(index2)
                analyses[analName]['lineIndices'] = list()

            analyses[analName]['samples'].append(vDict)           #assign sample to this analysis
            analyses[analName]['lineIndices'].append(i)           #record lines in samplesheet that correspond to this analysis

            # Build new samplesheet line
            self.ss[i] = ','.join( [vDict[x] for x in colNames] )

            # Done parsing samplesheet.

        self.subIDs = list(subIDs)
        self.analyses = analyses
        self.lanes = samplesheetLanes

    def validate_indexChars(self, index, lineIndex):
        if not re.match('[AGCT-]*$', index):
            raise Exception('Unexpected index in samplesheet %s, line %s: %s' % (self.file, lineIndex+1, index))

    def validate_indexTypeChars(self, indexType, lineIndex):
        if not re.match('[0-9]+([-_][0-9]+)?$', indexType):
            raise Exception('Unexpected indexType in samplesheet %s, line %s: %s' % (self.file, lineIndex+1, indexType))

    def validate_subIDChars(self, subID, lineIndex):
        if not re.match('SUB[A-Za-z0-9]+$', subID):
            raise Exception('Unexpected subID in samplesheet %s, line %s: %s' % (self.file, lineIndex+1, subID))

    def adjustIndexLength(self, index, rlen):
        rlen = int(rlen)
        if len(index) < rlen: #then lengthen index
            index += 'A' * (rlen - len(index))
        elif len(index) > rlen: #then shorten index
            index = index[:rlen]
        return index
