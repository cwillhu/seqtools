import pandas
import re
from lxml import etree

def byLaneStats(tile_df, qual_df):
    codeMap = { 100 : "cluster_density",
                101 : "cluster_density_pf",
                102 : "num_clusters",
                103 : "num_clusters_pf" }

    lanes = dict() 

    ##
    # Get density stats from tile data frame
    ##

    for index, row in tile_df.iterrows():
        if row['code'] in codeMap.keys():
            i = int(row['lane'])
            metricType = codeMap[int(row['code'])]
            if i not in lanes.keys():
                lanes[i] = dict()
            if metricType not in lanes[i].keys():
                lanes[i][metricType] = list()
            lanes[i][metricType].append(row['value'])

    #for each lane, for each metric, average over the tiles
    for i in lanes.keys():
        for metricType in lanes[i].keys():
            m = lanes[i][metricType]
            numTiles = len(m)
            mySum = sum(m)
            if numTiles > 0:
                lanes[i][metricType] = sum(m)/len(m)
            else:
                lanes[i][metricType] = 0
            if "num_tiles" not in lanes[i].keys():
                lanes[i]["num_tiles"] = numTiles

    for i in lanes.keys():
        lanes[i]["percent_pf_clusters"] = 100.0 * lanes[i]["cluster_density_pf"]/lanes[i]["cluster_density"]

    ##
    # Get stats from quality data frame
    ##


    threshold = 30 #threshold quality score 
    # Note: Before changing this threshold consider possible binning of q scores in v.5 format quality metrics file 
    # (See seqstats/packages/illuminate/quality_metrics.py)

    colnames = qual_df.columns.values.tolist()
    qnames = [x for x in colnames if x.startswith('q')]
    qnames_below_threshold = [x for x in qnames if int(x[1:]) <= threshold]
    qnames_above_threshold = [x for x in qnames if int(x[1:]) > threshold]

    for index, row in qual_df.iterrows():
        i = int(row['lane'])
        if "q_below_thresh" not in lanes[i].keys():
            lanes[i]["q_below_thresh"] = sum(row[qnames_below_threshold])
            lanes[i]["q_above_thresh"] = sum(row[qnames_above_threshold])
        else:
            lanes[i]["q_below_thresh"] = lanes[i]["q_below_thresh"] + sum(row[qnames_below_threshold])
            lanes[i]["q_above_thresh"] = lanes[i]["q_above_thresh"] + sum(row[qnames_above_threshold])    

    for i in lanes.keys():
        lanes[i]["perc_q_ge_" + str(threshold)] = 100.0*lanes[i]["q_above_thresh"]/(lanes[i]["q_above_thresh"]+lanes[i]["q_below_thresh"])
        del lanes[i]["q_above_thresh"]
        del lanes[i]["q_below_thresh"]

    return lanes


def parseSampleSheet(samplesheet):
    with open(samplesheet,"r") as fh:
        ss = fh.read()

    ss = re.sub(r'\n[,\s]*\n','\n',ss) #delete any blank lines
    ss = re.sub(r' ','',ss) #delete any spaces
    lines = ss.splitlines()
    ssdict = dict()
    firstval = lines[0].split(',')[0]
    if firstval == 'FCID':  #samplesheet is in HiSeq format
        ssdict['Format'] = 'HiSeq'
        colnames = lines[0].split(',')
        for name in colnames:
            ssdict[name] = list()
        for i in range(1,len(lines)):
            vals = lines[i].split(',')
            for j in range(0,len(vals)): 
                ssdict[colnames[j]].append(vals[j])
    elif firstval == '[Header]': #samplesheet is in NextSeq format
        ssdict['Format'] = 'NextSeq'
        ssdict['Data'] = dict()
        fields=['IEMFileVersion','InvestigatorName','ExperimentName','Date','Workflow','Application','Assay','Description','Chemistry','Adapter','AdapterRead2']
        #Actual sample sheet might not have all these col names, depending on whether there are one or two indices:
        colnames=['Sample_ID','Sample_Name','Sample_Plate','Sample_Well','I7_Index_ID','index','I5_Index_ID','index2','Sample_Project','Description']
        inSampleData = False
        section = ''
        for i in range(0,len(lines)):
            vals = lines[i].split(',')
            if vals[0] == '[Header]' or vals[0] == '[Settings]':
                section = 'Header/Settings'
            elif vals[0] == '[Reads]':
                section = 'Reads'
            elif vals[0] == '[Data]':
                section = 'Data'
            elif section == 'Header/Settings':
                if vals[0] in fields:
                    ssdict[vals[0]] = vals[1]
            elif section == 'Reads':
                if vals[0].isdigit():
                    readLengthStr = str(int(vals[0])-1)  #read length is one fewer than the number of cycles
                    if "Read1_length" not in ssdict.keys():
                        ssdict['Read1_length'] = readLengthStr
                    else:
                        ssdict['Read2_length'] = readLengthStr
            elif section == 'Data' and vals[0] == "Sample_ID" and inSampleData == False:
                inSampleData = True
                for val in vals:
                    if val not in colnames:
                        raise Exception("Unrecognized samplesheet column name: " + val)
                    else:
                        ssdict['Data'][val] = list()
                actual_colnames = vals
            elif inSampleData:
                for j in range(0,len(vals)): 
                    ssdict['Data'][actual_colnames[j]].append(vals[j])

        #Remove samplesheet columns containing only empty fields
        for field in ssdict['Data'].keys():
            if ''.join(ssdict['Data'][field]) == '':
                del ssdict['Data'][field]

    return ssdict


def parseRunInfo(rifile):
    with open (rifile, 'r') as myfile:
        xmlstr=myfile.read().replace('\n', '')

    root = etree.fromstring(xmlstr)
    datetext=root.find('Run/Date').text

    reads = root.find('Run/Reads').getchildren()
    rdict = dict()
    for read in reads:
        read_num = read.attrib['Number']
        rdict["Read" + read_num] = dict()
        rdict["Read" + read_num]['is_index'] = read.attrib['IsIndexedRead']
        rdict["Read" + read_num]['num_cycles'] = read.attrib['NumCycles']

    return rdict, datetext
