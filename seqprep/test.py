
import pdb; pdb.run('from seqprep import nextSeqClass; a=nextSeqClass.NextSeq("140825_NS500422_0015_AH1235BGXX"); a.bcl2fastq()')


from seqprep import hiSeqClass
b = hiSeqClass.HiSeq("")
b.processRun()

from seqprep import nextSeqClass
b = nextSeqClass.NextSeq("")
b.processRun()

#----------------------------------------


run = "141028_D00365_0368_AHAY0NADXX"
from seqprep import hiSeqClass
b = hiSeqClass.HiSeq(run, verbose = True)
b.postProcess()

from seqprep import nextSeqClass
b = nextSeqClass.NextSeq("141031_NS500422_0049_AH14AVBGXX", verbose=True)
b.summarizeDemuxResults()

from seqprep import nextSeqClass
b = nextSeqClass.NextSeq("141027_NS500422_0045_AH02RCAFXX")
b.processRun()

run = '141018_D00365_0364_BC57K7ANXX'

run = '141017_D00365_0363_AC57WLANXX'
from seqprep import hiSeqClass
b = hiSeqClass.HiSeq(run, suffix = '_2', verbose=True)
b.processRun()

run = '141028_D00365_0369_BHAWYRADXX'
from seqprep import hiSeqClass
b = hiSeqClass.HiSeq(run, verbose=True)
b.postProcess()



import gzip
#filename = '/n/home_rc/cwill/test/St5_3_90_TAGGCATG-CTAAGCCT.R1.fastq.gz'  #empty gz file from /n/ngsdata/141024_D00365_0365_AHAY0KADXX/Lane2...
filename = '/n/home_rc/cwill/test/St5_3_18_TAGGCATG-CTCTCTAT.R1.fastq.gz'  #small gz file
fh = gzip.open(filename, 'rb')
#a = fh.readline()
data = fh.read(100)
print data

from seqprep import hiSeqClass
b = hiSeqClass.HiSeq("141011_D00365_0361_AC4R08ANXX", suffix = "_lane1", lanesStr = "1")
b.processRun()

# Magali's parameters
from seqprep import nextSeqClass
b = nextSeqClass.NextSeq("140923_NS500422_0033_AH13E1BGXX", verbose=True, suppressAdapterTrimming=True, customBasesMask="Y16,I8,Y45N")
b.processRun()

