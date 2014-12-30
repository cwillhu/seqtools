
import pdb; pdb.run('from seqprep import nextSeqClass; a=nextSeqClass.NextSeq("140825_NS500422_0015_AH1235BGXX"); a.bcl2fastq()')


from seqprep import hiSeqClass
b = hiSeqClass.HiSeq(run, verbose=True)
b.processRun()

from seqprep import nextSeqClass
b = nextSeqClass.NextSeq(run, verbose=True)
b.processRun()

#----------------------------------------

run = '141223_SN343_0503_AC5K1MACXX'
from seqprep import hiSeqClass
b = hiSeqClass.HiSeq(run, suffix='_2', verbose=True)
b.processRun()


#Gracida's run.
#running in 'second':
run = '141011_D00365_0362_BC4PPLANXX'
from seqprep import hiSeqClass
b = hiSeqClass.HiSeq(run, suffix = "_2", verbose = True)
b.copyToFinal()
b.setPermissions(self.finalDir)
b.validateFinalDir()
b.summarizeDemuxResults()


#running in 'third':
run = "141120_NS500422_0053_AH2GJTBGXX"
from seqprep import nextSeqClass
b = nextSeqClass.NextSeq(run)
b.processRun()

#rerun andres' run. Lane 1 has already been rerun once?.
run = "141011_D00365_0361_AC4R08ANXX"
from seqprep import hiSeqClass
b = hiSeqClass.HiSeq(run, suffix = "_2", verbose=True)
b.processRun()

run = "141119_D00365_0388_AC57TRANXX"
from seqprep import hiSeqClass
b = hiSeqClass.HiSeq(run, verbose = True)
b.processRun()

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

