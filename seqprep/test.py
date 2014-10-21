import pdb; pdb.run('from seqprep import nextSeqClass; a=nextSeqClass.NextSeq("140825_NS500422_0015_AH1235BGXX"); a.bcl2fastq()')

from seqprep import hiSeqClass
b = hiSeqClass.HiSeq("")
b.processRun()

from seqprep import nextSeqClass
b = nextSeqClass.NextSeq("")
b.processRun()

# Magali's parameters
from seqprep import nextSeqClass
b = nextSeqClass.NextSeq("140923_NS500422_0033_AH13E1BGXX", verbose=True, suppressAdapterTrimming=True, customBasesMask="Y16,I8,Y45N")
b.processRun()


# sandy test:
from seqprep import hiSeqClass
b = hiSeqClass.HiSeq("140904_D00365_0326_AH9YRCADXX")
b.processRun()

from seqprep import nextSeqClass
b = nextSeqClass.NextSeq("140915_NS500422_0028_AH13EWBGXX")
b.verbose=True
b.summarizeDemuxResults()

