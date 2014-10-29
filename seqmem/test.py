from seqmem.util import diskUsage, formatBytes, diskSize

a = diskSize('/n/seqcfs/sequencing/analysis_in_progress') 
formatBytes(a)
