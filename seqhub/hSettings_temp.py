
#SeqTools Settings

PRIMARY_PARENT = "/n/illumina01/primary_data"

MACHINE_TYPE = { 'SN343'  : 'HiSeq', #map machine id to machine type
                 'D00365' : 'HiSeq',
                 'NS500422' : 'NextSeq' }


SEQ_CRONLOG = "/n/informatics/seq/seq_cronlog/log.txt"

ADDRESS_FILE = "/n/informatics/saved/seqhub_notification_list.txt"  #File containing one line of comma-separated email addresses to be sent notifications

with open(ADDRESS_FILE) as fh:
    NOTIFY_EMAILS = fh.read().rstrip()

