import re

#SeqTools Settings

PRIMARY_PARENT = "/n/illumina01/primary_data"

MACHINE_TYPE = { 'SN343'  : 'HiSeq', #map machine id to machine type
                 'D00365' : 'HiSeq',
                 'NS500422' : 'NextSeq' }


USERS_FILE = "/n/informatics/saved/seqhub_users_list.txt"  #File containing comma-separated email addresses to be sent all SeqTools notifications
with open(USERS_FILE) as fh:
    users_string = fh.read().rstrip()
SEQTOOLS_USERS_EMAILS = re.split("[,\s]+", users_string)
