

#fix links from lanes to seqrun:
from seqstats.models import SeqRun, Lane

allLanes = Lane.objects.all()

for myLane in allLanes:
    runName = myLane.run_name
    print "run name: %s" % runName
    run = SeqRun.objects.get(run_name__exact=runName)
    myLane.seqrun = run
    myLane.save()

