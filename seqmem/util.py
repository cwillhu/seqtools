import os, calendar, datetime, re
import os.path as path
from os.path import join, getsize
from seqhub import hUtil, hSettings 

def diskPercentUsage(myPath): #for the disk on which myPath resides, report the percent of disk that is currently in use 
    # f_blocks   Total number of blocks on file system in units of f_frsize
    # f_frsize   Fundamental file system block size. 
    # f_bfree    Total number of free blocks.
    # f_bavail   Number of blocks available to non-privileged process
    statvfs = os.statvfs(myPath)

    userAvailBlocks = statvfs.f_bavail
    restrictedBlocks = statvfs.f_bfree - userAvailBlocks
    totalUserBlocks = statvfs.f_blocks - restrictedBlocks
    
    percUsage = 100.0 * (totalUserBlocks - userAvailBlocks)/totalUserBlocks
    return percUsage

def du(myPath): #implements linux du function
    dirsDict = {}
    for root,dirs,files in os.walk(myPath, topdown = False):
        files_size = sum(getsize(join(root, name)) for name in files) # Loop through every non directory file in this directory and sum their sizes
        subdirs_size = sum(dirsDict[join(root,d)] for d in dirs) # Look at all of the subdirectories and add up their sizes from dirsDict
        dirSize = dirsDict[root] = files_size + subdirs_size # store the size of this directory (plus subdirectories) in dirsDict
        #print '%s: %s' % (root, formatBytes(dirSize))
    return dirSize

def formatBytes(num):
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0
    return "%3.1f %s" % (num, 'TB')

def months2days(number_of_months=3):
    c = calendar.Calendar()
    d = datetime.datetime.now()
    total = 0
    for offset in range(0, number_of_months):
        current_month = d.month - offset
        while current_month <= 0:
            current_month = 12 + current_month
        days_in_month = len( filter(lambda x: x != 0, c.itermonthdays(d.year, current_month)))
        total = total + days_in_month
    return total

def modDate(filename): #get modification time. Note: simply editing subfiles/directories does NOT change the mod time of the top directory
    ts = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(ts)

def deleteOldRuns(myDir, maxDays, verbose = False):
    if verbose: print 'Scanning %s ...' % myDir

    if (maxDays < 60 and re.search('ngsdata', myDir)) \
            or (maxDays < 120 and re.search('analysis_in_progress', myDir)):
        raise Exception('Unexpected maxDays, myDir parameters: "%s", "%s"' % (maxDays, myDir))
    elif re.search('analysis_finished', myDir):
        raise Exception('Runs in /n/seqcfs/sequencing/analysis_finished should be deleted manually after retention of >= 2 years')

    for item in os.listdir(myDir):
        numDeleted = 0
        #allow extra chars at end of run name, in case it has a suffix:
        if re.match('[0-9]{6}_[0-9A-Za-z]+_[0-9A-Za-z]+_[0-9A-Za-z]{10}', item) \
                and not re.search('_ToDelete$', item):
            runName = item
            runPath = path.join(myDir,runName)
            if verbose: 'deleteOldRuns: Checking age of %s ...' % runPath
            today = datetime.datetime.today()
            modified_date = modDate(runPath)
            elapsed = today - modified_date
            if elapsed.days > maxDays:  #mark run for deletion  (after debugging, change this to an actual delete)
                newRunPath = path.join(myDir,runName + '_ToDelete')
                if verbose: print '  Renaming %s to %s (elapsed: %s days)' % (runPath, newRunPath, elapsed.days)
                os.rename(runPath, newRunPath)
                numDeleted += 1

    if verbose:
        if numDeleted == 0:
            print 'Found no run folders in %s to delete' % myDir
        else:
            print 'Deleted %s folders in %s' % (numDeleted, myDir)

def scanDiskUsage(dirList, verbose = False):
    message = ''
    for myDir in dirList:
        perc = diskPercentUsage(myDir)
        if verbose: print 'Disk usage for %s: %.2f %%\n' % (myDir, perc)
        if perc > 95.0:
            message += 'Disk usage for %s: %.2f %%\n' % (myDir, perc)

    if message:
        hUtil.email(hSettings.NOTIFY_EMAILS, 'SeqMem Warning: disk usage > 95%', message)
    
