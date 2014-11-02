import subprocess, sys, os, select 
import os.path as path
import shutil, errno

def shquote(text):
    """Return the given text as a single, safe string in sh code.

    Note that this leaves literal newlines alone; sh and bash are fine with 
    that, but other tools may require special handling.
    """
    return "'%s'" % text.replace("'", r"'\''")

def runCmd(cmd):
    """Run shell code and yield stdout lines.
    
    This raises an Exception if exit status is non-zero or stderr is non-empty. 
    Be sure to fully iterate this or you will probably leave orphans.
    """
    BLOCK_SIZE = 4096
    p = subprocess.Popen(
        ['/bin/bash', '-c', cmd],
        shell=False,
        stdin=open('/dev/null', 'r'),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdoutDone, stderrDone = False, False
    out = ''
    while not (stdoutDone and stderrDone):
        rfds, ignored, ignored2 = select.select([p.stdout.fileno(), p.stderr.fileno()], [], [])
        if p.stdout.fileno() in rfds:
            s = os.read(p.stdout.fileno(), BLOCK_SIZE)
            if s=='': stdoutDone = True
            if s:
                i = 0
                j = s.find('\n')
                while j!=-1:
                    yield out + s[i:j+1]
                    out = ''
                    i = j+1
                    j = s.find('\n',i)
                out += s[i:]
        if p.stderr.fileno() in rfds:
            s = os.read(p.stderr.fileno(), BLOCK_SIZE)
            if s=='': stderrDone = True
            if s:
                i = 0
                j = s.find('\n')
                while j!=-1:
                    yield out + s[i:j+1]
                    out = ''
                    i = j+1
                    j = s.find('\n',i)
                out += s[i:]
    if out!='':
        yield out 
    p.wait()

def recursiveChmod(item, filePermissions, dirPermissions):
    if path.isfile(item):
        raise Exception('First input arg "%s" is a file. Expected directory.' % item)
    os.chmod(item, dirPermissions)
    for root,dirs,files in os.walk(item):
        for d in dirs:
            os.chmod(path.join(root,d), dirPermissions)
        for f in files:
            os.chmod(path.join(root,f), filePermissions)

def mkdir_p(myDir):  #implements bash's mkdir -p
    if not path.isdir(myDir):
        os.makedirs(myDir)

def email(addresses, subject, message):
    if isinstance(addresses, basestring): addresses = addresses.split(',')
    for address in addresses:
        for error in runCmd("echo \"" + message + "\"|  mail -s \"" + subject + "\" " + address):
            raise(error)  #if there is anything written to stdout/stderr from this cmd, it will be an error

def append(text, filename, echo = False):
    parentDir = path.dirname(filename)
    if not path.isdir(parentDir):
        mkdir_p(parentDir)
    if echo: print text
    fh = open(filename, 'a')
    fh.write(text + "\n") #append text to file                                                                                                              
    fh.close()

def copy(src, dst):  #copy a file or directory.
    try:
        shutil.copytree(src, dst)
    except OSError as exc:
        if exc.errno == errno.ENOTDIR:
            shutil.copy(src, dst)
        else: raise

def intersect(a, b):
     return list(set(a) & set(b))

def touch(fname):
    open(fname, 'a').close()

