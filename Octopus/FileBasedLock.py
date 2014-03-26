import fcntl

class FileBasedLock(object):
    WRITE_LOCK=fcntl.LOCK_EX
    READ_LOCK=fcntl.LOCK_SH
    def __init__(self, filename , type=None):
        self._fn = filename
        self._f=open(self._fn, 'w')
        self.type=type if type!=None else self.WRITE_LOCK

    def __enter__(self):
        self.aquire()

    def __exit__(self, type, value, traceback):
        self.release()
        
    def aquire(self, type=None):
        if type==None: type=self.type
        fcntl.flock(self._f.fileno(), type)

    def release(self):
        fcntl.flock(self._f.fileno(), fcntl.LOCK_UN)

class FileBasedReadLock(FileBasedLock):
    def __init__(self, filename):
        FileBasedLock.__init__(self, filename, FileBasedLock.READ_LOCK)
