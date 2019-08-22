import threading


class LockWithTrace(object):

    def __init__(self):
        self._locker = threading.Lock()
        self._current_trace = None

    def acquire(self, trace):
        self._locker.acquire()
        self._current_trace = trace
        return self

    def release(self):
        self._current_trace = None
        self._locker.release()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    @property
    def current_trace(self):
        return self._current_trace
