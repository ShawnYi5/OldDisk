import threading

from data_access import models as m
from business_logic import locker_manager as lm

_journal_manager = None
_journal_manager_locker = threading.Lock()


class JournalManager(object):
    def __init__(self):
        self.cache = dict()
        self._locker = lm.LockWithTrace()
        self.journal_create_types = m.Journal.JOURNAL_CREATE_TYPES

    @staticmethod
    def get_journal_manager():
        global _journal_manager

        if _journal_manager is None:
            with _journal_manager_locker:
                if _journal_manager is None:
                    _journal_manager = JournalManager()
        return _journal_manager

    def get_locker(self, trace):
        """获取锁对象

        :remark:
            调用其他接口前都需要保证其进入该锁空间

            调试接口需要支持查询当前锁空间被谁持有，有哪些调用希望获取锁空间
        """

        return self._locker.acquire(trace)
