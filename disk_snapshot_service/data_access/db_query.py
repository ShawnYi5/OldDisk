# import json
# from basic_library import xfunctions as xf
#
# import sqlalchemy
# from sqlalchemy import orm
#
# from data_access import models as m
#
# db_connect_str = 'postgresql+psycopg2://postgres:f@127.0.0.1:21115/DiskSnapshotService'
# create_engine = sqlalchemy.create_engine(db_connect_str, echo=False, pool_size=20, )
# session_maker = orm.sessionmaker(bind=create_engine)
#
#
# class SessionForRead(object):
#     """读session"""
#
#     def __init__(self, session=None, close_when_exit=True):
#         if session is None:
#             self.session = session_maker()
#         else:
#             self.session = session
#         self.close_when_exit = close_when_exit
#
#     def __enter__(self):
#         return self.session
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         if self.close_when_exit:
#             self.session.close()
#
#
# class SessionForReadWrite(object):
#     """读写session"""
#
#     def __init__(self, session=None, close_when_exit=True):
#         if session is None:
#             self.session = session_maker()
#         else:
#             self.session = session
#         self.close_when_exit = close_when_exit
#
#     def __enter__(self):
#         return self.session
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         try:
#             if exc_type is None:
#                 self.session.commit()
#             else:
#                 self.session.rollback()
#         finally:
#             if self.close_when_exit:
#                 self.session.close()
#
#
# class SessionWithTrans(object):
#     """带事务的session"""
#
#     def __init__(self):
#         self.session = session_maker()
#         self.session_trans = None
#
#     def __enter__(self):
#         self.session_trans = self.session.begin()
#         return self.session_trans.__enter__().session
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         try:
#             if self.session_trans is not None:
#                 self.session_trans.__exit__(exc_type, exc_val, exc_tb)
#                 self.session_trans = None
#         finally:
#             self.session.close()
#
#
# class JournalNotExist(Exception):
#     pass
#
#
# class JournalQuery(object):
#     """获取 Journal"""
#
#     def __init__(self, token):
#         self.token = token
#
#     def get_obj(self):
#         """获取数据
#
#         :raise
#             JournalNotExist
#         """
#
#         with SessionForRead() as s:
#             obj = s.query(m.Journal).filter(m.Journal.token == self.token).first()
#             if not obj:
#                 raise JournalNotExist(f'not exist token : {self.token}')
#             return obj
#
#     def get_obj_dict(self) -> dict:
#         """获取数据字典
#
#         :raise
#             JournalNotExist
#         """
#
#         return self.get_obj().obj_to_dict()
#
#     def get_inst(self):
#         """获取实例
#
#         :raise
#             JournalNotExist
#         """
#
#         return generate_journal_inst(self.get_obj())
#
#     @staticmethod
#     def get_inst_from_journal_obj(journal_obj):
#         """获取journal_obj的inst"""
#
#         if journal_obj:
#             return generate_journal_inst(journal_obj)
#         raise JournalNotExist(f'not exist journal')
#
#     @staticmethod
#     def update_children_idents(parent_journal_obj, children_idents):
#         with SessionForReadWrite as s:
#             q = s.query(m.Journal).filter(m.Journal.token == parent_journal_obj['token']).first()
#             q.children_idents = children_idents
#             s.commit()
#
#
# class ConsumeJournalsQuery(object):
#     """批量消费Journals"""
#
#     def __init__(self, tokens: list):
#         self.tokens = tokens
#
#     def consume(self):
#         with SessionWithTrans() as s:
#             for token in self.tokens:
#                 s.query(m.Journal).filter(m.Journal.token == token).update(
#                     {"consumed_timestamp": xf.current_timestamp()}
#                 )
#
#
# class UnconsumedJournalsQuery(object):
#     """获取未消费的Journals"""
#
#     def __init__(self, tree_ident=None, journal_types=None):
#         self.tree_ident = tree_ident
#         self.journal_types = journal_types
#
#     def query_objs(self):
#         """获取创建日志"""
#
#         with SessionForRead() as s:
#             q = s.query(m.Journal).filter(m.Journal.consumed_timestamp.is_(None))
#             if self.tree_ident:
#                 q = q.filter(m.Journal.tree_ident == self.tree_ident)
#             if self.journal_types:
#                 q = q.filter(m.Journal.operation_type.in_(self.journal_types))
#             return q.order_by(m.Journal.id).all()
#
#     def journal_objs(self):
#         """objs to dicts"""
#
#         return [obj.obj_to_dict() for obj in self.query_objs()]
#
#     def query_insts(self):
#         """获取为创建实例"""
#
#         result = list()
#         for obj in self.query_objs():
#             result.append(generate_journal_inst(obj))
#         return result


class SnapshotStorageTreeQuery(object):
    """获取 SnapshotStorageTree"""

    def __init__(self, tree_ident):
        self.tree_ident = tree_ident

    def query_valid_objs(self):
        """获取有效的数据"""

        with SessionForRead() as s:
            objs = (s.query(m.SnapshotStorage)
                    .filter(m.SnapshotStorage.tree_ident == self.tree_ident)
                    .filter(m.SnapshotStorage.status.notin_(m.SnapshotStorage.INVALID_STORAGE_STATUS))
                    .all()
                    )
            return objs

    def valid_obj_dicts(self):
        """有效数据字典对象集"""

        return [obj.to_dict for obj in self.query_valid_objs()]

    def query_all_objs(self):
        """获取所有的数据"""

        with SessionForRead() as s:
            objs = (s.query(m.SnapshotStorage)
                    .filter(m.SnapshotStorage.tree_ident == self.tree_ident)
                    .all()
                    )
            return objs

    def all_obj_dict(self):
        """所有数据字典对象集"""

        return [obj.to_dict for obj in self.query_all_objs()]


class SnapshotStorageAdd(object):
    def __init__(self, normal_create_inst, image_path, parent_storage_obj, tree_ident):
        self.normal_create_inst = normal_create_inst
        self.image_path = image_path
        self.parent_storage_obj = parent_storage_obj
        self.tree_ident = tree_ident

    def add(self):
        assert isinstance(self.normal_create_inst, NormalCreateInJournal)

        new_storage_info = m.SnapshotStorage(
            ident=self.normal_create_inst.new_ident,
            parent_ident=self.parent_storage_obj['ident'] if self.parent_storage_obj else None,
            parent_timestamp=self.parent_storage_obj['start_timestamp'] if self.parent_storage_obj else None,
            type=self.normal_create_inst.new_type,
            disk_bytes=self.normal_create_inst.new_disk_bytes,
            status=m.SnapshotStorage.STATUS_CREATING,
            image_path=self.image_path,
            tree_ident=self.tree_ident,
        )

        with SessionForReadWrite() as s:
            s.add(new_storage_info)
            new_storage_obj = (s.query(m.SnapshotStorage)
                               .filter(m.SnapshotStorage.ident == self.normal_create_inst.new_ident)
                               .first()
                               )
            return new_storage_obj.obj_to_dict()


class SnapshotStorageQuery(object):
    def __init__(self, ident):
        self.ident = ident

    @staticmethod
    def update_parent_ident(parent_ident, children_idents):
        with SessionForReadWrite() as s:
            for child_ident in children_idents:
                q = s.query(m.SnapshotStorage).filter(m.SnapshotStorage.ident == child_ident).first()
                q.parent_ident = parent_ident
            s.commit()


    @property
    def get_obj(self):
        """获取快照对象"""

        with SessionForRead() as s:
            return s.query(m.SnapshotStorage).filter(m.SnapshotStorage.ident == self.ident).first()

    @property
    def get_obj_dict(self):
        """快照对象转为字典对象"""

        return self.get_obj.obj_to_dict()

    @property
    def is_ident_exist(self):
        """查询某ident是否存在"""

        with SessionForRead() as s:
            if s.query(m.SnapshotStorage).filter(m.SnapshotStorage.ident == self.ident).first():
                return True
            else:
                return False


# class OperationInJournal(object):
#     """获取操作信息基类"""
#
#     def __init__(self, journal_obj):
#         self.journal_obj = journal_obj
#         self._operation_cache = None
#
#     @property
#     def operation(self) -> dict:
#         if self._operation_cache is None:
#             self._operation_cache = json.loads(self.journal_obj.operation_str)
#         return self._operation_cache
#
#     @property
#     def token(self):
#         return self.journal_obj.token
#
#
# class NormalCreateInJournal(OperationInJournal):
#     """创建普通备份点（QCOW and CDP）信息"""
#
#     def __init__(self, journal_obj):
#         super(NormalCreateInJournal, self).__init__(journal_obj)
#
#     @property
#     def parent_ident(self):
#         return self.operation['parent_ident']
#
#     @property
#     def parent_timestamp(self):
#         return self.operation['parent_timestamp']
#
#     @property
#     def new_ident(self):
#         return self.operation['new_ident']
#
#     @property
#     def new_type(self):
#         return self.operation['new_type']
#
#     @property
#     def new_storage_folder(self):
#         return self.operation['new_storage_folder']
#
#     @property
#     def new_disk_bytes(self):
#         return self.operation['new_disk_bytes']
#
#     @property
#     def new_hash_type(self):
#         return self.operation['new_hash_type']
#
#
# class DestroyInJournal(OperationInJournal):
#     """删除备份点信息"""
#
#     def __init__(self, journal_obj):
#         super(DestroyInJournal, self).__init__(journal_obj)
#
#     @property
#     def idents(self):
#         return self.operation['idents']
#
#
# class CreateFromQcowInJournal(OperationInJournal):
#     """源为QCOW备份点的创建信息"""
#
#     def __init__(self, journal_obj):
#         super(CreateFromQcowInJournal, self).__init__(journal_obj)
#
#     @property
#     def source_ident(self):
#         return self.operation['source_ident']
#
#     @property
#     def new_ident(self):
#         return self.operation['new_ident']
#
#
# class CreateFromCdpInJournal(OperationInJournal):
#     """源为CDP备份点的创建信息"""
#
#     def __init__(self, journal_obj):
#         super(CreateFromCdpInJournal, self).__init__(journal_obj)
#
#     @property
#     def source_idents(self):
#         return self.operation['source_idents']
#
#     @property
#     def new_ident(self):
#         return self.operation['new_ident']
#
#
# _journal_sub_class = {
#     m.Journal.TYPE_NORMAL_CREATE: NormalCreateInJournal,
#     m.Journal.TYPE_DESTROY: DestroyInJournal,
#     m.Journal.TYPE_CREATE_FROM_QCOW: CreateFromQcowInJournal,
#     m.Journal.TYPE_CREATE_FROM_CDP: CreateFromCdpInJournal,
# }
#
#
# # 表驱动
# def generate_journal_inst(journal_obj):
#     if journal_obj:
#         return _journal_sub_class[journal_obj.operation_type](journal_obj)
#     else:
#         return None
