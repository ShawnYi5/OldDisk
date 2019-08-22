from data_access.db_operation import session
from data_access import models as m


class SnapshotStorageTreeQuery(object):
    """获取 SnapshotStorageTree"""

    def __init__(self, tree_ident):
        self.tree_ident = tree_ident

    def query_valid_objs(self):
        """获取有效的数据"""

        with session.SessionForRead() as s:
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

        with session.SessionForRead() as s:
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

        self.parent_ident = (
            self.parent_storage_obj['parent_ident'] if self.parent_storage_obj else None)
        self.parent_timestamp = (
            self.parent_storage_obj['parent_timestamp'] if self.parent_storage_obj else None)

    def add(self):
        assert self.normal_create_inst['operation_type'] == m.Journal.TYPE_NORMAL_CREATE

        new_storage_info = m.SnapshotStorage(
            ident=self.normal_create_inst.new_ident,
            parent_ident=self.parent_ident,
            parent_timestamp=self.parent_timestamp,
            type=self.normal_create_inst.new_type,
            disk_bytes=self.normal_create_inst.new_disk_bytes,
            status=m.SnapshotStorage.STATUS_CREATING,
            image_path=self.image_path,
            tree_ident=self.tree_ident,
        )

        with session.SessionForReadWrite() as s:
            s.add(new_storage_info)
            s.commit()
            new_storage_obj = (s.query(m.SnapshotStorage)
                               .filter(m.SnapshotStorage.ident == self.normal_create_inst.new_ident)
                               .first())
            return new_storage_obj.obj_to_dict()


class SnapshotStorageQuery(object):
    def __init__(self, ident):
        self.ident = ident

    @property
    def get_obj(self):
        """获取快照对象"""

        with session.SessionForRead() as s:
            return s.query(m.SnapshotStorage).filter(m.SnapshotStorage.ident == self.ident).first()

    @property
    def get_obj_dict(self):
        """快照对象转为字典对象"""

        return self.get_obj.obj_to_dict()

    @property
    def is_ident_exist(self):
        """查询某ident是否存在"""

        with session.SessionForRead() as s:
            if s.query(m.SnapshotStorage).filter(m.SnapshotStorage.ident == self.ident).first():
                return True
            else:
                return False


class UpdateSnapshotStorage(object):
    def __init__(self, ident, column_name, new_data):
        self.ident = ident
        self.column_name = column_name  # 更新的字段名
        self.new_data = new_data  # 更新的数据

    def update(self):
        with session.SessionForReadWrite() as s:
            s.query(m.Journal).filter(m.SnapshotStorage.ident == self.ident).update({self.column_name: self.new_data})
            s.commit()
