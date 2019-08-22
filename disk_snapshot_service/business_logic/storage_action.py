import os
import uuid

from data_access import models as m


class DiskSnapshotAction(object):

    @staticmethod
    def generate_flag(pid, trace_debug):
        """flag 为不超过255字符的字符串，表明调用者的身份，格式为 "PiD十六进制pid 模块名|创建原因"""

        return "PiD{} {}".format(str(hex(int(pid))), trace_debug)[:255]

    @staticmethod
    def create_disk_snapshot(chain, disk_bytes, raw_flag):
        """long create(ImageSnapshotIdent ident, ImageSnapshotIdents lastSnapshot, long diskByteSize, string flag)"""
        pass

    @staticmethod
    def close_disk_snapshot(raw_handle, ice_endpoint):
        pass

    @staticmethod
    def open_disk_snapshot(acquired_chain, raw_flag):
        pass


class NewPathBase(object):
    def __init__(self, new_create_inst):
        self.new_create_inst = new_create_inst
        self.folder = self.new_create_inst.new_storage_folder

    def path(self) -> str:
        raise NotImplementedError


class NewQcowPathWithParent(NewPathBase):
    """生成新建qcow文件的路径

    需要创建新的文件的情况
        1. 如果与父的disk_bytes不同，
        2. 如果与父的存储目录路径不同，
        3. 如果父文件不是qcow，
        4. 如果父文件正在创建或写入中，

    复用文件
        其他情况
    """

    def __init__(self, new_create_inst, parent_obj):
        super(NewQcowPathWithParent, self).__init__(new_create_inst)
        self.parent_obj = parent_obj
        self.parent_image_path = self.parent_obj['image_path']
        self.parent_folder = os.path.split(self.parent_image_path)[0]

    def path(self) -> str:
        if (self.new_create_inst.new_disk_bytes != self.parent_obj['disk_bytes'] or
                self.new_create_inst.new_storage_folder != self.parent_folder or
                self.parent_obj['type'] != m.SnapshotStorage.TYPE_QCOW or
                self.parent_obj['status'] in m.SnapshotStorage.CREATING_AND_WRITING):

            return os.path.join(self.parent_folder, (self.new_create_inst.new_ident + '.qcow'))

        else:
            return self.parent_image_path


class NewRootQcowPath(NewPathBase):

    def __init__(self, new_create_inst):
        super(NewRootQcowPath, self).__init__(new_create_inst)

    def path(self) -> str:
        """生成一个指定文件夹中的文件名，后缀qcow"""

        return os.path.join(self.folder, (self.new_create_inst.new_ident + '.qcow'))


class NewCdpImagePath(NewPathBase):

    def __init__(self, new_create_inst):
        super(NewCdpImagePath, self).__init__(new_create_inst)

    def path(self) -> str:
        """生成一个指定文件夹中的文件名，后缀cdp"""

        return os.path.join(self.folder, (self.new_create_inst.new_ident + '.cdp'))
