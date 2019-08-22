from anytree import LevelOrderIter
from anytree import Node
from anytree import find

from data_access import db_query as db
from business_logic import locker_manager as lm

_locker = lm.LockWithTrace()


class NodeOfDiskSnapshotStorage(Node):
    """磁盘快照存储对象树节点"""

    def __init__(self, storage_obj: dict):
        """storage_obj：从 SnapshotStorage 表中获取到的已经存在的"快照对象"""

        super(NodeOfDiskSnapshotStorage, self).__init__(name=storage_obj['ident'])
        self.storage = storage_obj


class NodeFromJournal(Node):
    """磁盘快照存储创建对象树节点"""

    def __init__(self, storage_inst):
        """storage_inst：从 Journal 表中获取到的"在业务逻辑层已经生成，但在真实磁盘数据I/O层尚未生成的 快照对象"""

        super(NodeFromJournal, self).__init__(name=storage_inst['new_ident'])
        self.storage = storage_inst


class DiskSnapshotStorageTree(object):
    """磁盘快照存储对象树

    :remark:
        将关联的"磁盘快照存储对象"缓存到内存中，提高性能
    """

    def __init__(self):
        self.root_node = None
        self.node_dict = dict()

    def init_root(self, storage_objs):
        """磁盘快照存储对象转换为树节点对象，并加入树中

        :remark:
            这里的storage_objs(磁盘快照存储对象),是从 SnapshotStorage 表获取的
        """

        for obj in storage_objs:
            self.node_dict[obj['ident']] = NodeOfDiskSnapshotStorage(obj)

        for ident, node in self.node_dict.items():
            parent_ident = node.storage_obj['parent_ident']
            if parent_ident:
                node.parent = self.node_dict[parent_ident]
            else:
                assert self.root_node is None
                self.root_node = node

    @staticmethod
    def create_tree_inst(tree_ident):
        """tree_ident所关联的有效快照存储节点，生成树"""

        storage_tree = DiskSnapshotStorageTree()
        storage_objs = db.SnapshotStorageTreeQuery(tree_ident).valid_obj_dicts()
        storage_tree.init_root(storage_objs)
        return storage_tree

    def is_empty(self) -> bool:
        return self.root_node is None

    @property
    def leaves(self):
        if self.root_node is None:
            return
        for leaf in self.root_node.leaves:
            yield leaf

    @property
    def nodes_by_bfs(self):
        if self.root_node is None:
            return
        for node in LevelOrderIter(self.root_node):  # 广度优先
            yield node

    def get_node_by_ident(self, ident):
        """根据ident获取node"""

        if ident:
            assert self.root_node is not None
            return find(
                self.root_node,
                lambda node: node.storage['ident'] == ident if 'ident' in node.storage else node.storage['new_ident'])
            # return find(
            #     self.root_node,
            #     lambda node: node.storage['ident'] == ident if ident in node.storage else 'sdd'
            # )
        else:
            return None

    @staticmethod
    def get_ident_by_node(node):
        """根据node获取ident"""

        assert node
        if isinstance(node, NodeFromJournal):
            return node.storage['new_ident']
        elif isinstance(node, NodeOfDiskSnapshotStorage):
            return node.storage['ident']
        else:
            raise NodeNotExist(f'not exist node : {node}')

    @staticmethod
    def get_locker(trace):

        return _locker.acquire(trace)


class NodeNotExist(Exception):
    pass
