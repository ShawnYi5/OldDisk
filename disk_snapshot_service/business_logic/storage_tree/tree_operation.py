from business_logic.storage_tree import tree
from business_logic.storage_tree import tree_apply

from data_access.db_operation import journal
from data_access import models as m


class CreateTree(object):
    """创建完整树(虚拟节点+真实节点)"""

    def __init__(self, tree_ident):
        self.tree_ident = tree_ident

    @property
    def storage_tree(self):
        return tree.DiskSnapshotStorageTree.create_tree_inst(self.tree_ident)

    @property
    def unconsumed_create_insts(self):
        return journal.UnconsumedJournalsQuery(self.tree_ident, m.Journal.JOURNAL_CREATE_TYPES).query_insts()

    @property
    def complete_tree(self):
        return tree_apply.ApplyInTree(self.storage_tree, self.unconsumed_create_insts).apply()


class FetchNodes(object):
    """从ident节点回溯到根，获取树对象中相关联的节点列表"""

    def __init__(self, tree_obj, ident):
        self.tree_obj = tree_obj
        self.ident = ident

    @property
    def node_of_ident(self):
        return self.tree_obj.get_node_by_ident(self.ident)

    def fetch(self):
        return self._find_node_list_(self.node_of_ident)

    @staticmethod
    def _find_node_list_(node):
        """从叶子向根寻找，获取树上相关联的节点列表"""

        node_list = list()

        def _get_node_list(_node):
            """递归获取父节点，插入到node_list"""

            if not _node.parent:
                return _node
            node_list.insert(0, _node.parent)
            return _get_node_list(_node.parent)

        if node.parent is None:
            node_list.insert(0, node)

        else:
            node_list.insert(0, node)
            _get_node_list(node)

        return node_list


class GetStorageFromNode(object):
    """获取节点列表中真实存储节点对象"""

    def __init__(self, nodes):
        self.nodes = nodes

    def get(self):
        for node in self.nodes:
            if isinstance(node, tree.NodeFromJournal):
                self.nodes.remove(node)

        return [node.storage for node in self.nodes]


class FetchStorageForChain(object):
    """获取生成chain的node"""
    def __init__(self, tree_ident, ident):
        self.tree_ident = tree_ident
        self.ident = ident

    @property
    def complete_tree(self):
        return CreateTree(self.tree_ident).complete_tree

    @property
    def fetch_nodes_for_chain(self):
        """从ident节点回溯到根,获取相关的节点"""
        return FetchNodes(self.complete_tree, self.ident).fetch()

    def fetch(self):
        """ 获取节点列表中真实存储节点对象"""
        return GetStorageFromNode(self.fetch_nodes_for_chain).fetch()
