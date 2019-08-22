from data_access import models as m
from business_logic.storage_tree import tree as st


class ApplyCreateBase(object):
    """虚拟节点应用到树 基类"""

    def __init__(self, storage_tree, inst):
        self.inst = inst
        self.storage_tree = storage_tree
        self.new_node = st.NodeFromJournal(self.inst)
        self.new_ident = self.inst.new_ident

    def apply(self):
        # 新节点挂到树上
        self.storage_tree.node_dict[self.new_ident] = self.new_node


class ApplyNormalCreate(ApplyCreateBase):
    def __init__(self, storage_tree, inst):
        super(ApplyNormalCreate, self).__init__(storage_tree, inst)
        self.parent_ident = self.inst.parent_ident

    @property
    def _parent_node(self):
        if self.parent_ident:
            return self.storage_tree.get_node_by_ident(self.parent_ident)
        else:
            return None

    def apply(self):
        super(ApplyNormalCreate, self).apply()

        # 更新树的从属关系
        if self._parent_node:
            self.new_node.parent = self._parent_node


class ApplyCreateInstFromQcow(ApplyCreateBase):
    def __init__(self, storage_tree, inst):
        super(ApplyCreateInstFromQcow, self).__init__(storage_tree, inst)
        self.source_ident = self.inst.source_ident
        self.source_node = self.storage_tree.get_node_by_ident(self.source_ident)
        self.children_of_source_node = self.source_node.children

    def apply(self):
        super(ApplyCreateInstFromQcow, self).apply()

        # 更新树节点的从属关系
        self.new_node.parent = self.source_node
        self.new_node.children = self.children_of_source_node
        if self.children_of_source_node:
            for child in self.children_of_source_node:
                child.parent = self.new_node
        self.source_node.children = [self.new_node, ]


class ApplyCreateInstFromCdp(ApplyCreateBase):
    def __init__(self, storage_tree, inst):
        super(ApplyCreateInstFromCdp, self).__init__(storage_tree, inst)
        self.last_source_ident = self.inst.source_idents[-1]
        self.last_source_node = self.storage_tree.get_node_by_ident(self.last_source_ident)
        self.children_of_last_source_node = self.last_source_node.children

    def apply(self):
        super(ApplyCreateInstFromCdp, self).apply()

        # 更新树的从属关系
        self.new_node.parent = self.last_source_node
        self.new_node.children = self.children_of_last_source_node
        if self.children_of_last_source_node:
            for child in self.children_of_last_source_node:
                child.parent = self.new_node
        self.last_source_node.children = [self.new_node, ]


_apply_class = {
    m.Journal.TYPE_NORMAL_CREATE: ApplyNormalCreate,
    m.Journal.TYPE_CREATE_FROM_QCOW: ApplyCreateInstFromQcow,
    m.Journal.TYPE_CREATE_FROM_CDP: ApplyCreateInstFromCdp,
}


def _apply_inst_(storage_tree, journal_inst):
    _apply_class[journal_inst.__class__.__name__](storage_tree=storage_tree, inst=journal_inst).apply()
    return storage_tree


class ApplyInTree(object):
    """虚拟节点应用到树接口"""

    def __init__(self, storage_tree_obj, journal_create_insts):
        self.storage_tree_obj = storage_tree_obj
        self.journal_create_insts = journal_create_insts

    def apply(self):
        tree_obj = self.storage_tree_obj
        for inst in self.journal_create_insts:
            _apply_inst_(tree_obj, inst)

        return tree_obj
