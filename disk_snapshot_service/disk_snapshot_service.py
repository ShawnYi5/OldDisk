from business_logic import storage_reference_manager
from business_logic import journal_manager
from business_logic import handle_pool
from business_logic import storage_action

from business_logic.storage_tree import tree
from business_logic.storage_tree import tree_operation
from business_logic.storage_chain import chain
from business_logic.storage_chain import chain_operation

from data_access import models as m
from data_access.db_operation import journal
from data_access.db_operation import storage

storage_reference_manager = storage_reference_manager.StorageReferenceManager()
journal_manager = journal_manager.JournalManager.get_journal_manager()
handle_manager = handle_pool.HandleManager()


class CreateDiskSnapshotStorage(object):
    """创建磁盘快照存储"""

    def __init__(self, handle: str, token: str, trace_debug: str, caller_pid: int):
        self.token = token
        self.caller_pid = caller_pid
        self.trace_debug = trace_debug
        self.handle = handle

        self.journal_manager = journal_manager
        self.storage_reference_manager = storage_reference_manager
        self.handle_manager = handle_manager

    def __str__(self):
        return f'query chain for creating new snapshot storage : <{self._normal_create_inst.new_ident}>'

    @property
    def caller_name(self):
        return self

    @property
    def _trace_msg(self):
        params = (self._new_ident, self.caller_pid, self.trace_debug, self.handle)
        return 'create new_storage:{},PID:{},trace_debug:{},handle:{}'.format(*params)

    @property
    def _disk_bytes(self):
        return self._normal_create_inst['new_disk_bytes']

    @property
    def _journal_obj(self):
        journal_obj = journal.JournalQuery(self.token).get_obj()
        assert journal_obj['operation_type'] == m.Journal.TYPE_NORMAL_CREATE

        journal.ConsumeJournalsQuery([self.token, ]).consume()
        return journal_obj

    @property
    def _normal_create_inst(self):
        return journal.generate_create_inst(self._journal_obj)

    @property
    def _new_ident(self) -> str:
        return self._normal_create_inst['new_ident']

    @property
    def _is_root_node(self) -> bool:
        if self._normal_create_inst['parent_ident']:
            return False
        return True

    @property
    def _is_cdp_type(self) -> bool:
        if self._normal_create_inst['new_type'] == 'cdp':
            return True
        return False

    @property
    def _tree_ident(self):
        return self._journal_obj['tree_ident']

    @property
    def _parent_ident(self):
        if self._is_root_node:
            return None
        else:
            return self._normal_create_inst['parent_ident']

    @property
    def _storages_for_chain(self):
        """生成chain所依赖的真实存储节点"""

        if self._is_root_node:
            return None
        else:
            nodes_for_chain = tree_operation.FetchNodes(self._tree_ident, self._parent_ident).fetch()
            storages_for_chain = tree_operation.GetStorageFromNode(nodes_for_chain).get()
            return storages_for_chain

    @property
    def _image_path(self):
        if self._is_root_node:
            image_path = storage_action.NewRootQcowPath(self._normal_create_inst).path()
        else:
            if self._is_cdp_type:
                image_path = storage_action.NewCdpImagePath(self._normal_create_inst).path()
            else:
                image_path = storage_action.NewQcowPathWithParent(
                    self._normal_create_inst, self._storages_for_chain[-1]).path()
        return image_path

    @property
    def _unconsumed_create_journals(self):
        """未消费的/创建型 日志"""

        return journal.UnconsumedJournalsQuery(tree_ident=self._tree_ident,
                                               journal_types=m.Journal.JOURNAL_CREATE_TYPES
                                               ).query_insts()

    @property
    def _relied_storage_obj(self):
        """当前创建依赖的真实存储节点"""

        if self._is_root_node:
            parent_storage_obj = None
        else:
            if self._storages_for_chain:
                parent_storage_obj = self._storages_for_chain[-1]
            else:
                parent_storage_obj = None

        return parent_storage_obj

    @property
    def _new_storage_obj(self):
        """创建新快照点，返回新创建的快照对象"""

        params = self._normal_create_inst, self._image_path, self._relied_storage_obj, self._tree_ident
        return storage.SnapshotStorageAdd(*params).add()

    def update_children_parent(self):
        """更新子节点的 parent_ident """

        children_idents = list(self._journal_obj['children_idents'])
        new_ident = self._new_ident
        if children_idents:
            for child_ident in children_idents:
                storage.UpdateSnapshotStorage(child_ident, 'parent_ident', new_ident)

    def update_parent_journal(self):
        """若父节点为虚拟点，则更新父日志表的 children_idents 字段"""

        unconsumed_create_journals = self._unconsumed_create_journals
        if unconsumed_create_journals:
            for j in unconsumed_create_journals:
                # inst = journal.JournalQuery.get_inst_from_journal_obj(j)
                token = j.token
                inst = journal.JournalQuery(token).get_inst()
                inst_ident = inst.new_ident
                if inst_ident == self._parent_ident:
                    parent_journal_token = j['token']
                    children_idents_of_parent_journal = j['children_idents']
                    new_data = str(list(children_idents_of_parent_journal).append(self._new_ident))
                    journal.UpdateJournal(parent_journal_token, 'children_idents', new_data)

    @property
    def _acquired_chain(self):
        # new_storage_obj 添加到 storages_for_chain
        storages = self._storages_for_chain.append(self._new_storage_obj)
        parameter = (self.storage_reference_manager, self.caller_name, storages, chain.StorageChainForRW)
        return chain_operation.GenerateChain(*parameter).acquired_chain

    def _generate_handle(self):
        with self.journal_manager.get_locker(self._trace_msg):
            with tree.DiskSnapshotStorageTree.get_locker(self._trace_msg):
                acquired_chain = self._acquired_chain
                self.update_parent_journal()  # 更新父日志表的 children_idents 字段
                self.update_children_parent()  # 如果当前普通创建的父为普通创建，则更新父日志表的children字段
                return self.handle_manager.generate_write_handle(acquired_chain, self.handle)

    def _generate_raw_flag(self) -> str:
        return storage_action.DiskSnapshotAction.generate_flag(self.caller_pid, self.trace_debug)

    def execute(self):
        handle_inst = self._generate_handle()
        raw_flag = self._generate_raw_flag()
        handle_inst.raw_handle, handle_inst.ice_endpoint = (
            storage_action.DiskSnapshotAction.create_disk_snapshot(handle_inst.storage_chain, self._disk_bytes,
                                                                   raw_flag))

        return {'raw_handle': handle_inst.raw_handle, 'ice_endpoint': handle_inst.ice_endpoint}


class CloseDiskSnapshotStorage(object):
    """关闭磁盘快照"""

    def __init__(self, handle: str):
        self.handle = handle
        self.handle_manager = handle_manager

    def execute(self):
        handle_inst = self.handle_manager.cache.pop(self.handle)
        if not handle_inst:
            raise HandleNotExist(f'handle ({handle_inst}) not exists')

        # if create_handle:
        #     pass
        # else: # open_handle
        #     pass

        storage_action.DiskSnapshotAction.close_disk_snapshot(handle_inst.raw_handle, handle_inst.ice_endpoint)
        handle_inst.storage_chain.release()


class HandleNotExist(Exception):
    pass


class OpenDiskSnapshotStorage(object):
    def __init__(self, storage_ident: str, tree_ident, caller_pid: int, trace_debug: str, handle: str, timestamp=None):
        self.trace_debug = trace_debug
        self.tree_ident = tree_ident
        self.caller_pid = caller_pid
        self.handle = handle
        self.timestamp = timestamp
        self.storage_ident = storage_ident

        self.journal_manager = journal_manager
        self.handle_manager = handle_manager
        self.storage_reference_manager = storage_reference_manager

    def __str__(self):
        return f'query chain for opening snapshot storage : <{self.storage_ident}>'

    @property
    def caller_name(self):
        return self

    @property
    def _trace_msg(self) -> str:
        params = (self.storage_ident, self.caller_pid, self.trace_debug, self.handle)
        return 'open storage:{},PID:{},trace_debug:{},handle:{}'.format(*params)

    @property
    def _complete_tree(self):
        return tree_operation.CreateTree(self.tree_ident).complete_tree

    @property
    def parent_ident(self):
        complete_tree = self._complete_tree
        node = complete_tree.get_node_by_ident(self.storage_ident)
        parent_node = node.parent
        parent_ident = complete_tree.get_ident_by_node(parent_node)
        return parent_ident

    @property
    def _storages_for_chain(self):
        return tree_operation.FetchStorageForChain(self.tree_ident, self.parent_ident).fetch()

    def _acquired_chain(self):
        return chain_operation.GenerateChain(self.storage_reference_manager,
                                             self.caller_name,
                                             self._storages_for_chain,
                                             chain.StorageChainForRead,
                                             self.timestamp).acquired_chain

    def _generate_raw_flag(self) -> str:
        return storage_action.DiskSnapshotAction.generate_flag(self.trace_debug, self.caller_pid)

    def _generate_handle(self):
        with self.journal_manager.get_locker(self._trace_msg):
            with tree.DiskSnapshotStorageTree.get_locker(self._trace_msg):
                acquired_chain = self._acquired_chain()
                return self.handle_manager.generate_read_handle(acquired_chain, self.handle)

    def execute(self):
        handle_inst = self._generate_handle()
        raw_flag = self._generate_raw_flag()
        handle_inst.raw_handle, handle_inst.ice_endpoint = storage_action.DiskSnapshotAction.open_disk_snapshot(
            handle_inst.storage_chain, raw_flag)
        return {'raw_handle': handle_inst.raw_handle, 'ice_endpoint': handle_inst.ice_endpoint}
