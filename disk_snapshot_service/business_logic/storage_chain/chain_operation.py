class GenerateChain(object):
    """生成链对象"""

    def __init__(self, storage_reference_manager, caller_name, storages_for_chain, chain_class, timestamp=None):
        self.storage_reference_manager = storage_reference_manager
        self.caller_name = caller_name
        self.chain_class = chain_class  # 链class
        self.storages_for_chain = storages_for_chain  # 链相关联的节点列表
        self.timestamp = timestamp

    @property
    def chain_obj(self):
        chain_obj = self.chain_class(storage_reference_manager=self.storage_reference_manager,
                                     caller_name=self.caller_name,
                                     timestamp=self.timestamp)
        if self.storages_for_chain:
            for obj in self.storages_for_chain:
                chain_obj.insert_tail(obj)

        return chain_obj

    @property
    def acquired_chain(self):
        return self.chain_obj.acquire()

