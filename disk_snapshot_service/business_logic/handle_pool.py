class Handle(object):
    """句柄对象"""

    def __init__(self, handle=None, chain=None, raw_handle=None, ice_endpoint=None):
        self.handle = handle
        self.storage_chain = chain
        self.raw_handle = raw_handle
        self.ice_endpoint = ice_endpoint


class HandleManager(object):
    """handle 管理器"""

    def __init__(self):
        # {handle_ident: handle_inst, }
        self.cache = dict()

    def generate_handle(self):
        # 产生新的handle对象，并将其加入到 self.cache 中
        pass

    def generate_read_handle(self, chain, handle):
        """
            这里的入参没有handle，
            所以，read_handle不暂存到 self.cache 吗？
        """
        handle_inst = Handle()
        handle_inst.storage_chain = chain
        handle_inst.handle = handle
        self.cache[handle_inst.handle] = handle_inst.storage_chain
        return handle_inst

    def generate_write_handle(self, chain, handle):
        handle_inst = Handle()
        handle_inst.storage_chain = chain
        handle_inst.handle = handle
        self.cache[handle_inst.handle] = handle_inst.storage_chain
        return handle_inst


class PidChecker(object):
    """检查Pid是否有效，如果无效就释放资源"""
    pass
