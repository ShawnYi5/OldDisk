from ice_service import application

class CallableI(ice.Utils.Callable):

    def execute(self, call, in_json, in_raw, current=None):
        print(f'execute {call}  {in_json}  {in_raw}')
        out_json = '{"msg": "i am in kvm"}'
        out_raw = bytes()
        return out_json, out_raw


class Server(application.Application):
    def run(self, args):
        adapter = self.communicator().createObjectAdapter("DSS.Server")
        adapter.add(CallableI(), self.communicator().stringToIdentity("dss"))
        call_prx = ice.Utils.CallablePrx.uncheckedCast(
            adapter.createProxy(self.communicator().stringToIdentity("callable")))
        adapter.add(KVMI(call_prx), self.communicator().stringToIdentity("kvm"))
        adapter.activate()
        self.communicator().waitForShutdown()
        return 0