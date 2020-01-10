import asyncio

from arch.api.table.session import FateSession
from arch.api.transfer import Rubbish, Party, Federation
from typing import Union

#from eggroll.utils import log_utils
#from arch.api.utils.log_utils import getLogger 

from eggroll.roll_pair.roll_pair import RollPair
from arch.api.table.eggroll2.table_impl import DTable
#log_utils.setDirectory()
#LOGGER = log_utils.get_logger()

OBJECT_STORAGE_NAME = "__federation__"
STATUS_TABLE_NAME = "__status__"

from eggroll.utils import log_utils, file_utils

LOGGER = log_utils.get_logger()
#LOGGER = getLogger()

def init_roll_site_context(runtime_conf, session_id):
    from eggroll.roll_site.roll_site import RollSiteContext
    from eggroll.roll_pair.roll_pair import RollPairContext

    session_instance = FateSession.get_instance()._eggroll.get_session()
    rp_context = RollPairContext(session_instance)

    role = runtime_conf.get("local").get("role")
    partyId = runtime_conf.get("local").get("party_id")

    server_conf = file_utils.load_json_conf('/data/projects/qijun/apps/fate_host/arch/conf/server_conf.json')
    host = server_conf.get('servers').get('rollsite').get("host")
    port = server_conf.get('servers').get('rollsite').get("port")

    rs_context = RollSiteContext(session_id, self_role=role, self_partyId=partyId, rs_ip=host, rs_port=port, rp_ctx=rp_context)
    return rp_context, rs_context

class FederationRuntime(Federation):
    def __init__(self, session_id, runtime_conf):
        super().__init__(session_id, runtime_conf)
        self.rpc, self.rsc = init_roll_site_context(runtime_conf, session_id)
        self._loop = asyncio.get_event_loop()
        self.role = runtime_conf.get("local").get("role")

    def get(self, name, tag, parties: Union[Party, list]):
        if isinstance(parties, Party):
            parties = [parties]

        rubbish = Rubbish(name, tag)
        rtn = []
        rs = self.rsc.load(name=name, tag=tag)
        rs_parties = []
        for party in parties:
            rs_parties.append((party.role, party.party_id))

        LOGGER.info("rs_parties:{}".format(rs_parties))
        futures = rs.pull(parties=rs_parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                rtn.append(DTable.from_dtable(obj.ctx.get_session().get_session_id(),obj))
                rubbish.add_table(obj)
                LOGGER.info("RollPair:".format(obj))
            else:
                rtn.append(obj)
                LOGGER.info("obj:".format(obj))
        return rtn, rubbish

    def remote(self, obj, name, tag, parties):
        if isinstance(parties, Party):
            parties = [parties]

        rs_parties = []
        for party in parties:
            rs_parties.append((party.role, party.party_id))

        rubbish = Rubbish(name=name, tag=tag)
        rs = self.rsc.load(name=name, tag=tag)

        futures = rs.push(obj=obj, parties=rs_parties)
        for future in futures:
            role, party = future.result()
            LOGGER.info("RollSite push role:{} party:{}".format(role, party))
        return rubbish
