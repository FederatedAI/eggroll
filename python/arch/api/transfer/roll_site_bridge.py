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

from eggroll.utils import log_utils
LOGGER = log_utils.get_logger()
#LOGGER = getLogger()

def init_roll_site_context(runtime_conf, session_id):
    from eggroll.roll_site.roll_site import RollSiteContext
    from eggroll.roll_pair.roll_pair import RollPairContext
    default_opts = {'runtime_conf_path': 'python/eggroll/roll_site/conf/role_conf.json',
                    'server_conf_path': 'python/eggroll/roll_site/conf/server_conf.json'}
    if runtime_conf.get("local").get("role") == "host":
        options = default_opts
    elif runtime_conf.get("local").get("role") == "guest":
        options = {'runtime_conf_path': 'python/eggroll/roll_site/conf_guest/role_conf.json',
                   'server_conf_path': 'python/eggroll/roll_site/conf_guest/server_conf.json'}
    else:
        options = default_opts

    session_instance = FateSession.get_instance()._eggroll.get_session()
    rp_context = RollPairContext(session_instance)

    rs_context = RollSiteContext(session_id, options=options, rp_ctx=rp_context)
    return rp_context, rs_context


async def check_status_and_get_value(_table, _key):
    _value = _table.get(_key)
    while _value is None:
        await asyncio.sleep(0.1)
        _value = _table.get(_key)
        LOGGER.debug("k:{} v:{}".format(_key, _value))
    LOGGER.debug("[GET] Got {} type {}".format(_key, 'Table' if isinstance(_value, tuple) else 'Object'))
    return _value


def _remote__object_key(*args):
    return "-".join(["{}".format(arg) for arg in args])


class FederationRuntime(Federation):

    def __init__(self, session_id, runtime_conf):
        super().__init__(session_id, runtime_conf)
        self.rpc, self.rsc = init_roll_site_context(runtime_conf, session_id)
        self._loop = asyncio.get_event_loop()
        self.role = runtime_conf.get("local").get("role")

    def _send(self, name: str, tag: str, dst_party: Party, rubbish: Rubbish, table: RollPair, obj=None):
        tagged_key = f"{name}-{tag}"
        if isinstance(obj, object):
            table.put(tagged_key, obj)
            rubbish.add_obj(table, tagged_key)
        else:
            rubbish.add_table(table)

    def _get_meta_table(self, _name, _job_id):
        return self.rpc.load(name=_name, namespace=_job_id)

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
