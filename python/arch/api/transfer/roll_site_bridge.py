import asyncio
from typing import Union

from arch.api.table.eggroll2.table_impl import DTable
from arch.api.table.session import FateSession
from arch.api.transfer import Rubbish, Party, Federation
from eggroll.core.meta_model import ErEndpoint
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.utils import file_utils

# from eggroll.utils import log_utils
# from arch.api.utils.log_utils import getLogger
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
    LOGGER.debug("xxxruntime_conf")
    LOGGER.info("xxxruntime_conf: {}".format(runtime_conf))

    """
    default_opts = {'runtime_conf_path': 'python/eggroll/roll_site/conf/role_conf.json',
               'server_conf_path': '/data/projects/qijun/apps/fate_host/arch/conf/server_conf.json'}
    default_opts = {'runtime_conf_path': 'python/eggroll/roll_site/conf/role_conf.json',
               'server_conf_path': 'python/eggroll/roll_site/conf/server_conf.json'}
    if runtime_conf.get("local").get("role") == "host":
        LOGGER.info("init_roll_site_context ----host")
        options = default_opts
    elif runtime_conf.get("local").get("role") == "guest":
        LOGGER.info("init_roll_site_context ----guest")
        options = {'runtime_conf_path': 'python/eggroll/roll_site/conf_guest/role_conf.json',
                   'server_conf_path': 'python/eggroll/roll_site/conf_guest/server_conf.json'}
    else:
        options = default_opts
    options = default_opts
    """

    session_instance = FateSession.get_instance()._eggroll.get_session()
    rp_context = RollPairContext(session_instance)

    role = runtime_conf.get("local").get("role")
    party_id = runtime_conf.get("local").get("party_id")
    import os
    _path = os.environ['FATE_HOME'] + "/arch/conf/server_conf.json"

    server_conf = file_utils.load_json_conf(_path)
    host = server_conf.get('servers').get('proxy').get("host")
    port = server_conf.get('servers').get('proxy').get("port")

    options = {'self_role': role,
               'self_party_id': party_id,
               'proxy_endpoint': ErEndpoint(host, int(port))
              }

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
    LOGGER.debug("args123:{}".format(args))
    return "-".join(["{}".format(arg) for arg in args])


class FederationRuntime(Federation):

    def __init__(self, session_id, runtime_conf):
        LOGGER.info("### hwz session_id:{}".format(session_id))
        super().__init__(session_id, runtime_conf)
        self.rpc, self.rsc = init_roll_site_context(runtime_conf, session_id)
        self._loop = asyncio.get_event_loop()
        self.role = runtime_conf.get("local").get("role")


    def get(self, name, tag, parties: Union[Party, list]):
        LOGGER.info("get, name: {}".format(name))
        LOGGER.info("get, tag: {}".format(tag))
        LOGGER.info("get, parties: {}".format(parties))
        LOGGER.info("self role:{}".format(self.role))

        if isinstance(parties, Party):
            parties = [parties]

        rubbish = Rubbish(name, tag)
        rtn = []
        rs = self.rsc.load(name=name, tag=tag)
        rs_parties = []
        for party in parties:
            rs_parties.append((party.role, party.party_id))

        LOGGER.info("rs_parties:{}".format(rs_parties))
        # TODO:0: check if exceptions are swallowed
        futures = rs.pull(parties=rs_parties)
        LOGGER.debug(f"cc657:")
        for future in futures:
            obj = future.result()
            LOGGER.debug(f"cc650:{obj},{type(obj)}")
            if isinstance(obj, RollPair):
                LOGGER.debug(f"cc651:{obj},{obj.__dict__}, {obj.count()}")
                rtn.append(DTable.from_dtable(obj.ctx.get_session().get_session_id(),obj))
                rubbish.add_table(obj)
                LOGGER.info("RollPair:".format(obj))
            else:
                rtn.append(obj)
                LOGGER.info("obj:".format(obj))
        LOGGER.debug(f"get201:{rtn}")
        return rtn, rubbish


    def remote(self, obj, name, tag, parties):
        LOGGER.info("remote, name: {}".format(name))
        LOGGER.info("remote, tag: {}".format(tag))
        LOGGER.info("remote, parties: {}".format(parties))
        LOGGER.info("self role:{}".format(self.role))

        if isinstance(parties, Party):
            parties = [parties]

        rs_parties = []
        for party in parties:
            rs_parties.append((party.role, party.party_id))

        rubbish = Rubbish(name=name, tag=tag)
        rs = self.rsc.load(name=name, tag=tag)

        LOGGER.debug(f"type091:{obj}, {type(obj)}")
        #if isinstance(obj, DTable):
        #    obj = obj._dtable
        #    LOGGER.debug(f"obj081:{obj.__dict__}, {obj}, {obj.count()}")
        #if isinstance(obj, RollPair):
        #    LOGGER.debug(f"obj080:{obj.__dict__}, {obj}, {obj.count()}")
        futures = rs.push(obj=obj, parties=rs_parties)
        for future in futures:
            role, party = future.result()
            LOGGER.info("RollSite push role:{} party:{}".format(role, party))
        return rubbish
