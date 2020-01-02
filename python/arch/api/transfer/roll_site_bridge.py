import asyncio

from arch.api.table.session import FateSession
from arch.api.transfer import Rubbish, Party, Federation
from typing import Union

from eggroll.roll_pair.roll_pair import RollPair
from eggroll.roll_site.roll_site import RollSite

OBJECT_STORAGE_NAME = "__federation__"
STATUS_TABLE_NAME = "__status__"

from eggroll.utils import log_utils
LOGGER = log_utils.get_logger()

def init_roll_site_context(runtime_conf, session_id):
    from eggroll.roll_site.roll_site import RollSiteContext
    from eggroll.roll_pair.roll_pair import RollPairContext
    LOGGER.info("runtime_conf: {}".format(runtime_conf))
    default_opts = {'runtime_conf_path': 'python/eggroll/roll_site/conf/role_conf.json',
               'server_conf_path': 'python/eggroll/roll_site/conf/server_conf.json',
               'transfer_conf_path': 'python/eggroll/roll_site/conf/transfer_conf.json'}
    if runtime_conf.get("local").get("role") == "host":
        LOGGER.info("init_roll_site_context ----host")
        options = default_opts
    elif runtime_conf.get("local").get("role") == "guest":
        LOGGER.info("init_roll_site_context ----guest")
        options = {'runtime_conf_path': 'python/eggroll/roll_site/conf_guest/role_conf.json',
                   'server_conf_path': 'python/eggroll/roll_site/conf_guest/server_conf.json',
                   'transfer_conf_path': 'python/eggroll/roll_site/conf_guest/transfer_conf.json'}
    else:
        options = default_opts

    session_instance = FateSession.get_instance()._eggroll.get_session()
    rp_context = RollPairContext(session_instance)

    LOGGER.info("init session_id:{}".format(session_instance.get_session_id()))
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
        LOGGER.info("session_id:{}".format(session_id))
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

    def get_local(self, name, tag, parties: Union[Party, list]):
        if isinstance(parties, Party):
            parties = [parties]
        self._get_side_auth(name=name, parties=parties)

        _status_table = self._get_meta_table(STATUS_TABLE_NAME, self._session_id)
        LOGGER.debug(f"[GET] {self._local_party} getting {name}.{tag} from {parties}")
        tasks = []

        for party in parties:
            _tagged_key = _remote__object_key(self._session_id, name, tag, party.role, party.party_id, self._role,
                                                    self._party_id)
            tasks.append(check_status_and_get_value(_status_table, _tagged_key))
        results = self._loop.run_until_complete(asyncio.gather(*tasks))
        rtn = []
        rubbish = Rubbish(name, tag)
        _object_table = self._get_meta_table(OBJECT_STORAGE_NAME, self._session_id)
        for r in results:
            LOGGER.debug(f"[GET] {self._local_party} getting {r} from {parties}")
            if isinstance(r, tuple):
                LOGGER.info("result:{}".format(r))
                _persistent = True
                table = FateSession.get_instance().table(name=r[1], namespace=r[2], persistent=_persistent, partition=r[3],
                                                         in_place_computing=False, create_if_missing=True,
                                                         error_if_exist=True)
                rtn.append(table)
                rubbish.add_table(table)

            else:  # todo: should standalone mode split large object?
                obj = _object_table.get(r)
                rtn.append(obj)
                # rubbish.add_obj(_object_table, r)
                # rubbish.add_obj(_status_table, r)
        return rtn, rubbish

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
        from arch.api.table.eggroll2.table_impl import DTable
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

    def remote_local(self, obj, name: str, tag: str, parties: Union[Party, list]) -> Rubbish:
        if isinstance(parties, Party):
            parties = [parties]
        self._remote_side_auth(name=name, parties=parties)

        rubbish = Rubbish(name, tag)
        for party in parties:
            LOGGER.debug("debug for session id:{}".format(self._session_id))
            _tagged_key = _remote__object_key(self._session_id, name, tag, self._role, self._party_id, party.role,
                                                    party.party_id)
            _status_table = self._get_meta_table(STATUS_TABLE_NAME, self._session_id)

            from arch.api.table.eggroll2.table_impl import DTable
            if isinstance(obj, RollPair):
                # noinspection PyProtectedMember
                _status_table.put(_tagged_key, (obj.get_type(), obj.get_name(), obj.get_namespace(), obj.get_partitions()))
                LOGGER.debug("k:{} v:{}".format(_tagged_key, (obj.get_type(), obj.get_name(), obj.get_namespace(), obj.get_partitions())))
            elif isinstance(obj, DTable):
                _status_table.put(_tagged_key, (obj._name, obj._namespace, obj._partitions))
                LOGGER.debug("k:{} v:{}".format(_tagged_key, (obj._name, obj._namespace, obj._partitions)))
            else:
                _table = self._get_meta_table(OBJECT_STORAGE_NAME, self._session_id)
                _table.put(_tagged_key, obj)

                _status_table.put(_tagged_key, _tagged_key)
                LOGGER.debug("k:{} v:{}".format(_tagged_key, obj))
                #rubbish.add_obj(_table, _tagged_key)
                #rubbish.add_obj(_status_table, _tagged_key)
            LOGGER.debug("[REMOTE] Sent {}".format(_tagged_key))
        return rubbish

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
