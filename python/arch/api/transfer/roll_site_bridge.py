import asyncio

from arch.api.table.session import FateSession
from arch.api.transfer import Rubbish, Party, Federation
from typing import Union

from eggroll.utils import log_utils
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.roll_site.roll_site import RollSite
log_utils.setDirectory()
LOGGER = log_utils.getLogger()

OBJECT_STORAGE_NAME = "__federation__"
STATUS_TABLE_NAME = "__status__"

def init_roll_site_context():
    from eggroll.roll_site.roll_site import RollSiteContext
    from eggroll.roll_pair.roll_pair import RollPairContext
    options = {'runtime_conf_path': 'python/eggroll/roll_site/conf/role_conf.json',
               'server_conf_path': 'python/eggroll/roll_site/conf/server_conf.json',
               'transfer_conf_path': 'python/eggroll/roll_site/conf/transfer_conf.json'}
    session_instance = FateSession.get_instance()._eggroll.get_session()
    rp_context = RollPairContext(session_instance)
    rs_context = RollSiteContext(session_instance.get_session_id(), options=options, rp_ctx=rp_context)
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
        super().__init__(session_id, runtime_conf)
        self.rpc, self.rsc = init_roll_site_context()
        self._loop = asyncio.get_event_loop()

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
        rubbish = Rubbish(name, tag)
        rtn = []
        rs = self.rsc.load(name=name, tag=tag)
        futures = rs.pull(parties=parties)
        for future in futures:
            obj = future.result()
            if isinstance(obj, RollPair):
                rtn.append(obj)
                rubbish.add_table(obj)
                LOGGER.info("RollPair:".format(obj))
            else:
                rtn.append(obj)
                LOGGER.info("obj:".format(obj))
        return rtn ,rubbish

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
        if True:
            LOGGER.info("standalone federation remote")
            return self.remote_local(obj=obj, name=name, tag=tag, parties=parties)

        if isinstance(parties, Party):
            parties = [parties]
        self._remote_side_auth(name=name, parties=parties)
        rubbish = Rubbish(name, tag)

        # if obj is a dtable, remote it
        if isinstance(obj, RollPair):
            obj.set_gc_disable()
            for party in parties:
                log_msg = f"src={self.local_party}, dst={party}, name={name}, tag={tag}, session_id={self._session_id}"
                LOGGER.debug(f"[REMOTE] sending table: {log_msg}")
                self._send(name=name, tag=tag, dst_party=party, rubbish=rubbish, table=obj)
                LOGGER.debug(f"[REMOTE] table done: {log_msg}")
            return rubbish