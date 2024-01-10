import uuid

from eggroll.computing import runtime_init as _runtime_init, RollPairContext, RollPair
from eggroll.computing.tasks.store import StoreTypes
from eggroll.session import session_init as _session_init
from ._partitioner import mmh3_partitioner
from ._serdes import UnrestrictedSerdes


def session_init(session_id, options) -> "WrappedSession":
    _session = _session_init(session_id=session_id, options=options)
    return WrappedSession(session=_session)


def runtime_init(session: "WrappedSession") -> "WrappedRpc":
    rpc = _runtime_init(session=session._session)
    return WrappedRpc(rpc=rpc)


class WrappedSession:
    def __init__(self, session):
        self._session = session

    def get_session_id(self):
        return self._session.get_session_id()


class WrappedRpc:
    def __init__(self, rpc: "RollPairContext"):
        self._rpc = rpc

    @property
    def session_id(self):
        return self._rpc.session.get_session_id()

    def load(self, namespace, name, options):
        store_type = options.get("store_type", StoreTypes.ROLLPAIR_LMDB)
        return self._rpc.load_rp(namespace=namespace, name=name, store_type=store_type)

    def parallelize(self, data, options: dict = None):
        if options is None:
            options = {}
        namespace = options.get("namespace", None)
        name = options.get("name", None)
        if namespace is None:
            namespace = self.session_id
        if name is None:
            name = str(uuid.uuid1())
        include_key = options.get("include_key", False)
        total_partitions = options.get("total_partitions", 1)
        partitioner = mmh3_partitioner
        partitioner_type = 0
        key_serdes_type = 0
        value_serdes_type = 0
        store_type = options.get("store_type", StoreTypes.ROLLPAIR_IN_MEMORY)

        # generate data
        if include_key:
            data = (
                (UnrestrictedSerdes.serialize(key), UnrestrictedSerdes.serialize(value))
                for key, value in data
            )
        else:
            data = (
                (UnrestrictedSerdes.serialize(i), UnrestrictedSerdes.serialize(value))
                for i, value in enumerate(data)
            )
        return WrappedRp(
            self._rpc.parallelize(
                data=data,
                total_partitions=total_partitions,
                partitioner=partitioner,
                partitioner_type=partitioner_type,
                key_serdes_type=key_serdes_type,
                value_serdes_type=value_serdes_type,
                store_type=store_type,
                namespace=namespace,
                name=name,
            )
        )

    def cleanup(self, name, namespace):
        return self._rpc.cleanup(name=name, namespace=namespace)

    def stop(self):
        return self._rpc.session.stop()

    def kill(self):
        return self._rpc.session.kill()


class WrappedRp:
    def __init__(self, rp: RollPair):
        self._rp = rp

    def get_partitions(self):
        return self._rp.get_partitions()

    def get(self, k, options: dict = None):
        if options is None:
            options = {}
        value = self._rp.get(
            k=UnrestrictedSerdes.serialize(k), partitioner=mmh3_partitioner
        )
        if value is None:
            return None
        else:
            return UnrestrictedSerdes.deserialize(value)

    def put(self, k, v, options: dict = None):
        if options is None:
            options = {}
        self._rp.put(
            k=UnrestrictedSerdes.serialize(k),
            v=UnrestrictedSerdes.serialize(v),
            partitioner=mmh3_partitioner,
        )
