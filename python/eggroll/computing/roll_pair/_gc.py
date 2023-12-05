import logging
import os
import queue
from threading import Thread

from eggroll.core.constants import StoreTypes
from eggroll.core.datastructure import create_simple_queue
from eggroll.core.meta_model import ErStore

L = logging.getLogger(__name__)


class WrappedDict:
    def __init__(self, d):
        self.d = d

    def __getitem__(self, key):
        return self.d[key]

    def __setitem__(self, key, value):
        self.d[key] = value

    def __contains__(self, key):
        return key in self.d

    def get(self, key, default=None):
        return self.d.get(key, default)

    def items(self):
        return self.d.items()

    def pop(self, key, default=None):
        return self.d.pop(key, default)

    def __len__(self):
        return len(self.d)

    def __iter__(self):
        return iter(self.d)

    def __str__(self):
        return str(self.d)

    def __repr__(self):
        return repr(self.d)

    def __eq__(self, other):
        return self.d == other


class GcRecorder(object):
    def __init__(self, rpc):
        super(GcRecorder, self).__init__()
        self.should_stop = False
        self.record_rpc = rpc
        self._gc_recorder = dict()
        self.leveldb_recorder = set()
        self.gc_queue = create_simple_queue()
        if (
            "EGGROLL_GC_DISABLE" in os.environ
            and os.environ["EGGROLL_GC_DISABLE"] == "1"
        ):
            L.info(
                "global GC disabled, "
                "will not execute gc but only record temporary RollPair during the whole session"
            )
        else:
            L.info("global GC enabled. starting GC thread")
            self.gc_thread = Thread(target=self.run, daemon=True)
            self.gc_thread.start()

    @property
    def gc_recorder(self):
        return WrappedDict(self._gc_recorder)

    def stop(self):
        self.should_stop = True
        L.info("GC: gc_util.stop called")

    def run(self):
        if (
            "EGGROLL_GC_DISABLE" in os.environ
            and os.environ["EGGROLL_GC_DISABLE"] == "1"
        ):
            L.info(
                "global GC disabled, "
                "will not execute gc but only record temporary RollPair during the whole session"
            )
            return
        while not self.should_stop:
            try:
                rp_namespace_name = self.gc_queue.get(block=True, timeout=0.5)
            except queue.Empty:
                continue
            if not rp_namespace_name:
                continue
            L.trace(f"GC thread destroying rp={rp_namespace_name}")
            self.record_rpc.create_rp(
                id=-1,
                namespace=rp_namespace_name[0],
                name=rp_namespace_name[1],
                total_partitions=1,
                store_type=StoreTypes.ROLLPAIR_IN_MEMORY,
                key_serdes_type=0,
                value_serdes_type=0,
                partitioner_type=0,
                options={},
                no_gc=True,
            ).destroy()

        L.info(f"GC should_stop={self.should_stop}, stopping GC thread")

    def record(self, er_store: ErStore):
        store_type = er_store._store_locator._store_type
        name = er_store._store_locator._name
        namespace = er_store._store_locator._namespace
        if (
            store_type != StoreTypes.ROLLPAIR_IN_MEMORY
            and store_type != StoreTypes.ROLLPAIR_LEVELDB
        ):
            return
        elif store_type == StoreTypes.ROLLPAIR_LEVELDB:
            self.leveldb_recorder.add((namespace, name))
        else:
            L.trace(
                "GC recording in memory table namespace={}, name={}".format(
                    namespace, name
                )
            )
            count = self.gc_recorder.get((namespace, name))
            if count is None:
                count = 0
            self.gc_recorder[(namespace, name)] = count + 1
            L.trace(f"GC recorded count={len(self.gc_recorder)}")

    def decrease_ref_count(self, er_store):
        if er_store._store_locator._store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return
        t_ns_n = (er_store._store_locator._namespace, er_store._store_locator._name)
        ref_count = self.gc_recorder.get(t_ns_n)
        record_count = 0 if ref_count is None or ref_count == 0 else (ref_count - 1)
        self.gc_recorder[t_ns_n] = record_count
        if record_count == 0 and t_ns_n in self.gc_recorder:
            L.trace(f"GC put in queue. namespace={t_ns_n[0]}, name={t_ns_n[1]}")
            self.gc_queue.put(t_ns_n)
            self.gc_recorder.pop(t_ns_n)
