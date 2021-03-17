import os
import queue
from threading import Thread

from eggroll.core.constants import StoreTypes
from eggroll.core.meta_model import ErStore
from eggroll.core.datastructure import create_simple_queue
from eggroll.utils.log_utils import get_logger

L = get_logger()


class GcRecorder(object):

    def __init__(self, rpc):
        super(GcRecorder, self).__init__()
        self.should_stop = False
        self.record_rpc = rpc
        self.gc_recorder = dict()
        self.leveldb_recorder = set()
        self.gc_queue = create_simple_queue()
        if "EGGROLL_GC_DISABLE" in os.environ and os.environ["EGGROLL_GC_DISABLE"] == '1':
            L.info("global GC disabled, "
                   "will not execute gc but only record temporary RollPair during the whole session")
        else:
            L.info("global GC enabled. starting GC thread")
            self.gc_thread = Thread(target=self.run, daemon=True)
            self.gc_thread.start()

    def stop(self):
        self.should_stop = True
        L.info("GC: gc_util.stop called")

    def run(self):
        if "EGGROLL_GC_DISABLE" in os.environ and os.environ["EGGROLL_GC_DISABLE"] == '1':
            L.info("global GC disabled, "
                   "will not execute gc but only record temporary RollPair during the whole session")
            return
        options = dict()
        options['create_if_missing'] = True
        while not self.should_stop:
            try:
                rp_namespace_name = self.gc_queue.get(block=True, timeout=0.5)
            except queue.Empty:
                continue
            if not rp_namespace_name:
                continue
            L.trace(f"GC thread destroying rp={rp_namespace_name}")
            self.record_rpc.load(namespace=rp_namespace_name[0],
                                 name=rp_namespace_name[1], options=options).destroy()

        L.info(f"GC should_stop={self.should_stop}, stopping GC thread")

    def record(self, er_store: ErStore):
        store_type = er_store._store_locator._store_type
        name = er_store._store_locator._name
        namespace = er_store._store_locator._namespace
        if store_type != StoreTypes.ROLLPAIR_IN_MEMORY and store_type != StoreTypes.ROLLPAIR_LEVELDB:
            return
        elif store_type == StoreTypes.ROLLPAIR_LEVELDB:
            self.leveldb_recorder.add((namespace, name))
        else:
            L.trace("GC recording in memory table namespace={}, name={}"
                    .format(namespace, name))
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
            L.trace(f'GC put in queue. namespace={t_ns_n[0]}, name={t_ns_n[1]}')
            self.gc_queue.put(t_ns_n)
            self.gc_recorder.pop(t_ns_n)
