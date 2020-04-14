import os

import threading
import queue
from threading import Thread
import time

from eggroll.core.constants import StoreTypes
from eggroll.core.meta_model import ErStore
from eggroll.utils.log_utils import get_logger

L = get_logger()


class GcRecorder(object):

    def __init__(self, rpc):
        super(GcRecorder, self).__init__()
        self.should_stop = False
        L.debug('session:{} initializing recorder'.format(rpc.session_id))
        self.record_rpc = rpc
        self.gc_recorder = dict()
        self.gc_thread = Thread(target=self.run, daemon=True)
        self.record_start = False
        self.gc_queue = queue.Queue()
        self.gc_thread.start()
        self.gc_recorded = set()
        print("starting gc_thread......")

    def stop(self):
        self.should_stop = True

    def run(self):
        if "GC_SWITCH" in os.environ and os.environ["GC_SWITCH"] == 'close':
            L.info("global gc switch is close, "
                   "will not execute gc but only record temporary RollPair on the whole session")
            return
        while not self.should_stop:
            rp_name = self.gc_queue.get(block=True, timeout=0.8)
            if not rp_name:
                continue
            self.record_rpc.load(namespace=self.record_rpc.get_session().get_session_id(),
                                     name=rp_name).destroy()

    def record(self, er_store: ErStore):
        store_type = er_store._store_locator._store_type
        name = er_store._store_locator._name
        namespace = er_store._store_locator._namespace
        if store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return
        else:
            self.record_start = True
            print("record in memory table namespace:{}, name:{}, type:{}"
                  .format(namespace, name, store_type))
            count = self.gc_recorder.get(name)
            if count is None:
                count = 0
            self.gc_recorder[name] = count + 1
            print(f"recorded:{self.gc_recorder}")

    def decrease_ref_count(self, er_store):
        ref_count = self.gc_recorder.get(er_store._store_locator._name)
        record_count = 0 if ref_count is None or ref_count == 0 else (ref_count - 1)
        self.gc_recorder[er_store._store_locator._name] = record_count
        if record_count == 0 and er_store._store_locator._name in self.gc_recorder:
            print('put in queue')
            self.gc_queue.put(er_store._store_locator._name)
            self.gc_recorder.pop(er_store._store_locator._name)
