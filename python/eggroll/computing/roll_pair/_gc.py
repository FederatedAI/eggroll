import logging
import os
import queue
from threading import Thread, Lock

from eggroll.computing.tasks.store import StoreTypes
from eggroll.core.datastructure import create_simple_queue
from eggroll.core.meta_model import ErStore

L = logging.getLogger(__name__)

ReferenceCountLock = Lock()


class GcRecorder(object):
    def __init__(self, rpc):
        self._record_rpc = rpc
        self._gc_recorder = {}
        self._gc_worker_queue = create_simple_queue()
        self._runtime_gc_stopped = False
        if (
            "EGGROLL_GC_DISABLE" in os.environ
            and os.environ["EGGROLL_GC_DISABLE"] == "1"
        ):
            L.warning(
                "global GC disabled, "
                "will not execute gc but only record temporary RollPair during the whole session"
            )
        else:
            L.debug("global GC enabled. starting GC thread")
            self.gc_thread = Thread(target=self._runtime_gc_worker, daemon=True)
            self.gc_thread.start()

    def runtime_gc_stop(self):
        L.debug("stop runtime gc thread")
        self._runtime_gc_stopped = True

    def _runtime_gc_worker(self):
        """
        Infinite loop to retrieve store represented by namespace and name
        from gc_queue that is expected to be destroyed, and destroy it.

        This method is expected to be called in a thread and won't return until
        self.should_stop is set to True.
        """
        if (
            "EGGROLL_GC_DISABLE" in os.environ
            and os.environ["EGGROLL_GC_DISABLE"] == "1"
        ):
            L.warning(
                "global GC disabled, "
                "will not execute gc but only record temporary RollPair during the whole session"
            )
            return
        while not self._runtime_gc_stopped:
            try:
                namespace_name_tuple = self._gc_worker_queue.get(
                    block=True, timeout=0.5
                )
            except queue.Empty:
                continue
            if not namespace_name_tuple:
                continue

            if L.isEnabledFor(logging.DEBUG):
                L.debug(
                    f"GC thread destroying store: namespace={namespace_name_tuple[0]}, name={namespace_name_tuple[1]}"
                )

            self._record_rpc.destroy_store(
                name=namespace_name_tuple[1],
                namespace=namespace_name_tuple[0],
                store_type=StoreTypes.ROLLPAIR_IN_MEMORY,
            )
        L.info("GC thread stopped")

    def increase_ref_count(self, er_store: ErStore):
        store_type = er_store.store_locator.store_type
        name = er_store.store_locator.name
        namespace = er_store.store_locator.namespace
        if store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return
        else:
            with ReferenceCountLock:
                count = self._gc_recorder.get((namespace, name))
                if count is None:
                    count = 0
                count += 1
                self._gc_recorder[(namespace, name)] = count
            if L.isEnabledFor(logging.DEBUG):
                L.debug(
                    f"GC increase ref count. namespace={namespace}, name={name}, count={count}"
                )
                L.debug(f"GC recorded count={len(self._gc_recorder)}")

    def decrease_ref_count(self, er_store):
        if er_store.store_locator.store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return
        name = er_store.store_locator.name
        namespace = er_store.store_locator.namespace
        with ReferenceCountLock:
            ref_count = self._gc_recorder.get((namespace, name))
            if ref_count is None:
                ref_count = 0
            else:
                ref_count -= 1
            self._gc_recorder[(namespace, name)] = ref_count
            if L.isEnabledFor(logging.DEBUG):
                L.debug(
                    f"GC decrease ref count. namespace={namespace}, name={name}, count={ref_count}"
                )
            if ref_count == 0:
                self._gc_worker_queue.put((namespace, name))
                self._gc_recorder.pop((namespace, name))

    def flush(self):
        # stop gc thread
        self.runtime_gc_stop()

        if self._gc_recorder is None or len(self._gc_recorder) == 0:
            return

        for (namespace, name), v in dict(self._gc_recorder.items()).items():
            try:
                self._record_rpc.destroy_store(
                    name=name,
                    namespace=namespace,
                    store_type=StoreTypes.ROLLPAIR_IN_MEMORY,
                )
            except Exception as e:
                raise RuntimeError(
                    f"fail to destroy store with name={name}, namespace={namespace}"
                ) from e
