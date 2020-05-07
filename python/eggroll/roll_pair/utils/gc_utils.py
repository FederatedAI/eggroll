import os

from collections import deque
from queue import Empty
import threading
import queue
from threading import Thread

from eggroll.core.constants import StoreTypes
from eggroll.core.meta_model import ErStore
from eggroll.utils.log_utils import get_logger


L = get_logger()


# from Python 3.7.5
class _PySimpleQueue:
    '''Simple, unbounded FIFO queue.

    This pure Python implementation is not reentrant.
    '''
    # Note: while this pure Python version provides fairness
    # (by using a threading.Semaphore which is itself fair, being based
    #  on threading.Condition), fairness is not part of the API contract.
    # This allows the C version to use a different implementation.

    def __init__(self):
        self._queue = deque()
        self._count = threading.Semaphore(0)

    def put(self, item, block=True, timeout=None):
        '''Put the item on the queue.

        The optional 'block' and 'timeout' arguments are ignored, as this method
        never blocks.  They are provided for compatibility with the Queue class.
        '''
        self._queue.append(item)
        self._count.release()

    def get(self, block=True, timeout=None):
        '''Remove and return an item from the queue.

        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until an item is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Empty exception if no item was available within that time.
        Otherwise ('block' is false), return an item if one is immediately
        available, else raise the Empty exception ('timeout' is ignored
        in that case).
        '''
        if timeout is not None and timeout < 0:
            raise ValueError("'timeout' must be a non-negative number")
        if not self._count.acquire(block, timeout):
            raise Empty
        return self._queue.popleft()

    def put_nowait(self, item):
        '''Put an item into the queue without blocking.

        This is exactly equivalent to `put(item)` and is only provided
        for compatibility with the Queue class.
        '''
        return self.put(item, block=False)

    def get_nowait(self):
        '''Remove and return an item from the queue without blocking.

        Only get an item if one is immediately available. Otherwise
        raise the Empty exception.
        '''
        return self.get(block=False)

    def empty(self):
        '''Return True if the queue is empty, False otherwise (not reliable!).'''
        return len(self._queue) == 0

    def qsize(self):
        '''Return the approximate size of the queue (not reliable!).'''
        return len(self._queue)


try:
    from queue import SimpleQueue
    L.info(f'GC: using SimpleQueue from cython')
except ImportError:
    SimpleQueue = _PySimpleQueue
    L.info(f'GC: using SimpleQueue from _PySimpleQueue')


class GcRecorder(object):

    def __init__(self, rpc):
        super(GcRecorder, self).__init__()
        self.should_stop = False
        L.debug('session:{} initializing gc recorder'.format(rpc.session_id))
        self.record_rpc = rpc
        self.gc_recorder = dict()
        self.gc_queue = SimpleQueue()
        if "EGGROLL_GC_DISABLE" in os.environ and os.environ["EGGROLL_GC_DISABLE"] == '1':
            L.info("global gc is disable, "
                  "will not execute gc but only record temporary RollPair during the whole session")
        else:
            self.gc_thread = Thread(target=self.run, daemon=True)
            self.gc_thread.start()
            L.debug("starting gc_thread......")

    def stop(self):
        self.should_stop = True

    def run(self):
        if "EGGROLL_GC_DISABLE" in os.environ and os.environ["EGGROLL_GC_DISABLE"] == '1':
            L.info("global gc switch is close, "
                  "will not execute gc but only record temporary RollPair during the whole session")
            return
        while not self.should_stop:
            try:
                rp_name = self.gc_queue.get(block=True, timeout=0.5)
            except queue.Empty:
                continue
            if not rp_name:
                continue
            L.info(f"GC thread destroying rp:{rp_name}")
            self.record_rpc.load(namespace=self.record_rpc.get_session().get_session_id(),
                                     name=rp_name).destroy()

    def record(self, er_store: ErStore):
        store_type = er_store._store_locator._store_type
        name = er_store._store_locator._name
        namespace = er_store._store_locator._namespace
        if store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return
        else:
            L.info("GC recording in memory table namespace={}, name={}, type={}"
                  .format(namespace, name, store_type))
            count = self.gc_recorder.get(name)
            if count is None:
                count = 0
            self.gc_recorder[name] = count + 1
            L.info(f"GC recorded count={len(self.gc_recorder)}")

    def decrease_ref_count(self, er_store):
        if er_store._store_locator._store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return
        ref_count = self.gc_recorder.get(er_store._store_locator._name)
        record_count = 0 if ref_count is None or ref_count == 0 else (ref_count - 1)
        self.gc_recorder[er_store._store_locator._name] = record_count
        if record_count == 0 and er_store._store_locator._name in self.gc_recorder:
            L.info(f'GC put in queue:{er_store._store_locator._name}')
            self.gc_queue.put(er_store._store_locator._name)
            self.gc_recorder.pop(er_store._store_locator._name)
