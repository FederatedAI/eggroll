from queue import Queue, Empty

import time

from eggroll.core.pair_store.adapter import PairAdapter


class _StatedQueue(Queue):
    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self.status = "ready"

    def size(self):
        return self.qsize()


class QueueAdapter:
    queues = {}

    def __init__(self, options=None):
        if options is None:
            options = {}
        self.path = options["path"]
        self.capacity = options.get("capacity", 1000*1000)
        self.action_timeout = int(options["action_timeout"]) if "action_timeout" in options else 60*60
        if self.path not in self.queues:
            self.queues[self.path] = _StatedQueue(maxsize=self.capacity)
        self.data = self.queues[self.path]

    def _time_limit(self, func, *args, **kwargs):
        start = time.time()
        while time.time() - start < self.action_timeout:
            if self.is_destroyed():
                raise InterruptedError("queue has been destroyed:get")
            try:
                return func(*args, **kwargs)
            except Empty:
                # continue
                pass
        raise TimeoutError(f"QueueAdapter action timeout:{func.__name}, cost:{time.time() - start}")

    def count(self):
        return self.data.size()

    def get(self):
        return self._time_limit(self.data.get, timeout=0.1)

    def put(self, item):
        return self._time_limit(self.data.put, item, timeout=0.1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        pass

    def is_destroyed(self):
        return self.data.status == "destroyed"

    def destroy(self):
        self.close()
        self.data.status = "destroyed"
        del self.queues[self.path]
