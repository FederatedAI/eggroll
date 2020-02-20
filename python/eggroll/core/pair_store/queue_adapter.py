from queue import Queue
from eggroll.core.pair_store.adapter import PairAdapter


class QueueAdapter:
    queues = {}

    def __init__(self, options=None):
        if options is None:
            options = {}
        self.path = options["path"]
        self.capacity = options.get("capacity", 1000*1000)
        self.action_timeout = int(options["action_timeout"]) if "action_timeout" in options else None
        self.destroyed = False
        if self.path not in self.queues:
            self.queues[self.path] = Queue(maxsize=self.capacity)
        self.data = self.queues[self.path]

    def get(self):
        if self.destroyed:
            raise InterruptedError("queue has been destroyed:get")
        return self.data.get(timeout=self.action_timeout)

    def put(self, item):
        if self.destroyed:
            raise InterruptedError("queue has been destroyed:put")
        return self.data.put(item, timeout=self.action_timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        pass

    def destroy(self):
        self.close()
        self.destroyed = True
        del self.queues[self.path]
