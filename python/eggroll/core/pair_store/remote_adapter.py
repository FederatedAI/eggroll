#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#
from eggroll.core.pair_store.adapter import PairAdapter, PairWriteBatch, \
    PairIterator


class RemoteAdapter(PairAdapter):
    """
    Pair(key->value) store adapter
    """

    def __init__(self, options):
        self._options = options
        self._rp_ctx = options['rp_ctx']

        # gets the replica
        self._rp = self._rp_ctx.load(name=options['rp_name'], namespace=options['rp_namespace'], options=self._options)

    def __del__(self):
        pass

    def close(self):
        return

    def iteritems(self):
        return RemoteIterator()

    def new_batch(self):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def put(self, key, value):
        raise NotImplementedError()

    def is_sorted(self):
        raise NotImplementedError()

    def destroy(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# TODO:0: thread safety
class RemoteWriteBatch(PairWriteBatch):

    def __init__(self, remote_adapter: RemoteAdapter, options: dict = None):
        if options is None:
            options = {}
        self._options = options
        self._adapter = remote_adapter
        self._rp = remote_adapter._rp
        self._buffer = {}
        # TODO:0: configurable
        self._max_buffer_size = 10_000

    def get(self, k):
        return self._buffer.get(k)

    def put(self, k, v):
        self._buffer[k] = v

    def merge(self, merge_func, k, v):
        if k in self._buffer:
            self._buffer[k] = merge_func(self._buffer[k], v)
        else:
            old_value = self._rp.get(k)
            if old_value is None:
                self._buffer[k] = v
            else:
                self._buffer[k] = merge_func(old_value, v)

        if len(self._buffer) > self._max_buffer_size:
            self.write()

    def write(self):
        old_buffer = self._buffer
        self._buffer = {}
        self._rp.put_all(old_buffer.items())

    def close(self):
        return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class RemoteIterator(PairIterator):
    def __init__(self, remote_adapter: RemoteAdapter, options: dict = None):
        if options is None:
            options = {}
        self._adapter = remote_adapter
        self._rp = self._adapter._rp
        self._partition_id = options['partition_id']
        self._iter = self._rp.get_partition(self._partition_id)

    def close(self):
        return

    def __iter__(self):
        next(self._iter)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
