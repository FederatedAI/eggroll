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
import mmap
import os
from collections import OrderedDict

from eggroll.core.datastructure.broker import Broker
from eggroll.core.pair_store.format import PairBinReader, PairBinWriter, \
    FileByteBuffer, ArrayByteBuffer
from eggroll.utils import log_utils

log_utils.setDirectory()
LOGGER = log_utils.getLogger()

# TODO:0: usage?
class AdapterManager:
    pass

class PairAdapter(object):
    """
    Pair(key->value) store adapter
    """

    def __init__(self, options):
        pass

    def __del__(self):
        pass

    def close(self):
        raise NotImplementedError()

    def iteritems(self):
        raise NotImplementedError()

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


class PairWriteBatch:
    def put(self, k, v):
        raise NotImplementedError()

    def write(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class PairIterator:
    def close(self):
        raise NotImplementedError()

    def __iter__(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class FileAdapter(PairAdapter):
    def destroy(self):
        self.close()
        os.remove(self._file.name)

    def __init__(self, options):
        super().__init__(options)
        path = options["path"]
        self._file = open(path, "w+b")

    def is_sorted(self):
        return False

    def close(self):
        self._file.close()

    def iteritems(self):
        return FileIterator(self._file)

    def new_batch(self):
        return FileWriteBatch(self._file)

    def get(self, key):
        raise NameError("unsupported")

    def put(self, key, value):
        raise NameError("unsupported")

class FileIterator(PairIterator):
    def __init__(self, file):
        file.seek(0)
        self.reader = PairBinReader(FileByteBuffer(file))

    def close(self):
        pass

    def __iter__(self):
        return self.reader.read_all()

class FileWriteBatch(PairWriteBatch):
    def __init__(self, file):
        self.writer = PairBinWriter(FileByteBuffer(file))

    def put(self, k, v):
        self.writer.write(k, v)

    def write(self):
        pass

    def close(self):
        pass

class CacheAdapter(PairAdapter):
    caches = {}
    def __init__(self, options=None):
        super().__init__(options)
        self.path = options["path"]
        if self.path not in self.caches:
            self.caches[self.path] = OrderedDict()
        self.data = self.caches[self.path]

    def is_sorted(self):
        return True

    def close(self):
        pass

    def destroy(self):
        del self.caches[self.path]
        del self.data

    def iteritems(self):
        return CacheIterator(self.data)

    def new_batch(self):
        return CacheWriteBatch(self.data)

    def get(self, key):
        return self.data[key]

    def put(self, key, value):
        self.data[key] = value

    def count(self):
        return len(self.data)

class CacheIterator(PairIterator):
    def __init__(self, data):
        self.data = data

    def close(self):
        pass

    def __iter__(self):
        return iter(self.data.items())

class CacheWriteBatch(PairWriteBatch):
    def __init__(self, data):
        self.data = data

    def put(self, k, v):
        self.data[k] = v

    def write(self):
        pass

    def close(self):
        pass


class MmapAdapter(PairAdapter):
    def destroy(self):
        self.close()
        os.remove(self._file.name)

    def __init__(self, options):
        super().__init__(options)
        path = options["path"]
        self._file = open(path, "w+b")

    def is_sorted(self):
        return False

    def close(self):
        self._file.close()

    def iteritems(self):
        return MmapIterator(self._file)

    def new_batch(self):
        return MmapWriteBatch(self._file)

    def get(self, key):
        raise NameError("unsupported")

    def put(self, key, value):
        raise NameError("unsupported")

class MmapIterator(PairIterator):
    def __init__(self, file):
        self.mm = mmap.mmap(file.fileno(), 0)
        self.mm.seek(0)
        self.reader = PairBinReader(ArrayByteBuffer(self.mm))

    def close(self):
        pass

    def __iter__(self):
        return self.reader.read_all()

class MmapWriteBatch(PairWriteBatch):
    def __init__(self, file):
        self._file = open(file.name, "wb")
        self.writer = PairBinWriter(FileByteBuffer(self._file ))

    def put(self, k, v):
        self.writer.write(k, v)

    def write(self):
        pass

    def close(self):
        self._file.close()

class BrokerAdapter(PairAdapter):
    def __init__(self, broker: Broker, options={}):
        super().__init__(options=options)
        self.__broker = broker

    def close(self):
        pass

    def iteritems(self):
        return BrokerIterator(self.__broker)

    def new_batch(self):
        return BrokerWriteBatch(self.__broker)

    def is_sorted(self):
        return False


class BrokerIterator(PairIterator):
    def __init__(self, broker):
        self.__broker = broker

    def __iter__(self):
        return self

    def __next__(self):
        from queue import Empty
        while True:
            try:
                if not self.__broker.is_closable():
                    return self.__broker.get(block=True, timeout=0.1)
                else:
                    raise StopIteration()
            except Empty:
                #print('waiting for broker to fill')
                pass


class BrokerWriteBatch(PairWriteBatch):
    def __init__(self, broker):
        self.__broker = broker

    def put(self, k, v):
        if self.__broker.get_active_writers_count():
            self.__broker.put((k, v))

    def write(self):
        pass

    def close(self):
        self.__broker.signal_write_finish()
