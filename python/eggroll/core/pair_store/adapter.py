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

import os
import threading
from collections import OrderedDict

from eggroll.core.pair_store.format import create_byte_buffer, PairBinReader, PairBinWriter
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
    def __init__(self, options):
        super().__init__(options)
        self._file = open(options["path"], "r+b")

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
        self.reader = PairBinReader(create_byte_buffer(file, {"buffer_type": "file"}))

    def close(self):
        pass

    def __iter__(self):
        self.reader.iteritems()

class FileWriteBatch(PairWriteBatch):
    def __init__(self, file):
        self.writer = PairBinWriter(create_byte_buffer(file, {"buffer_type": "file"}))

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
        self.data = self.caches.get(self.path, OrderedDict())

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
        pass

    def get(self, key):
        pass

    def put(self, key, value):
        pass

class CacheIterator(PairIterator):
    def __init__(self, data):
        self.data = data

    def close(self):
        pass

    def __iter__(self):
        self.data.items()

class CacheWriteBatch(PairWriteBatch):
    def __init__(self, data):
        self.data = data

    def put(self, k, v):
        self.data[k] = v

    def write(self):
        pass

    def close(self):
        pass

