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

from eggroll.core.pair_store.format import PairBinReader, PairBinWriter, FileByteBuffer, ArrayByteBuffer
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
        pass

    def put(self, key, value):
        pass

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
    block_size = 64
    # block_size = 64 * 1024 * 1024
    def destroy(self):
        self.close()
        os.remove(self._file.name)

    def __init__(self, options):
        super().__init__(options)
        path = options["path"]
        self._file = open(path, "w+b")
        self.__grow_file(self._file, self.block_size)
        self._mm = mmap.mmap(self._file.fileno(), 0)

    def __grow_file(self, fd, size):
        old_offset = fd.tell()
        fd.seek(size - 1)
        fd.write(b"\0")
        fd.seek(old_offset)

    def _resize(self):
        file_size = os.fstat(self._file.fileno()).st_size
        self.__grow_file(self._file, file_size * 2)
        if self._mm:
            self._mm.close()
        self._mm = mmap.mmap(self._file.fileno(), 0)

    def is_sorted(self):
        return False

    def close(self):
        self._file.close()
        self._mm.close()

    def iteritems(self):
        return MmapIterator(self._mm)

    def new_batch(self):
        return MmapWriteBatch(self)

    def get(self, key):
        raise NameError("unsupported")

    def put(self, key, value):
        raise NameError("unsupported")

class MmapIterator(PairIterator):
    def __init__(self, mm):
        mm.seek(0)
        self.reader = PairBinReader(ArrayByteBuffer(mm))

    def close(self):
        pass

    def __iter__(self):
        return self.reader.read_all()
# TODO:1: bathes in file?
class MmapWriteBatch(PairWriteBatch):
    def __init__(self, db: MmapAdapter):
        self._db = db
        self._buffer = ArrayByteBuffer(db._mm)
        PairBinWriter.write_head(self._buffer)

    def put(self, k, v):
        try:
            PairBinWriter.write_pair(self._buffer, k, v)
        except IndexError as e:
            # increase file
            self._db._resize()
            buf = ArrayByteBuffer(self._db._mm)
            buf.set_offset(self._buffer.get_offset())
            self._buffer = buf
            PairBinWriter.write_pair(self._buffer, k, v)

    def write(self):
        pass

    def close(self):
        pass