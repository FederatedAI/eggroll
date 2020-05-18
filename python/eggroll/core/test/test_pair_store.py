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
import time
import unittest
from eggroll.core.pair_store import *
from eggroll.core.pair_store.adapter import PairAdapter
from eggroll.core.pair_store.format import ArrayByteBuffer, PairBinReader, PairBinWriter
from eggroll.core.constants import StoreTypes

class TestPairStore(unittest.TestCase):
    dir = "./"
    # total = 1000 * 1000
    total = 100000
    def _run_case(self, db: PairAdapter):
        start = time.time()
        value = 's' * 1000
        with db.new_batch() as wb:
            for i in range(self.total):
                wb.put(str(i).encode(), value.encode())
        print("put:", time.time() - start)
        for i in range(3):
            start = time.time()
            with db.iteritems() as rb:
                cnt = 0
                for k, v in rb:
                    if cnt % 100000 == 0:
                        print("item:",cnt, k, v)
                    cnt += 1
                print(cnt)
                assert cnt == self.total
            print("time:", time.time() - start)

    def _run_join(self, db1: PairAdapter,  db2: PairAdapter):
        start = time.time()
        value = 's' * 1000
        with db1.new_batch() as wb:
            for i in range(self.total):
                wb.put(str(i).encode(), value.encode())
        with db2.new_batch() as wb:
            for i in range(self.total):
                wb.put(str(i).encode(), value.encode())
        print("put:", time.time() - start)
        with db1.iteritems() as rb1:
            cnt = 0
            for k, v in rb1:
                if cnt % 100000 == 0:
                    print("item:",cnt, k, v)
                cnt += 1
                db2.get(k)
            print(cnt)
            assert cnt == self.total
        print("time:", time.time() - start)

    def test_lmdb(self):
        with create_pair_adapter({"store_type": StoreTypes.ROLLPAIR_LMDB, "path": self.dir + "LMDB"}) as db:
            self._run_case(db)
            db.destroy()

    def test_lmdb_seek(self):
        with create_pair_adapter({"store_type": StoreTypes.ROLLPAIR_LMDB, "path": "./data/LMDB/test_pair_store"}) as db:
            with db.new_batch() as wb:
                for i in range(7):
                    if i == 5:
                        continue
                    wb.put(('k' + str(i)).encode(), ('v' + str(i)).encode())
            with db.iteritems() as it:
                for k, v in it:
                    print(k, v)
                print("++++++++++++++++++++++++++++++++++++++++++++++++")
                it.first()
                print(it.seek(b'k5'))
                for k, v in it:
                    print(k, v)

    def test_rocksdb(self):
        with create_pair_adapter({"store_type": StoreTypes.ROLLPAIR_LEVELDB, "path": self.dir + "rocksdb"}) as db:
            self._run_case(db)
            db.destroy()

    def test_file(self):
        with create_pair_adapter({"store_type": StoreTypes.ROLLPAIR_FILE, "path": self.dir + "file"}) as db:
            self._run_case(db)
            db.destroy()

    def test_mmap(self):
        with create_pair_adapter({"store_type": StoreTypes.ROLLPAIR_MMAP, "path": self.dir + "mmap"}) as db:
            self._run_case(db)
            db.destroy()

    def test_cache(self):
        with create_pair_adapter({"store_type": StoreTypes.ROLLPAIR_CACHE, "path": self.dir + "cache"}) as db:
            self._run_case(db)
            db.destroy()

    def test_byte_buffer(self):
        bs = bytearray(1024)
        buf = ArrayByteBuffer(bs)
        buf.write_int32(12)
        buf.write_bytes(b"34")
        buf.set_offset(0)
        assert buf.read_int32() == 12
        assert buf.read_bytes(2) == b"34"

    def test_pair_bin(self):
        bs = bytearray(32)
        buf = ArrayByteBuffer(bs)
        writer = PairBinWriter(buf)
        for i in range(10):
            try:
                writer.write(str(i).encode(), str(i).encode())
            except IndexError as e:
                print(buf.read_bytes(buf.get_offset(), 0))
                buf.set_offset(0)
                writer = PairBinWriter(buf)
                writer.write(str(i).encode(), str(i).encode())
        buf.set_offset(0)
        reader = PairBinReader(buf)
        print("last")
        print(list(reader.read_all()))

    def test_pair_bin_no_abb(self):
        bs = bytearray(32)
        buf = ArrayByteBuffer(bs)
        writer = PairBinWriter(pair_buffer=buf, data=bs)
        for i in range(10):
            try:
                writer.write(str(i).encode(), str(i).encode())
            except IndexError as e:
                writer.set_offset(0)
                writer = PairBinWriter(pair_buffer=buf, data=bs)
                writer.write(str(i).encode(), str(i).encode())
                pbr = PairBinReader(pair_buffer=buf, data=writer.get_data())
                print(pbr.read_bytes(writer.get_offset(), 0))
        writer.set_offset(0)
        reader = PairBinReader(pair_buffer=buf, data=bs)
        print("last")
        print(list(reader.read_all()))

    def test_join(self):
        with create_pair_adapter({"store_type": StoreTypes.ROLLPAIR_LMDB, "path": self.dir + "lmdb"}) as db1, \
                create_pair_adapter({"store_type": StoreTypes.ROLLPAIR_LMDB, "path": self.dir + "lmdb2"}) as db2:
            self._run_join(db1, db2)
            db1.destroy()
            db2.destroy()
