import unittest

from eggroll.core.io.kv_adapter import LmdbSortedKvAdapter, RocksdbSortedKvAdapter

namePath = "/tmp/eggroll/rollpair.lmdb/namespace/name/"
testPath = "/tmp/eggroll/rollpair.lmdb/namespace/test/"
lmdbMapValuesPath = "/tmp/eggroll/rollpair.lmdb/test_storage_namespace/testMapValues/"
lmdbMapPartitions2Path = "/tmp/eggroll/rollpair.lmdb/test_storage_namespace/testMapPartitions2/"
leveldbPath = "/tmp/eggroll/LEVEL_DB/test_storage_namespace/storage_name/"
leveldbMapvaluesPath = "/tmp/eggroll/LEVEL_DB/test_storage_namespace/testMapValues/"

class TestStorage(unittest.TestCase):

  def testLmdbAdapterPut(self):
    lmdb_adapter = LmdbSortedKvAdapter(options={"path" : namePath})
    for i in range(10):
      lmdb_adapter.put(bytes(str(i), encoding='utf8'), bytes("a", encoding='utf8'))
    print("finish put")
    for i in range(10):
      print("value of key:{} is: {}".format(i, lmdb_adapter.get(bytes(str(i), encoding='utf8'))))
    #must call close, otherwise will not put sucessfully
    lmdb_adapter.close()

  def testLmdbAdapterPutInt(self):
    lmdb_adapter = LmdbSortedKvAdapter(options={"path" : reduceDBPath})
    for i in range(10):
      lmdb_adapter.put(bytes(str(i), encoding='utf8'), bytes(str(i), encoding='utf8'))
    print("finish put")
    for i in range(10):
      print("value of key:{} is: {}".format(i, lmdb_adapter.get(bytes(str(i), encoding='utf8'))))
    #must call close, otherwise will not put sucessfully
    lmdb_adapter.close()

  def testLmdbAdapterGet(self):
    lmdb_adapter = LmdbSortedKvAdapter(options={"path" : namePath})
    for i in range(10):
      print("value of key:{} is: {}".format(i, lmdb_adapter.get(bytes(str(i), encoding='utf8'))))
    lmdb_adapter.close()

  def testLmdbIterator(self):
    lmdb_adapter = LmdbSortedKvAdapter(options={"path" : namePath})
    lmdb_iterator = lmdb_adapter.iteritems()
    for k, v in lmdb_iterator:
      print("iterate table of key:{} value: {}".format(k, v))
    #for i in range(10):
    #  print("value of key:{} is: {}".format(i, lmdb_adapter.get(bytes(str(i), encoding='utf8'))))
    lmdb_adapter.close()

  def testLmdbAdapterGetMapValues(self):
    lmdb_adapter = LmdbSortedKvAdapter(options={"path" : lmdbMapValuesPath})
    for i in range(10):
      print("value of key:{} is: {}".format(i, lmdb_adapter.get(bytes(str(i), encoding='utf8'))))
    lmdb_adapter.close()

  def testLmdbAdapterGetMapPartitions2(self):
    lmdb_adapter = LmdbSortedKvAdapter(options={"path" : lmdbMapPartitions2Path})
    for i in range(10):
      print("value of key:{} is: {}".format(f"{i}_{i}_0", lmdb_adapter.get(bytes(f"{i}_{i}_0", encoding='utf8'))))
      print("value of key:{} is: {}".format(f"{i}_{i}_1", lmdb_adapter.get(bytes(f"{i}_{i}_1", encoding='utf8'))))
    lmdb_adapter.close()

  def testRocksdbAdapterPut(self):
    rocksdb_adapter = RocksdbSortedKvAdapter(options={"path" : leveldbPath})
    for i in range(10):
      rocksdb_adapter.put(bytes(str(i), encoding='utf8'), bytes("a", encoding='utf8'))
    print("finish put")
    for i in range(10):
      print("value of key:{} is: {}".format(i, rocksdb_adapter.get(bytes(str(i), encoding='utf8'))))
    #must call close, otherwise will not put sucessfully
    rocksdb_adapter.close()

  def testRocksdbAdapterGet(self):
    rocksdb_adapter = RocksdbSortedKvAdapter(options={"path" : leveldbPath})
    for i in range(10):
      print("value of key:{} is: {}".format(i, rocksdb_adapter.get(bytes(str(i), encoding='utf8'))))
    rocksdb_adapter.close()

  def testRocksdbIterator(self):
    rocksdb_adapter = RocksdbSortedKvAdapter(options={"path" : leveldbPath})
    rocksdb_iterator = rocksdb_adapter.iteritems()
    for k, v in rocksdb_iterator:
      print("iterate table of key:{} value: {}".format(k, v))
    rocksdb_adapter.close()

  def testRocksdbAdapterGetMapValues(self):
    rocksdb_adapter = RocksdbSortedKvAdapter(options={"path" : leveldbMapvaluesPath})
    for i in range(10):
      print("value of key:{} is: {}".format(i, rocksdb_adapter.get(bytes(str(i), encoding='utf8'))))
    rocksdb_adapter.close()

if __name__ == "__main__":
  unittest.main()
