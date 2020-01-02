import unittest
from arch.api.table.eggroll2.session_impl import FateSessionImpl
from arch.api import session
import uuid

class TestFateApi(unittest.TestCase):
  def setUp(self):
    session.init("testing")

  def test_parallelize(self):
    print("session id:{}".format(session.get_session_id()))
    print(session.parallelize(range(10)).get_all())

  def test_put_all(self):
    table = session.table(name='put_all_name', namespace='put_all_namespace', partition=3)
    print(table.put_all(range(15)).get_all())

  def test_mapValues(self):
    table = session.table(name='mapValues_name', namespace='mapValues_namespace', partition=3)
    print(table.put_all(range(15)).map_values(lambda v: v + 1).get_all())

  def test_mapPartitions(self):
    table = session.table(name='mapPartitions_name', namespace='mapPartitions_namespace', partition=3)
    def func(iter):
      ret = []
      for k, v in iter:
        ret.append((f"{k}_{v}_0", v ** 2))
        ret.append((f"{k}_{v}_1", v ** 3))
      return ret
    print(table.put_all(range(15)).map_partitions(func).get_all())

  def test_map(self):
    table = session.table(name='map_name', namespace='map_namespace', partition=3).put_all(['a', 'b', 'c'])
    print(table.map(lambda k, v: (v, v + '1')).get_all())

  def test_collapsePartitions(self):
    table = session.table(name='collapsePartitions_name',
                          namespace='collapsePartitions_namespace',
                          partition=3)
    def f(iterator):
      sum = []
      for k, v in iterator:
        sum.append((k, v))
      return sum
    print(table.put_all([1, 2, 3, 4, 5]).collapse_partitions(f).get_all())

  def test_filter(self):
    table = session.table(name='filter_name',
                          namespace='filter_namespace',
                          partition=3)
    print(table.put_all(range(15)).filter(lambda k, v: v % 2 != 0).get_all())

  def test_flatMap(self):
    table = session.table(name='flatMap_name',
                          namespace='flatMap_namespace',
                          partition=3)
    def foo(k, v):
      result = []
      import random
      r = random.randint(10000, 99999)
      for i in range(0, k):
        result.append((k + r + i, v + r + i))
      return result
    print(table.put_all(range(5)).flat_map(foo).get_all())

  def test_glom(self):
    table = session.table(name='glom_name',
                          namespace='glom_namespace',
                          partition=3)
    print(table.put_all(range(5)).glom().get_all())

  def test_join(self):
    left_table = session.table(name="leftJoinName",
                               namespace="leftJoinNamespace",
                               partition=3)\
      .put_all([('a', 1), ('b', 4)], options={"include_key": True})
    right_table = session.table(name="rightJoinName",
                               namespace="rightJoinNamespace",
                               partition=3) \
      .put_all([('a', 2), ('c', 4)], options={"include_key": True})
    print(left_table.join(right_table, lambda v1, v2: v1 + v2).get_all())

  def test_reduce(self):
    table = session.table(name='reduce_name',
                          namespace='reduce_namespace',
                          partition=3)
    from operator import add
    print(table.put_all(range(5)).reduce(add))

  def test_sample(self):
    table = session.table(name='sample_name',
                          namespace='sample_namespace',
                          partition=3)
    print(6 <= table.put_all(range(100)).sample(0.1, 81).count() <= 14)

  def test_subtractByKey(self):
    left_rp = session.table(name="leftSubtractByKeyName120611",
                            namespace="leftSubtractByKeyNamespace120611",
                            partition=1)\
      .put_all(range(10))
    right_rp = session.table(name="rightSubtractByKeyName120611",
                            namespace="rightSubtractByKeyNamespace120611",
                            partition=1) \
      .put_all(range(5))
    print('left:{}'.format(left_rp.get_all()))
    print('right:{}'.format(right_rp.get_all()))
    print(left_rp.subtract_by_key(right_rp).get_all())

  def test_union(self):
    left_rp = session.table(name="leftUnionName120611",
                            namespace="leftUnionNamespace120611",
                            partition=1) \
      .put_all([1, 2, 3])
    right_rp = session.table(name="rightUnionName120611",
                             namespace="rightUnionNamespace120611",
                             partition=1) \
      .put_all([(1, 1), (2, 2), (3, 3)], options={"include_key": True})
    print("left:{}, partitions:{}".format(left_rp.get_all(), left_rp.get_partitions()))
    print("right:{}, partitions:{}".format(right_rp.get_all(), right_rp.get_partitions()))
    print(left_rp.union(right_rp, lambda v1, v2 : v1 + v2).get_all())

  def test_remote(self):
    table = session.table(name='remote_name', namespace='remote_namespace', partition=1)
    table.put_all(range(12))
    from arch.api import federation
    federation.init(session.get_session_id(), runtime_conf=None)
    federation.remote(table, name="roll_pair_name.table", tag="roll_pair_tag")

