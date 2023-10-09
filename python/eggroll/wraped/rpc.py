import random
from typing import Any, Callable, Tuple, Iterable, Generic, TypeVar
from eggroll.roll_pair.roll_pair import RollPairContext as RPC, RollPair as RP
from .unsafe_serdes import get_unsafe_serdes


class RollPairContext:
    def __init__(self, rpc: RPC):
        self.rpc = rpc

    def parallelize(
        self, data, include_key=True, total_partitions=None, partitioner=None, key_serdes=None, value_serdes=None
    ) -> "KVTable":
        if key_serdes is None:
            key_serdes = get_unsafe_serdes()
        if value_serdes is None:
            value_serdes = get_unsafe_serdes()
        if not include_key:
            data = ((key_serdes.serialize(i), value_serdes.serialize(v)) for i, v in enumerate(data))
        else:
            data = ((key_serdes.serialize(k), value_serdes.serialize(v)) for k, v in data)
        return KVTable(
            self.rpc.parallelize(data=data, total_partitions=total_partitions, partitioner=partitioner),
            key_serdes=key_serdes,
            value_serdes=value_serdes,
        )


K = TypeVar("K")
V = TypeVar("V")


class KVTable(Generic[K, V]):
    def __init__(self, rp: RP, key_serdes, value_serdes):
        self.rp = rp
        self.key_serdes = key_serdes
        self.value_serdes = value_serdes

    def map_reduce_partitions_with_index(
        self,
        map_partition_op: Callable[[int, Iterable], Iterable],
        reduce_partition_op: Callable[[Any, Any], Any] = None,
        shuffle=True,
        output_key_serdes=None,
        output_value_serdes=None,
    ):
        if output_key_serdes is None:
            output_key_serdes = get_unsafe_serdes()
        if output_value_serdes is None:
            output_value_serdes = get_unsafe_serdes()

        return KVTable(
            self.rp.map_partitions_with_index(
                map_op=_lifted_mpwi_map_to_serdes(
                    map_partition_op, self.key_serdes, self.value_serdes, output_key_serdes, output_value_serdes
                ),
                reduce_op=_lifted_mpwi_reduce_to_serdes(reduce_partition_op, output_value_serdes),
                shuffle=shuffle,
            ),
            key_serdes=output_key_serdes,
            value_serdes=output_value_serdes,
        )

    def map_reduce_partitions(
        self,
        map_partition_op: Callable[[Iterable], Iterable],
        reduce_partition_op: Callable[[Any, Any], Any] = None,
        shuffle=True,
        output_key_serdes=None,
        output_value_serdes=None,
    ):
        return self.map_reduce_partitions_with_index(
            map_partition_op=_lifted_map_reduce_partitions_to_mpwi(map_partition_op),
            reduce_partition_op=reduce_partition_op,
            shuffle=shuffle,
            output_key_serdes=output_key_serdes,
            output_value_serdes=output_value_serdes,
        )

    def map(
        self,
        map_op: Callable[[Any, Any], Tuple[Any, Any]],
        output_key_serdes=None,
        output_value_serdes=None,
        output_partitioner=None,
    ):
        return self.map_reduce_partitions_with_index(
            _lifted_map_to_mpwi(map_op),
            shuffle=True,
            output_key_serdes=output_key_serdes,
            output_value_serdes=output_value_serdes,
        )

    def map_values(self, map_value_op: Callable[[Any], Any], output_value_serdes=None):
        return self.map_reduce_partitions_with_index(
            _lifted_map_values_to_mpwi(map_value_op),
            shuffle=False,
            output_key_serdes=self.key_serdes,
            output_value_serdes=output_value_serdes,
        )

    def flat_map(
        self,
        flat_map_op: Callable[[Any, Any], Iterable[Tuple[Any, Any]]],
        output_key_serdes=None,
        output_value_serdes=None,
    ):
        return self.map_reduce_partitions_with_index(
            _lifted_flat_map_to_mpwi(flat_map_op),
            shuffle=True,
            output_key_serdes=output_key_serdes,
            output_value_serdes=output_value_serdes,
        )

    def filter(self, filter_op: Callable[[Any], bool]):
        return self.map_reduce_partitions_with_index(
            lambda i, x: ((k, v) for k, v in x if filter_op(v)),
            shuffle=False,
            output_key_serdes=self.key_serdes,
            output_value_serdes=self.value_serdes,
        )

    def sample(self, fraction, seed=None):
        return self.map_reduce_partitions_with_index(
            _lifted_sample_to_mpwi(fraction, seed),
            shuffle=False,
            output_key_serdes=self.key_serdes,
            output_value_serdes=self.value_serdes,
        )

    def collect(self):
        for k, v in self.rp.get_all():
            yield self.key_serdes.deserialize(k), self.value_serdes.deserialize(v)

    def reduce(self, func: Callable[[V, V], V]) -> V:
        return self.value_serdes.deserialize(self.rp.reduce(_lifted_reduce_to_serdes(func, self.value_serdes)))

    def count(self) -> int:
        return self.rp.count()

    def join(self, other: "KVTable", merge_op: Callable[[V, V], V] = None):
        return KVTable(
            self.rp.join(other.rp, _lifted_join_merge_to_serdes(merge_op, self.value_serdes)),
            key_serdes=self.key_serdes,
            value_serdes=self.value_serdes,
        )

    def union(self, other, merge_op: Callable[[V, V], V] = None):
        return KVTable(
            self.rp.union(other.rp, _lifted_join_merge_to_serdes(merge_op, self.value_serdes)),
            key_serdes=self.key_serdes,
            value_serdes=self.value_serdes,
        )

    def destroy(self):
        return self.rp.destroy()

    def put_all(self, kv_list):
        return self.rp.put_all(kv_list)

    def put(self, k, v):
        return self.rp.put(k, v)

    def get(self, k):
        return self.value_serdes.deserialize(self.rp.get(k))

    def delete(self, k):
        return self.rp.delete(k)

    def save_as(self, name, namespace, partition=None, options=None):
        return self.rp.save_as(name=name, namespace=namespace, partition=partition, options=options)


def _serdes_wrapped_generator(_iter, key_serdes, value_serdes):
    for k, v in _iter:
        yield key_serdes.deserialize(k), value_serdes.deserialize(v)


def _lifted_mpwi_map_to_serdes(_f, input_key_serdes, input_value_serdes, output_key_serdes, output_value_serdes):
    def _lifted(_index, _iter):
        for out_k, out_v in _f(_index, _serdes_wrapped_generator(_iter, input_key_serdes, input_value_serdes)):
            yield output_key_serdes.serialize(out_k), output_value_serdes.serialize(out_v)

    return _lifted


def _lifted_mpwi_reduce_to_serdes(_f, output_value_serdes):
    if _f is None:
        return None

    def _lifted(x, y):
        return output_value_serdes.serialize(
            _f(
                output_value_serdes.deserialize(x),
                output_value_serdes.deserialize(y),
            )
        )

    return _lifted


def _lifted_map_values_to_mpwi(map_value_op: Callable[[Any], Any]):
    def _lifted(_index, _iter):
        for _k, _v in _iter:
            yield _k, map_value_op(_v)

    return _lifted


def _lifted_map_to_mpwi(map_op: Callable[[Any, Any], Tuple[Any, Any]]):
    def _lifted(_index, _iter):
        for _k, _v in _iter:
            yield map_op(_k, _v)

    return _lifted


def _lifted_map_reduce_partitions_to_mpwi(map_partition_op: Callable[[Iterable], Iterable]):
    def _lifted(_index, _iter):
        return map_partition_op(_iter)

    return _lifted


def _lifted_flat_map_to_mpwi(flat_map_op: Callable[[Any, Any], Iterable[Tuple[Any, Any]]]):
    def _lifted(_index, _iter):
        for _k, _v in _iter:
            yield from flat_map_op(_k, _v)

    return _lifted


def _lifted_sample_to_mpwi(fraction, seed=None):
    def _lifted(_index, _iter):
        # TODO: should we use the same seed for all partitions?
        random_state = random.Random(seed)
        for _k, _v in _iter:
            if random_state.random() < fraction:
                yield _k, _v

    return _lifted


def _lifted_reduce_to_serdes(reduce_op, value_serdes):
    def _lifted(x, y):
        return value_serdes.serialize(
            reduce_op(
                value_serdes.deserialize(x),
                value_serdes.deserialize(y),
            )
        )

    return _lifted


def _lifted_join_merge_to_serdes(join_merge_op, value_serdes):
    def _lifted(x, y):
        return value_serdes.serialize(
            join_merge_op(
                value_serdes.deserialize(x),
                value_serdes.deserialize(y),
            )
        )

    return _lifted
