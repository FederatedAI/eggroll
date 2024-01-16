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

import typing
from typing import Callable, Iterable, Any, Tuple

from ._partitioner import mmh3_partitioner
from ._serdes import Serdes

if typing.TYPE_CHECKING:
    from ._wrap_session import WrappedSession
    from eggroll.computing import RollPairContext, RollPair


def runtime_init(session: "WrappedSession") -> "WrappedRpc":
    from eggroll.computing import runtime_init as _runtime_init

    rpc = _runtime_init(session=session._session)
    return WrappedRpc(rpc=rpc)


class WrappedRpc:
    def __init__(self, rpc: "RollPairContext" = None, session: "WrappedSession" = None):
        if rpc is not None:
            self._rpc = rpc
        if session is not None:
            from eggroll.computing import runtime_init as _runtime_init

            self._rpc = _runtime_init(session=session._session)

    @property
    def session_id(self):
        return self._rpc.session.get_session_id()

    def load(self, namespace, name, options):
        from eggroll.computing.tasks.store import StoreTypes

        store_type = options.get("store_type", StoreTypes.ROLLPAIR_LMDB)
        total_partitions = options.get("total_partitions", 1)
        return WrappedRp(
            self._rpc.create_rp(
                id=-1,
                name=name,
                namespace=namespace,
                total_partitions=total_partitions,
                store_type=store_type,
                key_serdes_type=0,
                value_serdes_type=0,
                partitioner_type=0,
                options=options,
            )
        )

    def parallelize(self, data, options: dict = None):
        from eggroll.computing.tasks.store import StoreTypes
        import uuid

        if options is None:
            options = {}
        namespace = options.get("namespace", None)
        name = options.get("name", None)
        if namespace is None:
            namespace = self.session_id
        if name is None:
            name = str(uuid.uuid1())
        include_key = options.get("include_key", False)
        total_partitions = options.get("total_partitions", 1)
        partitioner = mmh3_partitioner
        partitioner_type = 0
        key_serdes_type = 0
        value_serdes_type = 0
        store_type = options.get("store_type", StoreTypes.ROLLPAIR_IN_MEMORY)

        # generate data
        if include_key:
            data = (
                (Serdes.serialize(key), Serdes.serialize(value)) for key, value in data
            )
        else:
            data = (
                (Serdes.serialize(i), Serdes.serialize(value))
                for i, value in enumerate(data)
            )
        return WrappedRp(
            self._rpc.parallelize(
                data=data,
                total_partitions=total_partitions,
                partitioner=partitioner,
                partitioner_type=partitioner_type,
                key_serdes_type=key_serdes_type,
                value_serdes_type=value_serdes_type,
                store_type=store_type,
                namespace=namespace,
                name=name,
            )
        )

    def cleanup(self, name, namespace):
        return self._rpc.cleanup(name=name, namespace=namespace)

    def stop(self):
        return self._rpc.session.stop()

    def kill(self):
        return self._rpc.session.kill()


class WrappedRp:
    def __init__(self, rp: "RollPair"):
        self._rp = rp

    def get_namespace(self):
        return self._rp.get_namespace()

    def get_name(self):
        return self._rp.get_name()

    def get_partitions(self):
        return self._rp.get_partitions()

    def save_as(self, name, namespace, partition=None, options: dict = None):
        if partition is not None and partition <= 0:
            raise ValueError('partition cannot <= 0')

        if partition is not None and partition != self.num_partitions:
            repartitioned = self.repartition(num_partitions=partition)
            return repartitioned.save_as(name, namespace, options=options)

        if options is None:
            options = {}

        from eggroll.computing.tasks.store import StoreTypes
        store_type = options.get("store_type", StoreTypes.ROLLPAIR_LMDB)
        return WrappedRp(self._rp.copy_as(name=name, namespace=namespace, store_type=store_type))

    def get(self, k, options: dict = None):
        if options is None:
            options = {}
        value = self._rp.get(k=Serdes.serialize(k), partitioner=mmh3_partitioner)
        if value is None:
            return None
        else:
            return Serdes.deserialize(value)

    def put(self, k, v, options: dict = None):
        if options is None:
            options = {}
        self._rp.put(
            k=Serdes.serialize(k),
            v=Serdes.serialize(v),
            partitioner=mmh3_partitioner,
        )

    def delete(self, k, options: dict = None):
        if options is None:
            options = {}
        self._rp.delete(k=Serdes.serialize(k), partitioner=mmh3_partitioner)

    def count(self):
        return self._rp.count()

    def get_all(self, limit=None, options: dict = None):
        for k, v in self._rp.get_all(limit=limit):
            yield Serdes.deserialize(k), Serdes.deserialize(v)

    def put_all(self, items, output=None, options: dict = None):
        if options is None:
            options = {}
        include_key = options.get("include_key", True)
        if include_key:
            self._rp.put_all(
                ((Serdes.serialize(k), Serdes.serialize(v)) for k, v in items),
                partitioner=mmh3_partitioner,
            )
        else:
            self._rp.put_all(
                (
                    (Serdes.serialize(i), Serdes.serialize(v))
                    for i, v in enumerate(items)
                ),
                partitioner=mmh3_partitioner,
            )

    def take(self, n: int, options: dict = None):
        keys_only = options.get("keys_only", False)
        if keys_only:
            return [Serdes.deserialize(k) for k in self._rp.take(n=n, options=options)]
        else:
            return [
                (Serdes.deserialize(k), Serdes.deserialize(v))
                for k, v in self._rp.take(num=n, options=options)
            ]

    def first(self, options: dict = None):
        resp = self.take(1, options=options)
        if resp:
            return resp[0]
        else:
            return None

    def destroy(self, options: dict = None):
        self._rp.destroy()

    def _map_reduce_partitions_with_index(
            self,
            map_partition_op: Callable[[int, Iterable], Iterable],
            reduce_partition_op: Callable[[Any, Any], Any] = None,
            shuffle=True,
            output_num_partitions=None,
    ):
        if not shuffle and reduce_partition_op is not None:
            raise ValueError(
                "when shuffle is False, it is not allowed to specify reduce_partition_op"
            )
        if output_num_partitions is None:
            output_num_partitions = self._rp.get_partitions()

        mapper = _lifted_map_to_io_serdes(
            map_partition_op,
            Serdes,
            Serdes,
            Serdes,
            Serdes,
        )
        return WrappedRp(
            self._rp.map_reduce_partitions_with_index(
                map_partition_op=mapper,
                reduce_partition_op=_lifted_mpwi_reduce_to_serdes(
                    reduce_partition_op, Serdes
                ),
                shuffle=shuffle,
                input_key_serdes=Serdes,
                input_key_serdes_type=0,
                input_value_serdes=Serdes,
                input_value_serdes_type=0,
                input_partitioner=mmh3_partitioner,
                input_partitioner_type=0,
                output_key_serdes=Serdes,
                output_key_serdes_type=0,
                output_value_serdes=Serdes,
                output_value_serdes_type=0,
                output_partitioner=mmh3_partitioner,
                output_partitioner_type=0,
                output_num_partitions=output_num_partitions,
            )
        )

    def map_values(self, func, output=None, options: dict = None):
        if options is None:
            options = {}
        return self._map_reduce_partitions_with_index(
            _lifted_map_values_to_mpwi(func),
            shuffle=False,
        )

    def map(self, func, output=None, options: dict = None):
        if options is None:
            options = {}
        return self._map_reduce_partitions_with_index(
            _lifted_map_to_mpwi(func),
            shuffle=False,
        )

    def map_partitions(self, func, reduce_op=None, output=None, options: dict = None):
        if options is None:
            options = {}
        shuffle = options.get("shuffle", True)
        return self._map_reduce_partitions_with_index(
            _lifted_map_partitions_to_mpwi(func),
            reduce_partition_op=reduce_op,
            shuffle=shuffle,
        )

    def collapse_partitions(self, func, output=None, options: dict = None):
        if options is None:
            options = {}
        return self._map_reduce_partitions_with_index(
            _lifted_apply_partitions_to_mpwi(func),
            shuffle=False,
        )

    def map_partitions_with_index(self, func, output=None, options: dict = None):
        if options is None:
            options = {}
        shuffle = options.get("shuffle", True)
        return self._map_reduce_partitions_with_index(
            map_partition_op=func,
            shuffle=shuffle,
        )

    def flat_map(self, func, output=None, options: dict = None):
        if options is None:
            options = {}
        return self._map_reduce_partitions_with_index(
            _lifted_flat_map_to_mpwi(func),
            shuffle=False,
        )

    def reduce(self, func, output=None, options: dict = None):
        if options is None:
            options = {}

        reduced = self._rp.reduce(
            func=_lifted_reduce_to_serdes(func, Serdes),
        )
        if reduced is None:
            return None
        else:
            return Serdes.deserialize(reduced)

    def sample(self, fraction, seed=None, output=None, options: dict = None):
        if options is None:
            options = {}
        return self._map_reduce_partitions_with_index(
            _lifted_sample_to_mpwi(fraction, seed),
            shuffle=False,
        )

    def filter(self, func, output=None, options: dict = None):
        if options is None:
            options = {}
        return self._map_reduce_partitions_with_index(
            lambda i, x: ((k, v) for k, v in x if func(k, v)),
            shuffle=False,
        )

    @property
    def num_partitions(self):
        return self._rp.get_partitions()

    def repartition(self, num_partitions) -> "WrappedRp":
        if self.num_partitions == num_partitions:
            return self

        return WrappedRp(
            self._rp.map_reduce_partitions_with_index(
                map_partition_op=lambda i, x: x,
                reduce_partition_op=None,
                shuffle=True,
                input_key_serdes=Serdes,
                input_key_serdes_type=0,
                input_value_serdes=Serdes,
                input_value_serdes_type=0,
                input_partitioner=mmh3_partitioner,
                input_partitioner_type=0,
                output_key_serdes=Serdes,
                output_key_serdes_type=0,
                output_value_serdes=Serdes,
                output_value_serdes_type=0,
                output_partitioner=mmh3_partitioner,
                output_partitioner_type=0,
                output_num_partitions=num_partitions,
            )
        )

    def repartition_with(self, other: "WrappedRp") -> Tuple["WrappedRp", "WrappedRp"]:
        if self._rp.num_partitions == other._rp.num_partitions:
            return self, other
        if self._rp.num_partitions > other._rp.num_partitions:
            return self, other.repartition(self.num_partitions)
        else:
            return self.repartition(other.num_partitions), other

    def binarySortedMapPartitionsWithIndex(
            self,
            other: "WrappedRp",
            binary_sorted_map_partitions_with_index_op: Callable[
                [int, Iterable, Iterable], Iterable
            ],
    ):
        first, second = self.repartition_with(other)

        # apply binary_sorted_map_partitions_with_index_op
        return WrappedRp(
            first._rp.binary_sorted_map_partitions_with_index(
                other=second._rp,
                binary_map_partitions_with_index_op=_lifted_sorted_binary_map_partitions_with_index_to_serdes(
                    binary_sorted_map_partitions_with_index_op,
                    Serdes,
                    Serdes,
                    Serdes,
                ),
                key_serdes=Serdes,
                key_serdes_type=0,
                partitioner=mmh3_partitioner,
                partitioner_type=0,
                first_input_value_serdes=Serdes,
                first_input_value_serdes_type=0,
                second_input_value_serdes=Serdes,
                second_input_value_serdes_type=0,
                output_value_serdes=Serdes,
                output_value_serdes_type=0,
            )
        )

    def subtract_by_key(self, other, output=None, options: dict = None):
        if options is None:
            options = {}
        return self.binarySortedMapPartitionsWithIndex(
            other=other,
            binary_sorted_map_partitions_with_index_op=_lifted_subtract_by_key_to_sbmpwi(),
        )

    def union(self, other, func=lambda v1, v2: v1, output=None, options: dict = None):
        if options is None:
            options = {}
        return self.binarySortedMapPartitionsWithIndex(
            other=other,
            binary_sorted_map_partitions_with_index_op=_lifted_union_merge_to_sbmpwi(
                func
            ),
        )

    def join(self, other, func, output=None, options: dict = None):
        if options is None:
            options = {}
        return self.binarySortedMapPartitionsWithIndex(
            other=other,
            binary_sorted_map_partitions_with_index_op=_lifted_join_merge_to_sbmpwi(
                func
            ),
        )


def _lifted_map_to_io_serdes(
        _f, input_key_serdes, input_value_serdes, output_key_serdes, output_value_serdes
):
    def _lifted(_index, _iter):
        for out_k, out_v in _f(
                _index,
                _serdes_wrapped_generator(_iter, input_key_serdes, input_value_serdes),
        ):
            yield output_key_serdes.serialize(out_k), output_value_serdes.serialize(
                out_v
            )

    return _lifted


def _serdes_wrapped_generator(_iter, key_serdes, value_serdes):
    for k, v in _iter:
        yield key_serdes.deserialize(k), value_serdes.deserialize(v)


def _value_serdes_wrapped_generator(_iter, value_serdes):
    for k, v in _iter:
        yield k, value_serdes.deserialize(v)


def _lifted_mpwi_map_to_serdes(
        _f, input_key_serdes, input_value_serdes, output_key_serdes, output_value_serdes
):
    def _lifted(_index, _iter):
        for out_k, out_v in _f(
                _index,
                _serdes_wrapped_generator(_iter, input_key_serdes, input_value_serdes),
        ):
            yield output_key_serdes.serialize(out_k), output_value_serdes.serialize(
                out_v
            )

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


def _lifted_map_reduce_partitions_to_mpwi(
        map_partition_op: Callable[[Iterable], Iterable]
):
    def _lifted(_index, _iter):
        return map_partition_op(_iter)

    return _lifted


def _get_generator_with_last_key(_iter):
    cache = [None]

    def _generator():
        for k, v in _iter:
            cache[0] = k
            yield k, v

    return _generator, cache


def _lifted_apply_partitions_to_mpwi(apply_partition_op: Callable[[Iterable], Any]):
    def _lifted(_index, _iter):
        _iter_set_cache, _cache = _get_generator_with_last_key(_iter)
        value = apply_partition_op(_iter_set_cache())
        key = _cache[0]
        if key is None:
            return []
        return [(key, value)]

    return _lifted


def _lifted_map_partitions_to_mpwi(map_partition_op: Callable[[Iterable], Iterable]):
    def _lifted(_index, _iter):
        return map_partition_op(_iter)

    return _lifted


def _lifted_flat_map_to_mpwi(
        flat_map_op: Callable[[Any, Any], Iterable[Tuple[Any, Any]]]
):
    def _lifted(_index, _iter):
        for _k, _v in _iter:
            yield from flat_map_op(_k, _v)

    return _lifted


def _lifted_sample_to_mpwi(fraction, seed=None):
    def _lifted(_index, _iter):
        import random

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


def _lifted_sorted_binary_map_partitions_with_index_to_serdes(
        _f, left_value_serdes, right_value_serdes, output_value_serdes
):
    def _lifted(_index, left_iter, right_iter):
        for out_k_bytes, out_v in _f(
                _index,
                _value_serdes_wrapped_generator(left_iter, left_value_serdes),
                _value_serdes_wrapped_generator(right_iter, right_value_serdes),
        ):
            yield out_k_bytes, output_value_serdes.serialize(out_v)

    return _lifted


def _lifted_join_merge_to_sbmpwi(join_merge_op):
    def _lifted(_index, _left_iter, _right_iter):
        return _merge_intersecting_keys(_left_iter, _right_iter, join_merge_op)

    return _lifted


def _lifted_union_merge_to_sbmpwi(join_merge_op):
    def _lifted(_index, _left_iter, _right_iter):
        return _merge_union_keys(_left_iter, _right_iter, join_merge_op)

    return _lifted


def _lifted_subtract_by_key_to_sbmpwi():
    def _lifted(_index, _left_iter, _right_iter):
        return _subtract_by_key(_left_iter, _right_iter)

    return _lifted


def _merge_intersecting_keys(iter1, iter2, merge_op):
    try:
        item1 = next(iter1)
        item2 = next(iter2)
    except StopIteration:
        return

    while True:
        key1, value1 = item1
        key2, value2 = item2

        if key1 == key2:
            yield key1, merge_op(value1, value2)
            try:
                item1 = next(iter1)
                item2 = next(iter2)
            except StopIteration:
                break
        elif key1 < key2:
            try:
                item1 = next(iter1)
            except StopIteration:
                break
        else:  # key1 > key2
            try:
                item2 = next(iter2)
            except StopIteration:
                break


def _merge_union_keys(iter1, iter2, merge_op):
    try:
        item1 = next(iter1)
    except StopIteration:
        item1 = None

    try:
        item2 = next(iter2)
    except StopIteration:
        item2 = None

    if item1 is None and item2 is None:
        return

    while item1 is not None and item2 is not None:
        key1, value1 = item1
        key2, value2 = item2

        if key1 == key2:
            yield key1, merge_op(value1, value2)
            try:
                item1 = next(iter1)
            except StopIteration:
                item1 = None
            try:
                item2 = next(iter2)
            except StopIteration:
                item2 = None
        elif key1 < key2:
            yield key1, value1
            try:
                item1 = next(iter1)
            except StopIteration:
                item1 = None
        else:  # key1 > key2
            yield key2, value2
            try:
                item2 = next(iter2)
            except StopIteration:
                item2 = None

    if item1 is not None:
        yield item1
        yield from iter1
    elif item2 is not None:
        yield item2
        yield from iter2


def _subtract_by_key(iter1, iter2):
    try:
        item1 = next(iter1)
    except StopIteration:
        return

    try:
        item2 = next(iter2)
    except StopIteration:
        yield item1
        yield from iter1
        return

    while item1 is not None and item2 is not None:
        key1, value1 = item1
        key2, value2 = item2

        if key1 == key2:
            try:
                item1 = next(iter1)
            except StopIteration:
                item1 = None
            try:
                item2 = next(iter2)
            except StopIteration:
                item2 = None
        elif key1 < key2:
            yield item1
            try:
                item1 = next(iter1)
            except StopIteration:
                item1 = None
        else:  # key1 > key2
            try:
                item2 = next(iter2)
            except StopIteration:
                item2 = None

    if item1 is not None:
        yield item1
        yield from iter1
