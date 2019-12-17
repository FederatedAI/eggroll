from eggroll.core.meta_model import ErPartition


def get_db_path(partition: ErPartition):
    store_locator = partition._store_locator
    db_path_prefix = '/tmp/eggroll/'

    return db_path_prefix + "/".join(
        [store_locator._store_type, store_locator._namespace, store_locator._name,
         str(partition._id)])


def generator(key_serde, value_serde, iterator):
    for k, v in iterator:
        yield key_serde.deserialize(k), value_serde.deserialize(v)


def partitioner(hash_func, total_partitions):
    return lambda k: hash_func(k) % total_partitions
