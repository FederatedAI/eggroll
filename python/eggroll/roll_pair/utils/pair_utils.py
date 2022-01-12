from eggroll.core.meta_model import ErPartition

__data_dir = '/tmp/eggroll'


def set_data_dir(data_dir):
    global __data_dir
    if data_dir:
        from pathlib import Path
        abs_path = Path(data_dir).resolve()
        print(abs_path)
        __data_dir = str(abs_path)


def get_data_dir():
    global __data_dir
    return __data_dir


def get_db_path(partition: ErPartition):
    store_locator = partition._store_locator

    return "/".join(
        [__data_dir, store_locator._store_type, store_locator._namespace, store_locator._name,
         str(partition._id)])


def get_db_path_expanded(data_dir, store_type, namespace, name, partition_id):
    return "/".join(
            [data_dir, store_type, namespace, name, str(partition_id)])


def generator(key_serde, value_serde, iterator):
    for k, v in iterator:
        yield key_serde.deserialize(k), value_serde.deserialize(v)


def partitioner(hash_func, total_partitions):
    return lambda k: hash_func(k) % total_partitions
