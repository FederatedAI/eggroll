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


def generator(key_serde, value_serde, iterator):
    for k, v in iterator:
        yield key_serde.deserialize(k), value_serde.deserialize(v)


def partitioner(hash_func, total_partitions):
    return lambda k: hash_func(k) % total_partitions


def atof(text: str):
    try:
        val = float(text)
    except ValueError:
        val = text
    return val


# stupid way to judge float
def is_float(text: str):
    try:
        float(text)
        return True
    except ValueError:
        return False


# only works for tuple list, eg: kv list
def natural_keys(pair):
    import re
    key = pair[0]
    if str(key).isdigit():
        return int(key)
    elif is_float(str(key)):
        return atof(key)
    # works for str+int, str+float and reverse
    return [atof(c) for c in re.split(r'[+-]?([0-9]+(?:[.][0-9]*)?|[.][0-9]+)', pair[0])]
