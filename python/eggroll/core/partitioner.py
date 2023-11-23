from .constants import PartitionerTypes


def partitioner(hash_func, total_partitions):
    def partition(key):
        return hash_func(key) % total_partitions

    return partition


def integer_partitioner(total_partitions):
    return partitioner(lambda x: int.from_bytes(x, "big"), total_partitions)


def mmh3_partitioner(total_partitions):
    import mmh3

    return partitioner(mmh3.hash, total_partitions)


def create_partitioner(partitioner_type, total_partitions):
    if partitioner_type == PartitionerTypes.INTEGER_HASH:
        return integer_partitioner(total_partitions)
    elif partitioner_type == PartitionerTypes.MMH3_BYTESTRING_HASH:
        return mmh3_partitioner(total_partitions)
    else:
        raise ValueError("invalid partitioner type: {}".format(partitioner_type))
