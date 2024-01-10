import mmh3


def mmh3_partitioner(key: bytes, total_partitions):
    return mmh3.hash(key) % total_partitions
