import os


def get_db_path_from_partition(data_dir, partition):
    store_locator = partition._store_locator
    return get_db_path_expanded(
        data_dir, store_locator._store_type, store_locator._namespace, store_locator._name, partition._id
    )


def get_db_path_expanded(data_dir, store_type, namespace, name, partition_id):
    return os.path.join(data_dir, store_type, namespace, name, str(partition_id))
