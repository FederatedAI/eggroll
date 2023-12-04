from eggroll.core.command.command_model import CommandURI

EGG_PAIR_URI_PREFIX = "v1/eggs-pair"
RUN_TASK = "runTask"
EGGPAIR_TASK_SERVICE_NAME = f"{EGG_PAIR_URI_PREFIX}/{RUN_TASK}"

CLEANUP = "cleanup"
COUNT = "count"
DELETE = "delete"
DESTROY = "destroy"
GET = "get"
GET_ALL = "getAll"
MAP_REDUCE_PARTITIONS_WITH_INDEX = "mapReducePartitionsWithIndex"
BINARY_SORTED_MAP_PARTITIONS_WITH_INDEX = "binarySortedMapPartitionsWithIndex"
PUT = "put"
PUT_ALL = "putAll"
REDUCE = "reduce"
WITH_STORES = "withStores"
EGGPAIR_TASK_URI = CommandURI(EGGPAIR_TASK_SERVICE_NAME)
