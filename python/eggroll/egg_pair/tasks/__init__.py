import logging
import typing

from eggroll.egg_pair.tasks._aggregate import _Aggregate
from eggroll.egg_pair.tasks._binary_sorted_map_partitions_with_index import _BinarySortedMapPartitionsWithIndex
from eggroll.egg_pair.tasks._count import _Count
from eggroll.egg_pair.tasks._delete import _Delete
from eggroll.egg_pair.tasks._destroy import _Destroy
from eggroll.egg_pair.tasks._get import _Get
from eggroll.egg_pair.tasks._get_all import _GetAll
from eggroll.egg_pair.tasks._map_partitions_with_index import _MapReducePartitionsWithIndex
from eggroll.egg_pair.tasks._put import _Put
from eggroll.egg_pair.tasks._put_all import _PutAll
from eggroll.egg_pair.tasks._reduce import _Reduce
from eggroll.egg_pair.tasks._task import Task, EnvOptions
from eggroll.egg_pair.tasks._with_stores import _WithStores

L = logging.getLogger(__name__)

task_mapping: typing.Dict[str, Task] = {
    "get": _Get,
    "getAll": _GetAll,
    "count": _Count,
    "put": _Put,
    "putAll": _PutAll,
    "delete": _Delete,
    "withStores": _WithStores,
    "destroy": _Destroy,
    "reduce": _Reduce,
    "aggregate": _Aggregate,
    "mapReducePartitionsWithIndex": _MapReducePartitionsWithIndex,
    "binarySortedMapPartitionsWithIndex": _BinarySortedMapPartitionsWithIndex,
}

__all__ = ["task_mapping", "Task", "EnvOptions"]
