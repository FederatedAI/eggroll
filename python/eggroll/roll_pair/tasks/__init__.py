import logging

from eggroll.core.meta_model import ErTask
from eggroll.roll_pair.tasks._aggregate import _Aggregate
from eggroll.roll_pair.tasks._binary_sorted_map_partitions_with_index import _BinarySortedMapPartitionsWithIndex
from eggroll.roll_pair.tasks._count import _Count
from eggroll.roll_pair.tasks._delete import _Delete
from eggroll.roll_pair.tasks._destroy import _Destroy
from eggroll.roll_pair.tasks._get import _Get
from eggroll.roll_pair.tasks._get_all import _GetAll
from eggroll.roll_pair.tasks._map_partitions_with_index import _MapReducePartitionsWithIndex
from eggroll.roll_pair.tasks._put import _Put
from eggroll.roll_pair.tasks._put_all import _PutAll
from eggroll.roll_pair.tasks._put_batch import _PutBatch
from eggroll.roll_pair.tasks._reduce import _Reduce
from eggroll.roll_pair.tasks._with_stores import _WithStores
from eggroll.roll_pair.tasks.roll_site._pull_get_header import _PullGetHeader
from eggroll.roll_pair.tasks.roll_site._pull_get_partition_status import _PullGetPartitionStatus
from eggroll.roll_pair.tasks.roll_site._pull_clear_status import _PullClearStatus
from eggroll.utils.log_utils import get_logger

_mapping = {}
_mapping.update(
    {
        "get": _Get,
        "getAll": _GetAll,
        "count": _Count,
        "putBatch": _PutBatch,
        "put": _Put,
        "putAll": _PutAll,
        "delete": _Delete,
    }
)
_mapping.update(
    {
        "withStores": _WithStores,
        "destroy": _Destroy,
    }
)
_mapping.update(
    {
        "reduce": _Reduce,
        "aggregate": _Aggregate,
    }
)
_mapping.update(
    {
        "mapReducePartitionsWithIndex": _MapReducePartitionsWithIndex,
    }
)
_mapping.update(
    {
        "binarySortedMapPartitionsWithIndex": _BinarySortedMapPartitionsWithIndex,
    }
)

_mapping.update(
    {
        "pullGetHeader": _PullGetHeader,
        "pullGetPartitionStatus": _PullGetPartitionStatus,
        "pullClearStatus": _PullClearStatus,
    }
)

L = get_logger()


class EggTaskHandler(object):
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def run_task(self, task: ErTask):
        if L.isEnabledFor(logging.TRACE):
            L.trace(f"[RUNTASK] start. task={task}")
        else:
            L.debug(f"[RUNTASK] start. task_name={task.name}, task_id={task.id}")
        handler = _mapping.get(task.name, None)
        if handler is None:
            raise ValueError(f"no handler for task:{task.name}")
        result = handler.run(self.data_dir, task.job, task)

        if L.isEnabledFor(logging.TRACE):
            L.trace(f"[RUNTASK] end. task={task}")
        else:
            L.debug(f"[RUNTASK] end. task_name={task.name}, task_id={task.id}")
        if result is None:
            return task
        return result
