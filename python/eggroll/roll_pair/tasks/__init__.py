import logging

from eggroll.core.meta_model import ErTask
from eggroll.roll_pair.tasks._aggregate import _Aggregate
from eggroll.roll_pair.tasks._count import _Count
from eggroll.roll_pair.tasks._delete import _Delete
from eggroll.roll_pair.tasks._destroy import _Destroy
from eggroll.roll_pair.tasks._get import _Get
from eggroll.roll_pair.tasks._get_all import _GetAll
from eggroll.roll_pair.tasks._join import _MergeJoin
from eggroll.roll_pair.tasks._map_partitions_with_index import (
    _MapPartitionsWithIndex,
)
from eggroll.roll_pair.tasks._put import _Put
from eggroll.roll_pair.tasks._put_all import _PutAll
from eggroll.roll_pair.tasks._put_batch import _PutBatch
from eggroll.roll_pair.tasks._reduce import _Reduce
from eggroll.roll_pair.tasks._union import _MergeUnion
from eggroll.roll_pair.tasks._with_stores import _WithStores
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
        "mapPartitionsWithIndex": _MapPartitionsWithIndex,
    }
)
_mapping.update(
    {
        "join": _MergeJoin,
        "union": _MergeUnion,
    }
)

L = get_logger()


class EggTaskHandler(object):
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def run_task(self, task: ErTask):
        if L.isEnabledFor(logging.TRACE):
            L.trace(
                f"[RUNTASK] start. task_name={task.name}, inputs={task._inputs}, outputs={task._outputs}, task_id={task.id}"
            )
        else:
            L.debug(f"[RUNTASK] start. task_name={task.name}, task_id={task.id}")
        handler = _mapping.get(task.name, None)
        if handler is None:
            raise ValueError(f"no handler for task:{task.name}")
        result = handler.run(self.data_dir, task.job, task)

        if L.isEnabledFor(logging.TRACE):
            L.trace(
                f"[RUNTASK] end. task_name={task.name}, inputs={task._inputs}, outputs={task._outputs}, task_id={task.id}"
            )
        else:
            L.debug(f"[RUNTASK] end. task_name={task.name}, task_id={task.id}")
        if result is None:
            return task
        return result
