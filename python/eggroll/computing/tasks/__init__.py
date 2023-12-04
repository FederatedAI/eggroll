import logging
import typing

from eggroll.trace import get_tracer
from . import consts
from .impl import (
    Get,
    Put,
    Delete,
    GetAll,
    PutAll,
    Count,
    Take,
    Destroy,
    Reduce,
    BinarySortedMapPartitionsWithIndex,
    MapReducePartitionsWithIndex,
    WithStores,
    Aggregate,
    Task,
    EnvOptions,
)

if typing.TYPE_CHECKING:
    from eggroll.core.meta_model import ErTask
    from eggroll.core.command.command_router import CommandRouter


L = logging.getLogger(__name__)
tracer = get_tracer(__name__)

task_mapping: typing.Dict[str, Task] = {
    "get": Get,
    "put": Put,
    "delete": Delete,
    "getAll": GetAll,
    "putAll": PutAll,
    "count": Count,
    "destroy": Destroy,
    "reduce": Reduce,
    # "aggregate": Aggregate,
    "mapReducePartitionsWithIndex": MapReducePartitionsWithIndex,
    "binarySortedMapPartitionsWithIndex": BinarySortedMapPartitionsWithIndex,
    "withStores": WithStores,
}


class TaskHandler(object):
    def __init__(self, env_options: EnvOptions):
        self.env_options = env_options

    def exec(self, task: "ErTask"):
        with tracer.start_span(f"handle {task.name}: id={task.id}"):
            handler = task_mapping.get(task.name, None)
            if handler is None:
                raise ValueError(f"no handler for task:{task.name}")
            result = handler.run(self.env_options, task.job, task)
            # TODO: maybe add empty result
            if result is None:
                return task
            return result


def register(command_router: "CommandRouter", env_options: EnvOptions):
    """
    register task handler to command router
    :param command_router:
    :param env_options:
    :return:
    """
    eggpair_task_handler_instance = TaskHandler(env_options)
    command_router.register_handler(
        service_name=consts.EGGPAIR_TASK_SERVICE_NAME,
        route_to_method=TaskHandler.exec,
        route_to_call_based_class_instance=eggpair_task_handler_instance,
    )


__all__ = [
    "task_mapping",
    "Get",
    "Put",
    "Delete",
    "GetAll",
    "PutAll",
    "Count",
    "Take",
    "Destroy",
    "Reduce",
    "BinarySortedMapPartitionsWithIndex",
    "MapReducePartitionsWithIndex",
    "WithStores",
    "Aggregate",
    "Task",
    "EnvOptions",
    "consts",
    "TaskHandler",
    "register",
]
