import logging

from eggroll.egg_pair.tasks import task_mapping, EnvOptions
from eggroll.core.meta_model import ErTask

L = logging.getLogger(__name__)


class TaskHandler(object):
    def __init__(self, env_options: EnvOptions):
        self.env_options = env_options

    def exec(self, task: ErTask):
        if L.isEnabledFor(logging.TRACE):
            L.trace(f"[RUNTASK] start. task={task}")
        else:
            L.debug(f"[RUNTASK] start. task_name={task.name}, task_id={task.id}")
        handler = task_mapping.get(task.name, None)
        if handler is None:
            raise ValueError(f"no handler for task:{task.name}")
        result = handler.run(self.env_options, task.job, task)

        if L.isEnabledFor(logging.TRACE):
            L.trace(f"[RUNTASK] end. task={task}")
        else:
            L.debug(f"[RUNTASK] end. task_name={task.name}, task_id={task.id}")
        if result is None:
            return task
        return result
