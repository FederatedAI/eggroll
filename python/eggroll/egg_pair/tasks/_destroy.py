import logging
import os

from eggroll.core import shutil
from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from ._task import Task, EnvOptions

L = logging.getLogger(__name__)


class _Destroy(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        if not os.path.isabs(env_options.data_dir):
            raise ValueError(
                f"destroy operation on data_dir with relative path could be dangerous: {env_options.data_dir}")
        namespace = task.first_input.store_locator.namespace
        name = task.first_input.store_locator.name
        store_type = task.first_input.store_locator.store_type
        L.info(f"destroying store_type={store_type}, namespace={namespace}, name={name}")
        if name == "*":
            from eggroll.core._data_path import get_db_path_from_partition

            target_paths = list()
            if store_type == "*":
                store_types = os.listdir(env_options.data_dir)
                for store_type in store_types:
                    target_paths.append(os.path.join(env_options.data_dir, store_type, namespace))
            else:
                db_path = get_db_path_from_partition(env_options.data_dir, task.first_input)
                target_paths.append(db_path[: db_path.rfind("*")])

            for path in target_paths:
                realpath = os.path.realpath(path)
                if os.path.exists(path):
                    if realpath == "/" or realpath == env_options.data_dir or not realpath.startswith(
                            env_options.data_dir):
                        raise ValueError(
                            f"trying to delete a dangerous path: realpath={realpath} and data_dir={env_options.data_dir}"
                        )
                    else:
                        shutil.rmtree(realpath, ignore_errors=True)
        else:
            path = os.path.join(env_options.data_dir, store_type, namespace, name)
            shutil.rmtree(path, ignore_errors=True)
