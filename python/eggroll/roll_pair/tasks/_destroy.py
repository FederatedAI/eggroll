import os
import shutil

from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _Destroy(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        input_store_locator = task.first_input._store_locator
        namespace = input_store_locator._namespace
        name = input_store_locator._name
        store_type = input_store_locator._store_type
        L.debug(f"destroying store_type={store_type}, namespace={namespace}, name={name}")
        if name == "*":
            from eggroll.core._data_path import get_db_path_from_partition

            target_paths = list()
            if store_type == "*":
                store_types = os.listdir(data_dir)
                for store_type in store_types:
                    target_paths.append("/".join([data_dir, store_type, namespace]))
            else:
                db_path = get_db_path_from_partition(data_dir, task.first_input)
                target_paths.append(db_path[: db_path.rfind("*")])

            for path in target_paths:
                realpath = os.path.realpath(path)
                if os.path.exists(path):
                    if realpath == "/" or realpath == data_dir or not realpath.startswith(data_dir):
                        raise ValueError(f"trying to delete a dangerous path: {realpath}")
                    else:
                        shutil.rmtree(path)
        else:
            options = task._job._options
            with task.first_input.get_adapter(data_dir, options=options) as input_adapter:
                input_adapter.destroy(options=options)
