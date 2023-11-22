import os
import shutil

from eggroll.core.meta_model import ErJob
from eggroll.core.meta_model import ErTask
from eggroll.utils.log_utils import get_logger

L = get_logger()


class _Destroy(object):
    @classmethod
    def run(cls, data_dir: str, job: ErJob, task: ErTask):
        namespace = task.first_input.store_locator.namespace
        name = task.first_input.store_locator.name
        store_type = task.first_input.store_locator.store_type
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
                        raise ValueError(
                            f"trying to delete a dangerous path: realpath={realpath} and data_dir={data_dir}"
                        )
                    else:
                        shutil.rmtree(path, ignore_errors=True)
        else:
            options = task.job._options
            input_adapter = task.first_input.get_adapter(data_dir, options=options)
            input_adapter.destroy(options=options)
