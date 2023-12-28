import logging
import typing
from typing import List

from eggroll.computing.tasks import consts, job_util
from eggroll.computing.tasks.submit_utils import block_submit_unary_unit_job
from eggroll.core.meta_model import (
    ErJob,
    ErFunctor,
    ErJobIO,
    ErTask,
    WithStoresResponse,
)
from ._task import Task, EnvOptions

if typing.TYPE_CHECKING:
    from eggroll.computing.roll_pair import RollPair
L = logging.getLogger(__name__)


class WithStores(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        f = job.first_functor.func
        value = f(env_options.config, env_options.data_dir, task)
        return WithStoresResponse(id=task.first_input.id, value=value)

    @classmethod
    def submit(
        cls,
        rp: "RollPair",
        func,
        others: List["RollPair"] = None,
        options: dict = None,
        description: str = None,
    ):
        if options is None:
            options = {}
        stores = [rp.get_store()]
        if others is None:
            others = []
        for other in others:
            stores.append(rp.get_store())
        total_partitions = rp.num_partitions
        for other in others:
            if other.get_partitions() != total_partitions:
                raise ValueError(
                    f"diff partitions: expected={total_partitions}, actual={other.get_partitions()}"
                )
        job_id = job_util.generate_job_id(
            session_id=rp.session_id, tag=consts.WITH_STORES, description=description
        )
        job = ErJob(
            id=job_id,
            name=consts.WITH_STORES,
            inputs=[ErJobIO(store) for store in stores],
            functors=[ErFunctor.from_func(name=consts.WITH_STORES, func=func)],
            options=options,
        )
        responses = block_submit_unary_unit_job(
            command_client=rp.command_client,
            job=job,
            output_types=[WithStoresResponse],
        )
        results = []
        for response in responses:
            response = response[0]
            results.append((response.id, response.value))
        return results
