import logging
import typing
from concurrent.futures import wait, FIRST_EXCEPTION
from typing import Callable
from typing import Type, TypeVar, List, Tuple

from eggroll.computing.tasks import consts
from eggroll.core.client import CommandClient
from eggroll.core.meta_model import ErFunctor, ErJobIO, ErEndpoint, ErJob, ErTask
from eggroll.core.utils import generate_job_id, generate_task_id

if typing.TYPE_CHECKING:
    from eggroll.computing.roll_pair import RollPair

L = logging.getLogger(__name__)

T = TypeVar("T")


def _process_key_request(
    rp: "RollPair",
    k: bytes,
    partitioner: Callable[[bytes, int], int],
    job_name,
    request,
    response_type: Type[T],
) -> T:
    partition_id = partitioner(k, rp.num_partitions)
    partition = rp.get_store().get_partition(partition_id)
    job_id = generate_job_id(rp.session_id, job_name)
    task = ErTask(
        id=generate_task_id(job_id, partition_id),
        name=job_name,
        inputs=[partition],
        job=ErJob(
            id=job_id,
            name=job_name,
            inputs=[ErJobIO(rp.get_store())],
            functors=[ErFunctor(name=job_name, body=request.to_proto_string())],
        ),
    )
    return block_partition_task(
        command_client=rp.command_client,
        endpoint=partition.processor.command_endpoint,
        task=task,
        output_type=response_type,
    )


def block_partition_task(
    command_client: CommandClient, endpoint, task: ErTask, output_type: Type[T]
) -> T:
    task_resp = command_client.simple_sync_send(
        input=task,
        output_type=output_type,
        endpoint=endpoint,
        command_uri=consts.EGGPAIR_TASK_URI,
    )
    return task_resp


def block_submit_unary_unit_job(
    command_client: "CommandClient",
    job: ErJob,
    output_types: List[Type[T]],
) -> List[List[T]]:
    return block_submit_unary_unit_tasks(
        command_client=command_client,
        tasks=job.decompose_tasks(),
        output_types=output_types,
    )


def block_submit_unary_unit_tasks(
    command_client: "CommandClient",
    tasks: List[Tuple[List[ErTask], ErEndpoint]],
    output_types: List[Type[T]],
) -> List[List[T]]:
    futures = command_client.async_call(
        args=tasks, output_types=output_types, command_uri=consts.EGGPAIR_TASK_URI
    )
    return _futures_wait(futures)


def _futures_wait(futures):
    """
    wait for futures to complete, if any future fails, cancel all futures and raise exception
    :param futures:
    :return:
    """
    done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
    if len(not_done) > 0:
        for future in not_done:
            future.cancel()
    results = [future.result() for future in done]
    return results
