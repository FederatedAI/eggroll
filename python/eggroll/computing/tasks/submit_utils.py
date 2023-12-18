import logging
import typing
from concurrent.futures import wait, FIRST_EXCEPTION
from typing import Callable
from typing import Type, TypeVar, List, Tuple

from eggroll.computing.tasks import consts
from eggroll.core.command.command_client import CommandClient
from eggroll.core.meta_model import ErFunctor, ErJobIO, ErEndpoint, ErJob, ErTask
from .job_util import generate_job_id, generate_task_id

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
    job_id = generate_job_id(session_id=rp.session_id, tag=job_name)
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
        tasks=decompose_tasks(job),
        output_types=output_types,
    )


def decompose_tasks(job: ErJob) -> List[Tuple[List[ErTask], ErEndpoint]]:
    input_total_partitions = job.first_input.num_partitions
    output_total_partitions = (
        job.first_output.num_partitions if job.has_first_output else 0
    )
    total_partitions = max(input_total_partitions, output_total_partitions)
    tasks = []
    for i in range(total_partitions):
        task_input_partitions = []
        task_output_partitions = []
        task_endpoint = None

        def _fill_partitions(job_ios: List[ErJobIO], partitions, target_endpoint):
            for job_io in job_ios:
                partition = job_io.store.get_partition(i)
                partitions.append(partition)
                endpoint = partition.processor.command_endpoint
                if target_endpoint is None:
                    target_endpoint = endpoint
                elif target_endpoint != endpoint:
                    raise ValueError(
                        f"store {job_io.store} partition {i} has different processor endpoint"
                    )
            return target_endpoint

        if i < input_total_partitions:
            task_endpoint = _fill_partitions(
                job.inputs, task_input_partitions, task_endpoint
            )
        if i < output_total_partitions:
            task_endpoint = _fill_partitions(
                job.outputs, task_output_partitions, task_endpoint
            )
        if not task_endpoint:
            raise ValueError(f"task endpoint is null for task {i}")

        tasks.append(
            (
                [
                    ErTask(
                        id=generate_task_id(job.id, i),
                        name=f"{job.name}",
                        inputs=task_input_partitions,
                        outputs=task_output_partitions,
                        job=job,
                    )
                ],
                task_endpoint,
            ),
        )
    return tasks


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
