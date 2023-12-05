import logging
import os
import queue
import time
import typing
from threading import Thread
from typing import TypeVar, Iterable, Tuple, Callable

from eggroll.computing.roll_pair.transfer_pair import TransferPair, BatchBroker
from eggroll.computing.tasks import consts
from eggroll.computing.tasks.submit_utils import (
    block_submit_unary_unit_job,
    block_submit_unary_unit_tasks,
)
from eggroll.core import shutil
from eggroll.core.constants import StoreTypes
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.meta_model import (
    ErFunctor,
    ErJobIO,
    ErStore,
    ErStoreLocator,
    ErPartition,
)
from eggroll.core.meta_model import ErJob, ErTask
from eggroll.core.model.task import (
    GetAllRequest,
    CountResponse,
)
from eggroll.core.transfer.transfer_service import TransferService
from eggroll.core.utils import generate_job_id, generate_task_id
from ._task import Task, EnvOptions

if typing.TYPE_CHECKING:
    from eggroll.computing.roll_pair import RollPair
    from eggroll.computing.roll_pair import RollPairContext

L = logging.getLogger(__name__)

T = TypeVar("T")


class GetAll(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        tag = task.id
        request = job.first_functor.deserialized_as(GetAllRequest)

        def generate_broker():
            with task.first_input.get_adapter(
                env_options.data_dir
            ) as db, db.iteritems() as rb:
                limit = None if request.limit < 0 else request.limit
                try:
                    yield from TransferPair.pair_to_bin_batch(
                        config=env_options.config, input_iter=rb, limit=limit
                    )
                finally:
                    TransferService.remove_broker(tag)

        TransferService.set_broker(tag, generate_broker())

    @classmethod
    def submit(cls, rp: "RollPair", limit=None):
        if limit is not None and not isinstance(limit, int) and limit <= 0:
            raise ValueError(f"limit:{limit} must be positive int")
        if limit is None:
            limit = -1
        job_id = generate_job_id(rp.session_id, consts.GET_ALL)

        block_submit_unary_unit_job(
            command_client=rp.command_client,
            job=ErJob(
                id=job_id,
                name=consts.GET_ALL,
                inputs=[ErJobIO(rp.get_store())],
                functors=[
                    ErFunctor(
                        name=consts.GET_ALL,
                        body=GetAllRequest(limit=limit).to_proto_string(),
                    )
                ],
            ),
            output_types=[ErTask],
        )
        transfer_pair = TransferPair(config=rp.config, transfer_id=job_id)
        done_cnt = 0
        for k, v in transfer_pair.gather(config=rp.config, store=rp.get_store()):
            done_cnt += 1
            yield k, v


class PutAll(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        TransferPair(config=env_options.config, transfer_id=task.id).store_broker(
            config=env_options.config,
            data_dir=env_options.data_dir,
            store_partition=task.first_output,
            is_shuffle=False,
        ).result()

    @classmethod
    def submit(
        cls,
        rp: "RollPair",
        kv_list: Iterable[Tuple[bytes, bytes]],
        partitioner: Callable[[bytes, int], int],
    ):
        job_id = generate_job_id(rp.session_id, consts.PUT_ALL)

        # TODO:1: consider multiprocessing scenario. parallel size should be sent to egg_pair to set write signal count

        def send_command():
            block_submit_unary_unit_job(
                command_client=rp.command_client,
                job=ErJob(
                    id=job_id,
                    name=consts.PUT_ALL,
                    inputs=[ErJobIO(rp.get_store())],
                    outputs=[ErJobIO(rp.get_store())],
                    functors=[],
                ),
                output_types=[ErTask],
            )

        th = DaemonThreadWithExceptionPropagate.thread(
            target=send_command, name=f"put_all-send_command-{job_id}"
        )
        th.start()
        shuffler = TransferPair(config=rp.config, transfer_id=job_id)
        fifo_broker = FifoBroker(config=rp.config)
        bb = BatchBroker(config=rp.config, broker=fifo_broker)
        scatter_future = shuffler.scatter(
            rp.config, fifo_broker, partitioner, rp.get_store()
        )

        with bb:
            for k, v in kv_list:
                bb.put(item=(k, v))

        # TODO: sp3: check exceptions
        scatter_future.result()
        th.result()

        # _wait_all_done(scatter_future, th)
        return rp


class Count(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, _job: ErJob, task: ErTask):
        with task.first_input.get_adapter(env_options.data_dir) as input_adapter:
            return CountResponse(value=input_adapter.count())

    @classmethod
    def submit(cls, rp: "RollPair"):
        job_id = generate_job_id(rp.session_id, tag=consts.COUNT)
        job = ErJob(id=job_id, name=consts.COUNT, inputs=[ErJobIO(rp.get_store())])
        task_results = block_submit_unary_unit_job(
            command_client=rp.command_client, job=job, output_types=[CountResponse]
        )
        total = 0
        for task_result in task_results:
            partition_count = task_result[0]
            total += partition_count.value
        return total


class Take(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, _job: ErJob, task: ErTask):
        raise NotImplementedError("take not implemented, use get_all instead")

    @classmethod
    def submit(cls, rp: "RollPair", num: int, options: dict = None):
        if options is None:
            options = {}
        if num <= 0:
            num = 1

        keys_only = options.get("keys_only", False)
        ret = []
        count = 0
        for item in rp.get_all(limit=num):
            if keys_only:
                if item:
                    ret.append(item[0])
                else:
                    ret.append(None)
            else:
                ret.append(item)
            count += 1
            if count == num:
                break
        return ret


class Destroy(Task):
    @classmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        if not os.path.isabs(env_options.data_dir):
            raise ValueError(
                f"destroy operation on data_dir with relative path could be dangerous: {env_options.data_dir}"
            )
        namespace = task.first_input.store_locator.namespace
        name = task.first_input.store_locator.name
        store_type = task.first_input.store_locator.store_type
        L.info(
            f"destroying store_type={store_type}, namespace={namespace}, name={name}"
        )
        if name == "*":
            from eggroll.core._data_path import get_db_path_from_partition

            target_paths = list()
            if store_type == "*":
                store_types = os.listdir(env_options.data_dir)
                for store_type in store_types:
                    target_paths.append(
                        os.path.join(env_options.data_dir, store_type, namespace)
                    )
            else:
                db_path = get_db_path_from_partition(
                    env_options.data_dir, task.first_input
                )
                target_paths.append(db_path[: db_path.rfind("*")])

            for path in target_paths:
                realpath = os.path.realpath(path)
                if os.path.exists(path):
                    if (
                        realpath == "/"
                        or realpath == env_options.data_dir
                        or not realpath.startswith(env_options.data_dir)
                    ):
                        raise ValueError(
                            f"trying to delete a dangerous path: realpath={realpath} and data_dir={env_options.data_dir}"
                        )
                    else:
                        shutil.rmtree(realpath, ignore_errors=True)
        else:
            path = os.path.join(env_options.data_dir, store_type, namespace, name)
            shutil.rmtree(path, ignore_errors=True)

    @classmethod
    def submit(cls, rp: "RollPair"):
        block_submit_unary_unit_job(
            command_client=rp.command_client,
            job=ErJob(
                id=generate_job_id(rp.session_id, consts.DESTROY),
                name=consts.DESTROY,
                inputs=[ErJobIO(rp.get_store())],
                functors=[],
            ),
            output_types=[ErTask],
        )
        rp.ctx.session.cluster_manager_client.delete_store(rp.get_store())

    @classmethod
    def submit_cleanup(
        cls, rpc: "RollPairContext", name: str, namespace: str, options: dict = None
    ):
        from eggroll.computing import RollPair

        """store name only supports full name and reg: *, *abc ,abc* and a*c"""
        if not namespace:
            raise ValueError("namespace cannot be blank")

        L.debug(f"cleaning up namespace={namespace}, name={name}")
        if options is None:
            options = {}
        total_partitions = options.get("total_partitions", 1)

        if name == "*":
            store_type = options.get("store_type", "*")
            L.debug(
                f"cleaning up whole store_type={store_type}, namespace={namespace}, name={name}"
            )
            er_store = ErStore(
                store_locator=ErStoreLocator(
                    namespace=namespace, name=name, store_type=store_type
                )
            )
            job_id = generate_job_id(namespace, tag=consts.CLEANUP)
            job = ErJob(
                id=job_id,
                name=consts.DESTROY,
                inputs=[ErJobIO(er_store)],
                options=options,
            )

            args = list()
            cleanup_partitions = [
                ErPartition(id=-1, store_locator=er_store.store_locator)
            ]

            for server_node, eggs in rpc.session.eggs.items():
                egg = eggs[0]
                task = ErTask(
                    id=generate_task_id(job_id, egg.command_endpoint.host),
                    name=job.name,
                    inputs=cleanup_partitions,
                    job=job,
                )
                args.append(([task], egg.command_endpoint))

            block_submit_unary_unit_tasks(
                command_client=rpc.command_client,
                tasks=args,
                output_types=[ErTask],
            )
            rpc.session.cluster_manager_client.delete_store(er_store)
        else:
            # todo:1: add combine options to pass it through
            store_options = rpc.session.get_all_options()
            store_options.update(options)
            final_options = store_options.copy()

            store = ErStore(
                store_locator=ErStoreLocator(
                    store_type=StoreTypes.ROLLPAIR_LMDB,
                    namespace=namespace,
                    name=name,
                    total_partitions=total_partitions,
                ),
                options=final_options,
            )
            task_results = rpc.session.cluster_manager_client.get_store_from_namespace(
                store
            )
            L.debug("res={}".format(task_results._stores))
            if task_results._stores is not None:
                L.debug("item count={}".format(len(task_results._stores)))
                for item in task_results._stores:
                    L.debug(
                        "item namespace={} name={}".format(
                            item._store_locator._namespace, item._store_locator._name
                        )
                    )
                    rp = RollPair(er_store=item, rp_ctx=rpc)
                    rp.destroy()


class DaemonThreadWithExceptionPropagate:
    @classmethod
    def thread(cls, target, name, args=()):
        q = queue.Queue()
        th = Thread(
            target=_thread_target_wrapper(target),
            name=name,
            args=[q, *args],
            daemon=True,
        )
        return cls(q, th)

    def __init__(self, q, thread):
        self.q = q
        self.thread = thread

    def start(self):
        self.thread.start()

    def done(self):
        return not self.thread.is_alive()

    def result(self):
        self.thread.join()
        e = self.q.get()
        if e:
            raise e


def _thread_target_wrapper(target):
    def wrapper(q: queue.Queue, *args):
        try:
            target(*args)
        except Exception as e:
            q.put(e)
        else:
            q.put(None)

    return wrapper


def _wait_all_done(*futures):
    has_call_result = [False] * len(futures)
    while not all(has_call_result):
        for i, f in enumerate(futures):
            if not has_call_result[i] and f.done():
                f.result()
                has_call_result[i] = True
        time.sleep(0.001)