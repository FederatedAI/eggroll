#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#
from threading import Thread

from eggroll.core.client import CommandClient
from eggroll.core.conf_keys import RollPairConfKeys
from eggroll.core.meta_model import ErTask, ErJob, ErEndpoint
from eggroll.core.pair_store.adapter import PairIterator, PairWriteBatch, \
    PairAdapter
from eggroll.core.transfer.transfer_service import TransferClient
from eggroll.roll_pair.roll_pair import RollPair
from eggroll.roll_pair.transfer_pair import TransferPair
from eggroll.utils.log_utils import get_logger
from eggroll.core.command.command_model import CommandURI

L = get_logger()


class RemoteRollPairAdapter(PairAdapter):
    def __init__(self,
            remote_cmd_endpoint: ErEndpoint,
            remote_transfer_endpoint: ErEndpoint,
            options: dict = None):
        super().__init__(options)
        self._cm_client = CommandClient()
        self._er_partition = options['er_partition']
        store_locator = self._er_partition._store_locator
        self._replica_number = options['replica_number']
        self._replica_processor = options['replica_processor']
        self._partition_id = self._er_partition._id
        self._total_partitions = self._er_partition._store_locator._total_partitions
        self._replicate_job_id = f"{'/'.join([store_locator._store_type, store_locator._namespace, store_locator._name])}" \
                                 f"-replicate-{self._replica_number}-processor_id-{self._replica_processor._id}"
        self.remote_cmd_endpoint = remote_cmd_endpoint
        self.remote_transfer_endpoint = remote_transfer_endpoint

    def __del__(self):
        super().__del__()

    def close(self):
        pass

    def iteritems(self):
        return RemoteRollPairIterator(self)

    def new_batch(self):
        L.info(f"RemoteRollPairAdapter new_batch calling")
        return RemoteRollPairWriteBatch(self)

    def get(self, key):
        raise NotImplementedError()

    def put(self, key, value):
        raise NotImplementedError()

    def is_sorted(self):
        return True

    def destroy(self, options: dict=None):
        tasks = [ErTask(id=f"{self._replicate_job_id}-partition-{self._partition_id}",
                        name=RollPair.DESTROY,
                        inputs=[self._er_partition],
                        outputs=[],
                        job=ErJob(id=self._replicate_job_id,
                                  name=RollPair.DESTROY))]

        return self._cm_client.sync_send(inputs=tasks,
                                         output_types=[ErTask],
                                         endpoint=self.remote_cmd_endpoint,
                                         command_uri=RollPair.RUN_TASK_URI)

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)


class RemoteRollPairIterator(PairIterator):
    pass


class RemoteRollPairWriteBatch(PairWriteBatch):

    def __init__(self, adapter: RemoteRollPairAdapter) -> None:
        super().__init__()
        self.adapter = adapter
        self.batch = dict()
        self.batch_size = RollPairConfKeys.EGGROLL_ROLLPAIR_REMOTE_WRITEBATCH_SIZE.get()
        self.manual_merger = dict()
        self.has_write_op = False
        self.write_count = 0
        L.info(f"RemoteRollPairWriteBatch inited")

    def get(self, k):
        raise NotImplementedError()

    def put(self, k, v):
        if len(self.manual_merger) == 0:
            self.has_write_op = True

        self.manual_merger[k] = v

    def merge(self, merge_func, k, v):
        if k in self.manual_merger:
            self.manual_merger[k] = merge_func(self.manual_merger[k], v)
        else:
            self.manual_merger[k] = v

        # if len(self.manual_merger) >= self.batch_size:
        #     self.write()

    def write(self):
        L.info("RemoteRollPairWriteBatch write calling")
        if len(self.manual_merger) == 0:
            L.info(f"self.manual_merger={self.manual_merger}")
            return
        self.has_write_op = True
        batches = TransferPair.pair_to_bin_batch(sorted(self.manual_merger.items(), key=lambda kv: kv[0]))
        task_id = f"{self.adapter._replicate_job_id}-partition-{self.adapter._partition_id}"
        L.info(f"task_id={task_id}")

        tasks = [ErTask(id=task_id,
                        name=RollPair.PUT_BATCH,
                        inputs=[self.adapter._er_partition],
                        outputs=[self.adapter._er_partition],
                        job=ErJob(id=self.adapter._replicate_job_id,
                                  name=RollPair.PUT_BATCH))]

        def send_command(tasks, remote_cmd_endpoint):
            cmd_client = CommandClient()
            return cmd_client.sync_send(inputs=tasks,
                                        output_types=[ErTask],
                                        endpoint=remote_cmd_endpoint,
                                        command_uri=CommandURI(f'v1/egg-pair/runTask'))

        L.info(f"start to send cmd")
        t = Thread(target=send_command, name=task_id, args=[tasks, self.adapter.remote_cmd_endpoint])
        t.start()

        transfer_client = TransferClient()
        f = transfer_client.send(batches, endpoint=self.adapter.remote_transfer_endpoint, tag=task_id)

        f.result()
        t.join()

        self.manual_merger.clear()
        L.info("RemoteRollPairWriteBatch write called")

    def close(self):
        self.write()

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.write()