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

import grpc
from concurrent import futures
from eggroll.core.command.command_router import CommandRouter
from eggroll.core.command.command_service import CommandServicer
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter
from eggroll.core.meta import ErTask, ErPartition
from eggroll.core.proto import command_pb2_grpc, transfer_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.core.transfer.transfer_service import TransferServicer, \
  TransferClient
from grpc._cython import cygrpc


class EggPair(object):
  def run_task(self, task: ErTask):
    functors = task._job._functors
    result = task

    if task._name == 'mapValues':
      f = cloudpickle.loads(functors[0]._body)
      input_partition = task._inputs[0]
      output_partition = task._outputs[0]

      print("input partition: ", input_partition, "path: ",
            get_db_path(input_partition))
      print("output partition: ", output_partition, "path: ",
            get_db_path(output_partition))
      input_adapter = RocksdbSortedKvAdapter(
        options={'path': get_db_path(input_partition)})
      output_adapter = RocksdbSortedKvAdapter(
        options={'path': get_db_path(output_partition)})

      input_iterator = input_adapter.iteritems()
      output_writebatch = output_adapter.new_batch()

      for k_bytes, v_bytes in input_iterator:
        output_writebatch.put(k_bytes, f(v_bytes))

      output_writebatch.close()
      input_adapter.close()
      output_adapter.close()
    elif task._name == 'reduce':
      f = cloudpickle.loads(functors[0]._body)

      input_partition = task._inputs[0]
      input_adapter = RocksdbSortedKvAdapter(
        options={'path': get_db_path(input_partition)})
      seq_op_result = None

      for k_bytes, v_bytes in input_adapter.iteritems():
        if seq_op_result:
          seq_op_result = f(seq_op_result, v_bytes)
        else:
          seq_op_result = v_bytes

      partition_id = input_partition._id
      transfer_tag = task._job._name

      if "0" == partition_id:
        queue = TransferServicer.get_or_create_queue(transfer_tag)
        partition_size = len(task._job._inputs[0]._partitions)

        comb_op_result = seq_op_result

        for i in range(1, partition_size):
          other_seq_op_result = queue.get(block=True, timeout=10)

          comb_op_result = f(comb_op_result, other_seq_op_result)

        output_partition = task._outputs[0]
        output_adapter = RocksdbSortedKvAdapter(
          options={'path': get_db_path(output_partition)})

        output_writebatch = output_adapter.new_batch()
        output_writebatch.put('result'.encode(), comb_op_result)

        output_writebatch.close()
        output_adapter.close()
      else:
        transfer_client = TransferClient()
        transfer_client.send(data=seq_op_result, tag=transfer_tag,
                             server_node=task._outputs[0]._node)

      input_adapter.close()

    return result


def get_db_path(partition: ErPartition):
  store_locator = partition._store_locator
  db_path_prefix = '/tmp/eggroll/'

  return db_path_prefix + "/".join(
      [store_locator._store_type, store_locator._namespace, store_locator._name,
       partition._id])


def serve():
  port = 20001

  CommandRouter.get_instance().register(
    "com.webank.eggroll.rollpair.component.EggPair.mapValues",
    "eggroll.roll_pair.egg_pair", "EggPair", "run_task")
  CommandRouter.get_instance().register(
    "com.webank.eggroll.rollpair.component.EggPair.reduce",
    "eggroll.roll_pair.egg_pair", "EggPair", "run_task")

  server = grpc.server(futures.ThreadPoolExecutor(max_workers=5),
                       options=[
                         (cygrpc.ChannelArgKey.max_send_message_length, -1),
                         (cygrpc.ChannelArgKey.max_receive_message_length, -1)])

  command_servicer = CommandServicer()
  # todo: register egg_pair methods
  command_pb2_grpc.add_CommandServiceServicer_to_server(command_servicer,
                                                        server)

  transfer_servicer = TransferServicer()
  transfer_pb2_grpc.add_TransferServiceServicer_to_server(transfer_servicer,
                                                          server)

  server.add_insecure_port(f'[::]:{port}')

  server.start()
  import time
  time.sleep(10000)


if __name__ == '__main__':
  serve()
