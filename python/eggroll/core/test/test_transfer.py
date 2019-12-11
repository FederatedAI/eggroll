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

from eggroll.core.transfer.transfer_service import TransferClient
from eggroll.core.datastructure.broker import FifoBroker
from eggroll.core.transfer.transfer_service import TransferService, GrpcTransferService, TransferClient
from eggroll.core.conf_keys import TransferConfKeys
from eggroll.core.meta_model import ErEndpoint
import threading
import queue
import unittest


class TestTransfer(unittest.TestCase):
  def test_recv(self):
    def start_server():
      transfer_service = GrpcTransferService()

      options = {TransferConfKeys.CONFKEY_TRANSFER_SERVICE_PORT: 30001}
      transfer_service.start(options=options)

    thread = threading.Thread(target=start_server)
    thread.start()

    broker = TransferService.get_or_create_broker('test')
    i = 0
    while not broker.is_closable():
      try:
        data = broker.get(block=True, timeout=1)
        if data:
          print(f'recv: {i}: {data}')
          i += 1
      except queue.Empty as e:
        print(f'empty')

    thread.join(1)


  def test_start_server(self):
    transfer_service = GrpcTransferService()

    options = {TransferConfKeys.CONFKEY_TRANSFER_SERVICE_PORT: 30001}
    transfer_service.start(options=options)

  def test_send(self):
    transfer_client = TransferClient()

    broker = FifoBroker()

    broker.put(b'hello')
    broker.put(b'world')
    broker.put(b'this')
    broker.put(b'is')
    broker.put(b'a')
    broker.put(b'test')
    broker.signal_write_finish()
    future = transfer_client.send(broker=broker, endpoint=ErEndpoint(host='localhost', port=30001), tag='test')
    future.result()


if __name__ == '__main__':
  unittest.main()