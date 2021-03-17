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


import queue
import threading
import unittest


from eggroll.core.conf_keys import TransferConfKeys
from eggroll.core.datastructure import create_executor_pool
from eggroll.core.datastructure.broker import FifoBroker, BrokerClosed
from eggroll.core.meta_model import ErEndpoint
from eggroll.core.transfer.transfer_service import TransferService, \
    GrpcTransferService, TransferClient

transfer_port = 20002
transfer_endpoint = ErEndpoint('localhost', transfer_port)


class TestTransfer(unittest.TestCase):
    def setUp(self) -> None:
        self.__executor_pool = create_executor_pool(max_workers=5)

    def test_recv(self):
        def start_server():
            transfer_service = GrpcTransferService()

            options = {TransferConfKeys.CONFKEY_TRANSFER_SERVICE_PORT: transfer_port}
            transfer_service.start(options=options)

        thread = threading.Thread(target=start_server)
        thread.start()

        broker = TransferService.get_or_create_broker('test')
        i = 0
        while not broker.is_closable():
            try:
                data = broker.get(block=True, timeout=0.1)
                if data:
                    print(f'recv: {i}: {data}')
                    i += 1
            except queue.Empty as e:
                print(f'empty')
            except BrokerClosed:
                break

        thread.join(1)

    def test_start_server(self):
        transfer_service = GrpcTransferService()

        options = {TransferConfKeys.CONFKEY_TRANSFER_SERVICE_PORT: transfer_port}
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
        future = transfer_client.send(broker=broker, endpoint=ErEndpoint(host='localhost', port=transfer_port), tag='test')
        future.result()

    def test_pull(self):
        tag = 'test_pull'
        send_broker = TransferService.get_or_create_broker('test_pull')
        self.__put_to_broker(send_broker)
        send_broker.signal_write_finish()

        transfer_service = GrpcTransferService()

        options = {TransferConfKeys.CONFKEY_TRANSFER_SERVICE_PORT: transfer_port}
        future = self.__executor_pool.submit(self.test_start_server)

        from time import sleep
        sleep(1)

        transfer_client = TransferClient()
        recv_broker = transfer_client.recv(transfer_endpoint, tag)

        while not recv_broker.is_closable():
            print('recv:', recv_broker.get())

        future.result(timeout=10)

    def __put_to_broker(self, broker):
        broker.put(b'hello')
        broker.put(b'world')
        broker.put(b'this')
        broker.put(b'is')
        broker.put(b'a')
        broker.put(b'test')

if __name__ == '__main__':
    unittest.main()
