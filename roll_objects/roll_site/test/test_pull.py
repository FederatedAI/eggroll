#
#  Copyright 2019 The Eggroll Authors. All Rights Reserved.
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
import time
from threading import Thread
from threading import Event
from api import rollsite


class WriteThread(Thread):
    def __init__(self, w_event):
        Thread.__init__(self)
        self.w_event = w_event

    def run(self):
        self.w_event.wait()
        print("recv Event, received all model files")
        self.w_event.clear()


class ReadThread(Thread):
    ret_a = None
    ret_b = None

    def __init__(self, w_event):
        Thread.__init__(self)
        self.w_event = w_event

    def run(self):
        while True:
            if self.ret_a is None:
                print("pull a")
                fp = open('a.modle', 'w')
                self.ret_a = rollsite.pull(fp, "model_A", tag="{}".format(_tag))
                fp.close()

            if self.ret_b is None:
                print("pull b")
                fp = open('b.modle', 'w')
                self.ret_b = rollsite.pull(fp, "model_B", tag="{}".format(_tag))
                fp.close()

            if self.ret_a and self.ret_b:
                print("Send Event")
                self.w_event.set()
                break
            else:
                print("sleep")
                time.sleep(1)


if __name__ == '__main__':
    rollsite.init("atest", "roll_site/test/role_conf.json", "roll_site/test/server_conf.json")
    _tag = "Hello"
    a = _tag

    test_event = Event()
    WThread = WriteThread(test_event)
    RThread = ReadThread(test_event)

    WThread.start()
    RThread.start()


