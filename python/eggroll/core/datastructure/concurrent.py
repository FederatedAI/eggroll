# -*- coding: UTF-8 -*-
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

import threading


class CountDownLatch(object):
    def __init__(self, count=1):
        self._count = count
        self._lock = threading.Condition()

    def count_down(self):
        self._lock.acquire()
        if self._count <= 0:
            return

        self._count -= 1
        if self._count <= 0:
            self._lock.notify_all()
        self._lock.release()

    def await_timeout(self, timeout=None):
        self._lock.acquire()
        while self._count > 0:
            self._lock.wait(timeout)
        self._lock.release()

    def get_count(self):
        return self._count
