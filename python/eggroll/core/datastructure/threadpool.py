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

import atexit
import itertools
import os
import threading

from concurrent.futures import _base

from eggroll.utils.log_utils import get_logger

L = get_logger()
_shutdown = False


def _python_exit():
    global _shutdown
    _shutdown = True


atexit.register(_python_exit)


# hats off to Python 3.7's _WorkItem
class _ErWorkItem(object):
    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self, on_join=None):
        if not self.future.set_running_or_notify_cancel():
            return

        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as exc:
            self.future.set_exception(exc)
            # Break a reference cycle with the exception 'exc'
            self = None
        else:
            self.future.set_result(result)
        finally:
            on_join()


class ErThreadUnpooledExecutor(_base.Executor):
    """
    Executor with Threads not pooled at all. If max_workers is reached, new
    submits are blocked until an existing thread is finished and released.
    """
    _counter = itertools.count().__next__

    def __init__(self, max_workers=None, thread_name_prefix=''):
        if max_workers is None:
            # Use this number because ThreadPoolExecutor is often
            # used to overlap I/O instead of CPU work.
            max_workers = (os.cpu_count() or 1) * 5
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._max_workers = max_workers
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        self._thread_name_prefix = (thread_name_prefix or
                                    f"ErUnpooledExecutor-{self._counter()}")
        self._num_threads = 0
        self._num_threads_lock = threading.Lock()
        self._empty_event = threading.Event()
        self._empty_event.set()
        self._accept_event = threading.Event()
        self._accept_event.set()

    def submit(*args, **kwargs):
        if len(args) >= 2:
            self, fn, *args = args
        elif not args:
            raise TypeError("descriptor 'submit' of 'ErDummyThreadPool' object "
                            "needs an argument")
        elif 'fn' in kwargs:
            fn = kwargs.pop('fn')
            self, *args = args
        else:
            raise TypeError('submit expected at least 1 positional argument, '
                            'got %d' % (len(args)-1))

        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')
            if _shutdown:
                raise RuntimeError('cannot schedule new futures after '
                                   'interpreter shutdown')
            self.increase_thread_count()
            thread_name = f'{self._thread_name_prefix or self}_{self._num_threads}'
            f = _base.Future()
            w = _ErWorkItem(f, fn, args, kwargs)
            t = threading.Thread(name=thread_name, target=w.run, kwargs={"on_join": self.decrease_thread_count})
            t.start()

            return f
    submit.__doc__ = _base.Executor.submit.__doc__

    def increase_thread_count(self):
        wait_result = False
        while not wait_result:
            wait_result = self._accept_event.wait(30)
            if wait_result:
                with self._num_threads_lock:
                    if self._num_threads >= self._max_workers:
                        self._accept_event.clear()
                        continue
                    else:
                        self._num_threads += 1
                        self._empty_event.clear()
                        break
            else:
                L.debug(f"waiting for thread to release. thread_name_prefix={self._thread_name_prefix}, self._num_threads={self._num_threads}, max_workers={self._max_workers}")

    def decrease_thread_count(self):
        with self._num_threads_lock:
            self._num_threads -= 1
            self._accept_event.set()
            if self._num_threads == 0:
                self._empty_event.set()
            elif self._num_threads < 0:
                raise OverflowError(f'num thread of {self._thread_name_prefix} < 0 after decrease. _num_threads={self._num_threads}')

    def shutdown(self, wait=True):
        L.info(f"shutting down threadpool. wait={wait}, thread_name_prefix={self._thread_name_prefix}, self._num_threads={self._num_threads}, max_workers={self._max_workers}")
        with self._shutdown_lock:
            self._shutdown = True
            self._accept_event.clear()
        if wait:
            self._empty_event.wait()

    shutdown.__doc__ = _base.Executor.shutdown.__doc__