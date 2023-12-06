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
import logging

L = logging.getLogger(__name__)


# TODO:0: usage?
class AdapterManager:
    pass


class PairAdapter(object):
    """
    Pair(key->value) store adapter
    """

    def __init__(self, options):
        pass

    def __del__(self):
        pass

    def close(self):
        raise NotImplementedError()

    def iteritems(self):
        raise NotImplementedError()

    def new_batch(self):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def put(self, key, value):
        raise NotImplementedError()

    def is_sorted(self):
        raise NotImplementedError()

    def destroy(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PairWriteBatch:
    def get(self, k):
        raise NotImplementedError

    def put(self, k, v):
        raise NotImplementedError()

    def merge(self, merge_func, k, v):
        raise NotImplementedError

    def write(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PairIterator:
    def close(self):
        raise NotImplementedError()

    def __iter__(self):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def key(self):
        raise NotImplementedError()

    def last(self):
        raise NotImplementedError()

    def is_sorted(self):
        raise NotImplementedError()

    @property
    def adapter(self) -> PairAdapter:
        raise NotImplementedError()
