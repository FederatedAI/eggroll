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

from typing import Iterable, Sequence
from itertools import islice, chain

def split_every(original: Iterable, index, chunk_size, skip_chunk):
    if not chunk_size:
        chunk_size = 100000

    full_iter = iter(original)
    try:
        slice_iter = islice(full_iter, (index+skip_chunk)*chunk_size, (index+skip_chunk+1)*chunk_size)
        return slice_iter
    except StopIteration as e:
        return None 

def split_every_generator(original: Iterable, chunk_size, skip_chunk):
    if not chunk_size:
        chunk_size = 100000
    
    chunked_iter = list()
    index = 0
    for item in original:
        chunked_iter.append(item)
        index += 1
        if index == chunk_size:
            return chunked_iter
    return chunked_iter

def split_every_yield(original: Iterable, chunk_size):
    if not chunk_size:
        chunk_size = 100000

    full_iter = iter(original)
    if isinstance(original, Sequence):      # Sequence
        yield from iter(lambda: list(islice(full_iter, chunk_size)), [])
    else:                                   # other Iterable types
        try:
            while True:
                slice_iter = islice(full_iter, chunk_size)
                peek = next(slice_iter)
                yield chain([peek], slice_iter)
        except StopIteration as e:
            return
