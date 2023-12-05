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
import os
import typing

from ._format import PairBinReader, PairBinWriter, ArrayByteBuffer
from ._type import StoreTypes

if typing.TYPE_CHECKING:
    from eggroll.core.meta_model import ErPartition
L = logging.getLogger(__name__)


def create_pair_adapter(options: dict):
    L.info(f"options={options}")
    if options["store_type"] == StoreTypes.ROLLPAIR_IN_MEMORY:
        # FIXME: convert in-memory store to lmdb
        duplicate_options = options.copy()
        duplicate_options["store_type"] = StoreTypes.ROLLPAIR_LMDB
        ret = create_pair_adapter(options=duplicate_options)
    elif options["store_type"] == StoreTypes.ROLLPAIR_LMDB:
        from ._lmdb import LmdbAdapter

        ret = LmdbAdapter(options=options)
        L.info(f"return LmdbAdapter={ret}")
    else:
        raise NotImplementedError(options)
    return ret


def get_adapter(partition: "ErPartition", data_dir: str, options=None, watch=False):
    if options is None:
        options = {}
    options["store_type"] = partition.store_locator.store_type
    options["path"] = get_db_path_expanded(
        data_dir,
        partition.store_locator.store_type,
        partition.store_locator.namespace,
        partition.store_locator.name,
        partition.id,
    )
    options["er_partition"] = partition

    if watch:
        raise ValueError(f"watch is not supported for now, {options['path']}")

    return create_pair_adapter(options=options)


def get_db_path_expanded(data_dir, store_type, namespace, name, partition_id):
    return os.path.join(data_dir, store_type, namespace, name, str(partition_id))


def get_db_path_from_partition(partition: "ErPartition", data_dir: str):
    store_locator = partition.store_locator
    return get_db_path_expanded(
        data_dir,
        store_locator.store_type,
        store_locator.namespace,
        store_locator.name,
        partition.id,
    )
