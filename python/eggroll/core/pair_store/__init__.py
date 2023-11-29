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

from eggroll.core.constants import StoreTypes

L = logging.getLogger(__name__)


def create_pair_adapter(options: dict):
    L.info(f"options={options}")
    if options["store_type"] == StoreTypes.ROLLPAIR_IN_MEMORY:
        # FIXME: convert in-memory store to lmdb
        duplicate_options = options.copy()
        duplicate_options["store_type"] = StoreTypes.ROLLPAIR_LMDB
        ret = create_pair_adapter(options=duplicate_options)
    elif options["store_type"] == StoreTypes.ROLLPAIR_LMDB:
        from eggroll.core.pair_store.lmdb import LmdbAdapter
        ret = LmdbAdapter(options=options)
        L.info(f"return LmdbAdapter={ret}")
    else:
        raise NotImplementedError(options)
    return ret
