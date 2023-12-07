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

import logging
import typing

from eggroll.config import ConfigKey, ConfigUtils
from eggroll.core.datastructure import create_executor_pool

L = logging.getLogger(__name__)


if typing.TYPE_CHECKING:
    from ._rollsite_context import RollSiteContext


class RollSiteBase:
    _receive_executor_pool = None

    def __init__(
        self, name: str, tag: str, rs_ctx: "RollSiteContext", options: dict = None
    ):
        if options is None:
            options = {}

        self.name = name
        self.tag = tag
        self.ctx = rs_ctx
        self.options = options
        self.party_id = self.ctx.party_id
        self.dst_host = self.ctx.proxy_endpoint.host
        self.dst_port = self.ctx.proxy_endpoint.port
        self.roll_site_session_id = self.ctx.roll_site_session_id
        self.local_role = self.ctx.role

        if RollSiteBase._receive_executor_pool is None:
            receive_executor_pool_size = ConfigUtils.get_option(
                self.ctx.config,
                options,
                ConfigKey.eggroll.rollsite.receive.executor.pool.max.size,
            )
            receive_executor_pool_type = ConfigUtils.get_option(
                self.ctx.config, options, ConfigKey.eggroll.core.default.executor.pool
            )
            self._receive_executor_pool = create_executor_pool(
                canonical_name=receive_executor_pool_type,
                max_workers=receive_executor_pool_size,
                thread_name_prefix="rollsite-client",
            )
        self._push_start_time = None
        self._pull_start_time = None
        L.debug(
            f"inited RollSite. my party_id={self.ctx.party_id}. proxy endpoint={self.dst_host}:{self.dst_port}"
        )

    def _run_thread(self, fn, *args, **kwargs):
        return self._receive_executor_pool.submit(fn, *args, **kwargs)
