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

from eggroll.core.meta_model import ErRollSiteHeader
from ._rollsite_base import RollSiteBase

if typing.TYPE_CHECKING:
    from ._rollsite_context import RollSiteContext

L = logging.getLogger(__name__)


class RollSite(RollSiteBase):
    def __init__(
        self, name: str, tag: str, rs_ctx: "RollSiteContext", options: dict = None
    ):
        if options is None:
            options = dict()
        super().__init__(name, tag, rs_ctx)

        from ._rollsite_impl_grpc import RollSiteGrpc

        self._impl_instance = RollSiteGrpc(name, tag, rs_ctx, options)

    def push_bytes(self, obj, parties: list = None, options: dict = None):
        if options is None:
            options = {}

        futures = []
        for role_party_id in parties:
            self.ctx.pushing_latch.count_up()
            dst_role = role_party_id[0]
            dst_party_id = str(role_party_id[1])
            data_type = "object"
            rs_header = ErRollSiteHeader(
                roll_site_session_id=self.roll_site_session_id,
                name=self.name,
                tag=self.tag,
                src_role=self.local_role,
                src_party_id=self.party_id,
                dst_role=dst_role,
                dst_party_id=dst_party_id,
                data_type=data_type,
            )
            future = self._run_thread(
                self._impl_instance._push_bytes, obj, rs_header, options
            )
            futures.append(future)

        return futures

    def push_rp(self, obj, parties: list = None, options: dict = None):
        if options is None:
            options = {}

        futures = []
        for role_party_id in parties:
            self.ctx.pushing_latch.count_up()
            dst_role = role_party_id[0]
            dst_party_id = str(role_party_id[1])
            data_type = "rollpair"
            rs_header = ErRollSiteHeader(
                roll_site_session_id=self.roll_site_session_id,
                name=self.name,
                tag=self.tag,
                src_role=self.local_role,
                src_party_id=self.party_id,
                dst_role=dst_role,
                dst_party_id=dst_party_id,
                data_type=data_type,
            )

            future = self._run_thread(
                self._impl_instance._push_rollpair, obj, rs_header, options
            )
            futures.append(future)
        return futures

    def pull(self, parties: list = None):
        futures = []
        for src_role, src_party_id in parties:
            src_party_id = str(src_party_id)
            rs_header = ErRollSiteHeader(
                roll_site_session_id=self.roll_site_session_id,
                name=self.name,
                tag=self.tag,
                src_role=src_role,
                src_party_id=src_party_id,
                dst_role=self.local_role,
                dst_party_id=self.party_id,
            )
            futures.append(
                self._receive_executor_pool.submit(
                    self._impl_instance._pull_one, rs_header
                )
            )
        return futures
