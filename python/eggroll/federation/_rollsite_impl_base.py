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

L = logging.getLogger(__name__)


if typing.TYPE_CHECKING:
    from ._rollsite_context import RollSiteContext
    from eggroll.computing import RollPair


class RollSiteImplBase(RollSiteBase):
    def __init__(
        self, name: str, tag: str, rs_ctx: "RollSiteContext", options: dict = None
    ):
        super(RollSiteImplBase, self).__init__(name, tag, rs_ctx, options)

    def _push_bytes(self, obj, rs_header: ErRollSiteHeader, options: dict):
        raise NotImplementedError()

    def _push_rollpair(
        self, rp: "RollPair", rs_header: ErRollSiteHeader, options: dict
    ):
        raise NotImplementedError()

    def _pull_one(self, rs_header: ErRollSiteHeader):
        raise NotImplementedError()

    def cleanup(self):
        return
