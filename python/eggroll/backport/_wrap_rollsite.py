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

import typing

if typing.TYPE_CHECKING:
    from eggroll.federation._rollsite import RollSite
    from ._wrap_rollpair import WrappedRpc


def _init_roll_site_context(
    roll_site_session_id, rp_ctx: "WrappedRpc", options: dict = None
):
    from eggroll.federation import RollSiteContext as _RollSiteContext

    if options is None:
        raise ValueError("options is None")
    assert "self_role" in options, "self_role not in options"
    assert "self_party_id" in options, "self_party_id not in options"
    assert "proxy_endpoint" in options, "proxy_endpoint not in options"
    party = (options["self_role"], options["self_party_id"])
    proxy_endpoint = options["proxy_endpoint"]
    proxy_endpoint_host, proxy_endpoint_port = proxy_endpoint.split(":")
    proxy_endpoint_port = int(proxy_endpoint_port)
    return _RollSiteContext(
        roll_site_session_id=roll_site_session_id,
        rp_ctx=rp_ctx._rpc,
        party=party,
        proxy_endpoint_host=proxy_endpoint_host,
        proxy_endpoint_port=proxy_endpoint_port,
        options=options,
    )


class WrappedRollSiteContext:
    def __init__(
        self, roll_site_session_id, rp_ctx: "WrappedRpc", options: dict = None
    ):
        if options is None:
            options = {}
        self._options = options
        self._rsc = _init_roll_site_context(roll_site_session_id, rp_ctx, options)

    def load(self, name, tag, options: dict = None):
        if options is None:
            options = {}
        final_options = self._options.copy()
        final_options.update(options)
        from eggroll.federation._rollsite import RollSite

        return WrappedRollSite(
            RollSite(name=name, tag=tag, rs_ctx=self._rsc, options=final_options)
        )


class WrappedRollSite:
    def __init__(self, roll_site: "RollSite"):
        self._roll_site = roll_site

    def push(self, obj, parties: list = None, options: dict = None):
        from ._wrap_rollpair import WrappedRp
        from ._serdes import Serdes

        if isinstance(obj, WrappedRp):
            return self._roll_site.push_rp(obj._rp, parties, options)
        else:
            return self._roll_site.push_bytes(Serdes.serialize(obj), parties, options)

    def pull(self, parties: list = None, options: dict = None):
        return _pull_with_lift(self._roll_site, parties=parties)


def _pull_with_lift(
    rs: "RollSite",
    parties: list = None,
):
    from eggroll.federation._rollsite import ErRollSiteHeader

    futures = []
    for src_role, src_party_id in parties:
        src_party_id = str(src_party_id)
        rs_header = ErRollSiteHeader(
            roll_site_session_id=rs.roll_site_session_id,
            name=rs.name,
            tag=rs.tag,
            src_role=src_role,
            src_party_id=src_party_id,
            dst_role=rs.local_role,
            dst_party_id=rs.party_id,
        )
        futures.append(
            rs._receive_executor_pool.submit(
                _lifter(rs._impl_instance._pull_one), rs_header
            )
        )
    return futures


def _lifter(func):
    def _inner(*args, **kwargs):
        from ._wrap_rollpair import WrappedRp
        from ._serdes import Serdes
        from eggroll.computing import RollPair

        rp_or_obj = func(*args, **kwargs)
        if isinstance(rp_or_obj, RollPair):
            return WrappedRp(rp_or_obj)
        else:
            return Serdes.deserialize(rp_or_obj)

    return _inner
