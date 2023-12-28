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
import threading
import typing

from eggroll.computing import RollPairContext
from eggroll.config import ConfigKey
from eggroll.core.grpc.factory import GrpcChannelFactory
from eggroll.core.meta_model import ErEndpoint
from eggroll.session import ErSession
from eggroll.config import ConfigUtils

L = logging.getLogger(__name__)


class RollSiteContext:
    grpc_channel_factory = GrpcChannelFactory()

    def __init__(
        self,
        roll_site_session_id,
        rp_ctx: RollPairContext,
        party: typing.Tuple[str, str],
        proxy_endpoint_host: str,
        proxy_endpoint_port: int,
        options: dict = None,
    ):
        if options is None:
            options = {}
        self.roll_site_session_id = roll_site_session_id
        self.rp_ctx = rp_ctx
        self._config = rp_ctx.session.config

        self.role = party[0]
        self.party_id = party[1]
        self._options = options

        self._registered_comm_types = dict()
        self.proxy_endpoint = ErEndpoint(
            host=proxy_endpoint_host, port=proxy_endpoint_port
        )

        self.pushing_latch = CountDownLatch(0)
        self.rp_ctx.session.add_exit_task(self._wait_push_complete)

        # push session
        self.push_session_enabled = ConfigUtils.get_option(
            self._config, options, ConfigKey.eggroll.rollsite.push.session.enabled
        )
        if self.push_session_enabled:
            # create session for push roll_pair and object
            self._push_session = ErSession(
                config=self._config,
                session_id=roll_site_session_id + "_push",
                options=rp_ctx.session.get_all_options(),
            )
            self._push_rp_ctx = RollPairContext(session=self._push_session)
            L.info(f"push_session={self._push_session.get_session_id()} enabled")

            def stop_push_session():
                self._push_session.stop()

            self.rp_ctx.session.add_exit_task(stop_push_session)
        self._wait_push_exit_timeout = ConfigUtils.get_option(
            self._config, options, ConfigKey.eggroll.rollsite.push.overall.timeout.sec
        )

        L.info(f"inited RollSiteContext: {self.__dict__}")

    @property
    def config(self):
        return self.rp_ctx.session.config

    def _wait_push_complete(self):
        session_id = self.rp_ctx.session.get_session_id()
        L.info(
            f"running roll site exit func for er session={session_id},"
            f" roll site session id={self.roll_site_session_id}"
        )
        residual_count = self.pushing_latch.await_latch(self._wait_push_exit_timeout)
        if residual_count != 0:
            L.error(
                f"exit session when not finish push: "
                f"residual_count={residual_count}, timeout={self._wait_push_exit_timeout}"
            )

    def load(self, name: str, tag: str, options: dict = None):
        from ._rollsite import RollSite

        if options is None:
            options = {}
        final_options = self._options.copy()
        final_options.update(options)
        return RollSite(name, tag, self, options=final_options)


class CountDownLatch(object):
    def __init__(self, count):
        self.count = count
        self.lock = threading.Condition()

    def count_up(self):
        with self.lock:
            self.count += 1

    def count_down(self):
        with self.lock:
            self.count -= 1
            if self.count <= 0:
                self.lock.notifyAll()

    def await_latch(self, timeout=None, attempt=1, after_attempt=None):
        try_count = 0
        with self.lock:
            while self.count > 0 and try_count < attempt:
                try_count += 1
                self.lock.wait(timeout)
                if after_attempt:
                    after_attempt(try_count)
        return self.count
