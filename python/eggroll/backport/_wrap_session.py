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
    from eggroll.session import ErSession


def session_init(session_id, options) -> "WrappedSession":
    import os
    from eggroll.session import session_init as _session_init

    assert "EGGROLL_HOME" in os.environ, "EGGROLL_HOME not set"
    config_properties_file = os.path.join(
        os.environ["EGGROLL_HOME"], "conf", "eggroll.properties"
    )
    _session = _session_init(
        session_id=session_id,
        options=options,
        config_properties_file=config_properties_file,
    )
    return WrappedSession(session=_session)


class WrappedSession:
    def __init__(self, session: "ErSession"):
        self._session = session

    def get_session_id(self):
        return self._session.get_session_id()

    def kill(self):
        return self._session.kill()

    def stop(self):
        return self._session.stop()
