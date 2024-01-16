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

import importlib.abc
import importlib.machinery


class Eggroll2xPackageFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if (
                fullname.startswith("eggroll.core.session")
                or fullname.startswith("eggroll.roll_pair")
                or fullname.startswith("eggroll.roll_site")
        ):
            return importlib.machinery.ModuleSpec(
                fullname,
                Eggroll2xLoader(),
            )


class Eggroll2xLoader(importlib.abc.Loader):
    def create_module(self, spec):
        if spec.name == "eggroll.core.session":
            from . import _module_session_patch

            return _module_session_patch

        if spec.name == "eggroll.roll_pair":
            return _create_duck_package(spec.name)

        if spec.name == "eggroll.roll_pair.roll_pair":
            from . import _module_rollpair_patch

            return _module_rollpair_patch

        if spec.name == "eggroll.roll_site":
            return _create_duck_package(spec.name)

        if spec.name == "eggroll.roll_site.roll_site":
            from . import _module_rollsite_patch

            return _module_rollsite_patch

    def exec_module(self, module):
        pass


def _create_duck_package(name):
    import types
    import sys

    duck = types.ModuleType(name)
    duck.__path__ = []
    duck.__package__ = name
    sys.modules[name] = duck
    return duck
