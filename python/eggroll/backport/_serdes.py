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

import importlib
from pickle import Unpickler, UnpicklingError
from pickle import dumps as p_dumps


# from pickle import loads as p_loads


# class UnrestrictedSerdes:
#     @staticmethod
#     def serialize(obj) -> bytes:
#         return p_dumps(obj)
#
#     @staticmethod
#     def deserialize(bytes) -> object:
#         return p_loads(bytes)

class RestrictedSerdes:
    @staticmethod
    def serialize(obj) -> bytes:
        return p_dumps(obj)

    @staticmethod
    def deserialize(bytes) -> object:
        return RestrictedUnpickler(bytes).load()


class RestrictedUnpickler(Unpickler):

    def _load(self, module, name):
        try:
            return super().find_class(module, name)
        except:
            return getattr(importlib.import_module(module), name)

    def find_class(self, module, name):
        if name in _DeserializeWhitelist.get_whitelist().get(module, set()):
            return self._load(module, name)
        else:
            for m in _DeserializeWhitelist.get_whitelist_glob():
                if module.startswith(m):
                    return self._load(module, name)
        raise UnpicklingError(f"forbidden unpickle class {module} {name}")


class _DeserializeWhitelist:
    loaded = False
    deserialize_whitelist = {}
    deserialize_glob_whitelist = set()

    @classmethod
    def get_whitelist_glob(cls):
        if not cls.loaded:
            cls.load_deserialize_whitelist()
        return cls.deserialize_glob_whitelist

    @classmethod
    def get_whitelist(cls):
        if not cls.loaded:
            cls.load_deserialize_whitelist()
        return cls.deserialize_whitelist

    @classmethod
    def get_whitelist_path(cls):
        import os.path

        return os.path.abspath(
            os.path.join(
                __file__,
                os.path.pardir,
                os.path.pardir,
                os.path.pardir,
                os.path.pardir,
                "conf",
                "whitelist.json",
            )
        )

    @classmethod
    def load_deserialize_whitelist(cls):
        import json
        with open(cls.get_whitelist_path()) as f:
            for k, v in json.load(f).items():
                if k.endswith("*"):
                    cls.deserialize_glob_whitelist.add(k[:-1])
                else:
                    cls.deserialize_whitelist[k] = set(v)
        cls.loaded = True


Serdes = RestrictedSerdes
