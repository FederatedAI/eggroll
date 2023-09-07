#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
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
import inspect
import json

import requests


def _is_api_endpoint(obj):
    return isinstance(obj, BaseEggrollAPI)


class BaseEggrollClient:
    def __new__(cls, *args, **kwargs):
        self = super().__new__(cls)
        api_endpoints = inspect.getmembers(self, _is_api_endpoint)
        for name, api in api_endpoints:
            api_cls = type(api)
            api = api_cls(self)
            setattr(self, name, api)
        return self

    def __init__(self, ip, port):
        self._http = requests.Session()
        self.ip = ip
        self.port = port

    @staticmethod
    def _decode_result(response):
        try:
            result = json.loads(response.content.decode('utf-8', 'ignore'), strict=False)
        except (TypeError, ValueError):
            return response
        else:
            return result

    def _handle_result(self, response):
        if isinstance(response, requests.models.Response):
            return response.json()
        elif isinstance(response, dict):
            return response
        else:
            return self._decode_result(response)


class BaseEggrollAPI:
    def __init__(self, client=None):
        self._client = client

    @property
    def host(self):
        return self._client.ip

    @property
    def port(self):
        return self._client.port
