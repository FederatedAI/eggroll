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
import json
import os

import click
import requests
from ruamel import yaml


def prettify(response):
    # format json
    if isinstance(response, requests.models.Response):
        try:
            response = response.json()
        except json.decoder.JSONDecodeError:
            response = {
                "retcode": 100,
                "retmsg": response.text,
            }

    click.echo(json.dumps(response, indent=4, ensure_ascii=False))
    click.echo("")

    return response


def check_abs_path(path):
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(os.getcwd(), path))


def load_yaml(path):
    with open(path, "r") as fr:
        return yaml.safe_load(fr)


def unzip(zip_path, extra_dir):
    import zipfile

    zfile = zipfile.ZipFile(zip_path, "r")
    for name in zfile.namelist():
        dir_name = os.path.dirname(zip_path)
        file_path = os.path.join(dir_name, extra_dir, name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        data = zfile.read(name)
        with open(file_path, "w+b") as file:
            file.write(data)
