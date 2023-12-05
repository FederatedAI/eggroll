#
#  Copyright 2019 The EGGROLL Authors. All Rights Reserved.
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
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup


packages = find_packages(".")
package_data = {"": ["*"]}
install_requires = [
    "click",
    "omegaconf",
    "requests<3.0",
    "grpcio==1.46.3",
    "protobuf==3.19.6",
    "ruamel.yaml==0.16.10",
]

extras_require = {
    "full": [
        "cloudpickle==2.1.0",
        "grpcio-tools==1.46.3",
        "lmdb==1.3.0",
        "psutil>=5.7.0",
    ]
}


entry_points = {"console_scripts": ["eggroll = eggroll.client.cli.eggroll:eggroll_cli"]}

setup_kwargs = {
    "name": "eggroll-client",
    "version": "3.0.0",
    "description": "Clients for Eggroll",
    "long_description_content_type": "text/markdown",
    "long_description": "Clients for Eggroll",
    "author": "FederatedAI",
    "author_email": "contact@FedAI.org",
    "maintainer": None,
    "maintainer_email": None,
    "url": "https://fate.fedai.org/",
    "packages": packages,
    "include_package_data": True,
    "package_data": package_data,
    "install_requires": install_requires,
    "extras_require": extras_require,
    "entry_points": entry_points,
    "python_requires": ">=3.8",
}

setup(**setup_kwargs)
