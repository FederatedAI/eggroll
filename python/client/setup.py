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
# -*- coding: utf-8 -*-

import os
from setuptools import find_packages, setup


packages = find_packages('..')
filtered_packages = [pkg for pkg in packages if pkg.startswith("client")]
package_data = {"": ["*"]}
install_requires = [
    "click",
    "requests<2.26.0",
    "grpcio==1.46.3",
    "numba==0.53.0",
    "numpy==1.23.1",
    "protobuf==3.19.6",
    "ruamel.yaml==0.16.10"
]

entry_points = {"console_scripts": ["eggroll = client.cli.eggroll:eggroll_cli"]}

setup_kwargs = {
    "name": "eggroll-client",
    "version": "2.5.3",
    "description": "Clients for Eggroll",
    "long_description_content_type": "text/markdown",
    "long_description": "Clients for Eggroll",
    "author": "FederatedAI",
    "author_email": "contact@FedAI.org",
    "maintainer": None,
    "maintainer_email": None,
    "url": "https://fate.fedai.org/",
    "packages": filtered_packages,
    "include_package_data": True,
    "package_data": package_data,
    "install_requires": install_requires,
    "entry_points": entry_points,
    "python_requires": ">=3.8",
}

os.chdir('..')
setup(**setup_kwargs)
