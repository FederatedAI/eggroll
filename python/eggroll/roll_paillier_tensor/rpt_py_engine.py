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
from pprint import pprint

import numpy as np

from federatedml.secureprotol.fate_paillier import PaillierKeypair
from federatedml.secureprotol.fixedpoint import FixedPointNumber


def load(data):
    return data


def dump(data):
    return data


def load_pub_key(pub):
    return pub


def dump_pub_key(pub):
    return pub


def load_prv_key(priv):
    return priv


def dump_prv_key(priv):
    return priv


def num2Mng(data, pub):
    return data
    # return np.vectorize(FixedPointNumber.encode)(data)


def add(x, y, pub):
    return x + y


def scalar_mul(x, s, pub):
    return x * s


def mul(x, s, pub):
    return x * s


def vdot(x, v, pub):
    return x * v


def matmul(x, y, _pub):
    return x @ y


def transe(data):
    return data.T


def mean(data, pub):
    return np.array([data.mean(axis=0)])


def hstack(x, y, pub):
    return np.hstack((x, y))


def decryptdecode(data, pub, priv):
    return np.vectorize(priv.decrypt)(data)


def print(data, pub, priv):
    pprint(data)


def encrypt_and_obfuscate(data, pub):
    return np.vectorize(pub.encrypt)(data)


def keygen():
    return PaillierKeypair().generate_keypair()
