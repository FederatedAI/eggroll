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

from federatedml.secureprotol.fate_paillier import PaillierKeypair, PaillierPublicKey, PaillierEncryptedNumber
from federatedml.secureprotol.fixedpoint import FixedPointNumber
from federatedml.secureprotol import gmpy_math
import random


class AsyncPaillierPublicKey(PaillierPublicKey):
    def __init__(self, pub):
        super().__init__(pub.n)

    def gen_obfuscator(self, random_value=None):
        r = random_value or random.SystemRandom().randrange(1, self.n)
        obfuscator = gmpy_math.powmod(r, self.n, self.nsquare)
        return obfuscator

    def apply_obfuscator(self, ciphertext, random_value=None, obf=None):
        """
        """
        if obf is not None:
            obfuscator = obf
        else:
            obfuscator = self.gen_obfuscator(random_value)
        return (ciphertext * obfuscator) % self.nsquare

    def raw_encrypt(self, plaintext, random_value=None):
        if random_value is not None and random_value != 1:
            raise NotImplementedError("unsupported in this class, use PaillierPublicKey instead")
        if not isinstance(plaintext, int):
            raise TypeError("plaintext should be int, but got: %s" %
                            type(plaintext))
        if plaintext >= (self.n - self.max_int) and plaintext < self.n:
            # Very large plaintext, take a sneaky shortcut using inverses
            neg_plaintext = self.n - plaintext  # = abs(plaintext - nsquare)
            neg_ciphertext = (self.n * neg_plaintext + 1) % self.nsquare
            ciphertext = gmpy_math.invert(neg_ciphertext, self.nsquare)
        else:
            ciphertext = (self.n * plaintext + 1) % self.nsquare
        return ciphertext

    def encode(self, value, precision=None):
        return FixedPointNumber.encode(value, self.n, self.max_int, precision)

    def encrypt(self, value, precision=None, random_value=None):
        """Encode and Paillier encrypt a real number value.
        """
        encoding = self.encode(value)
        ciphertext = self.raw_encrypt(encoding.encoding)
        ciphertext = self.apply_obfuscator(ciphertext, random_value=random_value)
        encryptednumber = PaillierEncryptedNumber(self, ciphertext, encoding.exponent)

        return encryptednumber


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
    pprint(decryptdecode(data, pub, priv))


def encrypt_and_obfuscate(data, pub, obfs=None):
    if obfs is None:
        return np.vectorize(pub.encrypt)(data)

    def func(value, obf):
        encoding = pub.encode(value)
        ciphertext = pub.raw_encrypt(encoding.encoding)
        ciphertext = pub.apply_obfuscator(ciphertext, obf=obf)
        return PaillierEncryptedNumber(pub, ciphertext, encoding.exponent)
    return np.vectorize(func)(data, obfs)


def keygen():
    pub, priv = PaillierKeypair().generate_keypair()
    return AsyncPaillierPublicKey(pub), priv

