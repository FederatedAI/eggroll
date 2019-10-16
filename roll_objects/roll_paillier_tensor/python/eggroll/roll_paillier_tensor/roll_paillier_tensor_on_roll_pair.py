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

from eggroll.core.command.command_model import ErCommandRequest
from eggroll.core.io.kv_adapter import RocksdbSortedKvAdapter
from eggroll.core.meta import ErStoreLocator, ErJob, ErStore, ErFunctor
from eggroll.core.proto import command_pb2_grpc
from eggroll.core.serdes import cloudpickle
from eggroll.roll_pair.roll_pair import RollPair
import roll_paillier_tensor as rpt_engine

class RollPaillierTensorOnRollPair(object):
  def __init__(self, store_desc=None):
    self._store = RollPair(store_desc)

  def scalar_mul(self, scalar):
    def functor(ser_enc_mat, scalar):
      enc_mat_mpz = rpt_engine.load(ser_enc_mat)
      pub_key, pvt_key = rpt_engine.keygen()

      mpz_result = rpt_engine.scalar_mul(enc_mat_mpz, scalar, pub_key, pvt_key)
      return rpt_engine.dump(mpz_result)

    return self._store.map_values(lambda v: functor(v, float(scalar)))

  def add(self, other):
    def functor(left, right):
      left_mpz = rpt_engine.load(left)
      right_mpz = rpt_engine.load(right)
      pub_key, pvt_key = rpt_engine.keygen()

      mpz_result = rpt_engine.add(left_mpz, right_mpz, pub_key, pvt_key)
      return rpt_engine.dump(mpz_result)

    return self._store.join(other, lambda left, right : functor(left, right))

