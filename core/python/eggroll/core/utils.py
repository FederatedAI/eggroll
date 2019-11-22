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

from datetime import datetime

import traceback


def _to_proto(rpc_message):
  if rpc_message is not None:
    return rpc_message.to_proto()

def _from_proto(parser, rpc_message):
  if rpc_message is not None:
    return parser(rpc_message)


def _map_and_listify(map_func, a_list):
  return list(map(map_func, a_list))


def _repr_list(a_list):
  return ", ".join(_map_and_listify(repr, a_list))


def _elements_to_proto(rpc_message_list):
  return _map_and_listify(_to_proto, rpc_message_list)


def _exception_logger(func):
  def wrapper(*args, **kw):
    try:
      return func(*args, **kw)
    except:
      msg = (f"==== detail start ====\n"
             f"{traceback.format_exc()}"
             f"\n==== detail end ====\n\n")
      # LOGGER.error(msg)
      print(msg)
      raise RuntimeError(msg)

  return wrapper

_DEFAULT_DATETIME_FORMAT = '%Y%m%dT%H%M%S.%f'
def time_now(format: str = _DEFAULT_DATETIME_FORMAT):
  formatted = datetime.now().strftime(format)
  if format == _DEFAULT_DATETIME_FORMAT or ('%f' in format):
    return formatted[:-3]
  else:
    return formatted