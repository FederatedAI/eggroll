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
import json
import sys
import time
import traceback
from datetime import datetime


def _to_proto(rpc_message):
    if rpc_message is not None:
        return rpc_message.to_proto()


def _to_proto_string(rpc_message):
    if rpc_message is not None:
        return rpc_message.to_proto_string()


def _from_proto(parser, rpc_message):
    if rpc_message is not None:
        return parser(rpc_message)


def _map_and_listify(map_func, a_list):
    return list(map(map_func, a_list))


def _repr_list(a_list):
    return ", ".join(_map_and_listify(repr, a_list))


def _elements_to_proto(rpc_message_list):
    return _map_and_listify(_to_proto, rpc_message_list)

def string_to_bytes(string):
    return string if isinstance(string, bytes) else string.encode(encoding="utf-8")


def bytes_to_string(byte):
    return byte.decode(encoding="ISO-8859-1")


def json_dumps(src, byte=False):
    if byte:
        return string_to_bytes(json.dumps(src))
    else:
        return json.dumps(src)


def json_loads(src):
    if isinstance(src, bytes):
        return json.loads(bytes_to_string(src))
    else:
        return json.loads(src)


def current_timestamp():
    return int(time.time()*1000)

def _exception_logger(func):
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except:
            msg = (f"\n==== detail start ====\n"
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

def get_self_ip():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        self_ip = s.getsockname()[0]
    except:
        self_ip = '127.0.0.1'
    finally:
        s.close()
    return self_ip


def generate_job_id(session_id, tag='', delim='-'):
    result = f'{session_id}{delim}job{delim}{time_now()}'
    if not tag:
        return result
    else:
        return f'{result}_{tag}'


def generate_task_id(job_id, partition_id, delim='-'):
    return delim.join([job_id, "task", str(partition_id)])


#AI copy from java ByteString.hashCode()
def hash_code(s):
    if isinstance(s, bytes):
        s = bytes_to_string(s)
    seed = 31
    h = 0
    for c in s:
        h = int(seed * h) + ord(c)

    if h == sys.maxsize or h == -sys.maxsize - 1:
        print("hash code:{} out of int bound".format(str(h)))
        h = 0

    return h