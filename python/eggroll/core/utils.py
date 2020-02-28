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
import time
import traceback
import uuid
from datetime import datetime

import numba
import numpy as np
from google.protobuf.text_format import MessageToString

static_er_conf = {}
stringify_charset = 'iso-8859-1'
M = 2**31


def set_static_er_conf(a_dict):
    global static_er_conf
    if static_er_conf:
        raise ValueError('static_er_conf has already been set')
    static_er_conf = a_dict


def get_static_er_conf():
    return static_er_conf


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


def _stringify(data):
    from eggroll.core.base_model import RpcMessage
    if isinstance(data, str):
        return data
    elif isinstance(data, RpcMessage):
        return data.to_proto_string().decode(stringify_charset)
    elif isinstance(data, bytes):
        return data.decode(stringify_charset)
    else:
        return str(data)


def _stringify_dict(a_dict: dict):
    return {_stringify(k): _stringify(v) for k, v in a_dict.items()}



def _repr_list(a_list: list):
    return ", ".join(_map_and_listify(repr, a_list))


def _repr_bytes(a_bytes: bytes):
    if a_bytes is None:
        return f"(None)"
    else:
        return f"({a_bytes[:200]}, len={len(a_bytes)})"


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
            msg = (f"\n\n==== detail start, at {time_now()} ====\n"
                   f"{traceback.format_exc()}"
                   f"\n==== detail end ====\n\n")
            # LOGGER.error(msg)
            print(msg)
            raise RuntimeError(msg)

    return wrapper


def get_stack():
    return (f"\n\n==== stack start, at {time_now()} ====\n"
           f"{''.join(traceback.format_stack())}"
           f"\n==== stack end ====\n\n")


DEFAULT_DATETIME_FORMAT = '%Y%m%d.%H%M%S.%f'
def time_now(format: str = DEFAULT_DATETIME_FORMAT):
    formatted = datetime.now().strftime(format)
    if format == DEFAULT_DATETIME_FORMAT or ('%f' in format):
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


# TODO:0: replace uuid with simpler human friendly solution
def generate_job_id(session_id, tag='', delim='-'):
    result = delim.join([session_id, 'py', 'job', str(uuid.uuid1())])
    if not tag:
        return result
    else:
        return f'{result}_{tag}'


def generate_task_id(job_id, partition_id, delim='-'):
    return delim.join([job_id, "task", str(partition_id)])


'''AI copy from java ByteString.hashCode(), @see RollPairContext.partitioner'''
@numba.jit
def hash_code(s):
    seed = 31
    h = len(s)
    for c in s:
        # to singed int
        if c > 127:
            c = -256 + c
        h = h * seed
        if h > 2147483647 or h < -2147483648:
            h = (h & (M - 1)) - (h & M)
        h = h + c
        if h > 2147483647 or h < -2147483648:
            h = (h & (M - 1)) - (h & M)
    if h == 0 or h == -2147483648:
        h = 1
    return h if h >= 0 else abs(h)


def to_one_line_string(msg, as_one_line=True):
    if isinstance(msg, str) or isinstance(msg, bytes):
        return msg
    return MessageToString(msg, as_one_line=as_one_line)
