stringify_charset = "iso-8859-1"

from datetime import datetime


DEFAULT_DATETIME_FORMAT = "%Y%m%d.%H%M%S.%f"


def time_now_ns(format: str = DEFAULT_DATETIME_FORMAT):
    return datetime.now().strftime(format)


def _stringify(data):
    from ._base_model import RpcMessage

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


def _repr_bytes(a_bytes: bytes):
    if a_bytes is None:
        return f"(None)"
    else:
        return f"({a_bytes[:200]}, len={len(a_bytes)})"


def _repr_list(a_list: list):
    return ", ".join(_map_and_listify(repr, a_list))


def _elements_to_proto(rpc_message_list):
    return _map_and_listify(_to_proto, rpc_message_list)


def _map_and_listify(map_func, a_list):
    return list(map(map_func, a_list))


def _to_proto(rpc_message):
    if rpc_message is not None:
        return rpc_message.to_proto()


def _from_proto(parser, rpc_message):
    if rpc_message is not None:
        return parser(rpc_message)


def _to_proto_string(rpc_message):
    if rpc_message is not None:
        return rpc_message.to_proto_string()
