import pickle
import os


class UnsafeSerdes:
    def __init__(self):
        ...

    def serialize(self, obj) -> bytes:
        return pickle.dumps(obj)

    def deserialize(self, bytes) -> object:
        return pickle.loads(bytes)


def get_unsafe_serdes():
    if os.environ.get("SERDES_DEBUG_MODE") == "1":
        return UnsafeSerdes()
    else:
        raise PermissionError("UnsafeSerdes is not allowed in production mode")
