from pickle import dumps as p_dumps
from pickle import loads as p_loads


class UnrestrictedSerdes:
    @staticmethod
    def serialize(obj) -> bytes:
        return p_dumps(obj)

    @staticmethod
    def deserialize(bytes) -> object:
        return p_loads(bytes)
