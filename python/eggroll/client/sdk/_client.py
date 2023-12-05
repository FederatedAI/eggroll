from .api import Task
from .utils.base_utils import BaseEggrollClient


class EggrollClient(BaseEggrollClient):
    task = Task()

    def __init__(self, ip="127.0.0.1", port=9370):
        super().__init__(ip, port)
