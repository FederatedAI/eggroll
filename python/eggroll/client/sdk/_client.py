from eggroll.config import Config
from .api import Task
from .utils.base_utils import BaseEggrollClient


class EggrollClient(BaseEggrollClient):
    task = Task()

    def __init__(self, config: Config, ip="127.0.0.1", port=9370):
        super().__init__(config=config, ip=ip, port=port)
