from eggroll.config import Config
from eggroll.deepspeed.submit.client import DeepspeedJob


class EggrollClient(DeepspeedJob):

    def __init__(self, config: Config, host="127.0.0.1", port=9370):

        super().__init__(config=config, host=host, port=port)
