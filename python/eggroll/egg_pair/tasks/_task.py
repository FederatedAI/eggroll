import abc

from eggroll.core.meta_model import ErJob, ErTask


class EnvOptions:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self._server_node_id = None

    @property
    def server_node_id(self):
        return self._server_node_id

    @server_node_id.setter
    def server_node_id(self, server_node_id):
        self._server_node_id = server_node_id


class Task:
    @classmethod
    @abc.abstractmethod
    def run(cls, env_options: EnvOptions, job: ErJob, task: ErTask):
        ...
