import os
import typing
import configparser

from omegaconf import OmegaConf, SCMode
from eggroll.config.defaults import DefaultConfig


class Config(object):
    """
    Configuration object used to store configurable parameters for EGGROLL
    """

    def __init__(self):
        self.config = OmegaConf.create()

    def load_default(self):
        self.config = OmegaConf.merge(self.config, OmegaConf.structured(DefaultConfig))
        return self

    def load_properties(self, config_file):
        c = configparser.ConfigParser()
        c.read(config_file)
        dotlist = [f"{k}={v}" for k, v in c.items("eggroll") if v != ""]
        self.config = OmegaConf.merge(self.config, OmegaConf.from_dotlist(dotlist))
        return self

    def load_env(self):
        import os
        accept_envs = []
        for k, v in os.environ.items():
            if k.lower().startswith("eggroll."):
                accept_envs.append(f"{k.lower()}={v.lower()}")
        self.config = OmegaConf.merge(self.config, OmegaConf.from_dotlist(accept_envs))
        return self

    def from_structured(self, dataclass_type: typing.Type):
        self.config = OmegaConf.merge(self.config, OmegaConf.structured(dataclass_type))
        return self

    @property
    def eggroll(self) -> DefaultConfig.EggrollConfig:
        return self.config.eggroll


def load_config(properties_file):
    config = Config()
    config.load_default()
    if properties_file is not None:
        config.load_properties(properties_file)
    elif "EGGROLL_HOME" in os.environ:
        path = os.path.join(os.environ["EGGROLL_HOME"], "conf", "eggroll.properties")
        if os.path.exists(path):
            config.load_properties(path)
    config.load_env()
    return config


if __name__ == "__main__":
    config = Config()
    config.load_default()
    # config.load_properties("/Users/sage/MergeFATE/eggroll/conf/eggroll.properties")