import typing
import configparser

from omegaconf import OmegaConf

default = {
    "eggroll": {
        "rollpair": {
            "eggpair": {
                "server": {
                    "executor": {
                        "pool": {
                            "max": {
                                "size": 5000
                            }
                        }
                    }
                }
            }
        },
        "core": {
            "default": {
                "executor": {
                    "pool": "eggroll.core.datastructure.threadpool.ErThreadUnpooledExecutor"
                }
            }
        }
    }
}


class Config(object):
    """
    Configuration object used to store configurable parameters for EGGROLL
    """

    def __init__(self):
        self.config = OmegaConf.create()

    def load_default(self):
        self.config = OmegaConf.merge(self.config, default)

    def load_properties(self, config_file):
        c = configparser.ConfigParser()
        c.read(config_file)
        self.config = OmegaConf.merge(self.config, dict(c.items("eggroll")))

    def load_env(self):
        import os
        accept_envs = {}
        for k, v in os.environ.items():
            if k.lower().startswith("eggroll_"):
                accept_envs[k] = v
        self.config = OmegaConf.merge(self.config, accept_envs)

    def from_structured(self, dataclass_type: typing.Type):
        self.config = OmegaConf.merge(self.config, OmegaConf.structured(dataclass_type))

    @property
    def eggroll(self):
        return self.config.eggroll
