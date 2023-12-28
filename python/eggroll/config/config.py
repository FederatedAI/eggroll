import configparser
import logging
import pprint
import typing

import omegaconf

from eggroll.config.defaults import DefaultConfig

logger = logging.getLogger(__name__)


class Config(object):
    """
    Configuration object used to store configurable parameters for EGGROLL
    """

    def __init__(self):
        self.config = omegaconf.OmegaConf.create()
        self.loaded_history = []

    def load_default(self):
        default_config = omegaconf.OmegaConf.structured(DefaultConfig)
        self.config = omegaconf.OmegaConf.merge(self.config, default_config)
        self.loaded_history.append(
            f"load_default: {DefaultConfig.__module__}:{DefaultConfig.__qualname__}"
        )
        return self

    def load_properties(self, config_file):
        c = configparser.ConfigParser()
        c.read(config_file)
        for k, v in c.items("eggroll"):
            if v == "":
                continue
            ConfigUtils.maybe_update(self.config, k, v)
        self.loaded_history.append(f"load_properties: {config_file}")
        return self

    def load_options(self, options: dict):
        for k, v in options.items():
            ConfigUtils.maybe_update(self.config, k, v)
        self.loaded_history.append(f"load_options: {options}")
        return self

    def load_env(self):
        import os

        accept_envs = []
        for k, v in os.environ.items():
            if k.lower().startswith("eggroll."):
                if v == "":
                    continue
                ConfigUtils.maybe_update(self.config, k.lower(), v.lower())
                accept_envs.append(f"{k.lower()}={v.lower()}")
        self.loaded_history.append(f"load_env:\n      " + "\n      ".join(accept_envs))
        return self

    def from_structured(self, dataclass_type: typing.Type):
        self.config = omegaconf.OmegaConf.merge(
            self.config, omegaconf.OmegaConf.structured(dataclass_type)
        )
        return self

    @property
    def eggroll(self) -> DefaultConfig.EggrollConfig:
        # use `DefaultConfig.EggrollConfig` typing here is on purpose for better IDE support
        return WrappedDictConfig(self, self.config.eggroll)


class DotKey(str):
    def __init__(self, key):
        self.key = key

    def __getattr__(self, item):
        return DotKey(f"{self.key}.{item}")

    def __str__(self):
        return f"<{self.__class__.__name__} key=self.key>"

    def __repr__(self):
        return self.key


class ConfigUtils:
    @staticmethod
    def set(
        config: typing.Union[Config, typing.Dict], key: typing.Any, value: typing.Any
    ):
        assert isinstance(key, DotKey)
        if isinstance(config, Config):
            omegaconf.OmegaConf.update(config.config, key.key, value)
        elif isinstance(config, dict):
            config[key.key] = value
        else:
            raise ValueError(f"config type={type(config)} not supported")

    @staticmethod
    def maybe_update(c: omegaconf.Container, k, v):
        try:
            omegaconf.OmegaConf.update(c, k, v)
        except omegaconf.errors.OmegaConfBaseException as e:
            logger.warning(f"update `{k}` to `{v}` failed, skip:\n{e}")

    @staticmethod
    def get_option(config: Config, option: dict, key: typing.Any):
        assert isinstance(key, DotKey)
        if key.key in option:
            return option[key.key]
        else:
            value = omegaconf.OmegaConf.select(
                config.config, key.key, throw_on_missing=True
            )
            if value is None:
                raise ConfigError(
                    config,
                    key.key,
                    f"`{key.key}` not found both in option=`{option}` and config=`{config.config}`",
                )
            elif isinstance(value, omegaconf.Container):
                raise ConfigError(
                    config,
                    key.key,
                    f"`{key.key}` found in config but not in leaf: value=`{value}`",
                )
            return value


class ConfigKey:
    eggroll: DefaultConfig.EggrollConfig = DotKey("eggroll")


class WrappedDictConfig:
    def __init__(self, base: Config, config: omegaconf.DictConfig):
        self._base = base
        self._config = config

    def __getattr__(self, name):
        try:
            attr = getattr(self._config, name)
            if isinstance(attr, omegaconf.DictConfig):
                return WrappedDictConfig(self._base, attr)
            else:
                return attr

        except omegaconf.errors.MissingMandatoryValue as e:
            raise ConfigError(
                self._base, e.full_key, f"config on `{e.full_key}` is missing"
            ) from e

        except omegaconf.errors.OmegaConfBaseException as e:
            raise ConfigError(
                self._base, e.full_key, f"config on `{e.full_key}` error"
            ) from e

    def __str__(self):
        return str(self.config)


class ConfigError(Exception):
    def __init__(self, base: Config, full_key, msg):
        full_msg = f"{msg}:\n"
        full_msg += "DEBUG:\n"
        full_msg += f"  HISTORY:\n"
        for hist in base.loaded_history:
            full_msg += f"    {hist}\n"
        full_msg += f"  CONFIG:\n"
        full_msg += f"    {pprint.pformat(omegaconf.OmegaConf.to_container(base.config), indent=0, sort_dicts=False)}"
        super(ConfigError, self).__init__(full_msg)


def load_config(properties_file):
    config = Config()
    config.load_default()
    if properties_file is not None:
        config.load_properties(properties_file)
    config.load_env()
    return config
