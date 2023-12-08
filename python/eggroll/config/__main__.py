def main():
    import argparse
    import configparser
    import omegaconf

    from .config import Config

    arguments = argparse.ArgumentParser()
    arguments.add_argument("-c", "--config", type=str, required=True)
    args = arguments.parse_args()

    config = Config()
    config.load_default()
    c = configparser.ConfigParser()
    c.read(args.config)
    for k, v in c.items("eggroll"):
        try:
            if v == "":
                omegaconf.OmegaConf.select(config.config, k)
            else:
                config.config = omegaconf.OmegaConf.merge(
                    config.config, omegaconf.OmegaConf.from_dotlist([f"{k}={v}"])
                )
        except omegaconf.errors.ConfigKeyError:
            print(f"Error: {k} is not set, please add it to eggroll/config/defaults")


if __name__ == "__main__":
    main()
