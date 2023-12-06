import sys


def main(script_path):
    import runpy
    import pprint
    import os
    import sys
    from ._init import init_deepspeed
    from eggroll.config import Config

    print("===========current envs==============")
    pprint.pprint(dict(os.environ))
    print("===========current argv==============")
    pprint.pprint(sys.argv)

    config = Config().load_default()

    # TODO: remove this
    if "EGGROLL_HOME" not in os.environ:
        raise RuntimeError("EGGROLL_HOME is not set")
    eggroll_home = os.environ["EGGROLL_HOME"]
    config.load_properties(f"{eggroll_home}/conf/eggroll.properties")

    try:
        init_deepspeed(config)
    except Exception as e:
        import traceback

        print("===========init deepspeed failed=============")
        traceback.print_exc(file=sys.stdout)
        raise e
    runpy.run_path(script_path, run_name="__main__")


if __name__ == "__main__":
    main(sys.argv[1])
