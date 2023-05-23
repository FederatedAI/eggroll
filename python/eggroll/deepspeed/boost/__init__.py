import os


def init_deepspeed():
    import torch.distributed as distributed

    from ..store.client import EggrollStore
    if "EGGROLL_DEEPSPEED_STORE_HOST" not in os.environ:
        raise RuntimeError("EGGROLL_DEEPSPEED_STORE_HOST is not set")
    store_host = os.environ["EGGROLL_DEEPSPEED_STORE_HOST"]
    if "EGGROLL_DEEPSPEED_STORE_PORT" not in os.environ:
        raise RuntimeError("EGGROLL_DEEPSPEED_STORE_PORT is not set")
    store_port = int(os.environ["EGGROLL_DEEPSPEED_STORE_PORT"])
    if "EGGROLL_DEEPSPEED_STORE_PREFIX" not in os.environ:
        raise RuntimeError("EGGROLL_DEEPSPEED_STORE_PREFIX is not set")
    prefix = os.environ.get("EGGROLL_DEEPSPEED_STORE_PREFIX")
    store = EggrollStore(host=store_host, port=store_port, prefix=prefix)

    if "EGGROLL_DEEPSPEED_BACKEND" not in os.environ:
        raise RuntimeError("EGGROLL_DEEPSPEED_BACKEND is not set")
    backend = os.environ["EGGROLL_DEEPSPEED_BACKEND"]

    if "WORLD_SIZE" not in os.environ:
        raise RuntimeError("WORLD_SIZE is not set")
    world_size = int(os.environ["WORLD_SIZE"])

    if "RANK" not in os.environ:
        raise RuntimeError("RANK is not set")
    rank = int(os.environ["RANK"])

    distributed.init_process_group(backend=backend, store=store, world_size=world_size, rank=rank)
