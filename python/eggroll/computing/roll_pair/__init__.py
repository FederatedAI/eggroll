from ._roll_pair_context import ErSession, RollPairContext
from ._roll_pair import RollPair


def runtime_init(session: ErSession, allow_rp_serialize=False):
    rpc = RollPairContext(session=session, allow_rp_serialize=allow_rp_serialize)
    return rpc


__all__ = ["runtime_init", "RollPairContext", "RollPair"]
