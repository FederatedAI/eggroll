from ._roll_pair_context import ErSession, RollPairContext
from ._roll_pair import RollPair


def runtime_init(session: ErSession):
    rpc = RollPairContext(session=session)
    return rpc


__all__ = ["runtime_init", "RollPairContext", "RollPair"]
