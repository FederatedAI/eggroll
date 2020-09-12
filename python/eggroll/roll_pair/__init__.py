from eggroll.core.constants import SerdesTypes
from eggroll.core.meta_model import ErPartition
from eggroll.core.pair_store import create_pair_adapter
import cloudpickle
from eggroll.core.serdes.eggroll_serdes import PickleSerdes, \
    CloudPickleSerdes, EmptySerdes, eggroll_pickle_loads
from eggroll.roll_pair.utils.pair_utils import get_db_path


def create_adapter(er_partition: ErPartition, options: dict = None):
    if options is None:
        options = {}
    options['store_type'] = er_partition._store_locator._store_type
    options['path'] = get_db_path(er_partition)
    options['er_partition'] = er_partition
    return create_pair_adapter(options=options)


def create_serdes(serdes_type: SerdesTypes = SerdesTypes.CLOUD_PICKLE):
    if serdes_type == SerdesTypes.CLOUD_PICKLE or serdes_type == SerdesTypes.PROTOBUF:
        return CloudPickleSerdes
    elif not serdes_type or serdes_type == SerdesTypes.PICKLE:
        return PickleSerdes
    else:
        return EmptySerdes


def create_functor(func_bin):
    try:
        return cloudpickle.loads(func_bin)
    except:
        return eggroll_pickle_loads(func_bin)
