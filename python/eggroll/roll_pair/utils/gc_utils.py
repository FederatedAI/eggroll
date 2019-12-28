from eggroll.core.constants import StoreTypes
from eggroll.core.meta_model import ErStore
from eggroll.utils import log_utils

log_utils.setDirectory()
LOGGER = log_utils.getLogger()

class Recorder(object):

    def __init__(self, er_store: ErStore, rpc):
        self.record_store = er_store
        self.record_rpc = rpc
        self.table_recorder = self.record_rpc.get_session().get_table_recorder()

    def record(self):
        store_type = self.record_store._store_locator._store_type
        name = self.record_store._store_locator._name
        namespace = self.record_store._store_locator._namespace
        if store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return
        else:
            LOGGER.info("record in memory table namespace:{}, name:{}, type:{}"
                        .format(namespace, name, store_type))
            count = self.table_recorder.get(name)
            if count is None:
                count = 0
            self.table_recorder.put(name, (count+1))
            LOGGER.debug("table recorded:{}".format(list(self.table_recorder.get_all())))

    def check_table_deletable(self):
        store_type = self.record_store._store_locator._store_type
        if store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return False
        record_count = self.table_recorder.get(self.record_store._store_locator._name)
        if record_count > 1:
            LOGGER.debug("table:{} ref count is {}".format(self._name, record_count))
            self.table_recorder.put(self.record_store._store_locator._name, (record_count-1))
            return False
        elif 1 >= record_count >= 0:
            return True

    def delete_record(self):
        name = self.record_store._store_locator._name
        self.table_recorder.delete(name)
