from eggroll.core.constants import StoreTypes
from eggroll.core.meta_model import ErStore
from eggroll.utils.log_utils import get_logger

L = get_logger()


class GcRecorder(object):

    def __init__(self, rpc):
        L.debug('init recorder')
        self.record_store = None
        self.record_rpc = rpc
        self.gc_recorder = None
        self.gc_store_name = '__gc__' + self.record_rpc.get_session().get_session_id()

    def __set_gc_recorder(self):
        options = dict()
        options['store_type'] = StoreTypes.ROLLPAIR_LMDB
        _table_recorder = self.record_rpc.load(name=self.gc_store_name,
                                               namespace=self.record_rpc.get_session().get_session_id(),
                                               options=options)
        return _table_recorder

    def record(self, er_store: ErStore):
        if er_store._store_locator._name == self.gc_store_name:
            return
        if self.gc_recorder is None:
            self.gc_recorder = self.__set_gc_recorder()
        store_type = er_store._store_locator._store_type
        name = er_store._store_locator._name
        namespace = er_store._store_locator._namespace
        if store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return
        else:
            L.info("record in memory table namespace:{}, name:{}, type:{}"
                   .format(namespace, name, store_type))
            count = self.gc_recorder.get(name)
            if count is None:
                count = 0
            self.gc_recorder.put(name, (count+1))
            L.debug("table recorded:{}".format(list(self.gc_recorder.get_all())))

    def check_gc_executable(self, er_store: ErStore):
        store_type = er_store._store_locator._store_type
        if store_type != StoreTypes.ROLLPAIR_IN_MEMORY:
            return False
        record_count = self.gc_recorder.get(er_store._store_locator._name)
        if record_count is None:
            record_count = 0
        if record_count > 1:
            L.debug("table:{} ref count is {}".format(er_store._store_locator._name, record_count))
            self.gc_recorder.put(er_store._store_locator._name, (record_count-1))
            return False
        elif 1 >= record_count >= 0:
            return True

    def delete_record(self, er_store: ErStore):
        name = er_store._store_locator._name
        self.gc_recorder.delete(name)
