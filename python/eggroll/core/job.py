import os
import time

from eggroll.core.client import ClusterManagerClient
from eggroll.core.conf_keys import SessionConfKeys
from eggroll.core.constants import SessionStatus
from eggroll.core.meta_model import ErJobMeta


class ErJob:
    def __init__(self,
                 session_id,
                 name='',
                 world_size=1,
                 command_arguments: dict = None,
                 environment_variables: dict = None,
                 options: dict = None):
        if options is None:
            options = {}
        self._session_id = session_id
        self._name = name
        if not self._name:
            self._name = f"session_{self._session_id}"

        # self.__eggroll_home = os.getenv('EGGROLL_HOME', None)
        # if not self.__eggroll_home:
        #     raise EnvironmentError('EGGROLL_HOME is not set')
        #
        # if "EGGROLL_DEBUG" not in os.environ:
        #     os.environ['EGGROLL_DEBUG'] = "0"
        #
        # conf_path = options.get(CoreConfKeys.STATIC_CONF_PATH, f"{self.__eggroll_home}/conf/eggroll.properties")
        #
        # L.info(f"static conf path: {conf_path}")
        # configs = configparser.ConfigParser()
        # configs.read(conf_path)
        # set_static_er_conf(configs['eggroll'])

        self._options = options.copy()
        self._options[SessionConfKeys.CONFKEY_SESSION_ID] = self._session_id
        self._command_arguments = command_arguments
        self._environment_variables = environment_variables

        self._files = {}
        self._zipped_files = {}
        self._world_size = world_size

    def add_file(self, conf_path, name=None, zipped=False):
        file_path = os.path.abspath(conf_path)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{file_path} not found")
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"file {file_path} is not a file")
        if name is None:
            name = os.path.basename(file_path)
        if zipped:
            if name in self._zip_files:
                raise ValueError(f"zipped file name {name} already exists")
            self._zipped_files[name] = file_path
        else:
            if name in self._files:
                raise ValueError(f"file name {name} already exists")
            self._files[name] = file_path
        return self

    def submit(self, timeout):
        cluster_manager_client = ClusterManagerClient(options=self._options)
        files = {name: open(path, 'rb').read() for name, path in self._files.items()}
        zipped_files = {name: open(path, 'rb').read() for name, path in self._zipped_files.items()}

        submit_job_meta = ErJobMeta(id=self._session_id,
                                    name=self._name,
                                    job_type="deepspeed",
                                    world_size=self._world_size,
                                    command_arguments=self._command_arguments,
                                    environment_variables=self._environment_variables,
                                    files=files,
                                    zipped_files=zipped_files,
                                    options=self._options,
                                    status=SessionStatus.NEW)

        endtime = time.monotonic() + timeout

        while True:
            try:
                session_meta = cluster_manager_client.submit_job(submit_job_meta)
                break
            except Exception as e:
                print(e)
                if time.monotonic() < endtime:
                    time.sleep(0.1)
                else:
                    raise
        print(session_meta)
