#
#  Copyright 2019 The Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from eggroll.core.serdes import cloudpickle
from abc import ABCMeta
from abc import abstractmethod
from pickle import loads as p_loads
from pickle import dumps as p_dumps

from eggroll.utils import log_utils

LOGGER = log_utils.getLogger()


class ABCSerdes:
    __metaclass__ = ABCMeta

    @staticmethod
    @abstractmethod
    def serialize(_obj):
        pass

    @staticmethod
    @abstractmethod
    def deserialize(_bytes):
        pass


class CloudPickleSerdes(ABCSerdes):

    @staticmethod
    def serialize(_obj):
        return cloudpickle.dumps(_obj)

    @staticmethod
    def deserialize(_bytes):
        bytes_security_check(_bytes)
        return cloudpickle.loads(_bytes)


class PickleSerdes(ABCSerdes):

    @staticmethod
    def serialize(_obj):
        return p_dumps(_obj)

    @staticmethod
    def deserialize(_bytes):
        bytes_security_check(_bytes)
        return p_loads(_bytes)


deserialize_blacklist = [b'eval', b'execfile', b'compile', b'open', b'file', b'system', b'popen', b'popen2', b'popen3',
                          b'popen4', b'fdopen', b'tmpfile', b'fchmod', b'fchown', b'open', b'openpty', b'read', b'pipe',
                          b'chdir', b'fchdir', b'chroot', b'chmod', b'chown', b'link', b'lchown', b'listdir', b'lstat',
                          b'mkfifo', b'mknod', b'access', b'mkdir', b'makedirs', b'readlink', b'remove', b'removedirs',
                          b'rename', b'renames', b'rmdir', b'tempnam', b'tmpnam', b'unlink', b'walk', b'execl',
                          b'execle', b'execlp', b'execv', b'execve', b'dup', b'dup2', b'execvp', b'execvpe', b'fork',
                          b'forkpty', b'kill', b'spawnl', b'spawnle', b'spawnlp', b'spawnlpe', b'spawnv', b'spawnve',
                          b'spawnvp', b'spawnvpe', b'load', b'loads', b'load', b'loads', b'call', b'check_call',
                          b'check_output', b'Popen', b'getstatusoutput', b'getoutput', b'getstatus',
                          b'getline', b'copyfileobj', b'copyfile', b'copy', b'copy2', b'move', b'make_archive',
                          b'listdir', b'opendir', b'open', b'popen2', b'popen3', b'popen4', b'timeit', b'repeat',
                          b'call_tracing', b'interact', b'compile_command', b'compile_command', b'spawn', b'open',
                          b'fileopen', b'popen']

serdes_cache = {}
for cls in ABCSerdes.__subclasses__():
    cls_name = ".".join([cls.__module__, cls.__qualname__])
    serdes_cache[cls_name] = cls


def is_in_blacklist(_bytes):
    for item in deserialize_blacklist:
        if _bytes.find(item) != -1:
            LOGGER.info('blacklist found: {}'.format(item))
            return item
    return None


def bytes_security_check(_bytes):
    blacklisted = is_in_blacklist(_bytes)
    if blacklisted:
        raise RuntimeError('Insecure operation found {}'.format(blacklisted))


def get_serdes(serdes_id=None):
    try:
        return serdes_cache[serdes_id]
    except:
        return PickleSerdes
