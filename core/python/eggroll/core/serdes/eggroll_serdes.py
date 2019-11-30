from eggroll.core.serdes import cloudpickle
from eggroll.core.constants import SerdesTypes
from abc import ABCMeta
from abc import abstractmethod
from pickle import loads as p_loads
from pickle import dumps as p_dumps

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
    return p_loads(_bytes)


class PickleSerdes(ABCSerdes):

  @staticmethod
  def serialize(_obj):
    return p_dumps(_obj)

  @staticmethod
  def deserialize(_bytes):
    bytes_security_check(_bytes)
    return p_loads(_bytes)

class EmptySerdes(ABCSerdes):
  @staticmethod
  def serialize(_obj):
    return _obj

  @staticmethod
  def deserialize(_bytes):
    bytes_security_check(_bytes)
    return _bytes


deserialize_blacklist = [b'eval', b'execfile', b'compile', b'system', b'popen', b'popen2', b'popen3',
                         b'popen4', b'fdopen', b'tmpfile', b'fchmod', b'fchown', b'openpty',
                         b'chdir', b'fchdir', b'chroot', b'chmod', b'chown', b'lchown', b'listdir', b'lstat',
                         b'mkfifo', b'mknod', b'access', b'mkdir', b'makedirs', b'readlink', b'remove', b'removedirs',
                         b'rename', b'renames', b'rmdir', b'tempnam', b'tmpnam', b'unlink', b'execl',
                         b'execle', b'execlp', b'execv', b'execve', b'dup2', b'execvp', b'execvpe',
                         b'forkpty', b'spawnl', b'spawnle', b'spawnlp', b'spawnlpe', b'spawnv', b'spawnve',
                         b'spawnvp', b'spawnvpe', b'load', b'loads', b'call', b'check_call',
                         b'check_output', b'Popen', b'getstatusoutput', b'getoutput', b'getstatus',
                         b'getline', b'copyfileobj', b'copyfile', b'copy', b'copy2', b'make_archive',
                         b'listdir', b'opendir', b'timeit', b'repeat',
                         b'call_tracing', b'interact', b'compile_command', b'spawn',
                         b'fileopen']

future_blacklist = [b'read', b'dup', b'fork', b'walk', b'file', b'move', b'link', b'kill', b'open', b'pipe']

serdes_cache = {}
for cls in ABCSerdes.__subclasses__():
  cls_name = ".".join([cls.__module__, cls.__qualname__])
  serdes_cache[cls_name] = cls
serdes_cache[SerdesTypes.CLOUD_PICKLE] = CloudPickleSerdes
serdes_cache[SerdesTypes.PICKLE] = PickleSerdes
serdes_cache[SerdesTypes.PROTOBUF] = None
serdes_cache[SerdesTypes.EMPTY] = EmptySerdes

def is_in_blacklist(_bytes):
  for item in deserialize_blacklist:
    if _bytes.find(item) != -1:
      print('blacklist found: {}'.format(item))
      return item
  return None


def bytes_security_check(_bytes, need_check=False):
  if not need_check:
    return
  blacklisted = is_in_blacklist(_bytes)
  if blacklisted:
    raise RuntimeError('Insecure operation found {}'.format(blacklisted))

def get_serdes(serdes_name=None):
  try:
    return serdes_cache[serdes_name]
  except:
    return PickleSerdes