import os
from subprocess import Popen, PIPE
import time


def get_property(config_file, property_name):
  with open(config_file) as i_file: #Open the file
    values = {}

    for line in i_file:
      if line == "\n" or line.find("=") == -1: #Skip blank lines and lines with no equals sign
        continue

      values = line.strip("\n").strip(" ").split("=") #Split lines into two parts based on the "=" sign

      if values[0] == property_name:
        return values[1]

      continue

    return None


def start(config_file, session_id, server_node_id, processor_id, port, transfer_port, pname):

  print('sub id ：',os.getpid(),'parent id ：',os.getppid())

  pid = os.getpid()
  pname_pid = 'pid/' + pname + '.pid'
  #os.mkdir('pid')
  with open(pname_pid, 'w') as fp:
    fp.write(str(pid))

  if session_id is None:
    print("session-id is blank")
    return 1

  if processor_id is None:
    print("processor-id is blank")
    return 2

  if transfer_port is 0 and port is not 0:
    transfer_port = port

  #venv = get_property(config_file, "eggroll.resourcemanager.bootstrap.egg_pair.venv")
  #pythonpath = get_property(config_file, "eggroll.resourcemanager.bootstrap.egg_pair.pythonpath")
  filepath = get_property(config_file, "eggroll.resourcemanager.bootstrap.egg_pair.filepath")
  #logs_dir = get_property(config_file, "eggroll.logs.dir")
  node_manager_port = get_property(config_file, "eggroll.resourcemanager.nodemanager.port")
  cluster_manager_host = get_property(config_file, "eggroll.resourcemanager.clustermanager.host")
  cluster_manager_port = get_property(config_file, "eggroll.resourcemanager.clustermanager.port")

  javahome = get_property(config_file, "eggroll.resourcemanager.bootstrap.roll_pair_master.javahome")
  classpath = get_property(config_file, "eggroll.resourcemanager.bootstrap.roll_pair_master.classpath")
  mainclass = get_property(config_file, "eggroll.resourcemanager.bootstrap.roll_pair_master.mainclass")
  jvm_options = get_property(config_file, "eggroll.resourcemanager.bootstrap.roll_pair_master.jvm.options")

  eggroll_home = os.environ.get('EGGROLL_HOME')
  if eggroll_home is None:
    raise Exception("EGGROLL_HOME not set")

  eggroll_logs_dir = os.environ.get('EGGROLL_LOGS_DIR')
  if eggroll_logs_dir is None:
    eggroll_logs_dir = get_property(config_file, "eggroll.logs.dir")

    if eggroll_logs_dir is None:
      eggroll_logs_dir = os.path.join(eggroll_home, 'logs')

  if os.environ.get('EGGROLL_LOG_LEVEL') is None:
    os.environ['EGGROLL_LOG_LEVEL'] = "INFO"

  eggroll_log_conf = eggroll_home + '/conf' + '/log4j2.properties'
  if os.environ.get('EGGROLL_LOG_CONF') is None:
    os.environ['EGGROLL_LOG_CONF'] = eggroll_log_conf


  os.environ['EGGROLL_LOG_FILE'] ="egg_pair-" + processor_id


  if javahome is None:
    p = Popen(['which java'], stdout=PIPE, stderr=PIPE, stdin=PIPE)
    java_cmd = p.stdout.read()
  else:
    java_cmd = javahome + '/bin/java'

  if mainclass is None:
    mainclass = "com.webank.eggroll.rollpair.RollPairMasterBootstrap"


  cmd = 'java ' + jvm_options + ' -Dlog4j.configurationFile=' + eggroll_log_conf + ' -cp ' + \
        classpath +\
        " com.webank.eggroll.core.Bootstrap " + \
        ' --bootstraps ' + mainclass +\
        ' --config ' + config_file + \
        ' --session-id ' + session_id + \
        ' --server-node-id ' + server_node_id +\
        ' --cluster-manager ' + cluster_manager_host + ':' + cluster_manager_port + \
        ' --node-manager ' + node_manager_port +\
        ' --processor-id ' + processor_id

  #cmd = 'python ' + ' bar.py'

  if port is not None:
    cmd = cmd + ' --port ' + port
    if transfer_port is not None:
      cmd = cmd + ' --transfer-port ' + transfer_port

  os.environ["EGGROLL_LOGS_DIR"] = os.path.join(eggroll_logs_dir, session_id)

  if os.path.exists(os.path.join(eggroll_logs_dir, session_id)) is not True:
    os.makedirs(os.path.join(eggroll_logs_dir, session_id))

  eggroll_log_file = "roll_pair_master-" + processor_id + '.out'
  eggroll_err_file = "roll_pair_master-" + processor_id + '.err'

  log_file = os.path.join(eggroll_logs_dir, eggroll_log_file)
  err_file = os.path.join(eggroll_logs_dir, eggroll_err_file)
  cmd = cmd + ' >> ' + log_file + ' 2> ' + err_file + ' &'

  '''
  argument = ' --config ' + config_file + \
             ' --session-id ' + session_id + \
             ' --server-node-id ' + server_node_id + \
             ' --cluster-manager ' + cluster_manager_host + ':' + cluster_manager_port + \
             ' --node-manager ' + node_manager_port + ' --processor-id ' + processor_id
  '''
  #argument = '--config ../conf/eggroll.properties'
  '''
  argument = '...'
  proc = Popen(['python', 'roll_pair/bar.py --config ../conf/eggroll.properties --session-id testing'], stdout=PIPE, stderr=PIPE, stdin=PIPE)
  pid = proc.pid
  output = proc.stdout.read()
  print(output)
  print("pid:", pid)
  '''
  #os.system("python roll_pair/bar.py --config ../conf/eggroll.properties --session-id testing --node-manager 4670 >> logs/egg_pair-100.out 2> logs/egg_pair-100.err") #--server-node-id 1001 --cluster-manager 127.0.0.1:4670  --processor-id 100  &")
  os.system(cmd)
  #egg_pair(config, session_id, server_node_id, cluster_manager_host,
  #         cluster_manager_port, node_manager_port, processor_id)

  #print("egg_pair processor id:", processor_id, "os process id:" >> ${EGGROLL_LOGS_DIR}/pid.txt