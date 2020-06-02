import os
import platform
from subprocess import Popen, PIPE

def get_property(config_file, property_name):
    with open(config_file) as i_file:
        for line in i_file:
            if line == "\n" or line.find("=") == -1:
                continue

            values = line.strip("\n").strip(" ").split("=")
            if values[0] == property_name:
                return values[1]

            continue

        return None


def start(config_file, session_id, server_node_id, processor_id, port, transfer_port, pname):
    print('sub id ：', os.getpid(), 'parent id ：', os.getppid())

    if session_id is None:
        print("session-id is blank")
        return 1

    if processor_id is None:
        print("processor-id is blank")
        return 2

    if transfer_port is 0 and port is not 0:
        transfer_port = port

    venv = get_property(config_file, "eggroll.resourcemanager.bootstrap.egg_pair.venv")
    pythonpath = get_property(config_file, "eggroll.resourcemanager.bootstrap.egg_pair.pythonpath")
    filepath = get_property(config_file, "eggroll.resourcemanager.bootstrap.egg_pair.filepath")

    cluster_manager_host = get_property(config_file, "eggroll.resourcemanager.clustermanager.host")

    standalone_port = os.environ.get("EGGROLL_STANDALONE_PORT", None)
    if standalone_port is None:
        node_manager_port = get_property(config_file, "eggroll.resourcemanager.nodemanager.port")
        cluster_manager_port = get_property(config_file, "eggroll.resourcemanager.clustermanager.port")
    else:
        node_manager_port = standalone_port
        cluster_manager_port = standalone_port

    eggroll_logs_dir = os.environ.get('EGGROLL_LOGS_DIR')
    if eggroll_logs_dir is None:
        eggroll_logs_dir = get_property(config_file, "eggroll.logs.dir")

        if eggroll_logs_dir is None:
            eggroll_home = os.environ.get('EGGROLL_HOME')
            if eggroll_home is None:
                raise Exception("EGGROLL_HOME not set")

            eggroll_logs_dir = os.path.join(eggroll_home, 'logs')

        eggroll_logs_dir = os.path.join(eggroll_logs_dir, session_id)
        os.environ["EGGROLL_LOGS_DIR"] = eggroll_logs_dir

    os.makedirs(eggroll_logs_dir, exist_ok=True)

    if os.environ.get('EGGROLL_LOG_LEVEL') is None:
        os.environ['EGGROLL_LOG_LEVEL'] = "INFO"

    os.environ['PYTHONPATH'] = pythonpath
    os.environ['EGGROLL_LOG_FILE'] = "egg_pair-" + processor_id

    if platform.system() == "Windows":
        if venv is None:
            python_cmd = 'python.exe '
        else:
            python_cmd = venv + '\\python.exe '
    else:
        if venv is None:
            p = Popen(['which python'], stdout=PIPE, stderr=PIPE, stdin=PIPE)
            python_cmd = p.stdout.read()
        else:
            source_cmd = 'source ' + venv + '/bin/activate'
            os.system(source_cmd)
            python_cmd = venv + '/bin/python'


    cmd = python_cmd + filepath + \
          ' --config ' + config_file + \
          ' --session-id ' + session_id + \
          ' --server-node-id ' + server_node_id + \
          ' --cluster-manager ' + cluster_manager_host + ':' + cluster_manager_port + \
          ' --node-manager ' + node_manager_port + ' --processor-id ' + processor_id

    if port is not None:
        cmd = cmd + ' --port ' + port
        if transfer_port is not None:
            cmd = cmd + ' --transfer-port ' + transfer_port

    eggroll_log_file = "egg_pair-" + processor_id + '.out'
    eggroll_err_file = "egg_pair-" + processor_id + '.err'

    log_file = os.path.join(eggroll_logs_dir, eggroll_log_file)
    err_file = os.path.join(eggroll_logs_dir, eggroll_err_file)

    log_file_fp = open(log_file, 'ab')
    err_file_fp = open(err_file, 'ab')
    proc = Popen(cmd, shell=False, stdout=log_file_fp, stderr=err_file_fp)
    pid = proc.pid

    print("pid:", pid)
    pname_pid = 'bin/' + 'pid/' + pname + '.pid'
    with open(pname_pid, 'w') as fp:
        fp.write(str(pid))
        fp.close()



