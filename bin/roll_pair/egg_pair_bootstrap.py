import os
from subprocess import Popen, PIPE
import time
from socket import *
import win32pipe, win32file

def send_stop(pid):
    pipe_name = r'\\.\pipe\pid_pipe' + str(pid)
    print("egg_pair send data ok !", pipe_name)
    file_handle = win32file.CreateFile(pipe_name,
                                       win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                                       win32file.FILE_SHARE_WRITE, None,
                                       win32file.OPEN_EXISTING, 0, None)
    try:
        msg = 'stop ' + str(pid)
        msg_bytes = bytes(msg, encoding='utf-8')
        print('send msg:', msg_bytes)
        win32file.WriteFile(file_handle, msg_bytes)
        #time.sleep(1)
    finally:
        try:
            win32file.CloseHandle(file_handle)
        except:
            pass


def get_property(config_file, property_name):
    with open(config_file) as i_file:
        values = {}

        for line in i_file:
            if line == "\n" or line.find("=") == -1:  # Skip blank lines and lines with no equals sign
                continue

            values = line.strip("\n").strip(" ").split("=")  # Split lines into two parts based on the "=" sign

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
    node_manager_port = get_property(config_file, "eggroll.resourcemanager.nodemanager.port")
    cluster_manager_host = get_property(config_file, "eggroll.resourcemanager.clustermanager.host")
    cluster_manager_port = get_property(config_file, "eggroll.resourcemanager.clustermanager.port")

    eggroll_logs_dir = os.environ.get('EGGROLL_LOGS_DIR')
    if eggroll_logs_dir is None:
        eggroll_logs_dir = get_property(config_file, "eggroll.logs.dir")

        if eggroll_logs_dir is None:
            eggroll_home = os.environ.get('EGGROLL_HOME')
            if eggroll_home is None:
                raise Exception("EGGROLL_HOME not set")

            eggroll_logs_dir = os.path.join(eggroll_home, 'logs')

    if os.environ.get('EGGROLL_LOG_LEVEL') is None:
        os.environ['EGGROLL_LOG_LEVEL'] = "INFO"

    os.environ['PYTHONPATH'] = pythonpath
    os.environ['EGGROLL_LOG_FILE'] = "egg_pair-" + processor_id

    python_cmd = venv + 'python.exe '
    #print(python_cmd)
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

    os.environ["EGGROLL_LOGS_DIR"] = os.path.join(eggroll_logs_dir, session_id)

    if os.path.exists(os.path.join(eggroll_logs_dir, session_id)) is not True:
        os.makedirs(os.path.join(eggroll_logs_dir, session_id))

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


def stop(pid):
    send_stop(pid)
    time.sleep(3)
    cmd = 'taskkill /pid ' + pid + ' /F'
    print(cmd)
    os.system(cmd)
