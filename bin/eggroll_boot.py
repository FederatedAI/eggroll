import os
import sys
import platform
import time


def send_stop(pid):
    import win32file
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
    finally:
        try:
            win32file.CloseHandle(file_handle)
        except:
            pass


if __name__ == '__main__':
    sub_cmd = sys.argv[1]
    exe = sys.argv[2]
    pname = sys.argv[3]

    os_type = platform.system()

    print("exe:", exe)
    print(sys.executable)

    if sub_cmd == "start":
        params = exe.split(" ")
        module = params[0]
        config_file = params[2]
        session_id = params[4]
        server_node_id = params[6]
        processor_id = params[8]

        if "egg_pair" in module:
            import roll_pair.egg_pair_bootstrap as bootstrap
        if "roll_pair_master" in module:
            import roll_pair.roll_pair_master_bootstrap as bootstrap

        bootstrap.start(config_file, session_id, server_node_id, processor_id, port=None, transfer_port=None, pname=pname)

    elif sub_cmd == "stop":
        pid_file = os.path.join('bin', 'pid', pname+'.pid')
        with open(pid_file, 'r') as fp:
            pid = fp.readline()
            fp.close()

        if os_type == "Windows":
            send_stop(pid)
            time.sleep(3)
            cmd = 'taskkill /pid ' + pid + ' /F'
            print(cmd)
            os.system(cmd)
        else:
            cmd= 'kill ' + pid
            print(cmd)
            os.system(cmd)

    elif sub_cmd == "kill":
        pid_file = os.path.join('bin', 'pid', pname+'.pid')
        with open(pid_file, 'r') as fp:
            pid = fp.readline()
            fp.close()

        if os_type == "Windows":
            send_stop(pid)
            time.sleep(3)
            cmd = 'taskkill /pid ' + pid + ' /F'
            print(cmd)
            os.system(cmd)
        else:
            cmd= 'kill -9 ' + pid
            print(cmd)
            os.system(cmd)
