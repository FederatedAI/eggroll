import os
import argparse
import sys

def eggroll_boot(module, sub_cmd, config_file, session_id, server_node_id, processor_id, port, transfer_port, pname):
    #if module == "egg_pair":
    if "egg_pair" in module:
        import roll_pair.egg_pair_bootstrap as bootstrap
    #elif module == "roll_pair_master_bootstrap":
    if "roll_pair_master" in module:
        import roll_pair.roll_pair_master_bootstrap as bootstrap

    if sub_cmd == "start":
        bootstrap.start(config_file, session_id, server_node_id, processor_id, port, transfer_port, pname)

    elif sub_cmd == "stop":
        pid_file = os.path.join('bin', 'pid', pname+'.pid')
        with open(pid_file, 'r') as fp:
          pid = fp.readline()
          fp.close()
        bootstrap.stop(pid)

    elif sub_cmd == "kill":
        pid_file = os.path.join('bin', 'pid', pname+'.pid')
        with open(pid_file, 'r') as file_to_read:
          pid = file_to_read.readline()
        bootstrap.kill(pid)

if __name__ == '__main__':
    sub_cmd = sys.argv[1]
    exe = sys.argv[2]
    pname = sys.argv[3]

    print("exe:", exe)

    module = None
    config_file = None
    session_id = None
    server_node_id = None
    processor_id = None

    if exe != 'null':
        params = exe.split(" ")
        module = params[0]
        config_file = params[2]
        session_id = params[4]
        server_node_id = params[6]
        processor_id = params[8]
    else:
        module = pname
        print("module:", module)

    eggroll_boot(module, sub_cmd, config_file, session_id, server_node_id, processor_id, port=None, transfer_port=None, pname=pname)
