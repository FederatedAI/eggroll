import os
import argparse
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


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('-s', '--session-id')
    args_parser.add_argument('-p', '--port', default='0')
    args_parser.add_argument('-c', '--config')

    args = args_parser.parse_args()

    eggroll_home = os.environ.get('EGGROLL_HOME')
    if eggroll_home is None:
        raise Exception("EGGROLL_HOME not set")

    if args.config:
        conf_file = args.config
    else:
        conf_file = f'{eggroll_home}/conf/eggroll.properties'
        print(f'reading default config: {conf_file}')

    session_id = args.session_id
    cluster_manager_port = args.port

    eggroll_logs_dir = os.environ.get('EGGROLL_LOGS_DIR')
    if eggroll_logs_dir is None:
        eggroll_logs_dir = get_property(conf_file, "eggroll.logs.dir")
        if eggroll_logs_dir is None:
            eggroll_logs_dir = os.path.join(eggroll_home, 'logs')

    os.environ["EGGROLL_LOGS_DIR"] = os.path.join(eggroll_logs_dir, session_id)

    eggroll_log_conf = eggroll_home + '/conf' + '/log4j2.properties'

    if os.path.exists(os.path.join(eggroll_logs_dir, 'eggroll')) is not True:
        os.makedirs(os.path.join(eggroll_logs_dir, 'eggroll'))

    javahome = get_property(conf_file, "eggroll.resourcemanager.bootstrap.roll_pair_master.javahome")
    classpath = os.path.join(eggroll_home, 'jvm/core/target/lib/*') + ";" + os.path.join(eggroll_home, 'lib/*') + ";" + os.path.join(eggroll_home, 'jvm/roll_pair/target/lib/*')

    if platform.system() == "Windows":
        if javahome is None:
            java_cmd = 'java.exe'
        else:
            java_cmd = '\"' + javahome + '\\bin\\java.exe ' + '\"'
    else:
        if javahome is None:
            p = Popen(['which java'], stdout=PIPE, stderr=PIPE, stdin=PIPE)
            java_cmd = p.stdout.read()
        else:
            java_cmd = javahome + '/bin/java'

    print("EGGROLL_HOME:", eggroll_home)
    os.chdir(eggroll_home)

    standalone_tag = os.environ.get("EGGROLL_STANDALONE_TAG", None)
    if standalone_tag == None:
        java_define = ' -Dlog4j.configurationFile=' + eggroll_log_conf
    else:
        java_define = ' -Dlog4j.configurationFile=' + eggroll_log_conf + ' -Deggroll.standalone.tag=' + standalone_tag

    cmd = java_cmd + java_define + ' -cp ' + classpath +\
          ' com.webank.eggroll.core.Bootstrap ' +\
          ' --ignore-rebind ' +\
          ' --bootstraps com.webank.eggroll.core.resourcemanager.ClusterManagerBootstrap,com.webank.eggroll.core.resourcemanager.NodeManagerBootstrap ' +\
          ' -c ' + conf_file +\
          ' -s ' + session_id +\
          ' -p ' + cluster_manager_port

    eggroll_log_file = 'eggroll/bootstrap-standalone-manager.out'
    eggroll_err_file = 'eggroll/bootstrap-standalone-manager.err'

    log_file = os.path.join(eggroll_logs_dir, eggroll_log_file)
    err_file = os.path.join(eggroll_logs_dir, eggroll_err_file)
    print(cmd)

    log_file_fp = open(log_file, 'ab')
    err_file_fp = open(err_file, 'ab')
    proc = Popen(cmd, shell=False, stdout=log_file_fp, stderr=err_file_fp)
