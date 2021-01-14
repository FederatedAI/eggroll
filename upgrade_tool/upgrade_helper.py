import sys, os, getopt
import subprocess
import time


def read_ip_list(upgrade_ip):
    f = ''
    ip_set = set()
    try:
        f = open(upgrade_ip, 'r', -1, 'utf-8')
        ip_list = f.readlines()
        for ip in ip_list:
            ip_set.add(str(ip).replace("\n", ""))
    except Exception as e:
        print(f"read ip addr error value={e}")
        return ip_set
    finally:
        f.close()
    print(f"read ip info ={ip_set}")
    return ip_set


def backup_eggroll_data(egg_home: str):
        ts = str(time.time())
        print(f"start backup eggroll data ...")
        subprocess.check_call(['mv', egg_home + '/bin/', egg_home + '/bin_' + ts + '_bak/'])
        subprocess.check_call(['mv', egg_home + '/lib/', egg_home + '/lib_' + ts + '_bak/'])
        subprocess.check_call(['mv', egg_home + '/deploy/', egg_home + '/deploy_' + ts + '_bak/'])
        subprocess.check_call(['mv', egg_home + '/python/', egg_home + '/python_' + ts + '_bak/'])
        subprocess.check_call(
            ['mv', egg_home + '/conf/eggroll.properties', egg_home + '/conf/eggroll.properties.bak'+ts])
        print(f"end backup eggroll data.")
        return 0



def check_egg_home(egg_home: str):
    h = egg_home+'/python/eggroll/__init__.py'
    print(f'input eggroll home =={h}')
    p1 = subprocess.Popen(['grep', '-w', '__version__', h],
                          stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["awk", '-F', '" "', '"{print $3}"'], stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()
    s = bytes.decode(p2.communicate()[0])
    if len(s) > 0:
        print(f'check right current version={s}')
        return True
    else:
        print(f'please checking eggroll home path end ')
        return False


def manual_replace(s, char, index):
    return s[:index] + char + s[index + 1:]


def list_to_string(s):
    str1 = ","
    for ele in s:
        str1 += ele
    return manual_replace(str1, '', 0)


def split_statements(f_contents):
    stmnt_list = []
    for sql in f_contents.split("\n\n"):
        if sql == '':
            continue
        elif sql == '\n':
            continue
        else:
            stmnt_list.append(sql)
    return list_to_string(stmnt_list)


def get_db_upgrade_content(upgrade_sql_file: str):
    return open(upgrade_sql_file).read()


def backup_upgrade_db(mysql_home_path: str, host: str, port: int, database: str, username: str, passwd: str,
                      mysql_sock,db_upgrade_sql: str):
    print(f"start backup db data ...")
    ts = str(time.time())
    bak_cmd = mysql_home_path + "/bin/mysqldump" + " -h " + host + " -u " + username + " -p" + passwd + " -P" + str(
        port) + " -S " + mysql_sock + " " + database + " -r " + "./dump_backup_eggroll_upgrade_" + host + "_" + ts + ".sql"
    print(f'bak_cmd={bak_cmd}')
    os.popen("%s %s %s " % ('ssh', host, bak_cmd))
    print(f"end backup db data.")
    print(f'start upgrade db ...')
    sub_cmd = split_statements(get_db_upgrade_content(db_upgrade_sql))
    up_cmd = "'" + mysql_home_path + "/bin/mysql" + " -u " + username + " -p" + passwd + " -P " + str(
        port) + " -h " + host + " -S " + mysql_sock + " -Bse " + '"' + sub_cmd + '"' + "'"
    print(f'up_cmd={up_cmd}')
    os.popen("%s %s %s " % ('ssh', host, up_cmd))
    print(f'end upgrade db end.')
    return 0


def cluster_upgrade_sync(src_path: str, dst_path: str, remote_host: str):
    print(f'start upgrade eggroll 2.0.x -> 2.2.x ... src_path={src_path} dst_path={dst_path}')
    subprocess.check_call(
        ["rsync", "-aEvrzhK", "--delete", "--progress", src_path + '/bin/',
         "app@" + remote_host + ":" + dst_path + "/bin/"])
    subprocess.check_call(
        ["rsync", "-aEvrzhK", "--delete", "--progress", src_path + '/lib/',
         "app@" + remote_host + ":" + dst_path + "/lib/"])
    subprocess.check_call(
        ["rsync", "-aEvrzhK", "--delete", "--progress", src_path + '/deploy/',
         "app@" + remote_host + ":" + dst_path + "/deploy/"])
    subprocess.check_call(
        ["rsync", "-aEvrzhK", "--delete", "--progress", src_path + '/python/',
         "app@" + remote_host + ":" + dst_path + "/python/"])
    subprocess.check_call(
        ["rsync", "-aEvrzhK", "--delete", "--progress", src_path + '/conf/eggroll.properties',
         "app@" + remote_host + ":" + dst_path + "/conf/eggroll.properties"])
    print(f'upgrade eggroll 2.0.x finish')


def upgrade_main(cm_file, rs_file, egg_home, mysql_home, mysql_host, mysql_port, mysql_db, mysql_user, mysql_pwd,
                 mysql_sock,mysql_file):
    print(f"into upgrade main ...")
    cm_node = read_ip_list(cm_file)
    rs_node = read_ip_list(rs_file)
    if len(cm_node) > 0:
        if check_egg_home(egg_home) is False:
            print(f'input param eggroll home path error={egg_home}')
            return
        try:
            backup_eggroll_data(egg_home)
            backup_upgrade_db(mysql_home, mysql_host, mysql_port, mysql_db, mysql_user, mysql_pwd,mysql_sock, mysql_file)
        except Exception as e:
            print(f" upgrade error pleases checking.e={e}")
            return
        for h in cm_node:
            if h == '':
                continue
            cluster_upgrade_sync('eggroll', egg_home, h)
    elif len(rs_node) > 0:
        if check_egg_home(egg_home) is False:
            print(f'input param eggroll home path error={egg_home}')
            return
        try:
            backup_eggroll_data(egg_home)
            print(f"backup eggroll data repetion.")
        except Exception as e:
            print(f" upgrade error pleases checking.e={e}")
            return
        for h in rs_node:
            if h == '':
                continue
            cluster_upgrade_sync('eggroll', egg_home, h)
    else:
        pass


def check_upgrade_pkg_path(pkg_path: str):
    if not os.path.exists(pkg_path + '/eggroll'):
        print(f'upgrade eggroll upload dir not exists')
        return -1
    if not os.path.exists(pkg_path + '/eggroll/bin'):
        print(f'upgrade eggroll bin upload dir not exists')
        return -1
    if not os.path.exists(pkg_path + '/eggroll/lib'):
        print(f'upgrade eggroll lib upload dir not exists')
        return -1
    if not os.path.exists(pkg_path + '/eggroll/conf/eggroll.properties'):
        print(f'upgrade eggroll.properties update file not exists')
        return -1
    if not os.path.exists(pkg_path + '/eggroll/python'):
        print(f'upgrade eggroll python upload dir not exists')
        return -1
    if not os.path.exists(pkg_path + '/cm_ip_list'):
        print(f'upgrade eggroll cluster node ip file not exists')
        return -1
    if not os.path.exists(pkg_path + '/rs_ip_list'):
        print(f'upgrade eggroll rollsite node ip file not exists')
        return -1
    if not os.path.exists(pkg_path + '/mysql_file.sql'):
        print(f'upgrade eggroll upgrade sql file not exists')
        return -1


def main(argv):
    cm_file = ''
    rs_file = ''
    mysql_file = ''
    egg_home = ''
    mysql_home = ''
    mysql_host = ''
    mysql_port = 3306
    mysql_db = ''
    mysql_user = ''
    mysql_pwd = ''
    mysql_sock = ''
    try:
        opts, args = getopt.getopt(argv, "hc:r:e:m:t:p:b:u:w:s:S:",
                                   ["cm_file=", "rs_file=", "egg_home=", "mysql_home=", "mysql_host=", "mysql_port=",
                                    "mysql_db=", "mysql_user=", "mysql_pwd=", "mysql_file=","mysql_sock="])
    except getopt.GetoptError:
        print(
            'upgrade_helper.py \n'
            ' -c <input eggroll custermanager node ip sets>\n'
            ' -r <rollsite node ip sets>\n'
            ' -e <eggroll home path>\n'
            ' -m <mysql home path>\n'
            ' -t <mysql ip addr>\n'
            ' -p <mysql port>\n'
            ' -b <mysql database>\n'
            ' -u <mysql username>\n'
            ' -w <mysql passwd>\n'
            ' -S <mysql sock file\n>'
            ' -s <mysql upgrade content sql file sets>\n')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(
                'upgrade_helper.py \n'
                ' -c --cm_file <input eggroll custermanager node ip sets>\n'
                ' -r --rs_file <rollsite node ip sets>\n'
                ' -e --egg_home <eggroll home path>\n'
                ' -m --mysql_home <mysql home path>\n'
                ' -t --mysql_host <mysql ip addr>\n'
                ' -p --mysql_port <mysql port>\n'
                ' -b --mysql_db <mysql database>\n'
                ' -u --mysql_user <mysql username>\n'
                ' -w --mysql_pwd <mysql passwd>\n'
                ' -S --mysql_sock <mysql sock file\n>'
                ' -s --mysql_file <mysql upgrade content sql file sets>\n')
            sys.exit()
        elif opt in ("-c", "--cm_file"):
            cm_file = arg
        elif opt in ("-r", "--rs_file"):
            rs_file = arg
        elif opt in ("-e", "--egg_home"):
            egg_home = arg
        elif opt in ("-m", "--mysql_home"):
            mysql_home = arg
        elif opt in ("-t", "--mysql_host"):
            mysql_host = arg
        elif opt in ("-p", "--mysql_port"):
            mysql_port = arg
        elif opt in ("-b", "--mysql_db"):
            mysql_db = arg
        elif opt in ("-u", "--mysql_user"):
            mysql_user = arg
        elif opt in ("-w", "--mysql_pwd"):
            mysql_pwd = arg
        elif opt in ("-S", "--mysql_sock"):
            mysql_sock = arg
        elif opt in ("-s", "--mysql_file"):
            mysql_file = arg
    print(
        f'input params={cm_file, rs_file, egg_home, mysql_home, mysql_host, mysql_port, mysql_db, mysql_user, mysql_pwd,mysql_sock, mysql_file}')

    if len(opts) < 11:
        print(f'input param missing,please checing ...')
        return
    print(f'start ....')
    upgrade_main(cm_file, rs_file, egg_home, mysql_home, mysql_host, mysql_port,
                 mysql_db, mysql_user, mysql_pwd,mysql_sock, mysql_file)
    print(f'end ....')


if __name__ == '__main__':
    pkg_path = os.getcwd()
    code = check_upgrade_pkg_path(pkg_path)
    if code == -1:
        sys.exit()
    params = sys.argv[1:]
    main(params)
    sys.exit()
