import sys
import os
import subprocess
from datetime import datetime
import argparse


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
        print(f"start backup eggroll data ...")
        subprocess.check_call(['mv', egg_home + '/bin/', egg_home + '/bin_' + datetime.now().strftime("%Y%m%d") + '_bak/'])
        subprocess.check_call(['mv', egg_home + '/lib/', egg_home + '/lib_' + datetime.now().strftime("%Y%m%d") + '_bak/'])
        subprocess.check_call(['mv', egg_home + '/deploy/', egg_home + '/deploy_' + datetime.now().strftime("%Y%m%d") + '_bak/'])
        subprocess.check_call(['mv', egg_home + '/python/', egg_home + '/python_' + datetime.now().strftime("%Y%m%d") + '_bak/'])
        print(f"end backup eggroll data.")


def check_egg_home(egg_home: str):
    h = egg_home+'/python/eggroll/__init__.py'
    print(f'input eggroll home =={h}')
    p1 = subprocess.Popen(['grep', '-w', '__version__', h],
                          stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["awk", '-F', '" "', '"{print $3}"'], stdin=p1.stdout, stdout=subprocess.PIPE)
    p1.stdout.close()
    s = bytes.decode(p2.communicate()[0])
    if len(s) > 0:
        if len(s.split('=')) == 3:
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
    dump_sql = "~/dump_backup_eggroll_upgrade_"+host+'_' + datetime.now().strftime("%Y%m%d") + ".sql"
    bak_cmd = mysql_home_path + "/bin/mysqldump" + " -h " + host + " -u " + username + " -p" + passwd + " -P" + str(
        port) + " -S " + mysql_sock + " " + database + " -r " + dump_sql
    os.system("%s" %(bak_cmd))
    print(f'bak_cmd={bak_cmd}')
    print(f"end backup db data.")
    print(f'start upgrade db ...')
    sub_cmd = split_statements(get_db_upgrade_content(db_upgrade_sql))
    up_cmd =  mysql_home_path + "/bin/mysql" + " -u " + username + " -p" + passwd + " -P " + str(
        port) + " -h " + host + " -S " + mysql_sock + " -Bse " + '"' + sub_cmd + '"'
    print(f'up_cmd={up_cmd}')
    os.system("%s" %(up_cmd))

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
    print(f'upgrade eggroll 2.0.x finish')


def upgrade_main(ub_path,nm_file, rs_file, egg_home, mysql_home, mysql_host, mysql_port, mysql_db, mysql_user, mysql_pwd,
                 mysql_sock,mysql_file):
    print(f"into upgrade main ...")
    nm_node = read_ip_list(nm_file)
    rs_node = read_ip_list(rs_file)
    print(f'nm len={len(nm_node)}')
    print(f'rs len={len(rs_node)}')
    if len(nm_node) > 0:
        if check_egg_home(egg_home) is False:
            print(f'input param eggroll home path error={egg_home}')
            sys.exit(-1)
        try:
            backup_eggroll_data(egg_home)
            if os.path.exists( mysql_home + "/bin/mysql"):
                backup_upgrade_db(mysql_home, mysql_host, mysql_port, mysql_db, mysql_user, mysql_pwd,mysql_sock, mysql_file)
            else:
                print(f'current machine not install mysql .skip back {mysql_db} on mysql  and upgrade {mysql_db} on mysql \n')
                print(f'Please login in to MySQL backup to upgrade {mysql_db}')
        except Exception as e:
            print(f" upgrade error pleases checking.e={e}")
            sys.exit(-1)
        for h in nm_node:
            if h == '':
                continue
            cluster_upgrade_sync(ub_path+'/eggroll',egg_home,h)
    if len(rs_node) > 0:
        if check_egg_home(egg_home) is False:
            print(f'input param eggroll home path error={egg_home}')
            sys.exit(-1)
        for h in rs_node:
            if h == '':
                continue
            cluster_upgrade_sync(ub_path+'/eggroll',egg_home,h)



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
    if not os.path.exists(pkg_path + '/eggroll/python'):
        print(f'upgrade eggroll python upload dir not exists')
        return -1

def check_file_exits(path):
    if not os.path.exists(path):
        print(f'upgrade eggroll nm_ip_list or rs_ip_list ip file or mysql_file not exists')
        sys.exit(-1)

def remote_cluster_recover(nm_path,rs_path,eggroll_home_path: str):
    upgrade_all = read_ip_list(nm_path)
    upgrade_rollsite_node = read_ip_list(rs_path)
    if len(upgrade_all) > 0:
        for remote_host in upgrade_all:
            host = remote_host.strip()
            if host == '':
                continue
            else:
                cluster_upgrade_sync(eggroll_home_path, eggroll_home_path, remote_host)
    if len(upgrade_rollsite_node) > 0:
        for remote_host in upgrade_rollsite_node:
            host = remote_host.strip()
            if host == '':
                continue
            else:
                cluster_upgrade_sync(eggroll_home_path, eggroll_home_path, remote_host)
    else:
        pass



def recover_eggroll_data(nm_path,rs_path,db_home_path, eggroll_home_path: str, host: str, port, database: str, username: str,
                         passwd: str,db_sock:str):
    print(f'start recover eggroll data ...')
    if os.path.exists(eggroll_home_path + '/bin_' + datetime.now().strftime("%Y%m%d") + '_bak/'):
        subprocess.check_call(['mv', eggroll_home_path + '/bin_' + datetime.now().strftime("%Y%m%d") + '_bak/', eggroll_home_path + '/bin/'])
    if os.path.exists(eggroll_home_path + '/lib_' + datetime.now().strftime("%Y%m%d") + '_bak/'):
        subprocess.check_call(['mv', eggroll_home_path + '/lib_' + datetime.now().strftime("%Y%m%d") + '_bak/', eggroll_home_path + '/lib/'])
    if os.path.exists(eggroll_home_path + '/deploy_' + datetime.now().strftime("%Y%m%d") + '_bak/'):
        subprocess.check_call(['mv', eggroll_home_path + '/deploy_' + datetime.now().strftime("%Y%m%d") + '_bak/', eggroll_home_path + '/deploy/'])
    if os.path.exists(eggroll_home_path + '/python_' + datetime.now().strftime("%Y%m%d") + '_bak/'):
        subprocess.check_call(['mv', eggroll_home_path + '/python_' + datetime.now().strftime("%Y%m%d") + '_bak/', eggroll_home_path + '/python/'])
    print(f'eggroll data recover finish.')
    print(f'start recover eggroll db data...')

    bak_sql_f = "/home/app/dump_backup_eggroll_upgrade_"+host+'_' + datetime.now().strftime("%Y%m%d") + ".sql"
    if os.path.exists(bak_sql_f):
        sub_cmd = db_home_path+"/bin/mysql -u "+username+"  -p"+passwd+" -h "+host+" -P "+port +"  --default-character-set=utf8 "+database+" < "+bak_sql_f
        os.system("%s" %(sub_cmd)) 
        print(f'eggroll db data recover finish.')
    else:
        print(f'current machine not install mysql .skip recover {database} \n')
        print(f'Please login in to MySQL recover {database}')
    print(f'start cluster recover ...')
    remote_cluster_recover(nm_path,rs_path,eggroll_home_path)
    print(f'cluster recover finish.')

def pv_pkg_info():
    return '''
    The base path of the EggRoll upgrade package
    ,This directory contains the following files:
    eggroll/bin/
    eggroll/conf/eggroll.properties
    eggroll/deploy/
    eggroll/lib/
    eggroll/python/
'''

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--nm_file', type = str,help="namenode node ip sets", default="",required = True)
    parser.add_argument('-r', '--rs_file',type = str, help="rollsite node ip sets", default="",required = True)
    parser.add_argument('-e', '--egg_home', type = str,help="eggroll home path", default="",required = True)
    parser.add_argument('-m','--mysql_home',type = str, help="mysql home path", default="",required = True)
    parser.add_argument('-t','--mysql_host', type = str,help="mysql host info ", default="",required = True)
    parser.add_argument('-p', '--mysql_port',type = str, help="mysql port info ", default="",required = True)
    parser.add_argument('-b','--mysql_db',type = str,help="eggroll mysql database info ", default="",required = True)
    parser.add_argument('-u', '--mysql_user',type = str, help="mysql login user info", default="",required = True)
    parser.add_argument('-w', '--mysql_pwd', type = str,help="mysql login user passwd", default="",required = True)
    parser.add_argument('-S', '--mysql_sock',type = str, help="mysql login sock ", default="",required = True)
    parser.add_argument('-s', '--mysql_file',type = str, help="mysql upgrade update sql file info ", default="",required = True)
    parser.add_argument('-k', '--pkg_base_path',type = str, help=pv_pkg_info(), default="",required = True)
    parser.add_argument('-f', '--recover',type = int, help="recover 1 default 0 upgrade", default=0)
    args = parser.parse_args()
    print(
        f'input params={args.nm_file, args.rs_file, args.egg_home, args.mysql_home, args.mysql_host, args.mysql_port, args.mysql_db, args.mysql_user, args.mysql_pwd,args.mysql_sock, args.mysql_file,args.pkg_base_path,args.recover}')
    code = check_upgrade_pkg_path(args.pkg_base_path)
    if code == -1:
        sys.exit()
    check_file_exits(args.nm_file)
    check_file_exits(args.rs_file)
    check_file_exits(args.mysql_file)

    print(f'start ....')
    if args.recover == 0:
        print(f'upgrade main ... ...')
        upgrade_main(args.pkg_base_path,args.nm_file, args.rs_file, args.egg_home, args.mysql_home, args.mysql_host, args.mysql_port,
                     args.mysql_db, args.mysql_user, args.mysql_pwd,args.mysql_sock, args.mysql_file)
    elif args.recover == 1:
        print(f'recover main ... ...')
        recover_eggroll_data(args.nm_file, args.rs_file,args.mysql_home, args.egg_home , args.mysql_host, args.mysql_port, args.mysql_db, args.mysql_user,args.mysql_pwd,args.mysql_sock)
    print(f'end ....')


if __name__ == '__main__':
    main()
    sys.exit()
