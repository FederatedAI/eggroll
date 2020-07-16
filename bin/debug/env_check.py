#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#	  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#
import sys
import json
import time
import psutil
import datetime
import threading
import argparse
import subprocess
from eggroll.core.session import ErSession
from eggroll.roll_pair.roll_pair import RollPairContext
from eggroll.utils.log_utils import get_logger

L = get_logger()

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("-t","--time", type=int, help="Sleep time wait, default value 0s", default=0)
arg_parser.add_argument("-n","--nodes", type=int, help="Eggroll session processors per node, default value 1", default=1)
arg_parser.add_argument("-p","--partitions", type=int, help="Total partitions, default value 1", default=1)
args = arg_parser.parse_args()

def str_generator(include_key=True, row_limit=10, key_suffix_size=0, value_suffix_size=0):
    for i in range(row_limit):
        if include_key:
            yield str(i) + "s"*key_suffix_size, str(i) + "s"*value_suffix_size
        else:
            yield str(i) + "s"*value_suffix_size

def round2(x):
    return str(round(x / 1024 / 1024 / 1024, 2)) + 'G'

def print_red(str):
    print("\033[1;31;40m\t" + str + "\033[0m")

def print_green(str):
    print("\033[1;32;40m\t" + str + "\033[0m")

def print_yellow(str):
    print("\033[1;33;40m\t" + str + "\033[0m")

def check_actual_max_threads():
    def getMemInfo(fn):
        def query_cmd(cmd):
            result = True
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()[0].decode().strip().split('\n')
            print(p)
            for i in p:
                if int(i) < 65535:
                    result = False
            return result

        mem = psutil.virtual_memory()
        mem_total = round2(mem.total)
        mem_used = round2(mem.used)
        mem_used_per = str(round(mem.percent)) + '%'

        swap_mem = psutil.swap_memory()
        swap_total = round2(swap_mem.total)
        swap_used = round2(swap_mem.used)
        swap_use_per = str(round(swap_mem.percent)) + '%'

        data_disk = psutil.disk_usage('/data')
        disk_total = round2(data_disk.total)
        disk_used = round2(data_disk.used)
        disk_per = str(round(data_disk.percent)) + '%'

        mem_info = {}
        mem_info["MemTotal"] = mem_total
        mem_info["MemUsed"] = mem_used
        mem_info["MemUsedPer"] = mem_used_per

        mem_info["SwapTotal"] = swap_total
        mem_info["SwapUsed"] = swap_used
        mem_info["SwapUsePer"] = swap_use_per

        mem_info["DiskTotal"] = disk_total
        mem_info["DiskUsed"] = disk_used
        mem_info["DiskPer"] = disk_per

        mem_info["/proc/sys/kernel/threads-max"] = query_cmd("cat /proc/sys/kernel/threads-max")
        mem_info["/etc/sysctl.conf"] = query_cmd("grep kernel.pid_max /etc/sysctl.conf | awk -F= '{print $2}'")
        mem_info["/proc/sys/kernel/pid_max"] = query_cmd("cat /proc/sys/kernel/pid_max")
        mem_info["/proc/sys/vm/max_map_count"] = query_cmd("cat /proc/sys/vm/max_map_count")

        mem_info["/etc/security/limits.conf"] = query_cmd("cat /etc/security/limits.conf | grep nofile | awk '{print $4}'")
        mem_info["/etc/security/limits.d/80-nofile.conf"] = query_cmd("cat /etc/security/limits.d/80-nofile.conf | grep nofile | awk '{print $4}'")
        mem_info["/etc/sysctl.conf"] = query_cmd("grep fs.file-max  /etc/sysctl.conf | awk -F= '{print $2}'")
        mem_info["/proc/sys/fs/file-max"] = query_cmd("cat /proc/sys/fs/file-max")

        return mem_info

    session = ErSession(options={"eggroll.session.processors.per.node": args.nodes})
    try:
        ctx = RollPairContext(session)
        rp = ctx.parallelize(str_generator(row_limit=1000), options={'total_partitions': args.partitions})
        result = rp.with_stores(func=getMemInfo)
        print_green(str(datetime.datetime.now()))
        #print(json.dumps(result, indent=1))
        for node in result:
            print_green("==============This is node :" + str(node[0]) + "================")
            print_yellow("[WARNING] MemTotal:" + node[1]["MemTotal"] + ", MemUsed:" + node[1]["MemUsed"] + ", MemUsedPer:" + node[1]["MemUsedPer"])
            print_yellow("[WARNING] SwapTotal:" + node[1]["SwapTotal"] + ", SwapUsed:" + node[1]["SwapUsed"] + ", SwapUsePer:" + node[1]["SwapUsePer"])
            print_yellow("[WARNING] DiskTotal:" + node[1]["DiskTotal"] + ", DiskUsed:" + node[1]["DiskUsed"] + ", DiskPer:" + node[1]["DiskPer"])
            print_green("--------Max user processes and max file count--------")
            for key in ["/proc/sys/kernel/threads-max", "/etc/sysctl.conf", "/proc/sys/kernel/pid_max", "/proc/sys/vm/max_map_count", "/etc/security/limits.conf", "/etc/security/limits.d/80-nofile.conf", "/etc/sysctl.conf", "/proc/sys/fs/file-max"]:
                if node[1][key]:
                    print_green("[OK] " + key + " is ok.")
                else:
                    print_red("[ERROR] please check " + key + ", no less than 65535.")
            print("\n")
    finally:
        session.kill()


if __name__ == '__main__':
    if args.time == 0:
        check_actual_max_threads()
    else:
        while 1:
            check_actual_max_threads()
            time.sleep(args.time)
