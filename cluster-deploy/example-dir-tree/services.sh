#!/bin/bash

#
#  Copyright 2019 The eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

eval action=\$$#
dir=/data/projects/eggrolla
export JAVA_HOME=/data/projects/common/jdk/jdk1.8.0_192
export PATH=$PATH:$JAVA_HOME/bin
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION='python'
export PYTHONPATH=$dir/api
modules=(clustercomm meta-service egg roll proxy storage-service-cxx)


main() {
	case "$module" in
		clustercomm)
			main_class=com.webank.ai.eggroll.driver.ClusterComm
			;;
		proxy)
			main_class=com.webank.ai.eggroll.networking.Proxy
			;;
		egg)
			main_class=com.webank.ai.eggroll.framework.egg.Egg
			;;
		roll)
			main_class=com.webank.ai.eggroll.framework.Roll
			;;
		meta-service)
			main_class=com.webank.ai.eggroll.framework.MetaService
			;;
		storage-service-cxx)
			target=storage-service
			port=7778
			dirdata=$dir/data-dir
			export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:$dir/$module/third_party/lib
			;;
		*)
			echo "usage: $module {clustercomm|meta-service|egg|roll|proxy}"
			exit -1
	esac
}

action() {
	case "$action" in
		start)
			start
			status
			;;
		stop)
			stop
			;;
		status)
			status
			;;
		restart)
			stop
			start
			status
			;;
		*)
			echo "usage: $action {start|stop|status|restart}"
			exit -1
	esac
}

all() {
	for module in "${modules[@]}"; do
		main
        echo
		echo "[INFO] $module:${main_class}"
        echo "[INFO] processing: ${module} ${action}"
        echo "=================="
        action
        echo "--------------"
	done
}

usage() {
    echo "usage: $0 {all|[module1, ...]} {start|stop|status|restart}"
}

multiple() {
    total=$#
    action=${!total}
    for (( i=1; i<total; i++)); do
        module=${!i//\//}
		main
        echo
		echo "[INFO] $module:${main_class}"
        echo "[INFO] processing: ${module} ${action}"
        echo "=================="
        action
        echo "--------------"
    done
}

getpid() {
	if [ ! -f "${module}/${module}_pid" ];then
		echo "" > ${module}/${module}_pid
	fi
	module_pid=`cat ${module}/${module}_pid`
	pid=`ps aux | grep ${module_pid} | grep -v grep | grep -v $0 | awk '{print $2}'`
	
    if [[ -n ${pid} ]]; then
        return 0
    else
        return 1
    fi
}

mklogsdir() {
    if [[ ! -d "$dir/$module/logs" ]]; then
        mkdir -p $dir/$module/logs
    fi
}

status() {
    getpid
    if [[ -n ${pid} ]]; then
        echo "status:
        `ps aux | grep ${pid} | grep -v grep`"
        return 0
    else
        echo "service not running"
        return 1
    fi
}

start() {
    getpid
    if [[ $? -eq 1 ]]; then
        mklogsdir
		if [[ "$module" == "storage-service-cxx" ]]; then
			$module/${target} -p $port -d ${dirdata} >> $dir/$module/logs/console.log 2>>$dir/$module/logs/error.log &
			echo $!>${module}/${module}_pid
        else
			java -cp "$dir/${module}/conf/:${module}/lib/*:${module}/eggroll-${module}.jar" ${main_class} -c $dir/${module}/conf/${module}.properties >> $dir/${module}/logs/console.log 2>>$dir/${module}/logs/error.log &
			echo $!>${module}/${module}_pid
		fi
		getpid
		if [[ $? -eq 0 ]]; then
            echo "service start sucessfully. pid: ${pid}"
        else
            echo "service start failed"
        fi
    else
        echo "service already started. pid: ${pid}"
    fi
}

stop() {
    getpid
    if [[ -n ${pid} ]]; then
        echo "killing:
        `ps aux | grep ${pid} | grep -v grep`"
        kill -9 ${pid}
		getpid
        if [[ $? -eq 1 ]]; then
            echo "killed"
        else
            echo "kill error"
        fi
    else
        echo "service not running"
    fi
}

case "$1" in
    all)
        all $@
        ;;
    usage)
        usage
        ;;
    *)
        multiple $@
        ;;
esac



