#!/bin/bash

#
#  Copyright 2019 The eggroll Authors. All Rights Reserved.
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
cwd=$(cd `dirname $0`; pwd)

export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION='python'
version=2.0
cd ${EGGROLL_HOME}

eval action=\$$#
modules=(clustermanager nodemanager)


main() {
	case "$module" in
		clustermanager)
			main_class=com.webank.eggroll.core.resourcemanager.ClusterManagerBootstrap
			port=`cat ./conf/eggroll.properties |grep "eggroll.resourcemanager.clustermanager.port" | tail -n 1 | cut -d "=" -f2- | awk '{print $1}'`
			;;
		nodemanager)
			main_class=com.webank.eggroll.core.resourcemanager.NodeManagerBootstrap
			port=`cat ./conf/eggroll.properties |grep "eggroll.resourcemanager.nodemanager.port" | tail -n 1 | cut -d "=" -f2- | awk '{print $1}'`
			;;
		*)
			echo "usage: $module should be {clustermanager|nodemanager}"
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
			status
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
			echo "usage: $action should be {start|stop|status|restart}"
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
	echo "usage: $0 should be {all|[clustermanager|nodemanager...]} {start|stop|status|restart}"
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
	if [ ! -f "${module}_pid" ];then
		echo "" > ${module}_pid
	fi
	module_pid=`cat ${module}_pid`
	pid=`ps aux | grep $port | grep EGGROLL_DAEMON | grep -v grep | awk '{print $2}'`
	
	if [[ -n ${pid} ]]; then
		return 0
	else
		return 1
	fi
}

mklogsdir() {
	if [[ ! -d "${EGGROLL_HOME}/logs" ]]; then
		mkdir -p ${EGGROLL_HOME}/logs
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
		
		java -Dlog4j.configurationFile=${EGGROLL_HOME}/conf/log4j2.properties -cp ${EGGROLL_HOME}/lib/*: com.webank.eggroll.core.Bootstrap --bootstraps ${main_class} -c ${EGGROLL_HOME}/conf/eggroll.properties -p $port -s EGGROLL_DAEMON >> ${EGGROLL_HOME}/logs/${module}_console.log 2>>${EGGROLL_HOME}/logs/${module}_error.log &
		
		echo $!>${module}_pid
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
		sleep 1
		getpid
		if [[ $pid -eq $module_pid ]]; then
			echo "kill error"
		else
			echo "killed"
			echo "999999" >${module}_pid
		fi
	else
		echo "service not running"
		echo "999999" >${module}_pid
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

cd $cwd
