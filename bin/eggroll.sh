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
cd $cwd/..
export EGGROLL_HOME=`pwd`

export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION='python'
cd ${EGGROLL_HOME}
echo "EGGROLL_HOME=${EGGROLL_HOME}"

eval action=\$$#
start_mode=1
modules=(clustermanager nodemanager rollsite)

if [ $action = starting ];then
	action=start
	start_mode=0
elif [ $action = restarting ];then
	action=restart
	start_mode=0
fi

get_property() {
	property_value=`grep $1 ${EGGROLL_HOME}/conf/eggroll.properties | cut -d= -f 2-`
}

get_property "eggroll.resourcemanager.process.tag"
processor_tag=${property_value}
if [ -z "${processor_tag}" ];then
	processor_tag=EGGROLL_DAEMON
fi
echo "processor_tag=$processor_tag"

main() {
	case "$module" in
		clustermanager)
			main_class=com.webank.eggroll.core.resourcemanager.ClusterManagerBootstrap
			get_property "eggroll.resourcemanager.clustermanager.port"
			port=${property_value}
			get_property "eggroll.resourcemanager.clustermanager.jvm.options"
			jvm_options=${property_value}
			;;
		nodemanager)
			main_class=com.webank.eggroll.core.resourcemanager.NodeManagerBootstrap
			get_property "eggroll.resourcemanager.nodemanager.port"
			port=${property_value}
			get_property "eggroll.resourcemanager.nodemanager.jvm.options"
			jvm_options=${property_value}
			;;
		rollsite)
			main_class=com.webank.eggroll.rollsite.EggSiteBootstrap
			get_property "eggroll.rollsite.port"
			port=${property_value}
			get_property "eggroll.rollsite.jvm.options"
			jvm_options=${property_value}
			;;
		*)
			usage
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
		kill)
			shut
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
			usage
			exit -1
	esac
}

all() {
	for module in "${modules[@]}"; do
		main
		echo
		echo "[INFO] $module=${main_class}"
		echo "[INFO] processing: ${module} ${action}"
		echo "=================="
		action
		echo "--------------"
	done
}

usage() {
	echo "usage: `basename ${0}` {clustermanager | nodemanager | all} {start | stop | kill | restart | status}"
}

multiple() {
	total=$#
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
  pid=`ps aux | grep ${port} | grep ${processor_tag} | grep ${main_class} | grep -v grep | awk '{print $2}'`
	if [[ -n ${pid} ]]; then
		return 0
	else
		return 1
	fi
}

mklogsdir() {
	if [[ ! -d "${EGGROLL_HOME}/logs/eggroll" ]]; then
		mkdir -p ${EGGROLL_HOME}/logs/eggroll
	fi
}

status() {
	getpid
	if [[ -n ${pid} ]]; then
		echo "status:
		`ps aux | grep ${pid} | grep ${processor_tag} | grep ${main_class} | grep -v grep`"
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
		export EGGROLL_LOG_FILE=${module}
		cmd="java ${jvm_options} -Dlog4j.configurationFile=${EGGROLL_HOME}/conf/log4j2.properties -cp ${EGGROLL_HOME}/lib/*: com.webank.eggroll.core.Bootstrap --bootstraps ${main_class} -c ${EGGROLL_HOME}/conf/eggroll.properties -p $port -s ${processor_tag}"

		echo $cmd
		if [ $start_mode = 0 ];then
			exec $cmd >> ${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.out 2>>${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.err
		else
			exec $cmd >> ${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.out 2>>${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.err &
		fi

		getpid
		if [[ $? -eq 0 ]]; then
			echo "service start sucessfully. pid=${pid}"
		else
			echo "service start failed"
		fi
	else
		echo "service already started. pid=${pid}"
	fi
}

stop() {
	getpid
	if [[ -n ${pid} ]]; then
		echo "killing:
		`ps aux | grep ${pid} | grep ${processor_tag} | grep ${main_class} | grep -v grep`"
		kill ${pid}
		sleep 1
		flag=0
		while [ $flag -eq 0 ]
		do
			getpid
			flag=$?
		done
		echo "killed"
	else
		echo "service not running"
	fi
}

shut() {
	getpid
	if [[ -n ${pid} ]]; then
		echo "killing:
		`ps aux | grep ${pid} | grep ${processor_tag} | grep ${main_class} | grep -v grep`"
		kill -9 ${pid}
		sleep 1
		flag=0
		while [ $flag -eq 0 ]
		do
			getpid
			flag=$?
		done
		echo "killed"
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

cd $cwd

