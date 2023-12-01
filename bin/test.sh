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
# --------------- Color Definitions ---------------
#esc_c="\e[0m"
#error_c="\e[31m"
#ok_c="\033[32m"
#highlight_c="\e[43m"
esc_c="\033[0m"
error_c="\033[31m"
ok_c="\033[32m"
highlight_c="\033[43m"

# --------------- Logging Functions ---------------
print_info() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local overwrite=$2

    # Check if we need to overwrite the current line
    if [ "$overwrite" == "overwrite" ]; then
        echo -ne "\r${ok_c}[${timestamp}][INFO]${esc_c} $1"
    else
        echo -e "${ok_c}[${timestamp}][INFO]${esc_c} $1"
    fi
}
print_ok() {
    local overwrite=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    if [ "$overwrite" == "overwrite" ]; then
        echo -ne "\r${ok_c}[${timestamp}][OK]${esc_c} $1"
    else
        echo -e "${ok_c}[${timestamp}][OK]${esc_c} $1"
    fi
}
print_error() {
    local overwrite=$3
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    if [ "$overwrite" == "overwrite" ]; then
        echo -ne "\r${error_c}[${timestamp}][ER]${esc_c} $1: $2"
    else
        echo -e "${error_c}[${timestamp}][ER]${esc_c} $1: $2"
    fi
}

cwd=$(cd `dirname $0`; pwd)
cd $cwd/..
export EGGROLL_HOME=`pwd`

export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION='python'
cd ${EGGROLL_HOME}
print_info "EGGROLL_HOME=${EGGROLL_HOME}"

eval action=\$$#
start_mode=1
modules=(clustermanager nodemanager dashboard)

if [ $action = starting ];then
	action=start
	start_mode=0
elif [ $action = restarting ];then
	action=restart
	start_mode=0
fi



# Get the PID of the process
getpid() {
  pid=`ps aux | grep ${port} | grep ${processor_tag} | grep ${main_class} | grep -v grep | awk '{print $2}'`
	if [[ -n ${pid} ]]; then
		return 0
	else
		return 1
	fi
}

# Get the PID of the process using a specific port
get_port_pid() {
  pid=$(lsof -i:${port} | grep 'LISTEN' | awk 'NR==1 {print $2}')
  if [[ -n ${pid} ]]; then
  	return 0
  else
  	return 1
  fi
}

# --------------- Functions for stop---------------
# Function to kill a process
kill_process() {
    local pid=$1
    local signal=$2
    kill ${signal} "${pid}" 2>/dev/null
}

get_property() {
	property_value=`grep $1 ${EGGROLL_HOME}/conf/eggroll.properties | cut -d= -f 2-`
}

get_property "eggroll.resourcemanager.process.tag"
processor_tag=${property_value}
if [ -z "${processor_tag}" ];then
	processor_tag=EGGROLL_DAEMON
fi
print_info "processor_tag=$processor_tag"


main() {
	case "$module" in
		clustermanager)
			main_class=org.fedai.eggroll.clustermanager.Bootstrap
			get_property "eggroll.resourcemanager.clustermanager.port"
			port=${property_value}
			get_property "eggroll.resourcemanager.clustermanager.jvm.options"
			jvm_options=${property_value}
			;;
		nodemanager)
			main_class=org.fedai.eggroll.nodemanager.Bootstrap
			get_property "eggroll.resourcemanager.nodemanager.port"
			port=${property_value}
			get_property "eggroll.resourcemanager.nodemanager.jvm.options"
			jvm_options=${property_value}
			;;
	  dashboard)
  		main_class=org.fedai.eggroll.webapp.JettyServer
      get_property "eggroll.dashboard.server.port"
      port=${property_value}
  		;;
		*)
			usage
			exit 1
	esac
}

action() {
	case "$action" in
	  debug)
	  stop
	  sleep_time=${3:-5}
    print_info "Waiting ${sleep_time} seconds"
    sleep "$sleep_time"
	  debug
	  status
	  ;;
		start)
			start
			status
			;;
		stop)
			stop
			;;
#		kill)
#			shut
#			status
#			;;
		status)
			status
			;;
		restart)
			stop
			sleep_time=${3:-5}  # 默认 sleep_time 为 5，如果传入了参数，则使用传入的值
			print_info "Waiting ${sleep_time} seconds"
      sleep "$sleep_time"
			start
			status
			;;
		*)
			usage
			exit 1
	esac
}

all() {
	for module in "${modules[@]}"; do
		main
		print_info "$module=${main_class}"
		print_info "Processing: ${highlight_c}${module} ${action}"
		action "$@"
	done
}

# --------------- Functions for info---------------
# Print usage information for the script
usage() {

	    echo -e "${ok_c}eggroll${esc_c}"
      echo "------------------------------------"
      echo -e "${ok_c}Usage:${esc_c}"
      echo -e "  `basename ${0}` [component] start          - Start the server application."
      echo -e "  `basename ${0}` [component] stop           - Stop the server application."
      echo -e "  `basename ${0}` [component] shut           - Force kill the server application."
      echo -e "  `basename ${0}` [component] status         - Check and report the status of the server application."
      echo -e "  `basename ${0}` [component] restart [time] - Restart the server application. Optionally, specify a sleep time (in seconds) between stop and start."
      echo -e "  `basename ${0}` [component] debug          - Start the server application in debug mode."
      echo -e "  ${ok_c}The component${esc_c} include: {clustermanager | nodemanager | dashboard | all} "
      echo ""
      echo -e "${ok_c}Examples:${esc_c}"
      echo "  `basename ${0}` clustermanager start"
      echo "  `basename ${0}` clustermanager restart 5"
      echo ""
      echo -e "${ok_c}Notes:${esc_c}"
      echo "  - The restart command, if given an optional sleep time, will wait for the specified number of seconds between stopping and starting the service."
      echo "    If not provided, it defaults to 10 seconds."
      echo "  - Ensure that the required Java environment is correctly configured on the system."
      echo ""
      echo "For more detailed information, refer to the script's documentation or visit the official documentation website."
}

multiple() {
	total=$#
	for (( i=1; i<total; i++)); do
		module=${!i//\//}
		main
		print_info "$module:${main_class}"
		print_info "Processing: ${highlight_c}${module} ${action}"
		action "$@"
	done
}

## --------------- Functions for start---------------
## Check if the service is up and running
#check_service_up() {
#    local timeout_ms=$1 #2
#    local interval_ms=$2 #3
#    local http_port=$3 #4
#    local cluster_pid=0
#    local node_pid=0
#    local elapsed_ms=0
#    local spin_state=0
#    # 查找包含 "clustermanager" 关键词的进程，并获取其 PID 赋值给变量 cluster_pid
#    cluster_pid=$(ps aux | grep '[c]lustermanager' | grep -v grep | grep -v $$ | awk '{print $2}')
#    # 查找包含 "nodemanager" 关键词的进程，并获取其 PID 赋值给变量 node_pid
#    node_pid=$(ps aux | grep '[n]odemanager' | grep -v grep | grep -v $$ | awk '{print $2}')
#    while ((elapsed_ms < timeout_ms)); do
#        if ! kill -0 "${cluster_pid}" 2>/dev/null; then
#            print_error "Process clustermanager with PID ${cluster_pid} is not running." "" "overwrite"
#            echo
#            return 1
#        fi
#        if ! kill -0 "${node_pid}" 2>/dev/null; then
#            print_error "Process nodemanager with PID ${node_pid} is not running." "" "overwrite"
#            echo
#            return 1
#        fi
#
#        if lsof -i :${http_port} | grep -q LISTEN; then
#            print_ok "All service started successfully!" "overwrite"
#            echo
#            return 0
#        else
#            print_error "Process dashboard is not running" "overwrite"
#            return 1
#        fi
#
#        # Update spinning wheel
#        case $spin_state in
#            0) spinner_char="/" ;;
#            1) spinner_char="-" ;;
#            2) spinner_char="\\" ;;
#            3) spinner_char="|" ;;
#        esac
#        print_info "$spinner_char" "overwrite"
#        spin_state=$(((spin_state + 1) % 4))
#        sleep $((interval_ms / 1000)).$((interval_ms % 1000))
#        elapsed_ms=$((elapsed_ms + interval_ms))
#    done
#    print_error "Service did not start up within the expected time." "" "overwrite"
#    echo
#    return 1
#}

mklogsdir() {
	if [[ ! -d "${EGGROLL_HOME}/logs/eggroll" ]]; then
		mkdir -p ${EGGROLL_HOME}/logs/eggroll
	fi
}

# --------------- Functions for status---------------
# Check the status of the service
status() {
  print_info "---------------------------------status---------------------------------"
	if [ "${module}" = "dashboard" ]; then
    get_port_pid
  else
    getpid
  fi
	# check service is up and running
	if [[ -n ${pid} ]]; then
    print_ok "Check service ${highlight_c}${module}${esc_c} is started: PID=${highlight_c}${pid}${esc_c}"
    print_info "The service status is:
    `ps aux | grep ${pid} | grep ${processor_tag} | grep ${main_class} | grep -v grep`"
    return 0
	else
		print_error "The ${module} service is not running"
		return 1
	fi
}

# Start service
start() {
  print_info "--------------------------------starting--------------------------------"
  print_info "Checking Java environment..."
  # check the java environment
  if [[ -n ${JAVA_HOME} ]]; then
    print_ok "JAVA_HOME is already set, service starting..."
  else
    print_error "JAVA_HOME is not set, please set it first"
    exit 1
  fi
	if [ "${module}" = "dashboard" ]; then
    get_port_pid
  else
    getpid
  fi
	if [[ $? -eq 1 ]]; then
		mklogsdir
		export EGGROLL_LOG_FILE=${module}
		export module=${module}
		cmd="java -server ${jvm_options} -Dlog4j.configurationFile=${EGGROLL_HOME}/conf/log4j2.xml -Dmodule=${module} -cp ${EGGROLL_HOME}/lib/*: ${main_class}  -p $port -s ${processor_tag}"

		print_info "The command is: $cmd"

		if [ $start_mode = 0 ];then
			exec $cmd >> ${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.out 2>>${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.err
		else
			exec $cmd >> ${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.out 2>>${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.err &
		fi
    # wait for connect DB
    print_info "Waiting for connect DB..."
    sleep 5
    if [ "${module}" = "dashboard" ]; then
      get_port_pid
    else
      getpid
    fi
		if [[ $? -eq 0 ]]; then
      print_ok "The ${highlight_c}${module}${esc_c} service start sucessfully. PID=${pid}"
		else
			print_error "The ${highlight_c}${module}${esc_c} service start failed"
		fi
	else
		print_info "The ${highlight_c}${module}${esc_c} service already started. PID=${pid}"
	fi
}

debug() {
  print_info "--------------------------------debugging--------------------------------"
	if [ "${module}" = "dashboard" ]; then
    get_port_pid
  else
    getpid
  fi
	if [[ $? -eq 1 ]]; then
		mklogsdir
		export EGGROLL_LOG_FILE=${module}
		export module=${module}
		cmd="java -server -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=7007   ${jvm_options} -Dlog4j.configurationFile=${EGGROLL_HOME}/conf/log4j2.xml -Dmodule=${module} -cp ${EGGROLL_HOME}/lib/*: ${main_class}  -p $port -s ${processor_tag}"

		print_info "The command is: $cmd"
		if [ $start_mode = 0 ];then
			exec $cmd >> ${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.out 2>>${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.err
		else
			exec $cmd >> ${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.out 2>>${EGGROLL_HOME}/logs/eggroll/bootstrap.${module}.err &
		fi

		if [ "${module}" = "dashboard" ]; then
      get_port_pid
    else
      getpid
    fi
		if [[ $? -eq 0 ]]; then
			print_ok "The ${highlight_c}${module}${esc_c} service debug sucessfully. PID=${pid}"
		else
			print_error "The ${highlight_c}${module}${esc_c} service debug failed"
		fi
	else
		print_info "The ${highlight_c}${module}${esc_c} service already started. PID=${pid}"
	fi
}

# --------------- Functions for stop---------------
# Stop service
stop() {
  print_info "--------------------------------stopping--------------------------------"
	 if [ "${module}" = "dashboard" ]; then
     get_port_pid
   else
     getpid
   fi
	if [[ -n ${pid} ]]; then
		print_info "The system is stopping the ${highlight_c}${module}${esc_c} service. PID=${pid}"
		print_info "The more information:
		`ps aux | grep ${pid} | grep ${processor_tag} | grep ${main_class} | grep -v grep`"
	  for _ in {1..100}; do
        sleep 0.1
        kill_process "${pid}"
        if [ "${module}" = "dashboard" ]; then
          get_port_pid
        else
          getpid
        fi
        if [ -z "${pid}" ]; then
            print_ok "Stop ${port} success (SIGTERM)"
            return
        fi
    done
    kill_process "${pid}" -9 && print_ok "Stop the service ${highlight_c}${module}${esc_c} success (SIGKILL)" || print_error "Stop service failed"
	else
		print_ok "The ${highlight_c}${module}${esc_c} service is not running(NOT ACTIVE))"
	fi
}
# Shut service(FORCE KILL). now not use, stop has force kill
shut() {
  print_info "--------------------------------shutting--------------------------------"
	if [ "${module}" = "dashboard" ]; then
    get_port_pid
  else
    getpid
  fi
	if [[ -n ${pid} ]]; then
	  print_info "The ${highlight_c}${module}${esc_c} service is force killing. PID=${pid}"
		print_info "The more information:
		`ps aux | grep ${pid} | grep ${processor_tag} | grep ${main_class} | grep -v grep`"
		kill -9 ${pid}
		sleep 1
		flag=0
		while [ $flag -eq 0 ]
		do
			if [ "${module}" = "dashboard" ]; then
        get_port_pid
      else
        getpid
      fi
			flag=$?
		done
		print_info "The ${highlight_c}${module}${esc_c} service is force kill success"
	else
		print_info "The ${highlight_c}${module}${esc_c} service is not running"
	fi
}

# --------------- Main---------------
# Main case for control
case "$1" in
	all)
		all "$@"
		;;
	clustermanager|nodemanager|dashboard)
		multiple "$@"
		;;
	*)
	  usage
    exit 1
		;;
esac

cd $cwd

