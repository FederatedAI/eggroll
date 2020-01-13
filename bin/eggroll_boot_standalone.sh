#!/usr/bin/env bash

SHELL_FOLDER=$(dirname "$0")

if [[ -z ${EGGROLL_HOME} ]]; then
  echo "env variable EGGROLL_HOME not set"
  exit -1
fi


#set -x
session_id="null_sid"
manager_port=4670
version=2.0
while getopts ":s:p:e:" opt; do
  case $opt in
    s)
      session_id=$OPTARG
      ;;
    p)
      manager_port=$OPTARG
      ;;
    e)
      eggs=$OPTARG
      ;;
   ?)
      echo "Invalid option: -$OPTARG index:$OPTIND"
      ;;
  esac
done

if [[ -z ${EGGROLL_LOGS_DIR} ]]; then
  EGGROLL_LOGS_DIR=${EGGROLL_HOME}/logs/
fi

if [[ ! -d "${EGGROLL_LOGS_DIR}/eggroll" ]]; then
  mkdir -p ${EGGROLL_LOGS_DIR}/eggroll
fi

cd ${EGGROLL_HOME}
echo "EGGROLL_HOME: ${EGGROLL_HOME}"
cmd="java -Dlog4j.configurationFile=${EGGROLL_HOME}/conf/log4j2.properties -cp ${EGGROLL_HOME}/conf:${EGGROLL_HOME}/lib/* com.webank.eggroll.core.Bootstrap --ignore-rebind --bootstraps com.webank.eggroll.core.resourcemanager.ClusterManagerBootstrap,com.webank.eggroll.core.resourcemanager.NodeManagerBootstrap -c ${EGGROLL_HOME}/conf/eggroll.properties -s $session_id -p $manager_port &"
echo "cmd: ${cmd}"
eval ${cmd} >> ${EGGROLL_HOME}/logs/eggroll/bootstrap-standalone-manager.out 2>>${EGGROLL_HOME}/logs/eggroll/bootstrap-standalone-manager.err

#while [ 1 ]; do
#  sleep 1
#done
