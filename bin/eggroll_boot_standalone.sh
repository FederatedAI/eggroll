#!/usr/bin/env bash

SHELL_FOLDER=$(dirname "$0")

if [[ -z ${EGGROLL_HOME} ]]; then
  echo "env variable EGGROLL_HOME not set"
  exit -1
fi

get_property() {
  property_value=`grep $2 $1 | cut -d '=' -f 2-`
}

#set -x
session_id="null_sid"
version=2.0
while getopts ":s:p:e:c:" opt; do
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
    c)
      config=$OPTARG
      ;;
    ?)
      echo "Invalid option: -$OPTARG index:$OPTIND"
      ;;
  esac
done

if [[ -z ${EGGROLL_LOGS_DIR} ]]; then
  EGGROLL_LOGS_DIR=${EGGROLL_HOME}/logs/
fi

if [[ -z ${config} ]]; then
  config=${EGGROLL_HOME}/conf/eggroll.properties
fi

if [[ ! -d "${EGGROLL_LOGS_DIR}/eggroll" ]]; then
  mkdir -p ${EGGROLL_LOGS_DIR}/eggroll
fi

if [[ -z ${manager_port} ]]; then
  get_property ${config} "eggroll.resourcemanager.clustermanager.port"
  manager_port=${property_value}
fi


cd ${EGGROLL_HOME}
echo "EGGROLL_HOME: ${EGGROLL_HOME}"

if [[ -z ${EGGROLL_STANDALONE_TAG} ]]; then
    java_define="-Dlog4j.configurationFile=${EGGROLL_HOME}/conf/log4j2.properties"
else
    java_define="-Dlog4j.configurationFile=${EGGROLL_HOME}/conf/log4j2.properties -Deggroll.standalone.tag=${EGGROLL_STANDALONE_TAG}"
fi

cmd="java $java_define -cp ${EGGROLL_HOME}/jvm/core/target/lib/*:${EGGROLL_HOME}/lib/*:${EGGROLL_HOME}/jvm/roll_pair/target/lib/* com.webank.eggroll.core.Bootstrap --ignore-rebind --bootstraps com.webank.eggroll.core.resourcemanager.ClusterManagerBootstrap,com.webank.eggroll.core.resourcemanager.NodeManagerBootstrap -c ${config} -s $session_id -p $manager_port &"
echo "cmd: ${cmd}"
eval ${cmd} >> ${EGGROLL_HOME}/logs/eggroll/bootstrap-standalone-manager.out 2>>${EGGROLL_HOME}/logs/eggroll/bootstrap-standalone-manager.err

#while [ 1 ]; do
#  sleep 1
#done
