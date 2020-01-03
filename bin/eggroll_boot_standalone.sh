#!/usr/bin/env bash

SHELL_FOLDER=$(dirname "$0")
# EGGROLL_HOME

cd $SHELL_FOLDER/../
base_dir=$SHELL_FOLDER/..
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

echo "base_dir: ${base_dir}"
cmd="java -Dlog4j.configurationFile=${base_dir}/conf/log4j2.properties -cp ${base_dir}/lib/*:${base_dir}/jvm/core/target/eggroll-core-${version}.jar:${base_dir}/jvm/core/target/lib/*:${base_dir}/jvm/roll_pair/target/lib/*:${base_dir}/jvm/roll_pair/target/eggroll-roll-pair-${version}.jar com.webank.eggroll.core.Bootstrap --ignore-rebind --bootstraps com.webank.eggroll.core.resourcemanager.ClusterManagerBootstrap,com.webank.eggroll.core.resourcemanager.NodeManagerBootstrap -c ${base_dir}/conf/eggroll.properties -s $session_id -p $manager_port &"
echo "cmd: ${cmd}"
eval ${cmd} > ${base_dir}/logs/eggroll/bootstrap-standalone-manager.out 2>${base_dir}/logs/eggroll/bootstrap-standalone-manager.err

#while [ 1 ]; do
#  sleep 1
#done
