#!/usr/bin/env bash

echo
echo
echo "====== start of $0 ======"

echo "pwd: `pwd`"
echo "$0, pid: $$"
echo "script params: $@ "

echo "------ env starts -----"
env
echo "------ env ends -----"

echo "------ script body ------"

ONE_ARG_LIST=(
  "config"
  "session-id"
  "server-node-id"
  "processor-id"
  "port"
  "transfer-port"
)

get_property() {
  property_value=`grep $2 $1 | cut -d '=' -f 2-`
}

opts=$(getopt \
  --longoptions "$(printf "%s:," "${ONE_ARG_LIST[@]}")" \
  --name "$(basename "$0")" \
  --options "" \
  -- "$@"
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      config=$2
      shift 2
      ;;
    --session-id)
      session_id=$2
      shift 2
      ;;
    --processor-id)
      processor_id=$2
      shift 2
      ;;
    --port)
      port=$2
      shift 2
      ;;
    --transfer_port)
      transfer_port=$2
      shift 2
      ;;
    --server-node-id)
      server_node_id=$2
      shift 2
      ;;
    *)
      break
      ;;
  esac
done


if [[ -z ${session_id+x} ]]; then
  echo "session-id is blank"
  return 1
fi

if [[ -z ${processor_id+x} ]]; then
  echo "processor-id is blank"
  return 2
fi

if [[ ${transfer_port} -eq 0 ]] && [[ ${port} -ne 0 ]]; then
  transfer_port=${port}
fi

get_property ${config} "eggroll.resourcemanager.bootstrap.egg_frame.javahome"
javahome=${property_value}

get_property ${config} "eggroll.resourcemanager.bootstrap.egg_frame.classpath"
classpath=${property_value}

get_property ${config} "eggroll.resourcemanager.bootstrap.egg_frame.mainclass"
mainclass=${property_value}

get_property ${config} "eggroll.resourcemanager.bootstrap.egg_frame.jvm.options"
jvm_options=${property_value}

get_property ${config} "eggroll.logs.dir"
logs_dir=${property_value}

get_property ${config} "eggroll.resourcemanager.nodemanager.port"
node_manager_port=${property_value}

get_property ${config} "eggroll.resourcemanager.clustermanager.host"
cluster_manager_host=${property_value}

get_property ${config} "eggroll.resourcemanager.clustermanager.port"
cluster_manager_port=${property_value}

if [[ -z ${EGGROLL_LOGS_DIR} ]]; then
  get_property ${config} "eggroll.logs.dir"
  export EGGROLL_LOGS_DIR=${property_value}

  if [[ -z ${EGGROLL_LOGS_DIR} ]]; then
    export EGGROLL_LOGS_DIR=${EGGROLL_HOME}/logs
  fi
fi

export EGGROLL_SESSION_ID=${session_id}

if [[ -z ${EGGROLL_LOG_LEVEL} ]]; then
  export EGGROLL_LOG_LEVEL="INFO"
fi

if [[ -z ${EGGROLL_LOG_CONF} ]]; then
  if [[ -z ${EGGROLL_HOME} ]]; then
    export EGGROLL_LOG_CONF=./conf/log4j2.properties
  else
    export EGGROLL_LOG_CONF=${EGGROLL_HOME}/conf/log4j2.properties
  fi
fi

echo "EGGROLL_LOG_CONF: ${EGGROLL_LOG_CONF}"

if [[ -z ${javahome} ]]; then
  JAVA=`which java`
else
  JAVA=${javahome}/bin/java
fi

if [[ -z ${mainclass} ]]; then
  mainclass="com.webank.eggroll.rollframe.EggFrameBootstrap"
fi

export EGGROLL_LOG_FILE="egg_frame-${processor_id}"

echo "------ jvm version starts ------"
${JAVA} -version
echo "------ jvm version ends ------"


cmd="${JAVA} ${jvm_options} -Dlog4j.configurationFile=${EGGROLL_LOG_CONF} -cp ${classpath} com.webank.eggroll.core.Bootstrap --bootstraps ${mainclass} --config ${config} --session-id ${session_id} --server-node-id ${server_node_id} --cluster-manager ${cluster_manager_host}:${cluster_manager_port} --node-manager ${node_manager_port} --processor-id ${processor_id}"

if [[ -n ${port} ]]; then
  cmd="${cmd} --port ${port}"
  if [[ -n ${transfer_port} ]]; then
    cmd="${cmd} --transfer-port ${transfer_port}"
  fi
fi

final_logs_dir=${logs_dir}/${session_id}
mkdir -p ${final_logs_dir}
echo "egg_frame_bootstrap_cmd: ${cmd}"
${cmd} >> ${final_logs_dir}/${EGGROLL_LOG_FILE}.out 2>${final_logs_dir}/${EGGROLL_LOG_FILE}.err &
echo "egg_frame processor id:$processor_id, os process id:$!" >> ${final_logs_dir}/pid.txt