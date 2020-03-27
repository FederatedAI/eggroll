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


if [[ -z ${session_id} ]]; then
  echo "session-id is blank"
  return 1
fi

if [[ -z ${processor_id} ]]; then
  echo "processor-id is blank"
  return 2
fi

if [[ ${transfer_port} -eq 0 ]] && [[ ${port} -ne 0 ]]; then
  transfer_port=${port}
fi

get_property ${config} "eggroll.resourcemanager.bootstrap.egg_pair.venv"
venv=${property_value}

get_property ${config} "eggroll.resourcemanager.bootstrap.egg_pair.pythonpath"
pythonpath=${property_value}

get_property ${config} "eggroll.resourcemanager.bootstrap.egg_pair.filepath"
filepath=${property_value}

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
  EGGROLL_LOGS_DIR=${property_value}

  if [[ -z ${EGGROLL_LOGS_DIR} ]]; then
    EGGROLL_LOGS_DIR=${EGGROLL_HOME}/logs
  fi
fi


EGGROLL_SESSION_ID=${session_id}

if [[ -z ${EGGROLL_LOG_LEVEL} ]]; then
  EGGROLL_LOG_LEVEL="INFO"
fi

export EGGROLL_LOG_FILE="egg_pair-${processor_id}"


if [[ -z ${venv} ]]; then
  PYTHON=`which python`
else
  source ${venv}/bin/activate
  PYTHON=${venv}/bin/python
fi

export PYTHONPATH=${pythonpath}:${PYTHONPATH}
echo "PYTHONPATH: ${PYTHONPATH}"
echo "PYTHON: `which python`"

echo "------ python version starts ------"
${PYTHON} --version
echo "------ python version ends ------"

cmd="${PYTHON} ${filepath} --config ${config} --session-id ${session_id} --server-node-id ${server_node_id} --cluster-manager ${cluster_manager_host}:${cluster_manager_port} --node-manager ${node_manager_port} --processor-id ${processor_id}"

if [[ -n ${port} ]]; then
  cmd="${cmd} --port ${port}"
  if [[ -n ${transfer_port} ]]; then
    cmd="${cmd} --transfer-port ${transfer_port}"
  fi
fi

export EGGROLL_LOGS_DIR=${EGGROLL_LOGS_DIR}/${EGGROLL_SESSION_ID}
mkdir -p ${EGGROLL_LOGS_DIR}
echo "${cmd}"
${cmd} >> ${EGGROLL_LOGS_DIR}/${EGGROLL_LOG_FILE}.out 2>${EGGROLL_LOGS_DIR}/${EGGROLL_LOG_FILE}.err &
echo "egg_pair processor id:$processor_id, os process id:$!" >> ${EGGROLL_LOGS_DIR}/pid.txt