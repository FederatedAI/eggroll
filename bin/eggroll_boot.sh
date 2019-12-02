sub_cmd=$1
exe=$2
session_id=$3
node_id=$4
SHELL_FOLDER=$(dirname "$0")
mkdir -p $SHELL_FOLDER/pid
pid_file=$SHELL_FOLDER/pid/$session_id-$node_id.pid

if [ $sub_cmd == "start_node" ]
then
  exec $exe &
  pid=$!
  echo "start_node:$exe,pid $pid"
  echo $pid > $SHELL_FOLDER/pid/$session_id-$node_id.pid
else
  pid=`cat $pid_file`
  echo "stop_node:$exe,pid $pid"
  pkill -P $pid
fi