#!/usr/bin/env bash

sub_cmd=$1
exe=$2
pname=$3
SHELL_FOLDER=$(dirname "$0")
BASH=`which bash`

echo "$0, pid: $$"
echo "script params: $@ "


echo "------ mkdir starts ------"
mkdir -p $SHELL_FOLDER/pid
#pid_file=$SHELL_FOLDER/pid/$pname.pid
echo "------ mkdir ends ------"


echo "------ env starts -----"
env
echo "------ env ends -----"


echo "------ script body ------"
# TODO:2: check pid file and delete?
if [[ $sub_cmd == "start" ]]; then
  ${BASH} ${exe} &
  pid=$?
  echo "start: $exe, pid $pid"
  #echo $pid > $SHELL_FOLDER/pid/$pname.pid
elif [[ $sub_cmd == "stop" ]]; then
  #pid=`cat $pid_file`
  cmd="${exe} | awk '{print \$2}' | xargs kill"
  #echo "stop: $cmd, pid $pid"
  echo "stop: $cmd"
  eval ${cmd}
elif [[ $sub_cmd == "kill" ]]; then
  #pid=`cat $pid_file`
  cmd="${exe} | awk '{print \$2}' | xargs kill -9"
  #echo "kill: $cmd, pid $pid"
  echo "kill: $cmd"
  eval ${cmd}
fi