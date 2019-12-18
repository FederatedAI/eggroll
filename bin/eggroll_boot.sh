#!/usr/bin/env bash

sub_cmd=$1
exe=$2
pname=$3
SHELL_FOLDER=$(dirname "$0")
mkdir -p $SHELL_FOLDER/pid
pid_file=$SHELL_FOLDER/pid/$pname.pid
# TODO:2: check pid file and delete?
if [ $sub_cmd == "start" ]
then
  `which bash` ${exe} &
  pid=$!
  echo "start:$exe,pid $pid"
  echo $pid > $SHELL_FOLDER/pid/$pname.pid
elif [ $sub_cmd == "stop" ]
then
  pid=`cat $pid_file`
  echo "stop:$exe,pid $pid"
  pkill -P $pid
  kill $pid
fi