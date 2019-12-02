SHELL_FOLDER=$(dirname "$0")
base_dir=$SHELL_FOLDER/../jvm
set -x
session_id="null_sid"
manager_port=0
while getopts ":sp:" opt; do
  case $opt in
    s)
      sid=$OPTARG
      ;;
    p)
      manager_port=$OPTARG
      ;;
    ?)
      echo "Invalid option: -$OPTARG index:$OPTIND"
      ;;

  esac
done
java -cp ${base_dir}/core/main/resources:${base_dir}/roll_pair/target/classes:${base_dir}/core/target/classes:${base_dir}/core/target/lib/*:${base_dir}/roll_pair/target/lib/* com.webank.eggroll.rollpair.StandaloneManager -c ${base_dir}/core/main/resources/cluster-manager.properties -s $session_id -p $manager_port
#java -cp ${base_dir}/core/main/resources:${base_dir}/roll_pair/target/eggroll-roll-pair-2.0.jar:${base_dir}/core/target/eggroll-core-2.0.jar:${base_dir}/core/target/lib/*:${base_dir}/roll_pair/target/lib/* com.webank.eggroll.rollpair.StandaloneManager
#python $SHELL_FOLDER/../python/eggroll/roll_pair/egg_pair.py -p $port