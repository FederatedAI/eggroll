#!/bin/bash                                                                                                                                                                                           

user=$username
dir=$install_dir
mysqldir=$mysql_install_dir
javadir=$jdk_install_path
venvdir=$python_virtualenv_path

partylist=($party_id)
ip=$localhost_ip
exchange=$change_ip
jdbc=($mysql_ip $db_name $db_user $db_password)

clustercomm_port=9394
metaservice_port=8590
proxy_port=9370
roll_port=8011
egg_port=7888
storage_port=7778
exchange_port=9370
processor_port=50000
processor_count=16
