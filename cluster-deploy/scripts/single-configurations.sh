#!/bin/bash                                                                                                                                                                                           

user=$username
dir=$install_dir
mysqldir=$mysql_install_dir
javadir=$jdk_install_path
venvdir=$python_virtualenv_path

partyid=$partyid
ip=$server_ip
exchange=$exchange_ip
jdbc=($mysql_ip $db_name $db_user $db_password)

clustercomm_port=9394
metaservice_port=8590
proxy_port=9370
roll_port=8011
egg_port=7888
storage_port=7778
exchange_port=9370
processor_port=5000
processor_count=16
