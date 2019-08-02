#!/bin/bash                                                                                                                                                                                           

user=app
dir=$install_dir
mysqldir=$mysql_install_dir
javadir=$jdk_install_path
venvdir=$python_virtualenv_path

partylist=($party_id)
exchange_$party_id=$exchange_ip
clustercomm_$party_id=$clustercomm_ip
metaservice_$party_id=$metaservice_ip
proxy_$party_id=$proxy_ip
roll_$party_id=$roll_ip	
egglist_$party_id=($egg1_ip $egg2_ip $egg3_ip)
jdbc_$party_id=($mysql_ip $db_name $db_user $db_password)
clustercomm_port=9394
metaservice_port=8590
proxy_port=9370
roll_port=8011
egg_port=7888
storage_port=7778
exchange_port=9370
processor_port=50000
processor_count=16
