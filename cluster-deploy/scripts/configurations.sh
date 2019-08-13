#!/bin/bash                                                                                                                                                                                           

user=app
dir=/data/projects/eggroll
mysqldir=/data/projects/common/mysql/mysql-8.0.13
javadir=/data/projects/common/jdk/jdk1.8.0_192
venvdir=/data/projects/eggroll/venv

partylist=(10000)
iplist=()
exchange_10000=
clustercomm_10000=
metaservice_10000=
proxy_10000=
roll_10000=
egglist_10000=()
jdbc_10000=(ip name user passwd)

clustercomm_port=9394
metaservice_port=8590
proxy_port=9370
roll_port=8011
egg_port=7888
storage_port=7778
exchange_port=9370
processor_port=50000
processor_count=16