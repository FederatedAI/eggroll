

 

# **Eggroll Deployment Guide**



## 1.     Installation Preparation

### 1.1. Server Configuration

The following is the server configuration information: 

| Server                 |                                                              |
| ---------------------- | ------------------------------------------------------------ |
| **Configuration**      | 16 core / 32G memory / 300G hard disk / 50M bandwidth        |
| **Operating System**   | Version: CentOS Linux release 7.2                            |
| **Dependency Package** | yum source gcc gcc-c++ make autoconfig openssl-devel supervisor gmp-devel mpfr-devel libmpc-devel libaio numactl autoconf automake libtool libffi-dev snappy snappy-devel zlib zlib-devel bzip2 bzip2-devel lz4-devel libasan |
| **File System**        | 1. The 300G hard disk is mounted to the /data directory.                                                                                2. Created /data/projects directory, projects directory belongs to deploy user. |

<!--Each server in the same cluster should be able to SSH each other and communicate with each other.-->

### 1.2. Software Version Requirements

| Node                       | Node description | Install node                              | Notes                                                        |
| -------------------------- | ---------------- | ----------------------------------------- | ------------------------------------------------------------ |
| Jdk                        | 1.8              | Necessary for each node                   |                                                              |
| Python  +python virtualenv | 3.6              | Necessary for each node                   | Need to install this dependency list [requirements.txt](https://github.com/WeBankFinTech/Eggroll/requirements.txt) ,python environments in different clusters need to be separated by virtual environments. |
| Mysql                      | 5.7+(8.0)        | Only one database is needed in a cluster. | Every IP in the cluster must have privilege access to the database. |



## 2.      Project Deployment

*<u>Note: The default installation directory is /data/projects/. It is modified according to the actual situation during installation.</u>*

### 2.1. Project Pull

Go to the /data/projects/ directory of the execution node and execute the git command to pull the project from github:

```bash
cd /data/projects/
git clone -b master https://github.com/WeBankFinTech/Eggroll.git
```

### 2.2. Maven Packaging

Go into the project directory and do dependency packaging:

```bash
cd Eggroll
mvn clean package -DskipTests
wget https://webank-ai-1251170195.cos.ap-guangzhou.myqcloud.com/third_party_eggrollv1.tar.gz
tar -xzvf third_party_eggrollv1.tar.gz -C storage/storage-service-cxx
cd cluster-deploy/scripts
sh auto-packaging.sh
```

### 2.3. Modify Configuration File

There are two ways of deployment, please choose according to the actual deployment situation:

1. Multi-node Cluster

   This deployment method is suitable for deploying multiple modules on a multi-node cluster. Before deployment, the deployed nodes need to login to each target node confidentially. The deployment steps refer to steps 2.3.1.

2.  Single-node Cluster

   This deployment method is suitable for deploying multiple modules on a single node cluster. The deployment steps refer to steps 2.3.2.

#### 2.3.1. Multi-node Cluster

This deployment is based on SSH secret-free. Before executing the script, make sure that the executing node can log in to the target IP to be deployed  secret-free.

1. Modify the configurations.sh configuration file in directory Eggroll/cluster-deploy/scripts as the following meaning：

```bash
user=$username					   (the username of deploy user)
dir=$install_directory_path			(exist path eggroll prepare to install)
mysqldir=$mysql_install_path		(mysql install absolute path)
javadir=$jdk_install_path			(java_home absolute path)
venvdir=$python_virtualenv_path		 (python_virtualenv install absolute path)

partylist=($party_id)				 (the party.id of the cluster,eg 10000)
iplist=($clustercomm_ip $metaservice_ip $proxy_ip $roll_ip $egg1_ip...)  (the list of all  server ip appear below, no duplication)
exchange_$party_id=$exchange_ip		  (the ip of exchange module,eg 192.xxx.xxx.xxx)
clustercomm_$party_id=$clustercomm_ip  (the ip of clustercomm module,eg 192.xxx.xxx.xxx)
metaservice_$party_id=$metaservice_ip  (the ip of metaservice module,eg 192.xxx.xxx.xxx)
proxy_$party_id=$proxy_ip			  (the ip of proxy module,eg 192.xxx.xxx.xxx)
roll_$party_id=$roll_ip				  (the ip of roll module,eg 192.xxx.xxx.xxx)
egglist_$party_id=($egg1_ip $egg2_ip $egg3_ip) (the ip of egg module,can be a list)
jdbc_$party_id=($mysql_ip $db_name $db_user $db_password)(the configuration of mysql)

============(default port of each module,if you need use other ports, config them)==========

clustercomm_port=9394		
metaservice_port=8590
proxy_port=9370
roll_port=8011
egg_port=7888
storage_port=7778
exchange_port=9370
processor_port=5000

============(the count of processors,no more than you cpu cores count)==========

processor_count=16
```

<!--Every coincidence with '$' should be replaced with the actual value-->

If you are deploying two clusters, add a different party_id configuration on configurations.sh in directory Eggroll/cluster-deploy/scripts as follows:

```bash
user=$username					   (the username of deploy user)
dir=$install_directory_path			(exist path eggroll prepare to install)
mysqldir=$mysql_install_path		(mysql install absolute path)
javadir=$jdk_install_path			(java_home absolute path)
venvdir=$python_virtualenv_path		 (python_virtualenv install absolute path)

partylist=($partyA_id $partyB_id)		(the party.id of the cluster,eg 10000)
iplist=($clustercommA_ip $clustercommB_ip $metaserviceA_ip $metaserviceB_ip $proxyA_ip $proxyB_ip $rollA_ip $rollB_ip $eggA1_ip $eggB1_ip...)  (the list of all  server ip appear below, no duplication)
exchange_$partyA_id=$exchangeA_ip		(the ip of exchange module,eg 192.xxx.xxx.xxx)
clustercomm_$partyA_id=$clustercommA_ip  (the ip of clustercomm module,eg 192.xxx.xxx.xxx)
metaservice_$partyA_id=$metaserviceA_ip  (the ip of metaservice module,eg 192.xxx.xxx.xxx)
proxy_$partyA_id=$proxyA_ip				(the ip of proxy module,eg 192.xxx.xxx.xxx)
roll_$partyA_id=$rollA_ip				(the ip of roll module,eg 192.xxx.xxx.xxx)
egglist_$partyA_id=($eggA1_ip $eggA2_ip $eggA3_ip) (the ip of egg module,can be a list)
jdbc_$partyA_id=($mysqlA_ip $dbA_name $dbA_user $dbA_password)(the configuration of mysql)

exchange_$partyB_id=$exchangeB_ip		  (the ip of exchange module,eg 192.xxx.xxx.xxx)
clustercomm_$partyB_id=$clustercommB_ip   (the ip of clustercomm module,eg 192.xxx.xxx.xxx)
metaservice_$partyB_id=$metaserviceB_ip   (the ip of metaservice module,eg 192.xxx.xxx.xxx)
proxy_$partyB_id=$proxyB_ip			 	 (the ip of proxy module,eg 192.xxx.xxx.xxx)
roll_$partyB_id=$rollB_ip				 (the ip of roll module,eg 192.xxx.xxx.xxx)
egglist_$partyB_id=($eggB1_ip $eggB2_ip $eggB3_ip) (the ip of egg module,can be a list)
jdbc_$partyB_id=($mysqlB_ip $dbB_name $dbB_user $dbB_password)(the configuration of mysql)

==========(default port of each module,if you need use other ports, configure them)=========

clustercomm_port=9394		
metaservice_port=8590
proxy_port=9370
roll_port=8011
egg_port=7888
storage_port=7778
exchange_port=9370
processor_port=5000

============(the count of processors,no more than you cpu cores count)==========

processor_count=16
```

2. Executing the script in turn:

```bash
sh auto-packaging.sh
sh auto-module-deploy.sh all
```

If some modules fail to deploy or deploy some modules separately, execute the script as follows:

```bash
sh auto-module-deploy.sh $module1_name $module2_name
```



#### 2.3.2. Single-node Cluster

This is done by executing scripts on localhost-ip and supporting only a single-node cluster at a time.

1. Modify the single-configurations.sh configuration file in directory Eggroll/cluster-deploy/scripts as the following meaning：

```bash
user=$username					   (the username of deploy user)
dir=$install_directory_path			(exist path eggroll prepare to install)
mysqldir=$mysql_install_path		(mysql install absolute path)
javadir=$jdk_install_path			(java_home absolute path)
venvdir=$python_virtualenv_path		 (python_virtualenv install absolute path)

partyid=$partyid				(the party.id of the single-node,eg 10000)
ip=$server_ip					(the ip of single-node,eg 192.xxx.xxx.xxx)
exchange=$exchange_ip					(the ip of exchange module,eg 192.xxx.xxx.xxx)
jdbc=($mysql_ip $db_name $db_user $db_password)	(the configuration of mysql)

=========(default port of each module,if you need use other ports, configure them)========

clustercomm_port=9394		
metaservice_port=8590
proxy_port=9370
roll_port=8011
egg_port=7888
storage_port=7778
exchange_port=9370
processor_port=5000

============(the count of processors,no more than you cpu cores count)==========

processor_count=16
```

2. Executing the script in turn:

```bash
sh auto-single-deploy.sh all
```

If some modules fail to deploy or deploy some modules separately, execute the script again as follows:

```bash
sh auto-module-deploy.sh $module1_name $module2_name
```

## 3.     Configuration Check

After the execution, you can check whether the configuration of the corresponding module is accurate on each target server. Users can find a detailed configuration document in [cluster-deploy/doc](https://github.com/WeBankFinTech/Eggroll/cluster-deploy/doc) .



## 4.     Start And Stop Service

Use ssh to log in to each node with **deploy user**. Go to the install directory and run the following command to start services:

```bash
sh services.sh all start						  --start all module service on this server
sh services.sh $module_name1 $module_name2 start	--start some module service on this server
```

And you can replace 'start' with 'status' to see the status of the process, replace 'start' with 'restart' to restart service, and replace 'start' with 'stop' to stop service, such as:

```bash
sh services.sh all|$module_name start|stop|restart|status
```
