

 

# **Eggroll Deployment Guide**



## 1.     Installation Preparation

### 1.1. Server Configuration

The following is the server configuration information: 

| Server                 |                                                              |
| ---------------------- | ------------------------------------------------------------ |
| **Configuration**      | 16 core / 32G memory / 300G hard disk / 50M bandwidth        |
| **Operating System**   | Version: CentOS Linux release 7.2                            |
| **Dependency Package** | yum source gcc gcc-c++ make autoconfig openssl-devel supervisor gmp-devel mpfr-devel libmpc-devel libaio numactl autoconf automake libtool libffi-dev snappy snappy-devel zlib zlib-devel bzip2 bzip2-devel lz4-devel libasan |
| **Users**              | User: app owner:apps                                         |
| **File System**        | 1. The 300G hard disk is mounted to the /data directory.                                                                                2. Created /data/projects directory, projects directory belongs to app:apps |

<!--Each server in the same cluster should be able to SSH each other and communicate with each other.-->

### 1.2. Software Version Requirements

| Node                           | Node description | Install node                              | Notes                                                        |
| ------------------------------ | ---------------- | ----------------------------------------- | ------------------------------------------------------------ |
| **Jdk**                        | 1.8              | Necessary for each node                   | After installation, you need to export the JAVA_HOME variable to the **app** user variable |
| **Python  +python virtualenv** | 3.6              | Necessary for each node                   | Need to install this dependency list [requirements.txt](https://github.com/WeBankFinTech/Eggroll/requirements.txt) ,python environments in different clusters need to be separated by virtual environments. |
| **Mysql**                      | 5.7+(8.0)        | Only one database is needed in a cluster. | Every IP in the cluster must have privilege access to the database.**If you use the h2 database, the projects has prepared it.** |



## 2.      Project Deployment

*<u>Note: The default installation directory is /data/projects, and the execution user is app. It is modified according to the actual situation during installation.</u>*

### 2.1. Project Pull

Go to the install directory of the execution node and execute the git command to pull the project from github:

```bash
cd /data/projects	(go in your autual directory)
git clone -b v2.x https://github.com/WeBankFinTech/Eggroll.git
```

### 2.2. Maven Packaging

Go into the project directory and do dependency packaging:

```bash
cd Eggroll/jvm
mvn clean package -DskipTests
cd ..
```

### 2.3. Modify Configuration File And Deploy

1. Modify the **conf.sh** configuration file:

```bash
vi ./deploy/conf.sh
```

| Configuration item | Configuration item meaning    | Configuration Item Value                           | Notes                                 |
| ------------------ | ----------------------------- | -------------------------------------------------- | ------------------------------------- |
| EGGROLL_HOME       | Deploy path of Eggroll        | Default : /data/projects/Eggroll                   | Use the default value                 |
| IPLIST             | Server IP list of the cluster | The ip of each server in the cluser to be deployed | List of each server IP to be deployed |

2. Modify the **eggroll.properties** configuration file:

```
vi ./conf/eggroll.properties
```

*<u>Notes: this configuration file contains two database configuration modes. If the default H2 database is used, the first four JDBC configurations can be unmodified. If the MySQL database is used, the configuration can be modified in the JDBC mode of MySQL.</u>*

| Configuration item                             | Configuration item meaning               | Configuration Item Value                                     |
| ---------------------------------------------- | ---------------------------------------- | ------------------------------------------------------------ |
| eggroll.cluster.manager.jdbc.driver.class.name | The driver of  database                  | h2:org.h2.Driver/Mysql:com.mysql.cj.jdbc.Driver              |
| eggroll.cluster.manager.jdbc.url               | JDBC connection mode of database         | h2:use the default/Mysql:JDBC connection url.                |
| eggroll.cluster.manager.jdbc.username          | Username of database                     | h2:default null/Mysql:username of database                   |
| eggroll.cluster.manager.jdbc.password          | Password of database                     | h2:default null/Mysql:password of database                   |
| eggroll.logs.dir                               | The dir of logs                          | Use default value.                                           |
| eggroll.node.manager.port                      | The port of NodeManager                  | Modify to designated port,default use 4670.                  |
| eggroll.cluster.manager.host                   | The host of ClusterManager               | Use default value.                                           |
| eggroll.cluster.manager.port                   | The port of ClusterManager               | Modify to designated port,default use 9394.                  |
| eggroll.bootstrap.root.script                  | The startup script of scripts in exepath | Use default value.                                           |
| eggroll.bootstrap.egg_pair.exepath             | The startup script of egg_pair           | Use default value.                                           |
| eggroll.bootstrap.egg_pair.venv                | The path of venv_home                    | Use absolute path of venv_home or relative path to EGGROLL_HOME |
| eggroll.bootstrap.egg_pair.pythonpath          | The path of PATHON_PATH                  | Use default value.                                           |
| eggroll.bootstrap.egg_pair.filepath            | The path of egg_pair.py                  | Use default value.                                           |
| eggroll.bootstrap.roll_pair_master.exepath     | The startup script of roll_pair          | Use default value.                                           |
| eggroll.bootstrap.roll_pair_master.javahome    | The path of JAVA_HOME                    | Use default value and export the JAVA_HOME variable to the app user variable |
| eggroll.bootstrap.roll_pair_master.classpath   | The classpath of roll_pair               | Use default value.                                           |
| eggroll.bootstrap.roll_pair_master.mainclass   | The mainclass of roll_pair               | Use default value.                                           |
| eggroll.bootstrap.roll_pair_master.jvm.options | The jvm options                          | Use default value, can be modified as server configuration.  |



3. Executing the **deploy.sh** script:

```bash
cd deploy
sh deploy.sh
```

4. Executing the SQL script(if you use the h2 database, skip this step):

```bash
1   Scp conf/create-eggroll-meta-tables.sql to the server of Mysql;
2   Log in Mysql and run source create-eggroll-meta-tables.sql;
3   INSERT INTO server_node (host, port, node_type, status) values ('$cluster_ip', '$cluster_port', 'CLUSTER_MANAGER', 'HEALTHY');
    INSERT INTO server_node (host, port, node_type, status) values ('$node_ip', '$node_port', 'NODE_MANAGER', 'HEALTHY');
```

## 4.     Start And Stop Service

Use ssh to log in to each node with **app user**. Go to the install directory and run the following command to start services:

```bash
cd ${EGGROLL_HOME}
sh eggroll.sh all start						  --start all module service on this server
```

And you can replace 'start' with 'status' to see the status of the process, replace 'start' with 'restart' to restart service, and replace 'start' with 'stop' to stop service, such as:

```bash
sh eggroll.sh all|$module_name start|stop|restart|status
```

***Notes: value of $module_name: clustermanager|nodemanager***

## 5.     Test 

Log in the server of ClusterManager or NodeManager,running the commands:

```bash
source $venv_home/bin/activate
export PYTHONPATH=${EGGROLL_HOME}/python
cd ${EGGROLL_HOME}/python/eggroll/roll_pair/test
python -m unittest test_standalone.TestStandalone
```

Wait a few minutes, see the result show "OK" field to indicate that the operation is successful. In other cases, if FAILED or stuck, it means failure.