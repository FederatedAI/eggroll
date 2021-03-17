# Eggroll部署文档说明

## 1.    环境初始化

### 1.1.  环境要求

| **操作系统** | CentOS 7.2                                                   |
| :----------- | :----------------------------------------------------------- |
| **工具依赖** | 安装yum源工具：gcc gcc-c++ make autoconfig   openssl-devel supervisor gmp-devel mpfr-devel libmpc-devel libaio numactl   autoconf automake libtool libffi-dev snappy snappy-devel zlib zlib-devel   bzip2 bzip2-devel lz4-devel libasan |
| **操作用户** | 用户名: app 组:apps                                          |
| **系统配置** | 1. 挂载300G可用磁盘空间到/data目录    2. 创建/data/projects目录，属主app用户 |

### 1.2.  安装软件包

集群所有节点都需要安装：jdk1.8、virtualenv独立运行环境（可进入python3.6版本）

数据库节点：mysql8.0

## 2.    项目拉取及打包

从github拉取Eggroll项目，通过执行auto-packaging.sh自动打包脚本在同目录下生成eggroll.tar.gz

```shell
git clone -b v2.x https://github.com/WeBankFinTech/Eggroll.git
cd Eggroll
sh deploy/auto-packaging.sh
```



## 3.    部署发送

### 3.1.  解压

将eggroll.tar.gz移到或发送到Eggroll的安装目录下，然后执行：

```shell
tar -xzf eggroll.tar.gz
```

将eggroll.tar.gz解压后，其目录结构如下：

```properties
|--bin			--存放eggroll自带启动脚本
|--conf			--存放eggroll配置文件(包含部署需要修改文件)
|--data			--存放数据库初始化及缓存文件
|--deploy		--存放部署相关文件
|--lib			--存放eggroll所有jar包
|--python		--存放eggroll python代码部分
```

### 3.2.  修改配置文件

配置文件一共有三个需要修改：

```properties
|--conf
|----eggroll.properties
|----route_tabe.json
|----create-eggroll-meta-tables.sql
```

各配置文件修改说明如下：

- **修改eggroll.properties配置文件**

```shell
vi conf/eggroll.properties
```

```properties
<--数据库配置选项说明：
	eggroll提供两种数据库连接方式：
	1、项目自带h2数据库，适用于单节点部署，使用此方式则以下几项jdbc配置无需修改，使用原有默认配置即可；
	2、安装的mysql8.0数据库，建议集群版多节点使用，若使用此方式则需要按如下方式进行修改配置。-->

eggroll.resourcemanager.clustermanager.jdbc.driver.class.name=com.mysql.cj.jdbc.Driver
eggroll.resourcemanager.clustermanager.jdbc.url=jdbc:mysql://数据库服务器ip:端口/数据库名称?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true
eggroll.resourcemanager.clustermanager.jdbc.username=数据库用户名
eggroll.resourcemanager.clustermanager.jdbc.password=数据库密码

<--eggroll相关配置参数说明：
	1、以下包含的路径都是相对Eggroll的实际部署目录之下的相对路径，若不在Eggroll的实际部署目录之下，可用系统绝对路径；
	2、根据部署方法分为三个修改级别：需要修改、建议默认、默认即可，其中端口为建议默认但需要根据实际服务器端口是否可用或部署方式来考虑是否修改，以避免端口冲突。-->

eggroll.data.dir=data/			<--存放缓存数据目录，默认即可-->
eggroll.logs.dir=logs/			<--存放eggroll生成日志目录，默认即可-->
eggroll.resourcemanager.clustermanager.host=127.0.0.1	<--clustermanager服务ip地址，需要修改-->
eggroll.resourcemanager.clustermanager.port=4670	<--clustermanager服务端口，建议默认-->
eggroll.resourcemanager.nodemanager.port=9394	<--nodemanager服务端口：1、部署单机版与clustermanager相同，建议默认；2、部署集群版需修改为其他可用端口，需要修改-->
eggroll.resourcemanager.process.tag=	<--集群服务标签，对不同集群需要单独指，例如EGGROLL_TAG，需要修改-->

eggroll.bootstrap.root.script=bin/eggroll_boot.sh	<--eggroll_boot.sh启动脚本路径，默认即可-->

eggroll.resourcemanager.bootstrap.egg_pair.exepath=bin/roll_pair/egg_pair_bootstrap.sh		<--egg_pair启动脚本路径，默认即可-->
eggroll.resourcemanager.bootstrap.egg_pair.venv=		<--virtualenv安装路径，需要修改-->
eggroll.resourcemanager.bootstrap.egg_pair.pythonpath=python		<--python文件路径，也作PYTHONPATH，默认即可-->
eggroll.resourcemanager.bootstrap.egg_pair.filepath=python/eggroll/roll_pair/egg_pair.py	<--egg_pair.py文件路径，默认即可-->
eggroll.resourcemanager.bootstrap.egg_pair.ld_library_path=		<--egg_pair ld_library_path路径，默认即可-->

<--以下几项默认即可-->
eggroll.resourcemanager.bootstrap.egg_frame.exepath=bin/roll_frame/egg_frame_bootstrap.sh		<--egg_frame_bootstrap.sh文件路径-->
eggroll.resourcemanager.bootstrap.egg_frame.javahome=	<--java环境变量，系统安装jdk1.8-->
eggroll.resourcemanager.bootstrap.egg_frame.classpath=conf/:lib/*	<--eggroll启动时读取classpath文件路径-->
eggroll.resourcemanager.bootstrap.egg_frame.mainclass=com.webank.eggroll.rollframe.EggFrameBootstrap	<--roll_frame主类-->
eggroll.resourcemanager.bootstrap.egg_frame.jvm.options=	<--jvm启动参数-->
<--以上几项默认即可-->

# roll_frame
arrow.enable_unsafe_memory_access=true

# hadoop
hadoop.fs.defaultFS=file:///

# hadoop HA mode
hadoop.dfs.nameservices=
hadoop.dfs.namenode.rpc-address.nn1=
hadoop.dfs.namenode.rpc-address.nn2=

<--rollsite配置说明：其服务ip、端口与partyId需要与route_table.json配置文件中对应一致-->
eggroll.rollsite.coordinator=webank			<--rollsite服务标签，默认即可-->
eggroll.rollsite.host=127.0.0.1				<--rollsite服务ip，需要修改-->
eggroll.rollsite.port=9370					<--rollsite服务端口，建议默认-->
eggroll.rollsite.party.id=10001				<--集群partyId，不同集群需要使用不同的partyId，需要修改-->
eggroll.rollsite.route.table.path=conf/route_table.json	<--route_table.json路由配置文件路径，默认即可-->
eggroll.rollsite.jvm.options=			<--rollsite jvm启动参数添加，默认即可，有需要可自行添加-->

eggroll.session.processors.per.node=4		<--单节点启动egg pair个数，小于或等于cpu核数，建议16-->
eggroll.session.start.timeout.ms=180000		<--session超时设定ms数，默认即可-->
eggroll.rollsite.adapter.sendbuf.size=1048576	<--rollsite传输块大小，默认即可-->
eggroll.rollpair.transferpair.sendbuf.size=4150000		<--rollpair传输块大小，默认即可-->
```

- **修改route_table.json路由信息**

```shell
vi conf/route_table.json
```

```properties
{
  "route_table":
  {
    "集群一partyId":			<--此处需要修改-->
    {   
      "default":[
        {   
          "port": 集群一rollsite服务端口,		<--此处需要修改-->
          "ip": "集群一rollsite服务ip"		<--此处需要修改-->
        }   
      ]   
    },  
    "集群二partyId":			<--此处需要修改-->
    {   
      "default":[
        {   
          "port": 集群二rollsite服务端口,		<--此处需要修改-->
          "ip": "集群二rollsite服务ip"		<--此处需要修改-->
        }   
      ]   
    }   
  },  
  "permission":
  {
    "default_allow": true
  }
}
```



### 3.3.  多节点部署

按上述说明修改完配置文件后，若集群内需多节点部署，可使用部署脚本进行打包部署：

- 修改部署配置文件deploy/conf.sh

```shell
vi deploy/conf.sh

export EGGROLL_HOME=/data/projects/eggroll		<--部署到目标服务器的eggroll路径，修改为EGGROLL要部署的绝对路径-->
export MYSQL_HOME=/data/projects/mysql		<--mysql服务器上的mysql安装路径，直到mysql目录-->
iplist=(127.0.0.xxx 127.0.0.xxx)		<--本集群内所以节点的ip列表-->
```

- 执行部署脚本deploy/deploy.sh

```shell
cd deploy
sh deploy.sh
```



## 4.    添加元信息

集群多节点之间的服务之间是通过查询数据库存储的元信息来感知的，因此执行上述步骤需要登录数据库服务器对数据库进行检查节点信息，查询server_node表检查数据是否准确：

```sql
登录数据库执行：
>>use 数据库名称					<--切换到部署的数据库-->
>>show tables;					<--检查是否有以下7个表-->
    | server_node                       |
    | session_main                      |
    | session_option                    |
    | session_processor                 |
    | store_locator                     |
    | store_option                      |
    | store_partition                   |
    
>>select * from server_node;	<--检查是否包含所有节点及角色元信息-->
>>exit
```



## 5.    服务启动

Eggroll的bin目录中附带启动脚本bin/eggroll.sh使用说明：

```shell
source ${EGGROLL_HOME}/init_env.sh       --${EGGROLL_HOME} means the absolute path of eggroll
sh bin/eggroll.sh $1 $2		
<--
	$1：需要执行操作的服务名称，例如clustermanager，nodemanager，rollsite，all(表示所有服务)；
	$2：需要执行的操作，例如start(启动)，starting(阻塞启动)，status（查看状态），stop（关闭），kill(杀掉服务,stop失效时使用)，restart（重启），restarting（阻塞重启）
-->
```

使用例子：

```shell
source ${EGGROLL_HOME}/init_env.sh       --${EGGROLL_HOME} means the absolute path of eggroll
<--启动所有服务-->
sh bin/eggroll.sh all start

<--阻塞启动clustermanager服务-->
sh bin/eggroll.sh clustermanager starting

<--查看clustermanager服务状态-->
sh bin/eggroll.sh clustermanager status

<--重启rollsite服务-->
sh bin/eggroll.sh rollsite restart

<--阻塞重启rollsite服务-->
sh bin/eggroll.sh rollsite restarting

<--关闭nodemanager服务-->
sh bin/eggroll.sh nodemanager stop

<--杀掉nodemanager服务，当stop不成功时使用-->
sh bin/eggroll.sh nodemanager kill
```

将各节点对应的服务启动成功后，部署完成，进入测试步骤。



## 6.    测试

### 6.1.  初始化环境变量

登录服务器进行测试时需要执行以下语句进行环境变量初始化

```shell
source ${EGGROLL_HOME}/init_env.sh       --${EGGROLL_HOME} means the absolute path of eggroll
```

### 6.2.  roll_pair测试

```shell
cd ${EGGROLL_HOME}/python/eggroll/roll_pair/test
python -m unittest test_roll_pair.TestRollPairStandalone		--单机模式
python -m unittest test_roll_pair.TestRollPairCluster			--集群模式
```

等待执行完成出现"OK"字段为成功。



### 6.3.  roll_site测试

- **通信测试**

(a). guest方执行

```shell
cd ${EGGROLL_HOME}/python/eggroll/roll_site/test
python -m unittest test_roll_site.TestRollSiteCluster.test_remote
```

等待执行完成出现"OK"字段为guest方发送成功。

(b). host方执行

```shell
cd ${EGGROLL_HOME}/python/eggroll/roll_site/test
python -m unittest test_roll_site.TestRollSiteCluster.test_get
```

等待执行完成出现"OK"字段为host方接收成功。

- **多partition通信测试**

(a). guest方执行

```shell
cd ${EGGROLL_HOME}/python/eggroll/roll_site/test
python -m unittest test_roll_site.TestRollSiteCluster.test_remote_rollpair_big
```

等待执行完成出现"OK"字段为guest方发送成功。

(b). host方执行

```shell
cd ${EGGROLL_HOME}/python/eggroll/roll_site/test
python -m unittest test_roll_site.TestRollSiteCluster.test_get_rollpair_big
```

等待执行完成出现"OK"字段为host方接收成功。

- **rollpair通信测试**

(a). guest方执行

```shell
cd ${EGGROLL_HOME}/python/eggroll/roll_site/test
python -m unittest test_roll_site.TestRollSiteCluster.test_remote_rollpair
```

等待执行完成出现"OK"字段为guest方发送成功。

(b). host方执行

```shell
cd ${EGGROLL_HOME}/python/eggroll/roll_site/test
python -m unittest test_roll_site.TestRollSiteCluster.test_get_rollpair
```

等待执行完成出现"OK"字段为host方接收成功。

至此测试完成。
