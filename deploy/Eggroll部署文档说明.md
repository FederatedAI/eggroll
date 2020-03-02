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

方式一：从github拉取Eggroll项目，通过执行auto-packaging.sh自动打包脚本在同目录下生成eggroll.tar.gz

```shell
git clone -b v2.x https://github.com/WeBankFinTech/Eggroll.git
cd Eggroll
sh deploy/auto-packaging.sh
```

方式二：从webank-ai云直接拉取

```shell
wget https://webank-ai-1251170195.cos.ap-guangzhou.myqcloud.com/eggroll-v2.x-init.tar.gz
mv eggroll-v2.x-init.tar.gz eggroll.tar.gz
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

<--以下几项默认即可-->
eggroll.resourcemanager.bootstrap.roll_pair_master.exepath=bin/roll_pair/roll_pair_master_bootstrap.sh		<--roll_pair_master_bootstrap.sh文件路径-->
eggroll.resourcemanager.bootstrap.roll_pair_master.javahome=	<--java环境变量，系统安装jdk1.8-->
eggroll.resourcemanager.bootstrap.roll_pair_master.classpath=conf/:lib/*	<--eggroll启动时读取classpath文件路径-->
eggroll.resourcemanager.bootstrap.roll_pair_master.mainclass=com.webank.eggroll.rollpair.RollPairMasterBootstrap	<--roll_pair_master主类-->
eggroll.resourcemanager.bootstrap.roll_pair_master.jvm.options=	<--jvm启动参数-->
<--以上几项默认即可-->

<--rollsite配置说明：其服务ip、端口与partyId需要与route_table.json配置文件中对应一致-->
eggroll.rollsite.coordinator=webank			<--rollsite服务标签，默认即可-->
eggroll.rollsite.host=127.0.0.1				<--rollsite服务ip，需要修改-->
eggroll.rollsite.port=9370					<--rollsite服务端口，建议默认-->
eggroll.rollsite.party.id=10001				<--集群partyId，不同集群需要使用不同的partyId，需要修改-->
eggroll.rollsite.route.table.path=conf/route_table.json	<--route_table.json路由配置文件路径，默认即可-->

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

- **修改数据库初始化sql脚本**

```shell
<--修改说明：此文件为初始化mysql数据库建表及建库使用的sql脚本，若使用默认数据库名为eggroll_meta则跳过此步骤，若实际需要使用其他库名，可使用以下语句替换为实际的数据库名称，此处数据库名称应与eggroll.properties中所填数据库名称一致-->

sed -i "s/eggroll_meta/数据库名称/" conf/create-eggroll-meta-tables.sql
```

### 3.3.  多节点部署

按上述说明修改完配置文件后，若集群内需多节点部署，由于各节点的配置文件完全相同，将其打包发送到集群各个节点的Eggroll安装目录下即可。



## 4.    添加元信息

集群多节点之间的服务之间是通过查询数据库存储的元信息来感知的，因此需要登录数据库服务器对数据库初始化并插入节点信息，在数据库中执行以下sql步骤：

```sql
登录数据库执行：

<--此处注意使用create-eggroll-meta-tables.sql文件的绝对路径-->
>>source Eggroll安装目录/conf/create-eggroll-meta-tables.sql;

<--将集群内所有节点clustermanager和nodemanager服务信息插入server_node表中-->
>>INSERT INTO server_node (host, port, node_type, status) values ('clustermanager服务ip', 'clustermanager服务端口', 'CLUSTER_MANAGER', 'HEALTHY');
>>INSERT INTO server_node (host, port, node_type, status) values ('nodemanager服务ip', 'nodemanager服务port', 'NODE_MANAGER', 'HEALTHY');
```

执行完成执行查询server_node表检查数据是否准确：

```sql
>>select * from server_node;
>>exit
```



## 5.    服务启动

Eggroll的bin目录中附带启动脚本bin/eggroll.sh使用说明：

```shell
sh bin/eggroll.sh $1 $2		
<--
	$1：需要执行操作的服务名称，例如clustermanager，nodemanager，rollsite，all(表示所有服务)；
	$2：需要执行的操作，例如start(启动)，status（查看状态），stop（关闭），restart（重启）
-->
```

使用例子：

```shell
<--启动所有服务-->
sh bin/eggroll.sh all start

<--查看clustermanager服务状态-->
sh bin/eggroll.sh clustermanager status

<--重启rollsite服务-->
sh bin/eggroll.sh rollsite restart

<--关闭nodemanager服务-->
sh bin/eggroll.sh nodemanager stop
```

将各节点对应的服务启动成功后，部署完成，进入测试步骤。



## 6.    测试

### 6.1.  初始化环境变量

登录服务器进行测试时需要执行以下语句进行环境变量初始化

```shell
export EGGROLL_HOME=Eggroll安装绝对路径		--例如/data/projects/eggroll
export PYTHONPATH=${EGGROLL_HOME}/python		
source virtualenv/bin/activate				--进入virtualenv独立环境
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