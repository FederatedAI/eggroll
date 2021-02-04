
# eggroll升级工具文档说明
此文档兼容eggroll2.0.x -> 2.2.1、2.2.2 ...

## 1. 环境要求
### 1.1 python3环境
### 1.2 执行脚本在的集群节点机器必须是nodemanager任意节点
### 1.3 需要app用户登录执行
### 1.4 执行mysql备份等操作需要先确认是否允许ip操作权限

## 2. 参数说明
```
 -c --nm_file <必选:eggroll集群cm或nm节点ip集合>
 -r --rs_file <必选:eggroll集群仅仅部署rollsite节点的ip集合,内容可以为空,文件不能为空>
 -e --egg_home <必选:eggroll home目录>
 -k --pkg_base_path <必选:eggroll 升级版本文件的基路径,路径下同EGGROLL_HOME目录一样,见3.1.1
 -m --mysql_home <必选:mysql home 目录>
 -t --mysql_host  <必选:mysql主机ip地址>
 -p --mysql_port <必选:mysql端口号>
 -b --mysql_db <必选:mysql eggroll元数据库名称>
 -u --mysql_user <必选:mysql  eggroll元数据库登录用户名称>
 -w --mysql_pwd <必选:mysql eggroll元数据库登录用户密码>
 -S --mysql-sock <必选:mysql sock文件路径>
 -s --mysql_file <必选:mysql eggroll元数据修改内容sql文件集>
 -f --recover <可选项:eggroll 升级失败恢复默认参数0升级,1 回滚>
```

## 升级前准备

- 切换虚拟环境
进入fate安装目录,找到环境变量shell
```
eg:
source /data/projects/fate/bin/init_env.sh
```

- 停服务

> 进入eggroll home目录

- allinone 部署方式的停止服务方法
```
cd ${EGGROLL_HOME}
sh bin/eggroll.sh all stop
```
- ansible 部署方式的停止服务方法
```
cd /data/projects/common/supervisord
sh service.sh fate-clustermanager stop
sh service.sh fate-nodemanager stop
sh service.sh fate-rollsite stop
```

- eggroll手动备份

```
cd ${EGGROLL_HOME}
mv bin bin_bakbak
mv deploy deploy_bakbak
mv lib lib_bakbak
mv conf conf_bakbak
mv python python_bakbak

cp -r bin_bakbak bin
cp -r conf_bakbak conf
cp -r deploy_bakbak deploy
cp -r lib_bakbak lib
cp -r python_bakbak python

```

- 手动备份eggroll元数据库

> 1、只需备份`eggroll_meta`数据库即可

> 2、${MYSQL_HOME_PATH}为MYSQL安装路径

> 3、注意-p后面不能留空格

```
${MYSQL_HOME_PATH}/bin/mysqldump -h <mysql-host> -u <username> -p<passwd> -P <port> -S <sock-file> <database> > dump_bakbak.sql
```



## 3. 使用说明

### 3.1 

> 获取升级脚本

[升级脚本](https://github.com/WeBankFinTech/eggroll/blob/dev-2.2.1/deploy/upgrade_helper.py)

```
python upgrade_helper.py --help
```

> 获取升级包

> 1、解压

```
tar -xf eggroll-2.2.1.tar.gz
```

> 2、升级包目录结构
```
├─eggroll
      ├─bin 
      ├─conf 
      ├─deploy 
      ├─lib  
      └─python 
   
```
> 3、完善conf目录

- 复制一份`eggroll.properties`配置文件到升级包conf目录下

> ${PKG_BASE_PATH} 为最新升级的`base path`

```
export PKG_BASE_PATH=`Your own directory of upgrade packages`
cp ${EGGROLL_HOME}/conf/eggroll.properties ${PKG_BASE_PATH}/eggroll/conf/
```

> 检查`eggroll.properties`配置文件根据如下升级版本号是否决定修改

如果是eggroll_2.0.x -> 2.2.x 这个版本的升级,需要修改项如下:

```

vim ${PKG_BASE_PATH}/eggroll/conf/eggroll.properties

 #`eggroll.resourcemanager.bootstrap.roll_pair_master.exepath=bin/roll_pair/roll_pair_master_bootstrap.sh`
 #`eggroll.resourcemanager.bootstrap.roll_pair_master.javahome=`
 #`eggroll.resourcemanager.bootstrap.roll_pair_master.classpath=conf/:lib/*`
 #`eggroll.resourcemanager.bootstrap.roll_pair_master.mainclass=com.webank.eggroll.rollpair.RollPairMasterBootstrap`
 #`eggroll.resourcemanager.bootstrap.roll_pair_master.jvm.options=`

```

如果是eggroll_2.2.x -> 2.2.x 这个版本的升级,暂不需要修改


#### 3.1.2 创建nm_ip_list文件

即需要升级eggroll版本所在的nodemanager节点ip列表集合

```

touch nm_ip_list
vim  nm_ip_list
192.168.0.1
192.168.0.2

```

### 3.1.3 rs_ip_list文件

即需要升级eggroll版本所在的rollsite节点ip列表集合,可以为空,文件不能为空
此文件仅仅加入独立部署的rollsite节点ip,不与nm_ip_list文件内容ip冲突,否则升级失败自行排查问题

```
touch  rs_ip_list
```

### 3.1.4 mysql_file.sql文件

即升级eggroll元数据修改内容sql文件集
此文件不能包含带有注释的sql语句,否则升级失败

- 此文件需要根据eggroll升级版本号变更eggroll元数据库

> 1、eggroll_2.0.x -> 2.2.x

```
touch mysql_file.sql
vim mysql_file.sql

use eggroll_meta;
alter table store_option modify column store_locator_id bigint unsigned;
alter table store_option add store_option_id SERIAL PRIMARY KEY;
alter table session_option add store_option_id SERIAL PRIMARY KEY;
alter table session_main modify column session_id VARCHAR(767);
alter table session_processor modify column session_id VARCHAR(767);
```

> 2、eggroll_2.2.x -> eggroll_2.2.x

```
touch mysql_file.sql
vim mysql_file.sql

use eggroll_meta;
alter table store_option add store_option_id SERIAL PRIMARY KEY;
alter table session_option add store_option_id SERIAL PRIMARY KEY;
alter table session_main modify column session_id VARCHAR(767);
alter table session_processor modify column session_id VARCHAR(767);


```

## 4 脚本执行

- 4.1 使用-h 打印命令行帮助

```
python ${your put script path}/upgrade_helper.py --help
python upgrade_helper.py 
 -c --nm_file <input eggroll upgrade  namenode node ip sets>
 -r --rs_file <input eggroll upgrade only rollsite node ip sets>
 -e --egg_home <eggroll home path>
 -k --pkg_base_path <The base path of the EggRoll upgrade package,This directory contains the following files:见3.1.1>
 -m --mysql_home <mysql home path>
 -t --mysql_host <mysql ip addr>
 -p --mysql_port <mysql port>
 -b --mysql_db <mysql database>
 -u --mysql_user <mysql username>
 -w --mysql_pwd <mysql passwd>
 -S --mysql-sock <mysql sock文件路径>
 -s --mysql_file <mysql upgrade content sql file sets>
 -f --recover <upgrade fail recover default 0 upgrade recover 1 recover>
```

- 4.2 启动升级

```
python ${your put script path}/upgrade_helper.py \
-c ${your create nm_ip_list file path contains the file name} \
-r ${your create rs_ip_list file path contains the file name} \
-e ${EGGROLL_HOME} \
-k ${PKG_BASE_PATH} \
-m ${MYSQL_HOME} \
-t ${your install fate with mysql ip} \
-p ${your install fate with mysql port} \
-b eggroll_meta \
-u ${your install fate with mysql username} \
-w ${your install fate with mysql passwd} \
-S ${MYSQL_HOME}/run/mysql.sock \
-s ${your create mysql_file sql file path contains the file name} \
> log.info

```
> 执行一次失败后,恢复`eggroll手动备份`方可再次执行升级脚本

> 以上文件、目录均为绝对路径

- 4.3 检查日志
```
less log.info | grep "current machine not install mysql "
```

- 4.4 当前升级脚本所在机器没有安装MYSQL

> 1、前往mysql所在机器

> 2、登录

> 3、详细见#3.1.4更新


- 4.5 版本检查
```
cat $EGGROLL_HOME/python/eggroll/__init__.py

```

- 4.6 登录EGGROLL元数据库

>1 登录MYSQL
```
${MYSQL_HOME}/bin/mysql -ufate -p -S ${MYSQL_HOME}/run/mysql.sock -h ${your install fate with mysql ip} -P ${your install fate with mysql port}

```
>2 清空如下数据库表
```
delete from eggroll_meta.session_main;
delete from eggroll_meta.session_option;
delete from eggroll_meta.session_processor;
delete from eggroll_meta.store_locator;
delete from eggroll_meta.store_option;
delete from eggroll_meta.store_partition;   

delete from fate_flow.componentsummary   ; 
delete from fate_flow.t_engine_registry   ; 
delete from fate_flow.t_job    ; 
delete from fate_flow.t_machine_learning_model_info ; 
delete from fate_flow.t_model_operation_log   ; 
delete from fate_flow.t_model_tag        ; 
delete from fate_flow.t_session_record   ; 
delete from fate_flow.t_storage_table_meta    ; 
delete from fate_flow.t_tags   ; 
delete from fate_flow.t_task   ; 
delete from fate_flow.trackingmetric     ; 
delete from fate_flow.trackingoutputdatainfo        ;

```

>3 重启eggroll和fate服务

```
启动eggroll
cd ${EGGROLL_HOME}
sh bin/eggroll.sh all start

启动fate_flow
cd /data/projects/fate/python/fate_flow
sh service.sh restart

```

## 5. 升级失败恢复集群所有升级节点

- 5.1 eggroll还原

进入升级脚本所在节点的eggroll_home目录 -> 将bak结尾的目录或文件删除新的复制一份备份目录

```
cd ${EGGROLL_HOME}
rm -rf bin deploy lib python conf
cp -r bin_bakbak bin
cp -r conf_bakbak conf
cp -r deploy_bakbak deploy
cp -r lib_bakbak lib
cp -r python_bakbak python

```

- 5.2 失败回滚

```
python ${your put script path}/upgrade_helper.py \
-c ${your create nm_ip_list file path contains the file name} \
-r ${your create rs_ip_list file path contains the file name} \
-e ${EGGROLL_HOME} \
-k ${PKG_BASE_PATH} \
-m ${MYSQL_HOME} \
-t ${your install fate with mysql ip} \
-p ${your install fate with mysql port} \
-b eggroll_meta \
-u ${your install fate with mysql username} \
-w ${your install fate with mysql passwd} \
-S ${MYSQL_HOME}/run/mysql.sock \
-s ${your create mysql_file sql file path contains the file name} \
-f 1
> recover.info

```

- 5.3 当前升级脚本所在机器没有安装MYSQL

> 前往EGGROLL元数据库还原

```
${MYSQL_HOME}/bin/mysql -ufate -p -S ${MYSQL_HOME}/run/mysql.sock -h 192.168.0.1 -P 3306 --default-character-set=utf8 eggroll_meta < dump_bakbak.sql
```

- 5.4 重复#4.3 ~ 4.6步骤



