
# eggroll升级工具文档说明
此文档兼容eggroll2.0.0 -> 2.2.1、2.2.2

## 1. 环境要求
### 1.1 python3环境
### 1.2 执行脚本在的集群节点机器必须是clustermanager或者nodemanager所在节点
### 1.3 需要app用户登录执行

## 2. 参数说明
```
 -c --cm_file <eggroll集群cm或nm节点ip集合>
 -r --rs_file <eggroll集群仅仅部署rollsite节点的ip集合,内容可以为空,文件不能为空>
 -e --egg_home <eggroll家的目录>
 -m --mysql_home <mysql家的目录>
 -t --mysql_host  <mysql主机ip地址>
 -p --mysql_port <mysql端口号>
 -b --mysql_db <mysql eggroll元数据库名称>
 -u --mysql_user <mysql  eggroll元数据库登录用户名称>
 -w --mysql_pwd <mysql eggroll元数据库登录用户密码>
 -S --mysql-sock <mysql sock文件路径>
 -s --mysql_file <mysql eggroll元数据修改内容sql文件集>
```

## 3. 使用说明

### 3.1 解压eggroll升级工具包(upgrade-tool.zip)

进入升级目录

```
unzip upgrade-tool.zip
cd upgrade-tool
```

目录结构说明

#### 3.1.1 子目录是eggroll升级变动最新的文件目录

**注意:**
由于2.2版本已经删除掉了roll_pair_master，所以需要删除roll_pair_master所有相关配置以及脚本
请将原来的${eggroll_home}/conf/eggroll.properties复制一份到当前eggroll/conf目录下并修改如下注释掉以下配置：

```
cp ${EGGROLL_HOME}/conf/eggroll.properties eggroll/conf/
vim $EGGROLL_HOME/conf/eggroll.properties

 ###`eggroll.resourcemanager.bootstrap.roll_pair_master.exepath=bin/roll_pair/roll_pair_master_bootstrap.sh`
 ###`eggroll.resourcemanager.bootstrap.roll_pair_master.javahome=`
 ###`eggroll.resourcemanager.bootstrap.roll_pair_master.classpath=conf/:lib/*`
 ###`eggroll.resourcemanager.bootstrap.roll_pair_master.mainclass=com.webank.eggroll.rollpair.RollPairMasterBootstrap`
 ###`eggroll.resourcemanager.bootstrap.roll_pair_master.jvm.options=`
```

```
├─eggroll
      ├─bin 
      ├─conf 
      ├─deploy 
      ├─lib  
      └─python 
   
```

#### 3.1.2 nm_ip_list文件
即需要升级eggroll版本所在的clustermanager或nodemanager节点ip列表集合

```
touch nm_ip_list
vim  nm_ip_list
192.168.0.1
192.168.0.2
...
```

### 3.1.3 rs_ip_list文件
即需要升级eggroll版本所在的rollsite节点ip列表集合,可以为空,文件不能为空

```
touch  upgrade_tool/rs_ip_list
vim rs_ip_list

```

### 3.1.4 mysql_file.sql文件

即升级eggroll元数据修改内容sql文件集

```
touch mysql_file.sql
vim upgrade_tool/mysql_file.sql
use eggroll_mata;
alter table store_option modify column store_locator_id bigint unsigned;
alter table store_option add store_option_id SERIAL PRIMARY KEY;
alter table session_option add store_option_id SERIAL PRIMARY KEY;
alter table session_main modify column session_id VARCHAR(767);
alter table session_processor modify column session_id VARCHAR(767);
... ...
```

## 4 脚本执行

### 4.1 使用-h 打印命令行帮助

```
(venv) [app@node1 upgrade-tool]$ python upgrade_helper.py -h
python upgrade_helper.py 
 -c --cm_file <input eggroll upgrade clustermanager or namenode node ip sets>
 -r --rs_file <input eggroll upgrade only rollsite node ip sets>
 -e --egg_home <eggroll home path>
 -m --mysql_home <mysql home path>
 -t --mysql_host <mysql ip addr>
 -p --mysql_port <mysql port>
 -b --mysql_db <mysql database>
 -u --mysql_user <mysql username>
 -w --mysql_pwd <mysql passwd>
 -S --mysql-sock <mysql sock文件路径>
 -s --mysql_file <mysql upgrade content sql file sets>

```

### 4.2 脚本调用

```
(venv) [app@node1 upgrade-tool]$ python upgrade_helper.py \
-c cm_ip_list \
-r rs_ip_list \
-e /data/projects/fate/eggroll \
-m /data/projects/fate/common/mysql/mysql-8.0.13 \
-t 192.168.0.1 \
-p 3306 \
-b eggroll_meta \
-u fate \
-w pwsswd \
-S /data/projects/fate/common/mysql/mysql-8.0.13/run/mysql.sock \
-s mysql_file.sql >> log.log

```

### 4.3 检查日志

```
(venv) [app@node1 upgrade-tool]$ less log.log
```

## 5. 升级失败需要手动恢复集群所有升级节点

### 5.1 eggroll 手动恢复

进入eggroll部署目录 -> 依次将bak结尾的目录或文件改名为程序正常执行的目录
```
(venv) [app@node1 upgrade-tool]$ cd $EGGROLL_HOME
(venv) [app@node1 eggroll]$ mv bin_xxxx_bak bin
(venv) [app@node1 eggroll]$ mv deploy_xxxx_bak deploy
(venv) [app@node1 eggroll]$ mv lib_xxxxx_bak lib
(venv) [app@node1 eggroll]$ mv python_xxxx_bak python
(venv) [app@node1 eggroll]$ mv conf/eggroll.properties.bakxxxxx conf/eggroll.properties

```

### 5.2 mysql 手动恢复

*测试阶段 请注意查看已有eggroll_meta元数据库是不是最新的*

登录mysql主机 -> 进入mysql家目录 -> 使用mysql命令从备份文件还原eggroll MySQL元数据库 
```
(venv) [app@node1 ~] ssh app@${mysql_host}
(venv) [app@node1 ~] sudo su - app
(venv) [app@node1 ~] cd ${MYSQL_HOME}
(venv) [app@node1 mysql-8.0.13] sh /bin/mysql -u ${username} -p${passwd} -P${port} -h ${mysql_host} -S ${MYSQL_HOME}/run/mysql.soct < ~/dump_backup_eggroll_upgrade_xxxxxxx.sql

```



