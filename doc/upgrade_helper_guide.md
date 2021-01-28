
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
 -f --recover <可选项.eggroll 升级失败恢复默认参数0升级,1 回滚>
```

## 升级前准备
停服务
```
[app@node1 eggroll]$ source /data/projects/fate/bin/init_env.sh
(venv) [app@node1 eggroll]$ cd $EGGROLL_HOME
(venv) [app@node1 eggroll]$ sh /data/projects/fate/eggroll/bin/eggroll.sh all stop

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
由于eggroll2.0.x以上版本已经删除掉了roll_pair_master，所以需要删除roll_pair_master所有相关配置以及脚本
请将原来的${eggroll_home}/conf/eggroll.properties复制一份到当前eggroll/conf目录下并修改如下注释掉以下配置：

```
cp ${EGGROLL_HOME}/conf/eggroll.properties eggroll/conf/
vim eggroll.properties

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

#### 3.1.2 cm_ip_list文件
即需要升级eggroll版本所在的clustermanager或nodemanager节点ip列表集合

```
touch cm_ip_list
vim  cm_ip_list
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
此文件不能包含带有注释的sql语句
```
touch mysql_file.sql
vim upgrade_tool/mysql_file.sql
use eggroll_mata;
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
 -f --recover <upgrade fail recover default -1 upgrade recover 0 recover>
```

### 4.2 脚本调用

执行脚本前先进入EGGROLL_HOME目录下,以下目录本机备份,放置脚本中途因参数错误等业务因素操作不当,需要多次执行的情况,手动还原后方可再次执行升级脚本
```
(venv) [app@node1 eggroll]$cd $EGGROLL_HOME
(venv) [app@node1 eggroll]$mv bin bin_bakbak
(venv) [app@node1 eggroll]$mv deploy deplpy_bakbak
(venv) [app@node1 eggroll]$mv lib lib_bakbak
(venv) [app@node1 eggroll]$mv conf conf_bakbak
(venv) [app@node1 eggroll]$mv python python_bakbak

(venv) [app@node1 eggroll]$cp -r bin_bakbak bin
(venv) [app@node1 eggroll]$cp -r conf_bakbak conf
(venv) [app@node1 eggroll]$cp -r deploy_bakbak deploy
(venv) [app@node1 eggroll]$cp -r lib_bakbak lib
(venv) [app@node1 eggroll]$cp -r python_bakbak python

### 同理mysql 手动dump一份
$MYSQL_HOME_PATH/bin/mysqldump -h mysql-host -u username -p passwd -P port -S sock-file database > dump_bakbak.sql

```

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
-s mysql_file.sql \
> log.log

```

### 4.3 检查日志

```
(venv) [app@node1 upgrade-tool]$ less log.log
```

## 5. 升级失败恢复集群所有升级节点

### 5.1 eggroll还原

进入升级脚本所在节点的eggroll_home目录 -> 将bak结尾的目录或文件改名为程序正常执行的目录

增加升级脚本参数: **-f 1**
```
(venv) [app@node1 upgrade-tool]$ cd $EGGROLL_HOME

(venv) [app@node1 eggroll]$ rm -rf bin deploy lib python python
(venv) [app@node1 eggroll]$ mv bin_bakbak bin
(venv) [app@node1 eggroll]$ mv deploy_bakbak deploy
(venv) [app@node1 eggroll]$ mv lib_bakbak lib
(venv) [app@node1 eggroll]$ mv python_bakbak python
(venv) [app@node1 eggroll]$ mv conf_bakbak conf
(venv) [app@node1 eggroll]$ ls -l 


```

### 5.2 登录mysql检查
```
(venv) [app@node1 eggroll]$/data/projects/fate/common/mysql/mysql-8.0.13/bin/mysql -ufate -p -S ./data/projects/jiexiao/fate/common/mysql/mysql-8.0.13/run/mysql.sock -h 192.168.0.1 -P 3306

```



