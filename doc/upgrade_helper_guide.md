
# eggroll升级工具文档说明

## 1. 环境要求
### 1.1 python3环境
### 1.2 执行脚本在的集群节点机器必须是clustermanager或者nodemanager所在节点
### 1.3 需要app用户登录执行

## 2. 工具代码
[upgrade-helper](https://github.com/WeBankFinTech/eggroll/blob/feature-2.2.1-db-passwd/bin/upgrade_helper.py)

## 3. 使用说明
### 3.1创建升级文件包eggroll目录
子目录是eggroll升级变动最新的文件目录
**[warning]:conf目录放置最新的eggroll.properties**
```
├─eggroll
   │  ├─bin
   |  ├─conf
   │  ├─deploy
   │  ├─lib
   │  └─python
   |
```

### 3.2创建nm_ip_list文件
即需要升级eggroll版本所在的clustermanager或nodemanager节点ip列表集合
e.g
```
192.168.0.1
192.168.0.2
...
```

### 3.3创建rs_ip_list文件

即需要升级eggroll版本所在的rollsite节点ip列表集合
**[warning]:此文件是单独部署rollsite节点的ip集合,不与nm_ip_list文件冲突**
e.g
```
192.168.0.3
192.168.0.4
...

```

### 3.4创建mysql_file.sql文件

即升级eggroll依赖mysql变动sql文件集合
e.g:
```
use eggroll_mata;
alter table store_option modify column store_locator_id bigint unsigned;
alter table store_option add store_option_id SERIAL PRIMARY KEY;
alter table session_option add store_option_id SERIAL PRIMARY KEY;
alter table session_main modify column session_id VARCHAR(767);
alter table session_processor modify column session_id VARCHAR(767);
... ...
```

### 3.5 脚本使用

```
## 使用-h 打印命令行帮助
(venv) [app@node5 upgrade-tool]$ python upgrade_helper.py -h
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
 -s --mysql_file <mysql upgrade content sql file sets>

python upgrade_helper.py -c cm_ip_list -r rs_ip_list -e /data/projects/fate/eggroll -m /data/projects/fate/common/mysql/mysql-8.0.13 -t 192.168.0.1 -p 3306 -b eggroll_meta -u fate -w pwsswd -s mysql_file.sql 

```
*[warning] 脚本工具、升级包目录、文件建议放置在单独的目录*

## 4.备份目录说明

### 4.1 eggroll备份

eggroll备份在脚本所在节点上

```
cd  $EGGROLL_HOME
```

### 4.2 mysql 备份

mysql 备份在mysql所在节点的app用户home目录下

```
cat ~/dump_backup_eggroll_upgrade_xxxxx.sql
```


