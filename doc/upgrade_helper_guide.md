
# eggroll升级工具文档说明
此文档兼容eggroll2.0.x -> 2.2.1

## 1. 环境要求
### 1.1   python3环境
### 1.2  执行脚本的机器需要能免密登录Eggroll集群的所有节点
### 1.3  需要app用户登录执行
### 1.4  执行mysql备份等操作需要先确认是否允许ip操作权限

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
sh service.sh stop fate-clustermanager
sh service.sh stop fate-nodemanager
sh service.sh stop fate-rollsite
```

- eggroll手动备份

```
cd ${EGGROLL_HOME}
mv bin bin_bak
mv deploy deploy_bak
mv lib lib_bak
mv python python_bak

cp -r bin_bak bin
cp -r deploy_bak deploy
cp -r lib_bak lib
cp -r python_bak python

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
export PKG_BASE_PATH={your upgrade eggroll version eggroll home dir}
python ${PKG_BASE_PATH}/deploy/upgrade_helper.py --help
```

> 获取升级包

> 升级包目录结构

```
├─eggroll
      ├─bin 
      ├─deploy 
      ├─lib  
      └─python 
   
```


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
python ${PKG_BASE_PATH}/deploy/upgrade_helper.py --help
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
python ${PKG_BASE_PATH}/deploy/upgrade_helper.py \
-c ${your create nm_ip_list file path contains the file name} \
-r ${your create rs_ip_list file path contains the file name} \
-e ${EGGROLL_HOME} \
-k ${PKG_BASE_PATH} \
-m ${mysql home path} \
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


## 5. 升级失败恢复EGGROLL集群所有的升级节点

- 5.1 eggroll还原

进入升级脚本所在节点的eggroll_home目录 -> 将bak结尾的目录或文件删除新的复制一份备份目录

```
cd ${EGGROLL_HOME}
rm -rf bin deploy lib python
cp -r bin_bak bin
cp -r deploy_bak deploy
cp -r lib_bak lib
cp -r python_bak python

```

- 5.2 EGGROLL元数据库恢复

```
${MYSQL_HOME}/bin/mysql -ufate -p -S ${MYSQL_HOME}/run/mysql.sock -h 192.168.0.1 -P 3306 --default-character-set=utf8 eggroll_meta < dump_bakbak.sql
```
- 5.3 回滚执行

```
python ${PKG_BASE_PATH}/deploy/upgrade_helper.py \
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


- 5.4 重复#4.3 ~ 4.5步骤

## 6. 常见问题与注意事项

- 6.1

> **Q:** 如果以前ssh不能直接登录到目标机器，需要指定端口，怎么办呢？

> **A:** 在执行升级脚本前，执行以下语句，指定ssh端口：
```
export "RSYNC_RSH=ssh -p ${ssh_port}"
echo $RSYNC_RSH
```
> 其中${ssh_port}为以前登录到目标机器时需要指定的端口。

- 6.2

> **Q:** 如果我使用ansible模式部署fate，有什么要注意呢?

> **A:** 使用ansible部署fate，需要在完成升级脚本后进行以下操作：

```
cp ${EGGROLL_HOME}/bin/eggroll.sh ${EGGROLL_HOME}/bin/fate-eggroll.sh
sed -i '26 i source $cwd/../../bin/init_env.sh' ${EGGROLL_HOME}/bin/fate-eggroll.sh

```

