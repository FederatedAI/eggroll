#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#	  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#
cwd=$(cd `dirname $0`; pwd)
cd $cwd
source ./conf.sh
version=`grep version ../BUILD_INFO | awk -F= '{print $2}'`

sed -i "s#EGGROLL_HOME=.*#EGGROLL_HOME=${EGGROLL_HOME}#g" ./init_env.sh
cp ./init_env.sh ../

cd ..
if [[ ! -d "lib" ]]; then
    mkdir lib
fi

cp -r ./jvm/core/target/eggroll-core-${version}.jar ./lib
cp -r ./jvm/core/target/lib/* ./lib
cp -r ./jvm/roll_pair/target/eggroll-roll-pair-${version}.jar ./lib
cp -r ./jvm/roll_pair/target/lib/* ./lib
cp -r ./jvm/roll_site/target/eggroll-roll-site-${version}.jar ./lib
cp -r ./jvm/roll_site/target/lib/* ./lib
cp -r ./jvm/roll_frame/target/eggroll-roll-frame-${version}.jar ./lib
cp -r ./jvm/roll_frame/target/lib/* ./lib
cp ./jvm/core/main/resources/create-eggroll-meta-tables.sql ./deploy

tar -czf eggroll.tar.gz lib bin conf data python deploy init_env.sh

get_property() {
	property_value=`grep $1 $cwd/../conf/eggroll.properties | awk -F= '{print $2}'`
}

jdbc_ip=`grep 'jdbc:mysql' $cwd/../conf/eggroll.properties | awk -F// '{print $2}' |awk -F: '{print $1}'`
jdbc_database=`grep 'jdbc:mysql' $cwd/../conf/eggroll.properties | awk -F/ '{print $4}' | awk -F? '{print $1}'`
get_property "eggroll.resourcemanager.clustermanager.jdbc.username"
jdbc_name=$property_value
get_property "eggroll.resourcemanager.clustermanager.jdbc.password"
jdbc_password=$property_value
get_property "eggroll.resourcemanager.clustermanager.host"
clustermanager_ip=$property_value
get_property "eggroll.resourcemanager.clustermanager.port"
clustermanager_port=$property_value
get_property "eggroll.resourcemanager.nodemanager.port"
nodemanager_port=$property_value

sed -i "s/eggroll_meta/$jdbc_database/g" ./deploy/create-eggroll-meta-tables.sql
echo "insert into server_node (name,host,port,node_type,status) values('$clustermanager_ip','$clustermanager_ip','$clustermanager_port','CLUSTER_MANAGER','HEALTHY');" >> ./deploy/create-eggroll-meta-tables.sql

for ip in ${iplist[@]};do
	echo "insert into server_node (name,host,port,node_type,status) values('$ip','$ip','$nodemanager_port','NODE_MANAGER','HEALTHY');" >> ./deploy/create-eggroll-meta-tables.sql
	if ssh -tt app@$ip test -e ${EGGROLL_HOME};then
		echo "[INFO] ${EGGROLL_HOME} in $ip already exist"
	else
		echo "[INFO] ${EGGROLL_HOME} in app@$ip is not exist,execute: mkdir -p ${EGGROLL_HOME}"
		ssh -tt app@$ip "mkdir -p ${EGGROLL_HOME}"
		if ssh -tt app@$ip test -e ${EGGROLL_HOME};then
			echo "[INFO] execute: mkdir -p ${EGGROLL_HOME} success"
		else
			echo "[ERROR] execute: mkdir -p ${EGGROLL_HOME} fail,please login app@$ip and mkdir -p ${EGGROLL_HOME}"
		fi
	fi
	scp eggroll.tar.gz app@$ip:${EGGROLL_HOME}
	ssh -tt app@$ip "cd ${EGGROLL_HOME};tar -xzf eggroll.tar.gz;rm -f eggroll.tar.gz"
done

if ssh -tt app@$jdbc_ip test -e ${EGGROLL_HOME}/deploy;then
    echo "[INFO] ${EGGROLL_HOME}/deploy in $jdbc_ip already exist"
else
    echo "[INFO] ${EGGROLL_HOME}/deploy in app@$jdbc_ip is not exist,execute: mkdir -p ${EGGROLL_HOME}/deploy"
fi

scp ./deploy/create-eggroll-meta-tables.sql app@$jdbc_ip:${EGGROLL_HOME}/deploy

if ssh -tt app@$jdbc_ip test -e ${MYSQL_HOME};then
    echo "[INFO] ${MYSQL_HOME} in $jdbc_ip already exist"
    ssh -tt app@$jdbc_ip << eeooff
cd ${EGGROLL_HOME}/deploy
${MYSQL_HOME}/bin/mysql -u$jdbc_name -p$jdbc_password -S ${MYSQL_HOME}/mysql.sock
source ${EGGROLL_HOME}/deploy/create-eggroll-meta-tables.sql
exit;
exit
eeooff
else
    echo "[INFO] ${MYSQL_HOME} in app@$jdbc_ip is not exist,please check your mysql configuration."
fi

rm -f eggroll.tar.gz 

cd $cwd
