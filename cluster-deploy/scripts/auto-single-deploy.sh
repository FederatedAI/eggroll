#!/bin/bash
cwd=`pwd`

cd ../../
eggroll_dir=`pwd`
output_dir=$eggroll_dir/cluster-deploy/example-dir-tree

clustercomm_port=9394
metaservice_port=8590
proxy_port=9370
roll_port=8011
egg_port=7888
storage_port=7778
exchange_port=9370
processor_port=5000
processor_count=16

modules=(clustercomm metaservice egg proxy roll mysql)

cd $cwd
source ./single-configurations.sh

echo "[INFO] the port of clustercomm is ${clustercomm_port}"
echo "[INFO] the port of metaservice is ${metaservice_port}"
echo "[INFO] the port of proxy is ${proxy_port}"
echo "[INFO] the port of roll is ${roll_port}"
echo "[INFO] the port of egg is ${egg_port}"
echo "[INFO] the port of storageservice is ${storage_port}"
echo "[INFO] the port of processor is beginning at ${processor_port}"
echo "[INFO] the port of exchange is ${exchange_port}"

cd $output_dir
sed -i "s#export JAVA_HOME=.*#export JAVA_HOME=$javadir#g" ./services.sh
sed -i "s#installdir=.*#installdir=$dir#g" ./services.sh
sed -i "s/port=.*/port=${storage_port}/g" ./services.sh

if test -e $dir;then
	echo "[INFO] cp services.sh file to $dir:"
else
	echo "[INFO] $dir is not exist,execute: mkdir -p $dir"
	mkdir -p $dir
	if test -e $dir;then
		echo "[INFO] execute: mkdir -p $dir success"
		echo "[INFO] cp services.sh file to $dir:"
	else
		echo "[ERROR] execute: mkdir -p $dir fail,please mkdir -p $dir first"
	fi
fi

cp -r ./services.sh ./packages $dir/

echo "[INFO] the path of JAVA_HOME is $javadir"
echo "[INFO] deploy Eggroll in directory: $dir"
echo "====================================================="
echo "[INFO] the partyid is $partyid"
echo "[INFO] the single ip of $partyid is $ip"

all() {
	for module in "${modules[@]}"; do
        echo
		echo "[INFO] $module is deploying:"
        echo "=================================="
        $module
        echo "----------------------------------"
		echo "[INFO] $module is deployed over."
	done
}

multiple() {
    total=$#
    for (( i=1; i<total+1; i++)); do
        module=${!i//\//}
        echo
		echo "[INFO] $module is deploying:"
        echo "=================================="
        $module
        echo "-----------------------------------"
		echo "[INFO] $module is deployed over."
    done
}

usage() {
    echo "usage: $0 {all|[module1, ...]}"
}

clustercomm() {
	echo "[INFO] the clustercomm of $partyid is $ip"

	sed -i "s/party.id=.*/party.id=$partyid/g" ./clustercomm/conf/clustercomm.properties
	sed -i "s/service.port=.*/service.port=${clustercomm_port}/g" ./clustercomm/conf/clustercomm.properties
	sed -i "s/meta.service.ip=.*/meta.service.ip=$ip/g" ./clustercomm/conf/clustercomm.properties
	sed -i "s/meta.service.port=.*/meta.service.port=${metaservice_port}/g" ./clustercomm/conf/clustercomm.properties
		
	echo "[INFO] cp clustercomm file to $dir:"
	cp -r clustercomm $dir/
	
	if test -e $dir/clustercomm;then
		echo "[INFO] the clustercomm is deployed success."
	else
		echo "[ERROR] the clustercomm is deployed fail,please try it again."
	fi
	
	echo "[INFO] $dir/clustercomm/conf/clustercomm.properties of clustercomm in $partyid has been modified as follows:"
	cat $dir/clustercomm/conf/clustercomm.properties | grep -v "#" | grep -v ^$
}

metaservice() {
	eval jdbcip=${jdbc[0]}
	eval jdbcdbname=${jdbc[1]}
	eval jdbcuser=${jdbc[2]}
	eval jdbcpasswd=${jdbc[3]}
	
	echo "[INFO] the meta-service of $partyid is $ip"
	echo "[INFO] the mysql-server of $partyid is $jdbcip"
	echo "[INFO] the database name of $partyid is $jdbcdbname"
	echo "[INFO] the database user of $partyid is $jdbcuser"

	sed -i "s/party.id=.*/party.id=$partyid/g" ./meta-service/conf/meta-service.properties
	sed -i "s/service.port=.*/service.port=${metaservice_port}/g" ./meta-service/conf/meta-service.properties
	sed -i "s#//.*?#//$jdbcip:3306/$jdbcdbname?#g" ./meta-service/conf/meta-service.properties
	sed -i "s/jdbc.username=.*/jdbc.username=$jdbcuser/g" ./meta-service/conf/meta-service.properties
	sed -i "s/jdbc.password=.*/jdbc.password=$jdbcpasswd/g" ./meta-service/conf/meta-service.properties
	
	echo "[INFO] cp meta-service file to $dir:"
	cp -r meta-service $dir/
	
	if test -e $dir/meta-service;then
		echo "[INFO] the meta-service is deployed success."
	else
		echo "[ERROR] the meta-service is deployed fail,please try it again."
	fi
	
	echo "[INFO] $dir/meta-service/conf/meta-service.properties of meta-service in $partyid has been modified as follows:"
	cat $dir/meta-service/conf/meta-service.properties | grep -v "#" | grep -v ^$
}

proxy() {	
	echo "[INFO] the partyid is $partyid"
	echo "[INFO] the proxy of $partyid is $ip"
	
	sed -i "s/coordinator=.*/coordinator=$partyid/g" ./proxy/conf/proxy.properties
	sed -i "s/ip=.*/ip=$ip/g" ./proxy/conf/proxy.properties
	sed -i "s/port=.*/port=${proxy_port}/g" ./proxy/conf/proxy.properties
	sed -i "s#route.table=.*#route.table=$dir/proxy/conf/route_table.json#g" ./proxy/conf/proxy.properties
	
	sed -i "s/exchangeip=.*/exchangeip=\"$exchange\"/g" $cwd/modify_json.py
	sed -i "s/exchangeport=.*/exchangeport=${exchange_port}/g" $cwd/modify_json.py
	sed -i "s/clustercommip=.*/clustercommip=\"$ip\"/g" $cwd/modify_json.py
	sed -i "s/clustercommport=.*/clustercommport=${clustercomm_port}/g" $cwd/modify_json.py
	sed -i "s/partyid=.*/partyid=\"$partyid\"/g" $cwd/modify_json.py
	python $cwd/modify_json.py proxy proxy/conf/route_table.json
	
	echo "[INFO] cp proxy file to $dir:"
	cp -r proxy $dir/
	
	if test -e $dir/proxy;then
		echo "[INFO] the proxy is deployed success."
	else
		echo "[ERROR] the proxy is deployed fail,please try it again."
	fi
	
	echo "[INFO] $dir/proxy/conf/proxy.properties of proxy in $partyid has been modified as follows:"
	cat $dir/proxy/conf/proxy.properties | grep -v "#" | grep -v ^$
	
	echo "[INFO] $dir/proxy/conf/route_table.json of proxy in $partyid has been modified as follows:"
	cat $dir/proxy/conf/route_table.json | grep -v "#" | grep -v ^$
}

roll() {
	echo "[INFO] the partyid is $partyid"
	echo "[INFO] the roll of $partyid is $ip"
	
	sed -i "s/party.id=.*/party.id=$partyid/g" ./roll/conf/roll.properties
	sed -i "s/service.port=.*/service.port=${roll_port}/g" ./roll/conf/roll.properties
	sed -i "s/meta.service.ip=.*/meta.service.ip=$ip/g" ./roll/conf/roll.properties
	sed -i "s/meta.service.port=.*/meta.service.port=${metaservice_port}/g" ./roll/conf/roll.properties
	
	echo "[INFO] cp roll file to $dir:"
	cp -r roll $dir/
	
	if test -e $dir/roll;then
		echo "[INFO] the roll is deployed success."
	else
		echo "[ERROR] the roll is deployed fail,please try it again."
	fi
	
	echo "[INFO] $dir/roll/conf/roll.properties of roll in $partyid has been modified as follows:"
	cat $dir/roll/conf/roll.properties | grep -v "#" | grep -v ^$
}

egg() {

	echo "[INFO] the partyid is $partyid"
	echo "[INFO] the egg of $partyid is $ip"
	
	sed -i "s/party.id=.*/party.id=$partyid/g" ./egg/conf/egg.properties
	sed -i "s/service.port=.*/service.port=${egg_port}/g" ./egg/conf/egg.properties
	sed -i "s/engine.names=.*/engine.names=processor/g" ./egg/conf/egg.properties
	sed -i "s#bootstrap.script=.*#bootstrap.script=${dir}/api/eggroll/framework/egg/src/main/resources/processor-starter.sh#g" ./egg/conf/egg.properties
	sed -i "s#start.port=.*#start.port=${processor_port}#g" ./egg/conf/egg.properties
	sed -i "s#processor.venv=.*#processor.venv=${venvdir}#g" ./egg/conf/egg.properties
	sed -i "s#processor.python-path=.*#processor.python-path=${dir}/api#g" ./egg/conf/egg.properties
	sed -i "s#processor.engine-path=.*#processor.engine-path=${dir}/api/eggroll/computing/processor.py#g" ./egg/conf/egg.properties
	sed -i "s#data-dir=.*#data-dir=${dir}/data-dir#g" ./egg/conf/egg.properties
	sed -i "s#processor.logs-dir=.*#processor.logs-dir=${dir}/logs/processor#g" ./egg/conf/egg.properties
	sed -i "s#count=.*#count=${processor_count}#g" ./egg/conf/egg.properties
	sed -i "20s#-I. -I.*#-I. -I$dir/storage-service-cxx/third_party/include#g" ./storage-service-cxx/Makefile
	sed -i "34s#LDFLAGS += -L.*#LDFLAGS += -L$dir/storage-service-cxx/third_party/lib -llmdb -lboost_system -lboost_filesystem -lglog -lgpr#g" ./storage-service-cxx/Makefile
	sed -i "36s#PROTOC =.*#PROTOC = $dir/storage-service-cxx/third_party/bin/protoc#g" ./storage-service-cxx/Makefile
	sed -i "37s#GRPC_CPP_PLUGIN =.*#GRPC_CPP_PLUGIN = $dir/storage-service-cxx/third_party/bin/grpc_cpp_plugin#g" ./storage-service-cxx/Makefile
	
	sed -i "s/clustercommip=.*/clustercommip=\"$ip\"/g" $cwd/modify_json.py
	sed -i "s/clustercommport=.*/clustercommport=${clustercomm_port}/g" $cwd/modify_json.py
	sed -i "s/rollip=.*/rollip=\"$ip\"/g" $cwd/modify_json.py
	sed -i "s/rollport=.*/rollport=${roll_port}/g" $cwd/modify_json.py
	sed -i "s/proxyip=.*/proxyip=\"$ip\"/g" $cwd/modify_json.py
	sed -i "s/proxyport=.*/proxyport=${proxy_port}/g" $cwd/modify_json.py
	python $cwd/modify_json.py python api/eggroll/conf/server_conf.json
	
	echo "[INFO] cp egg file to $dir:"
	cp -r api egg storage-service-cxx $dir/
	
	if test -e $dir/egg;then
		echo "[INFO] the egg is deployed success."
	else
		echo "[ERROR] the egg is deployed fail,please try it again."
	fi
	
	cd $dir/storage-service-cxx/
	make
	cd $output_dir
	
	echo "[INFO] $dir/egg/conf/egg.properties of egg in $partyid has been modified as follows:"
	cat $dir/egg/conf/egg.properties | grep -v "#" | grep -v ^$
	
	echo "[INFO] $dir/api/eggroll/conf/server_conf.json of processor in $partyid has been modified as follows:"
	cat $dir/api/eggroll/conf/server_conf.json | grep -v "#" | grep -v ^$
}

mysql() {
	eval jdbcip=${jdbc[0]}
	eval jdbcdbname=${jdbc[1]}
	eval jdbcuser=${jdbc[2]}
	eval jdbcpasswd=${jdbc[3]}
	
	echo "[INFO] the meta-service of $partyid is $ip"
	echo "[INFO] the mysql-server of $partyid is $jdbcip"
	echo "[INFO] the database name of $partyid is $jdbcdbname"
	echo "[INFO] the database user of $partyid is $jdbcuser"
	
	cp ./api/eggroll/framework/meta-service/src/main/resources/create-meta-service.sql ${mysqldir}
	sed -i "s/eggroll_meta/$jdbcdbname/g" ${mysqldir}/create-meta-service.sql
	
	echo "[INFO] login mysql with $jdbcuser,and create database $jdbcdbname:
	INSERT INTO node (ip, port, type, status) values ('$ip', '${roll_port}', 'ROLL', 'HEALTHY');
	INSERT INTO node (ip, port, type, status) values ('$ip', '${proxy_port}', 'PROXY', 'HEALTHY');
	INSERT INTO node (ip, port, type, status) values ('$ip', '${egg_port}', 'EGG', 'HEALTHY');
	INSERT INTO node (ip, port, type, status) values ('$ip', '${storage_port}', 'STORAGE', 'HEALTHY');"
	
	${mysqldir}/bin/mysql -u$jdbcuser -p$jdbcpasswd -S ${mysqldir}/mysql.sock<<eeooff
source ${mysqldir}/create-meta-service.sql
INSERT INTO node (ip, port, type, status) values ('$ip', '${roll_port}', 'ROLL', 'HEALTHY');
INSERT INTO node (ip, port, type, status) values ('$ip', '${proxy_port}', 'PROXY', 'HEALTHY');
INSERT INTO node (ip, port, type, status) values ('$ip', '${egg_port}', 'EGG', 'HEALTHY');
INSERT INTO node (ip, port, type, status) values ('$ip', '${storage_port}', 'STORAGE', 'HEALTHY');
show tables;
select * from node;
exit
eeooff
}

case "$1" in
    all)
        all $@
        ;;
    usage)
        usage
        ;;
    *)
        multiple $@
        ;;
esac
