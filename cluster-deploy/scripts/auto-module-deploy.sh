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

modules=(clustercomm metaservice egg proxy roll exchange mysql)

cd $cwd
source ./configurations.sh

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

echo "[INFO] the path of JAVA_HOME is $javadir"
echo "[INFO] deploy Eggroll in directory: $dir"
echo "====================================================="

all() {
	packages
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
	packages
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

packages() {
	tar -czf packages.tar.gz packages services.sh
	for ip in ${iplist[@]}
	do
		echo "=================================="
		
		if ssh -tt $user@$ip test -e $dir;then
			echo "[INFO] $dir in $ip already exist"
		else
			echo "[INFO] $dir in $user@$ip is not exist,execute: mkdir -p $dir"
			ssh -tt $user@$ip "mkdir -p $dir"
			if ssh -tt $user@$ip test -e $dir;then
				echo "[INFO] execute: mkdir -p $dir success"
			else
				echo "[ERROR] execute: mkdir -p $dir fail,please login $user@$ip and mkdir -p $dir"
			fi
		fi
		
		echo "[INFO] scp packages file to $user@$ip:"
		scp packages.tar.gz $user@$ip:$dir
		ssh -tt $user@$ip "cd $dir;tar -xzf packages.tar.gz;rm -f packages.tar.gz"
		
		if ssh -tt $user@$ip test -e $dir/packages;then
			echo "[INFO] the packages of $ip has copyed success."
		else
			echo "[ERROR] the packages of $ip copyed fail,please try it again."
		fi
		
		echo "-----------------------------------"
	done
}

clustercomm() {
	for partyid in ${partylist[@]}
	do
		eval clustercommip=\${clustercomm_$partyid}
		eval metaserviceip=\${metaservice_$partyid}
		
		echo "[INFO] the partyid is $partyid"
		echo "[INFO] the clustercomm of $partyid is $clustercommip"
		echo "[INFO] the metaservice of $partyid is $metaserviceip"
		
		sed -i "s/party.id=.*/party.id=$partyid/g" ./clustercomm/conf/clustercomm.properties
		sed -i "s/service.port=.*/service.port=${clustercomm_port}/g" ./clustercomm/conf/clustercomm.properties
		sed -i "s/meta.service.ip=.*/meta.service.ip=$metaserviceip/g" ./clustercomm/conf/clustercomm.properties
		sed -i "s/meta.service.port=.*/meta.service.port=${metaservice_port}/g" ./clustercomm/conf/clustercomm.properties
		
		echo "[INFO] clustercomm/conf/clustercomm.properties of clustercomm in $partyid has been modified as follows:"
		cat clustercomm/conf/clustercomm.properties | grep -v "#" | grep -v ^$
		
		tar -czf clustercomm.tar.gz clustercomm packages services.sh
		
		echo "[INFO] scp clustercomm file to $user@$clustercommip:"
		scp clustercomm.tar.gz $user@$clustercommip:$dir
		rm -f clustercomm.tar.gz
		
		ssh -tt $user@$clustercommip "cd $dir;tar -xzf clustercomm.tar.gz;rm -f clustercomm.tar.gz"
		if ssh -tt $user@$clustercommip test -e $dir/clustercomm;then
			echo "[INFO] the clustercomm of $partyid is deployed success."
		else
			echo "[ERROR] the clustercomm of $partyid is deployed fail,please try it again."
		fi
	done
}

metaservice() {
	for partyid in ${partylist[@]}
	do
		eval metaserviceip=\${metaservice_$partyid}
		eval jdbcip=\${jdbc_$partyid[0]}
		eval jdbcdbname=\${jdbc_$partyid[1]}
		eval jdbcuser=\${jdbc_$partyid[2]}
		eval jdbcpasswd=\${jdbc_$partyid[3]}
		
		echo "[INFO] the partyid is $partyid"
		echo "[INFO] the metaservice of $partyid is $metaserviceip"
		echo "[INFO] the mysql-server of $partyid is $jdbcip"
		echo "[INFO] the database name of $partyid is $jdbcdbname"
		echo "[INFO] the database user of $partyid is $jdbcuser"
		
		sed -i "s/party.id=.*/party.id=$partyid/g" ./meta-service/conf/meta-service.properties
		sed -i "s/service.port=.*/service.port=${metaservice_port}/g" ./meta-service/conf/meta-service.properties
		sed -i "s#//.*?#//$jdbcip:3306/$jdbcdbname?#g" ./meta-service/conf/meta-service.properties
		sed -i "s/jdbc.username=.*/jdbc.username=$jdbcuser/g" ./meta-service/conf/meta-service.properties
		sed -i "s/jdbc.password=.*/jdbc.password=$jdbcpasswd/g" ./meta-service/conf/meta-service.properties
		
		echo "[INFO] meta-service/conf/meta-service.properties of meta-service in $partyid has been modified as follows:"
		cat meta-service/conf/meta-service.properties | grep -v "#" | grep -v ^$
		
		tar -czf meta-service.tar.gz meta-service
		
		echo "[INFO] scp meta-service file to $user@$metaserviceip:"
		scp meta-service.tar.gz $user@$metaserviceip:$dir
		rm -f meta-service.tar.gz
		
		ssh -tt $user@$metaserviceip "cd $dir;tar -xzf meta-service.tar.gz;rm -f meta-service.tar.gz"
		if ssh -tt $user@$metaserviceip test -e $dir/meta-service;then
			echo "[INFO] the meta-service of $partyid is deployed success."
		else
			echo "[ERROR] the meta-service of $partyid is deployed fail,please try it again."
		fi
	done
}

proxy() {
	for partyid in ${partylist[@]}
	do
		eval proxyip=\${proxy_$partyid}
		eval clustercommip=\${clustercomm_$partyid}
		eval exchangeip=\${exchange_$partyid}
		
		echo "[INFO] the partyid is $partyid"
		echo "[INFO] the proxy of $partyid is $proxyip"
		echo "[INFO] the clustercomm of $partyid is $clustercommip"
		echo "[INFO] the exchange of $partyid is $exchangeip"
		
		sed -i "s/coordinator=.*/coordinator=$partyid/g" ./proxy/conf/proxy.properties
		sed -i "s/ip=.*/ip=$proxyip/g" ./proxy/conf/proxy.properties
		sed -i "s/port=.*/port=${proxy_port}/g" ./proxy/conf/proxy.properties
		sed -i "s#route.table=.*#route.table=$dir/proxy/conf/route_table.json#g" ./proxy/conf/proxy.properties
		
		echo "[INFO] proxy/conf/proxy.properties of proxy in $partyid has been modified as follows:"
		cat proxy/conf/proxy.properties | grep -v "#" | grep -v ^$
		
		sed -i "s/exchangeip=.*/exchangeip=\"$exchangeip\"/g" $cwd/modify_json.py
		sed -i "s/exchangeport=.*/exchangeport=${exchange_port}/g" $cwd/modify_json.py
		sed -i "s/clustercommip=.*/clustercommip=\"$clustercommip\"/g" $cwd/modify_json.py
		sed -i "s/clustercommport=.*/clustercommport=${clustercomm_port}/g" $cwd/modify_json.py
		sed -i "s/partyid=.*/partyid=\"$partyid\"/g" $cwd/modify_json.py
		python $cwd/modify_json.py proxy proxy/conf/route_table.json
		
		echo "[INFO] proxy/conf/route_table.json of proxy in $partyid has been modified as follows:"
		cat proxy/conf/route_table.json | grep -v "#" | grep -v ^$
		
		tar -czf proxy.tar.gz proxy
		
		echo "[INFO] scp proxy file to $user@$proxyip:"
		scp proxy.tar.gz $user@$proxyip:$dir
		rm -f proxy.tar.gz
		
		ssh -tt $user@$proxyip "cd $dir;tar -xzf proxy.tar.gz;rm -f proxy.tar.gz"
		if ssh -tt $user@$proxyip test -e $dir/proxy;then
			echo "[INFO] the proxy of $partyid is deployed success."
		else
			echo "[ERROR] the proxy of $partyid is deployed fail,please try it again."
		fi
	done
}

roll() {
	for partyid in ${partylist[@]}
	do
		eval rollip=\${roll_$partyid}
		eval metaserviceip=\${metaservice_$partyid}
		
		echo "[INFO] the partyid is $partyid"
		echo "[INFO] the roll of $partyid is $rollip"
		echo "[INFO] the metaservice of $partyid is $metaserviceip"
		
		
		sed -i "s/party.id=.*/party.id=$partyid/g" ./roll/conf/roll.properties
		sed -i "s/service.port=.*/service.port=${roll_port}/g" ./roll/conf/roll.properties
		sed -i "s/meta.service.ip=.*/meta.service.ip=${metaserviceip}/g" ./roll/conf/roll.properties
		sed -i "s/meta.service.port=.*/meta.service.port=${metaservice_port}/g" ./roll/conf/roll.properties
		
		echo "[INFO] roll/conf/roll.properties of roll in $partyid has been modified as follows:"
		cat roll/conf/roll.properties | grep -v "#" | grep -v ^$
		
		tar -czf roll.tar.gz roll packages services.sh
		
		echo "[INFO] scp roll file to $user@$rollip:"
		scp roll.tar.gz $user@$rollip:$dir
		rm -f roll.tar.gz
		
		ssh -tt $user@$rollip "cd $dir;tar -xzf roll.tar.gz;rm -f roll.tar.gz"
		if ssh -tt $user@$rollip test -e $dir/roll;then
			echo "[INFO] the roll of $partyid is deployed success."
		else
			echo "[ERROR] the roll of $partyid is deployed fail,please try it again."
		fi
	done
}

exchange() {
	sed -i "s/ip=.*/ip=/g" ./proxy/conf/proxy.properties
	sed -i "s/port=.*/port=${exchange_port}/g" ./proxy/conf/proxy.properties
	sed -i "s#route.table=.*#route.table=$dir/proxy/conf/route_table.json#g" ./proxy/conf/proxy.properties
	for partyid in ${partylist[@]}
	do
		eval proxyip=\${proxy_$partyid}
		eval exchangeip=\${exchange_$partyid}
		
		echo "[INFO] the proxy of $partyid is $proxyip"
		echo "[INFO] the exchange of $partyid is $exchangeip"
		
		sed -i "s/partyid=.*/partyid=\"$partyid\"/g" $cwd/modify_json.py
		sed -i "s/proxyip=.*/proxyip=\"$proxyip\"/g" $cwd/modify_json.py
		sed -i "s/proxyport=.*/proxyport=${proxy_port}/g" $cwd/modify_json.py
		python $cwd/modify_json.py exchange proxy/conf/route_table.json
	done
	if ssh -tt $user@$exchangeip test -e $dir/proxy;then
		echo "[WARN] the exchange of $partyid has been deployed,please check it."
	else
		echo "[INFO] proxy/conf/proxy.properties of exchange:$exchangeip has been modified as follows:"
		cat proxy/conf/proxy.properties | grep -v "#" | grep -v ^$
		
		echo "[INFO] proxy/conf/route_table.json of exchange:$exchangeip has been modified as follows:"
		cat proxy/conf/route_table.json | grep -v "#" | grep -v ^$
		
		tar -czf proxy.tar.gz proxy
		
		echo "[INFO] scp exchange file to $user@$exchangeip:"
		scp proxy.tar.gz $user@$proxyip:$dir
		ssh -tt $user@$proxyip "cd $dir;tar -xzf proxy.tar.gz;rm -f proxy.tar.gz"
		rm -f proxy.tar.gz
	fi
}

egg() {
	for partyid in ${partylist[@]}
	do
		eval clustercommip=\${clustercomm_$partyid}
		eval rollip=\${roll_$partyid}
		eval proxyip=\${proxy_$partyid}
		
		echo "[INFO] the partyid is $partyid"
		echo "[INFO] the clustercomm of $partyid is $clustercommip"
		echo "[INFO] the roll of $partyid is $rollip"
		echo "[INFO] the proxy of $partyid is $proxyip"
		
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
		
		echo "[INFO] egg/conf/egg.properties of egg in $partyid has been modified as follows:"
		cat egg/conf/egg.properties | grep -v "#" | grep -v ^$
		
		sed -i "s/clustercommip=.*/clustercommip=\"$clustercommip\"/g" $cwd/modify_json.py
		sed -i "s/clustercommport=.*/clustercommport=${clustercomm_port}/g" $cwd/modify_json.py
		sed -i "s/rollip=.*/rollip=\"$rollip\"/g" $cwd/modify_json.py
		sed -i "s/rollport=.*/rollport=${roll_port}/g" $cwd/modify_json.py
		sed -i "s/proxyip=.*/proxyip=\"$proxyip\"/g" $cwd/modify_json.py
		sed -i "s/proxyport=.*/proxyport=${proxy_port}/g" $cwd/modify_json.py
		python $cwd/modify_json.py python api/eggroll/conf/server_conf.json
		
		echo "[INFO] api/eggroll/conf/server_conf.json of processor in $partyid has been modified as follows:"
		cat api/eggroll/conf/server_conf.json | grep -v "#" | grep -v ^$
		
		tar -czf egg.tar.gz egg api storage-service-cxx
		
		eval egglist=\${egglist_$partyid[@]}
		echo "[INFO] the egglist of $partyid is: $egglist"
		for eggip in $egglist
		do
			echo "[INFO] scp egg file to $user@$eggip:"
			scp egg.tar.gz $user@$eggip:$dir
			ssh -tt $user@$eggip "cd $dir;tar -xzf egg.tar.gz;rm -f egg.tar.gz;cd storage-service-cxx;make"
			if ssh -tt $user@$eggip test -e $dir/egg;then
				echo "[INFO] the egg of $partyid is deployed success."
			else
				echo "[ERROR] the egg of $partyid is deployed fail,please try it again."
			fi
		done
		rm -f egg.tar.gz
	done
}

mysql() {
	for partyid in ${partylist[@]}
	do
		eval jdbcip=\${jdbc_$partyid[0]}
		eval jdbcdbname=\${jdbc_$partyid[1]}
		eval jdbcuser=\${jdbc_$partyid[2]}
		eval jdbcpasswd=\${jdbc_$partyid[3]}
		
		echo "[INFO] the mysql-server of $partyid is $jdbcip"
		echo "[INFO] the database name of $partyid is $jdbcdbname"
		echo "[INFO] the database user of $partyid is $jdbcuser"
		
		eval rollip=\${roll_$partyid}
		eval proxyip=\${proxy_$partyid}
		
		echo "[INFO] the roll of $partyid is $rollip"
		echo "[INFO] the proxy of $partyid is $proxyip"

		scp ./api/eggroll/framework/meta-service/src/main/resources/create-meta-service.sql $user@$jdbcip:${mysqldir}
		ssh -tt $user@$jdbcip<< eeooff
sed -i "s/eggroll_meta/$jdbcdbname/g" ${mysqldir}/create-meta-service.sql
${mysqldir}/bin/mysql -u$jdbcuser -p$jdbcpasswd -S ${mysqldir}/mysql.sock
source ${mysqldir}/create-meta-service.sql
INSERT INTO node (ip, port, type, status) values ('${rollip}', '${roll_port}', 'ROLL', 'HEALTHY');
INSERT INTO node (ip, port, type, status) values ('${proxyip}', '${proxy_port}', 'PROXY', 'HEALTHY');
exit;
exit
eeooff
		eval egglist=\${egglist_$partyid[@]}
		echo "[INFO] the egglist of $partyid is: $egglist"
		for eggip in $egglist
		do
			ssh -tt $user@$jdbcip<< eeooff
${mysqldir}/bin/mysql -u$jdbcuser -p$jdbcpasswd -S ${mysqldir}/mysql.sock
use $jdbcdbname;
INSERT INTO node (ip, port, type, status) values ('${eggip}', '${egg_port}', 'EGG', 'HEALTHY');
INSERT INTO node (ip, port, type, status) values ('${eggip}', '${storage_port}', 'STORAGE', 'HEALTHY');
exit;
exit
eeooff
		done
	done
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
