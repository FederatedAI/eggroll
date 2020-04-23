cwd=$(cd `dirname $0`; pwd)
source ./conf.sh
version=`grep version ../BUILD_INFO | awk -F= '{print $2}'`

sed -i "s#EGGROLL_HOME=.*#EGGROLL_HOME=${EGGROLL_HOME}#g" ./init.sh
cp ./init.sh ../

cd ..
mkdir lib

cp -r ./jvm/core/target/eggroll-core-${version}.jar ./lib
cp -r ./jvm/core/target/lib/* ./lib
cp -r ./jvm/roll_pair/target/eggroll-roll-pair-${version}.jar ./lib
cp -r ./jvm/roll_pair/target/lib/* ./lib
cp -r ./jvm/roll_site/target/eggroll-roll-site-${version}.jar ./lib
cp -r ./jvm/roll_site/target/lib/* ./lib
cp ./jvm/core/main/resources/create-eggroll-meta-tables.sql ./conf

tar -czf eggroll.tar.gz lib bin conf data python deploy init.sh

for ip in ${iplist[@]};do
	
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

rm -f eggroll.tar.gz 

cd $cwd
