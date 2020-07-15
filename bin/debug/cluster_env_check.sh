cwd=$(cd `dirname $0`; pwd)
source ./check_iplist.sh

for ip in ${iplist[@]};do 
	if ! ssh -tt app@$ip test -e ${EGGROLL_HOME}/deploy/env_test.sh;then 
		echo "env_test.sh in $ip:${EGGROLL_HOME}/deploy is not exist, scp env_test.sh to $ip:${EGGROLL_HOME}/deploy" 
		scp ./env_test.sh $user@$ip:${EGGROLL_HOME}/deploy 
	fi 
	ssh app@$ip "sh ${EGGROLL_HOME}/deploy/env_test.sh" >> $ip
done
