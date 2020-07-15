cwd=$(cd `dirname $0`; pwd)
source ./check_iplist.sh

for ip in ${iplist[@]};do
	mkdir -p $1/$ip
	scp -r $user@$ip:$EGGROLL_HOME/logs/*$1* $1/$ip
done
cd $cwd
