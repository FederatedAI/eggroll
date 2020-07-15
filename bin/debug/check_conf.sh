cwd=$(cd `dirname $0`; pwd)
source ./check_iplist.sh

cd $EGGROLL_HOME
for ip in ${iplist[@]};do
    echo "diff $ip with ./conf/eggroll.properties"
    ssh $user@$ip "cat $EGGROLL_HOME/conf/eggroll.properties" | diff - conf/eggroll.properties
	echo ""
done

echo "===$EGGROLL_HOME/conf/eggroll.properties==="
cat $EGGROLL_HOME/conf/eggroll.properties | grep -v ^# | grep -v ^$
echo ""
echo "===$EGGROLL_HOME/conf/route_table.json==="
cat $EGGROLL_HOME/conf/route_table.json | grep -v ^# | grep -v ^$
cd $cwd
