cwd=$(cd `dirname $0`; pwd)

get_property() {
    property_value=`grep $1 $2 | awk -F= '{print $2}'`
    test_value $1 $2 ${property_value}
}

echo_red() {
    echo -e "\e[1;31m $1\e[0m"
}

echo_green() {
    echo -e "\e[1;32m $1\e[0m"
}

echo_yellow() {
    echo -e "\e[1;33m $1\e[0m"
}

check_max_count() {
    value=`cat $1`
    if [ $value -ge 65535 ];then
        echo_green "[OK] $1 is ok."
    else
        echo_red "[ERROR] please check $1, no less than 65535."
    fi  
}

check_file_count() {
    value=`cat $1 | grep $2 | awk '{print $4}'`
    for v in ${value[@]};do
        test_value $1 $2 $v
    done 
}

test_value() {
    if [ $3 -ge 65535 ];then
        echo_green "[OK] $1 in $2 is ok."
    else
        echo_red "[ERROR] please check $1 in $2, no less than 65535."
    fi
}

echo_green `date +"%Y-%m-%d_%H:%M:%S"`

echo_green "=============check max user processes============"
check_max_count "/proc/sys/kernel/threads-max"
get_property "kernel.pid_max" "/etc/sysctl.conf"
check_max_count "/proc/sys/kernel/pid_max"
check_max_count "/proc/sys/vm/max_map_count"

echo_green "=============check max files count=============="
check_file_count "/etc/security/limits.conf" "nofile"
check_file_count "/etc/security/limits.d/80-nofile.conf" "nofile"
get_property "fs.file-max" "/etc/sysctl.conf"
check_max_count "/proc/sys/fs/file-max"

MemTotal=`free -lh | grep Mem | awk '{print $2}' | tr -cd "[0-9,.]"`
MemUsed=`free -lh | grep Mem | awk '{print $3}' | tr -cd "[0-9],."`
SwapTotal=`free -lh | grep Swap | awk '{print $2}' | tr -cd "[0-9,.]"`
SwapUsed=`free -lh | grep Swap | awk '{print $3}' | tr -cd "[0-9,.]"`

echo_green "=============Memory used and total==============="
echo_yellow "[WARNING] MemTotal:${MemTotal}G, MemUsed:${MemUsed}G, MemUsed%:`awk 'BEGIN{printf "%.2f%%\n",('$MemUsed'/'$MemTotal')*100}'`"
echo_green "=============SwapMem used and total==============="
echo_yellow "[WARNING] SwapTotal:${SwapTotal}G, SwapUsed:${SwapUsed}G, SwapUsed%:`awk 'BEGIN{printf "%.2f%%\n",('$SwapUsed'/'$SwapTotal')*100}'`"
echo_green "=============Disk use and total=================="
echo_yellow "[WARNING] `df -lh | grep /data`"


