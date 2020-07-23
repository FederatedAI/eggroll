#!/bin/bash
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
source ./check_iplist.sh

for ip in ${iplist[@]};do 
	if ! ssh -tt app@$ip test -e ${EGGROLL_HOME}/deploy/env_test.sh;then 
		echo "env_test.sh in $ip:${EGGROLL_HOME}/deploy is not exist, scp env_test.sh to $ip:${EGGROLL_HOME}/deploy" 
		scp ./env_test.sh $user@$ip:${EGGROLL_HOME}/deploy 
	fi 
	ssh app@$ip "sh ${EGGROLL_HOME}/deploy/env_test.sh" >> $ip
done
