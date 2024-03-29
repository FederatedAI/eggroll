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
pwd=`pwd`
cwd=$(cd `dirname $0`; pwd)
cd $cwd
version=`grep version ../BUILD_INFO | awk -F= '{print $2}'`

cd ../jvm
mvn clean package -DskipTests

cd ..
if [[ ! -d "lib" ]]; then
    mkdir lib
fi
rm lib/*.jar
cp  jvm/core/target/core-${version}.jar lib
cp  jvm/core/target/lib/* ./lib
cp  jvm/cluster_manager/target/cluster_manager-${version}.jar lib
cp  jvm/cluster_manager/target/lib/* lib
cp  jvm/node_manager/target/node_manager-${version}.jar lib
cp  jvm/node_manager/target/lib/* lib
cp  jvm/cluster_dashboard/target/cluster_dashboard-${version}.jar lib
cp  jvm/cluster_dashboard/target/lib/* lib

tar -czf eggroll.tar.gz lib bin conf python deploy
cd $pwd
