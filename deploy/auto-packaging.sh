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

cp -r jvm/core/target/eggroll-core-${version}.jar lib
cp -r jvm/core/target/lib/* lib
cp -r jvm/roll_pair/target/eggroll-roll-pair-${version}.jar lib
cp -r jvm/roll_pair/target/lib/* ./lib
cp -r jvm/roll_site/target/eggroll-roll-site-${version}.jar lib
cp -r jvm/roll_site/target/lib/* lib
cp jvm/core/main/resources/create-eggroll-meta-tables.sql conf

tar -czf eggroll.tar.gz lib bin conf data python deploy
cd $pwd
