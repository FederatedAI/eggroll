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
