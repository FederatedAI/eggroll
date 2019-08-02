#!/bin/bash

version=0.3
cwd=`pwd`

cd ../../
eggroll_dir=`pwd`
base_dir=$eggroll_dir
output_dir=$eggroll_dir/cluster-deploy/example-dir-tree

cd $cwd
source ./configurations.sh

cd $base_dir
targets=`find "$base_dir" -type d -name "target" -mindepth 2`

module="test"
for target in ${targets[@]}; do
    echo
    echo $target | awk -F "/" '{print $(NF - 2), $(NF - 1)}' | while read a b; do 
        module=$b 
		
        cd $target
        jar_file="eggroll-$module-$version.jar"
        if [[ ! -f $jar_file ]]; then
            echo "[INFO] $jar_file does not exist. skipping."
            continue
        fi

        output_file=$output_dir/$module/eggroll-$module-$version.tar.gz
        echo "[INFO] $module output_file: $output_file"
		
		if [[ ! -d $output_dir/$module ]]
		then
			break
		fi

        rm -f $output_file
        gtar czf $output_file lib eggroll-$module-$version.jar
		cd $output_dir/$module
		tar -xzf eggroll-$module-$version.tar.gz
		rm -f eggroll-$module-$version.tar.gz
		ln -s eggroll-$module-$version.jar eggroll-$module.jar
    done
    echo "--------------"
done

cd $eggroll_dir
mkdir -p $output_dir/api/eggroll/
rsync -a --exclude 'cluster-deploy' $output_dir/api/eggroll/
cp -r storage/storage-service-cxx $output_dir/

cd $cwd
