#!/usr/bin/env bash

function verify() {
	arr=("$@")
	for i in "${arr[@]}";
		do
				if [ ! -f $i ]; then

					echo "Missing ${i}"
					exit 1
				fi
		done
}

req_files=("src/log/log_manager.cc" "src/include/log/log_manager.h")
verify "${req_files[@]}"	
if [[ $? -ne 0 ]]; then
    exit 1
fi

if [ $# -eq 1 ]
then
	zip "${1}.zip" src/optimizer/join_optimizer.cc src/include/optimizer/join_optimizer.h src/optimizer/table_stats.cc src/include/optimizer/table_stats.h REPORT.md
else
	echo 'Please provide a file name, eg ./submit Gaurav'
fi
