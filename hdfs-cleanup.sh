#!/bin/bash
#
# Script Name: hdfs-cleanup.sh
#
# Author: Dennis T Bielinski
#
# Description: This is a handy script for deleting old files in HDFS. I use it to clean up /tmp/hive
#              and other various directories.
#

usage="Usage: $0 [days]"

if [ ! "$1" ]
then
  echo $usage
  exit 1
fi

directory="/tmp/hive"

PARENTS=($(hdfs dfs -ls $directory | awk '{print $8}' | cut -c11-))

for parent in ${PARENTS[@]}; do
    now=$(date +%s)
    hdfs dfs -ls -R /tmp/hive/$parent/ | grep "^d" | while read f; do 
    dir_date=`echo $f | awk '{print $6}'`
    difference=$(( ( $now - $(date -d "$dir_date" +%s) ) / (24 * 60 * 60) ))
    if [ $difference -gt $1 ]; then
        delete_path=$(echo $f | awk '{print $8}')
        hdfs dfs -rm -r -skipTrash $delete_path
    fi
    done
done
