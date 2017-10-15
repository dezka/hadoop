#!/usr/bin/python
''' 
Description:
 * This program is meant for taking the -du output from HDFS directories and 
ETL'ing the output into a Hive table for further analysis in Zeppelin.

Authors:
 * Dennis T Bielinski
'''

import csv
import os
import sys
import subprocess
import time

# Define Initial vars below

# :`str`: General timestring to use for filename distinguishing.
timestr = time.strftime("%Y%m%d-%H%M%S")
# :`str`: Complete date string to use for date column in Hive.
timedate = time.strftime("%Y-%m-%d")
# :`str`: Complete time string to use for time column in Hive.
timestr_proper = time.strftime("%H:%M:%S")
# :`str`: Location of the "staging" csv file to be used to dump into HDFS as lines are transformed.
csvLocation = '/tmp/staging1.csv'
# :`str`: A location for the du output file to be used for the buildCSV function.
duFileLoc = '/tmp/du-' + timestr + '.out'
# :`str`: HDFS location for the final csv file to be ingested into Hive.
hdfsFileLoc = '/data/admin/du_reports/du-' + timestr + '.csv'
# :`str`: The location in HDFS where you want to grab sizes from. Default is /data.
duPath = '/data'
# :`str`: Command used in HDFS CLI to grab the du output.
duCMD = 'hdfs dfs -du ' + duPath + ' > ' + duFileLoc
# :`str`: HDFS CLI command to place file into HDFS.
hdfsPut = 'hdfs dfs -copyFromLocal ' + csvLocation + ' ' + hdfsFileLoc
# :`str`: Cleanup command to remove temp files.
cleanUp = 'rm -f ' + duFileLoc + ' && rm -f ' + csvLocation


def buildCsv(srcFile, dstFile):
    '''
    Reads in the du output file line by line and turns each line into an array to then transform into a csv row.
    '''
    with open(srcFile, 'r') as f:
        with open(dstFile, 'wb') as f2:
            writer = csv.writer(f2)
            writer.writerow(["location", "size", "date", "time"])
            for line in f:
                splitLine = line.split()
                splitLine.append(timedate)
                splitLine.append(timestr_proper)
                splitLine[0], splitLine[1] = splitLine[1], splitLine[0]
                writer.writerow(splitLine)


def findFSName():
    '''
    Goes out to the /etc/hadoop/conf/core-site.xml file and grabs the fs.defaultFS property value to use in the subsequent Hive load query to make this program environment agnostic.
    '''
    findFSCmd = "grep \"hdfs://\" /etc/hadoop/conf/core-site.xml | awk -F '[<>]' '/value/{print $3}'"
    proc = subprocess.Popen(findFSCmd,stdout=subprocess.PIPE,shell=True)
    (out, err) = proc.communicate()
    outcleaned = out.rstrip('\n')
    return outcleaned


fsDefaultFS = findFSName()

# Hive setup
createDbTbl = """CREATE EXTERNAL TABLE IF NOT EXISTS du_reports.du_daily(\`location\` string,\`size\` bigint,\`date\` string,\`time\` string) PARTITIONED BY (PARTITIONDATE STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' LINES TERMINATED BY \'\\n\' STORED AS TEXTFILE TBLPROPERTIES(\\"skip.header.line.count\\"=\\"1\\");"""
duImport = "LOAD DATA INPATH '%s%s' OVERWRITE INTO TABLE du_reports.du_daily partition(partitionDate='%s')" % (fsDefaultFS, hdfsFileLoc, timedate)
hiveDbCreateCmd = "hive -e \"" + createDbTbl + "\""
hiveLoadCmd = "hive -e \"" + duImport + "\""

os.system(duCMD)
buildCsv(duFileLoc, csvLocation)
os.system(hdfsPut)
os.system(cleanUp)
os.system(hiveDbCreateCmd)
os.system(hiveLoadCmd)
