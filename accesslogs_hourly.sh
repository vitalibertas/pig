#!/bin/bash
#Hourly access log roll-ups to Hive

processTimeFile="/home/user/states/access_hourly.state"
logFile="/home/user/logs/access_hourly.log"
email="user@domain.com"

function lockFile {
    mv "$processTimeFile" "$processTimeFile.lock"
}

function unlockFile {
    mv "$processTimeFile.lock" "$processTimeFile"
}

function setProcessTime {
    if [ ! -f "$processTimeFile" ]; then
        echo "Can not find $processTimeFile." | mail -s "Access Hourly Job State File Missing" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
        exit 1
    else
        processTime=$(date --date="$(cat $processTimeFile)" '+%Y-%m-%d %H:00 %z')
        processSeconds=$(date --date="$processTime" '+%s')
    fi

    if [ -z "$processTime" ]; then
        echo "The process time cannot be read." | mail -s "Access Hourly Job Time Corrupt" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
        exit 1
    fi
}

setProcessTime
lockFile

pig -stop_on_failure -useHCatalog -param PROCESSTIME="$processTime" /home/user/pig/accesslogs_hive_job_combined_hourly.pig

unlockFile

if [ $? -eq 0 ]; then
    printf "$processTime\n" > $processTimeFile
else
    echo "The Pig script did not process correctly for $processTime." | mail -s "Access Hourly Pig Script Update Failed" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
    exit 1
fi
