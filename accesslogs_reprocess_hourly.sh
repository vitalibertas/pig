#!/bin/bash

secondsEnd=$(date --date="$1" '+%s')
email=$2
processTimeFile=${3-"/home/user/states/access_reprocess_hourly.state"}
logFile="/home/user/logs/access_hourly.log"

echo "End Date: $(date --date="$1" '+%Y-%m-%d %H:00 %z') E-Mail: $email  State Fie: $processTimeFile"

if [ -z "$secondsEnd" ]; then
    echo "You must declare an end-time as a parameter. Optionally include a state file to read from:"
    echo "$0 \"2014-09-30 11:00\" user@domain.com /home/user/states/access_reprocess_hourly.state"
    exit 1
fi

if [ -z "$email" ]; then
    echo "You must declare an e-mail as a parameter. Optionally include a state file to read from:"
    echo "$0 \"2014-09-30 11:00\" user@domain.com /home/user/states/access_reprocess_hourly.state"
    exit 1
fi

function lockFile {
    mv "$processTimeFile" "$processTimeFile.lock"
}

function unlockFile {
    mv "$processTimeFile.lock" "$processTimeFile"
}

function setProcessTime {
    if [ ! -f "$processTimeFile" ]; then
        echo "Can not find $processTimeFile." | mail -s "Access Hourly Reprocess State File Missing" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
        exit 1
    else
        processTime=$(date --date="$(cat $processTimeFile)" '+%Y-%m-%d %H:00 %z')
        processSeconds=$(date --date="$processTime" '+%s')
    fi

    if [ -z "$processTime" ]; then
        echo "The process time cannot be read." | mail -s "Access Hourly Reprocess Time Corrupt" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
        exit 1
    fi
}

setProcessTime

while [ $processSeconds -lt $secondsEnd ]; do

    lockFile

pig -stop_on_failure -useHCatalog -param PROCESSTIME="$processTime" /home/user/pig/accesslogs_hive_job_combined_hourly.pig

    unlockFile

    if [ $? -eq 0 ]; then
        processTime=$(date --date="$processTime 60 minutes" '+%Y-%m-%d %H:00 %z')
        processSeconds=$(date --date="$processTime" '+%s')
        printf "$processTime\n" > $processTimeFile
    else
        echo "The Pig script did not process correctly for $processTime." | mail -s "Access Hourly Reprocess Script Update Failed" -r hosting-hadoop-noreply@`hostname -f` $email 2>> $logFile
        exit 1
    fi

    setProcessTime

done
