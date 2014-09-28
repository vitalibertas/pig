#!/bin/bash
#Hourly access log roll-ups to Hive
#do-as-hosting crontab -e

#echo "Renewing kerberos token"
#kinit -R -k -t /home/cbrickner/kerberos/cbrickner.keytab cbrickner@DC1.CORP.GD

processTimeFile="/home/cbrickner/pig/access_hourly.state"

if [ ! -f "$processTimeFile" ]; then
    echo "Can not find $processTimeFile."
    exit 1
else
    processTime=$(date --date="$(sed 's/T/ /' $processTimeFile)" '+%Y-%m-%d %H:00')
fi

if [ -z "$processTime" ]; then
    echo "The last process time cannot be read."
    exit 1
else
    processTime=$(date --date="$processTime 60 minutes" '+%Y-%m-%d %H:00 %z')
fi

/usr/local/bin/do-as-hosting pig -stop_on_failure -useHCatalog -param PROCESSTIME="$processTime" /home/cbrickner/pig/accesslogs_hive_job_combined_hourly.pig

if [ $? -eq 0 ]; then
    printf "$processTime\n" > $processTimeFile
else
    echo "The Pig script did not process correctly for $processTime."
    exit 1
fi
