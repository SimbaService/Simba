#!/bin/bash

source config.sh

if [ x$1 == "xstart" ]
then
	cd /mnt/sda4
	cp $HOME/simba/simbaserver-simbastore-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
	cp ${script_dir}/simbastore.properties .
	if [ x$2 == "x-fg" ]
	then
		java -Xms256m -Xmx2g -jar simbaserver-simbastore-0.0.1-SNAPSHOT-jar-with-dependencies.jar -p simbastore.properties
	else
		nohup java -Xms256m -Xmx2g -jar simbaserver-simbastore-0.0.1-SNAPSHOT-jar-with-dependencies.jar -p simbastore.properties >simbastore.out 2>&1 </dev/null &
	fi
elif [ x$1 == "xstatus" ]
then
	PID=`ps -ef | grep java | grep simbastore | grep -v grep | awk '{print $2;}'`
	if [ x$PID == x ]; then echo "Not running"; else echo "Running; pid=$PID"; fi
elif [ x$1 == "xstop" ]
then
	PID=`ps -ef | grep java | grep simbastore | grep -v grep | awk '{print $2;}'`
	if [ x$PID != x ]; then kill $PID; fi
else
	echo "usage: $0 [start|stop|status]"
fi
