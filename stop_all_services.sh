#!/bin/bash

function stop_service {
    pid_file=$1
    echo -n "$pid_file - "
    if [ -f $pid_file ]; then
	pid=`cat $pid_file`
	present=`ps ax|grep $pid|grep -v 'grep'`
	if ![ "x$present" == "x" ]; then
	    kill $pid
	    echo "Stoopped with pid: $pid"
	else
	    echo "Not found pid: $pid"
	fi
    else
	echo "Not runned"
    fi
}

stop_service /var/tmp/tcache2_sletat_loader.pid
stop_service /var/tmp/tcache2_worker_manager.pid
stop_service /var/tmp/tcache2_worker.pid
stop_service /var/tmp/tcache2_data2db_worker.pid

