#!/bin/bash

if ! [ -d /var/log/tcache2 ]; then
    echo "Log directory /var/log/tcache2 not exists. Please, create it."
    exit
fi

function check_service {
    pid_file=$1
    cmd=$2
    log_file=$3
    echo -n "$pid_file - "
    if [ -f $pid_file ]; then
	pid=`cat $pid_file`
	present=`ps ax|grep $pid|grep -v 'grep'`
	if [ "x$present" == "x" ]; then
	    echo "Not found pid: $pid. Run $cmd..."
	    $cmd 2>&1 >> $log_file &
	fi
    else
	    echo "Not found pid file: $pid_file. Run $cmd..."
	    $cmd 2>&1 >> $log_file &
    fi
}

check_service /var/tmp/tcache2_sletat_loader.pid ~/tcache2/sletat_loader_run.sh /var/log/tcache2/sletat_loader.log
check_service /var/tmp/tcache2_worker_manager.pid ~/tcache2/worker_manager_run.sh /var/log/tcache2/worker_manager.log
check_service /var/tmp/tcache2_worker.pid ~/tcache2/worker_run.sh /var/log/tcache2/worker.log
check_service /var/tmp/tcache2_data2db_worker.pid ~/tcache2/data2db_worker_run.sh /var/log/tcache2/data2db_worker.log
