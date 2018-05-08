#!/bin/bash

export JAVA_HOME=/usr/local/java/jdk1.8
# 一直运行
sudo rm -rf /home/hadoop/webdir/log/*
while [ 1 ]; do
    ./sample_web_log.py > ./log/"access.`date +'%s'`.log"
    echo "`date +"%F %T"` put $tmplog to /home/hadoop/webdir/log/ succeed"
    sleep 5
done