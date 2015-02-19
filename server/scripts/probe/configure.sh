#!/bin/bash

source config.sh

HS=`hostname`
exec > ${log_dir}/$HS-configure.log 2>&1
cd ${script_dir}
sudo ./node.py configure
