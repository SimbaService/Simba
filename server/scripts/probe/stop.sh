#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
source $DIR/config.sh

HS=`hostname`
exec > ${log_dir}/$HS-stop.log 2>&1
cd ${script_dir}
sudo ./node.py stop
