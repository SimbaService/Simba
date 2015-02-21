#!/bin/bash

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
source $DIR/config.sh

echo "STOPPING ALL SERVICES"
#stop everything
echo "simba"
python simba.py stop all
echo "cassandra"
parallel-ssh -t 0 -P -h ~/cassandra_nodes2 "cd "+${script_dir}+"; sudo python node.py stop"
echo "swift"
parallel-ssh -t 0 -P -h ~/swift_nodes2 "cd "+${script_dir}+"; sudo python node.py stop"
echo "rsync"
parallel-ssh -t 0 -P -h ~/swift_nodes2 "sudo service rsync stop"
