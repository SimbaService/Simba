#!/usr/local/bin/bash

#
# Runs user-supplised command on all nodes
#

DIR="${BASH_SOURCE%/*}"
if [[ ! -d "$DIR" ]]; then DIR="$PWD"; fi
source $DIR/config.sh

num_nodes=75
i=1
while [ $i -le $num_nodes ]
do
  ssh -o StrictHostKeyChecking=no h$i.${exp}.${proj} $*
  i=$(($i+1))
done
