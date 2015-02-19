#!/usr/local/bin/bash

#
# Runs user-supplised command on all nodes
#

source config.sh

num_nodes=75
i=1
while [ $i -le $num_nodes ]
do
  ssh -o StrictHostKeyChecking=no h$i.${exp}.${proj} $*
  i=$(($i+1))
done
