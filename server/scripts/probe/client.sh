#!/bin/bash

source config.sh

cd /mnt/sda4
cp $HOME/simba/simbaserver-common-0.0.1-SNAPSHOT-jar-with-dependencies.jar .
cp $HOME/simba/*jks .
cp ${script_dir}/client.properties .
