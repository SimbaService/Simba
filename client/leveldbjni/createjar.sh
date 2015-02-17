#!/bin/bash

mvn clean
mvn install -P all
jar xvf leveldbjni-all/target/leveldbjni-all-99-master-SNAPSHOT.jar 
jar xvf leveldbjni/target/leveldbjni-99-master-SNAPSHOT.jar 
jar cvf leveldb.jar org
#cp leveldb.jar /development/ygo/mobius/apps/Mobius/libs/
