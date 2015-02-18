Simba Server
============

Prerequisites
-------------
Before running the Simba Server you must have operational deployments of Apache Cassandra and OpenStack Swift.  

Configuration
-------------
You must set certain configuration options in the following files:  
```
./common/src/resources/client.properties  
./gateway/src/resources/gateway.properties  
./simbastore/src/resources/simbastore.properties  
```

In `client.properties`, set:  
  * `gateways`: Comma-separated list of gateway nodes.  
  * `consistency`: Consistency level used by test client (Default: `CAUSAL`).  

In `gateway.properties`, set:  
  * `simbastores`: Comma-separated list of store nodes.  
  * `backend.server.thread.count`: Number of Gateway backend server threads. Set equal to number of store nodes.   

In `simbastore.properties`, set:  
  * `simbastores`: Comma-separated list of store nodes.  
  * `cassandra.keyspace`: Keyspace name for Simba metadata. Must also create it in Cassandra (Default: `simbastore`).  
  * `cassandra.seed`: Set Cassandra seed node host or IP.  
  * `swift.container`: Container name for Simba objects. Must also create it in Swift (Default:  `simbastore`).  
  * `swift.identity`: Swift account name.   
  * `swift.password`: Pass key for Swift account.  
  * `swift.proxy.url`: Swift proxy URL.  

Compilation
-----------
To compile, run:  
```
./compile.sh  
```
JARs with dependencies will be built in folders ./{common,gateway,simbastore}/target/.

Starting Simba Store
--------------------
NOTE: Must start Store(s) before Gateway(s).  
```
java -jar simbaserver-simbastore-0.0.1-SNAPSHOT-jar-with-dependencies.jar -p simbastore.properties   
```

Starting Simba Gateway
----------------------
```
java -jar simbaserver-gateway-0.0.1-SNAPSHOT-jar-with-dependencies.jar -p gateway.properties  
```

Running Linux Test Client for Simba
-----------------------------------
```
java -cp simbaserver-common-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.necla.simba.testclient.Client <workload file> <output folder> <prefix>  
```

Scripts for generating tables/workloads
---------------------------------------
Can be found in ./scripts/  
