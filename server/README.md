Simba Server
============

To compile
----------
Run ./compile.sh.  
JARs with dependencies will build in ./{common,gateway,simbastore}/target/ folders.  

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
