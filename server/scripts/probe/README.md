PRObE deployment scripts for Simba
==================================

This directory contains scripts which are used to configure/deploy the Simba Server on PRObE nodes (http://nmc-probe.org/), specifically using the Kodiak cluster. These scripts configure Apache Cassandra, OpenStack Swift, and Simba Gateways and Stores. It assumes Cassandra and Swift are pre-installed in your PRObE disk image. For deployment elsewhere, please adapt these scripts for your needs.

Configuring and Starting Simba
------------------------------
1. Set the parameters for your project in `probe_config.py` and `config.sh`.  
`proj`: PRObE project name  
`exp`: PRObE experiment name  
`image`: PRObE disk image  
`num_nodes`: number of nodes to provision  

  NOTE: `proj` and `exp` are important  because they are also a part of the hostname of the assigned node.

2. Instantiate your PRObE experiment with the following command, if it hasn't already been instantiated.
Inside `startup/` run `./makebed`.


3. Set the role for each node in your experiment in `probe_config.py`. There are 5 roles:  
 `swift_nodes`: Swift storage nodes   
 `proxy_nodes`: Swift proxy nodes, and also Simba store nodes  
 `cassandra_nodes`: Cassandra nodes  
 `gateway_nodes`: Simba gateway nodes  
 `other_nodes`: Client node hosts  
4. Run `start_all.py` to configure and start Swift and Cassandra on each node.
5. Run `simba.py prep all` to configure the Simba service.
6. Run `simba.py start all` to start Simba service.

Stopping Simba
--------------
1. Run `simba.py stop all` to stop the Simba service.
2. Run `stop_all.py` to stop Cassandra and Swift.

