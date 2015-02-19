PRObE scripts
=============

This directory contains scripts which are used to configure/deploy the Simba Server on PRObE nodes (http://nmc-probe.org/), specifically using the Kodiak cluster. These scripts configure Apache Cassandra, OpenStack Swift, and Simba Gateways and Stores. It assumes Cassandra and Swift are pre-installed in your PRObE disk image. For deployment elsewhere, please adapt these scripts for your needs.

Configuration steps
-----
1. Instantiate your experiment with the following command, if it hasn't already been instantiated  
```   
cd ./startup/  
./makebed
```

  NOTE: `proj` and `exp` are important  because they are also a part of the domain of the assigned node.

2. Set the parameters for your project in `probe_config.py` and `config.sh` (as applicable).  
`proj`: PRObE project name  
`exp`: PRObE experiment name  
`image`: PRObE disk image  
`num_nodes`: number of nodes to provision  

3. Set the role for each node in your experiment in `probe_config.py`. There are 4 roles:  
 `swift_nodes`: Swift storage nodes  
 `proxy_nodes`: Swift proxy nodes, and also Simba store nodes  
 `cassandra_nodes`: Cassandra nodes  
 `other_nodes`: Client node hosts  

   This config files uses contiguous ranges for hosts, but can be changed.

4. Run `start_all.py`.  This will configure nodes one-by-one.  It is important that the first Swift node is configured first because it also creates the Swift ring files. Once all the nodes are configures, this script will start off the nodes one-by-one.

5.  Run `python simba.py prep all` to configure the Simba deployment.
6. Run `python simba.py start all` to start all Simba nodes.

