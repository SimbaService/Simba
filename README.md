Simba: Cloud Infrastructure for Mobile Clients
==============================================

Project Home
------------
http://www.nec-labs.com/~nitin/Simba

Licensing
---------
This code is released under the Apache License 2.0.

This code uses a modified version of "SSL NIO Framework" by WIT-Software, Lda., licensed under CC-BY-NC-SA 2.0. http://creativecommons.org/licenses/by-nc-sa/2.0/

Release Information
-------------------
The code is released in its current research-quality form. We plan on future improvements if time allows. We are open to contributions from the community. Please generate a pull request if you would like to introduce a change.

Requirements
------------
Java >= 1.6  

Client  
  * Android >= 4.0  

Cloud  
  * Apache Cassandra >= 1.2.5  
  * OpenStack Swift >= 1.8.0  
  * Google Protocol Buffers >= 2.5.0  
  * Maven >= 3.2.1  

Getting Started
---------------
Simba is partitioned into a few separate components.

###Client  
[Simba Content Service](client/SimbaContentService)  
[Simba Client Library](client/SimbaClientLib)  

###Cloud  
[Simba Gateway](server/gateway)  
[Simba Store](server/simbastore)  

Please see the Simba Cloud [README](server/README.md) for more information.  

###Example App
An [example app](client/apps/SimbaNoteApp) is included to demonstrate the ease-of-use of the Simba Client API.  

More Information
----------------
Please see our papers on Simba ([FAST 2015](https://www.usenix.org/conference/fast15/technical-sessions/presentation/go), EuroSys 2015) for more information.  

Questions?
----------
If you have questions, please join the discussion at our [Google Group](https://groups.google.com/forum/#!forum/simbasync).  

