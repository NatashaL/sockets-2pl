# sockets-2pl

####Python socket communication and 2PL mechanism for a distributed database

Creating and maintaing a duplicated database on multiple nodes in a network using 2 phase locking mechanism.

#####How to:
 - Specify a hostfile with IP addresses and ports for your nodes. When running, set the host_index to the corresponding line number (starting from 0).

 - Run node.py in different terminals or different machines.

 - Usage: python node.py \<hostfile\> \<host_index\>

This will create local databases(folder) for each node.
Nodes can create and change numerical variables in their database. Changed variables are updated on all nodes simultaneously. Use uppercase letters for the variable names.

Ex:
 - create a couple of variables by entering the expression ``` A = 1; B = 2 ```
   - A and B should be created and initialized at all nodes
 - enter any kind of expression regarding A and B, like ``` A = (A + B ** 2) % 6```
   - A should be updated at all nodes
 - enter ``` quit ``` to exit the program
 
 
