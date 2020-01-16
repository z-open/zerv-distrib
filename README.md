# zerv-distrib



### Scope
Simple process load distribution in a Zerv cluster to improve responsiveness of the main socket servers.


### pre-requisite
this relies on zerv-core and zerv-sync


### Principle
In order to free resources on the main socket servers, zerv can distribute the load on the zerv cluster.
The main socket servers which the browser apps connect via the socket to execute api funtions should be always be responsive for a better user experience.
All processes that requires lots of processing or do not require an immediate response should be either:
- handled by other dedicated Zerv servers
- or main server should limit their number running at the same time to remain responsive.

A distributed environment also provide redundancy and can scale easily.


A process queue shares by the zerv cluster contains the processes to execute. 
When a decicated server is available, it will execute the process.
The server that initiates the process might wait for its completion as per requirements.

A process can use zerv notifications to communicate changes or return a result.

If server crashes during the process, another server will restart it.

### Example



### To Implement
- currently queue is handled via a custom implementation using transaction row locks but it should use redis locking mechanism
- large result should not be broadcasted but store in redis, and process should return a cursor id


