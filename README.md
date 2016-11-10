# LAB 3
## Code Structure
The code structure is pretty similar to Lab 1. GroupService.scala acts as the client for executing transactions. Unlike previous labs, KVClient is now an akka actor so that it can send/receive messages for 2PL and 2PC. Locks are maintained by a central LockService. Transactions are a series of read and writes on KV store keys. Transaction IDs are combination of Node ID and random number.
## Assumptions
As stated in lab description and Piazza posts, the conditions being assumed are: KVClients and KVStores do not crash. KVClients release the locks after transaction has been committed or aborted.
## 2PL
For 2PL, each transaction keeps acquiring locks needed and releases them in a single go. To avoid deadlocks, transactions are aborted. If T1 has acquired certain locks and T2 needs one or more of these locks, T2 simply aborts and releases all locks held to avoid any deadlocks.
## 2PC
Whenever a transaction does a write, the changes made are stored to locally held cache and dirty sets at KVClient. Reads for any value written by the same transaction, are returned from the cache. For other values that can be stale in cache, read requires an interaction with KVStore. Hence at the beginning of transaction, KVClient safely reads from KVStore to avoid stale data. At the end of transaction, GroupServer sends EndTransaction message to KVClient to start polling process. There can be two cases as a result of this poll.
1-	All participants (KVStores) replies with commit. The dirty set data or cache are pushed to KVStore.
2-	One or more of the participants replies with abort. The whole transaction needs to be aborted. This is done by keeping a log. The log keeps previous transactions ids and changes made by previous transactions against keys. This log is used to safely abort the transaction and undo all changes.
   
For test cases, as in previous labs, the number of nodes can be changed from TestHarness.scala file. Some sample transactions are coded in GroupServer,scala file that can be called from command() function.