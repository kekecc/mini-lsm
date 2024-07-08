## Test Your Understanding
1. Why doesn't the memtable provide a delete API?
- The delete operation only put the key with an empty value into the memtable, not involving the actual delete operation in memory. It is called "a delete tombstone", which will be handled in compaction.
2. Is it possible to use other data structures as the memtable in LSM? What are the pros/cons of using the skiplist?
- Yes, i think. BTree、BRTree and ART are also suitable. 
- pros: Skiplist makes sure that the time complex of Insert and Put is O(log n). Also, the k-v pairs in skiplist are sorted, which means processing range find is easy.
- cons: Implementing a skiplist that support concurrency and high performance is not easy.
3. Why do we need a combination of state and state_lock? Can we only use state.read() and state.write()?
- No! In fact, state guarantees that mutithreads can access state inner safely with high performance, but when state is full, we are supposed to change it with a new state(memtable). In order to prevent multiple threads changing the memtable, we must have a mutex to lock the entire state to make it. Notice that, state_lock used in freeze memtable does not conflict with read or write.