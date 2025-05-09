# EthKV

1. Store the KV pairs: use <key_size (short) + value_size (short) + key + value>.
2. Slots management:
    1. Control status of all slots: E.g., uses a bit map to record the status of all slots.
    2. Slot merge and split: E.g., when the size of a slot is not enough, merge two slots into one larger slot (logically). When the size of the slot is too large, split it into two smaller slots (logically).
    3. Allocation:
       1. |1|1|0|: offset + slot number;
       2. |1|2|0|- > |0|2|1|1|: GC: copy the data from the old slot to the new slot (at least two free slots) and then delete the old slot.
3. Write path:
    1. Add write batch and write commit as Pebble.
    2. Write batch: use a write batch to record all the writes and then write them to the disk with a single write.
    3. In memory write batch -> create a skip list index (temp index).
    4. Orgainze slots based on the modified KV pairs.
    5. Reclaim old slots (append to free list). pwrite (offset, data, size);-> size % slot size == 0 (padding 0).
    6. Free slots list to manage all the free slots.
4. Read path:
    1. Read append only slots: reverse the order of KV pairs in the slots. If find duplicate KV pairs, then remove them.
    2. Read accessed slots: If find duplicate KV pairs, then remove them.
    3. All valid KV pairs are stored in the memory.
    4. Main thread serve the read requests (also include all these valid KV pairs in the Cache).
    5. Use **background thread** to perform GC (garbage collection):
       1. E.g., Use message queue to send the GC request to the background thread. <valid KV pairs, old slots ID>.
       2. Write the valid KV pairs to new slots.
       3. Update the prefix tree.
       4. Free the old slots.
5. Cache:
    1. Prefix-aware: If the leaf node is cached, then the sibling nodes are also cached.
    2. Cache eviction: use LFU to evict the least frequently used nodes (based on leaf node). Remove the sibling nodes from the cache (if not required by other nodes in the cache).
    3. Impl: reference counter for internal nodes.
    4. Cache + write batch: write the new KV pair into the cache, then flush the slots to the disk.
6. Look at the writing path in Geth. Find a way to determine which KV pairs belong to the same smart contract.
    1. Use the same prefix (based on the account) for all the KV pairs of the same smart contract. E.g., Code, Account, Storage.
