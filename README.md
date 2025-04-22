# EthKV


1. Store the KV pairs: use <key_size + value_size + key + value>.
2. Slots management:
    1. Control status of all slots: E.g., uses a bit map to record the status of all slots.
    2. Slot merge and split: E.g., when the size of a slot is not enough, merge two slots into one larger slot (logically). When the size of the slot is too large, split it into two smaller slots (logically).
3. Write path:
    1. Add write batch and write commit as Pebble.
    2. Write batch: use a write batch to record all the writes and then write them to the disk with a single write.
4. Cache:
    1. Prefix-aware: If the leaf node is cached, then the sibling nodes are also cached.
    2. Cache eviction: use LFU to evict the least frequently used nodes (based on leaf node). Remove the sibling nodes from the cache (if not required by other nodes in the cache).
5. Look at the writing path in Geth. Find a way to determine which KV pairs belong to the same smart contract.
    1. Use the same prefix (based on the account) for all the KV pairs of the same smart contract. E.g., Code, Account, Storage.
