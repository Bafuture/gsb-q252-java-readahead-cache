package com.example.gsb.storage;

/**
 * Block-oriented underlying storage. Blocks are fixed-size byte arrays addressed by a
 * non-negative {@code blockId}.
 *
 * <p>Implementations are allowed to be slow ({@link #readBlock} may block); the read-cache
 * layer ({@link CachedBlockStore}) is responsible for coalescing and avoiding such reads.
 */
public interface BlockStorage {

    /** Read one block. The returned array must contain the block's current contents. */
    byte[] readBlock(long blockId);

    /** Overwrite one block with the given contents. */
    void writeBlock(long blockId, byte[] data);
}
