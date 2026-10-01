package com.example.gsb.cache;

import java.io.IOException;

/**
 * 按块寻址的块存储抽象。所有索引均为块编号（非字节偏移）。
 */
public interface BlockStore {

    /** 单块大小（字节）。 */
    int blockSize();

    /** 存储中的总块数。 */
    long blockCount();

    /** 读取指定块，返回的数组长度等于 {@link #blockSize()}。 */
    byte[] readBlock(long blockIndex) throws IOException;

    /** 覆盖写入指定块（写穿语义由上层缓存负责）。 */
    void writeBlock(long blockIndex, byte[] data) throws IOException;
}
