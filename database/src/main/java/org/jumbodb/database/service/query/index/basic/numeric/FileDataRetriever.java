package org.jumbodb.database.service.query.index.basic.numeric;


import org.jumbodb.data.common.snappy.SnappyUtil;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public interface FileDataRetriever<T> {
//    byte[] getUncompressedBlock(long searchChunk) throws IOException;
    BlockRange<T> getBlockRange(long searchChunk) throws IOException;
}
