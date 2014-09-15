package org.jumbodb.database.service.query.index.common.numeric;


import org.jumbodb.database.service.query.index.common.BlockRange;

import java.io.IOException;

/**
 * @author Carsten Hufe
 */
public interface FileDataRetriever<T> {
//    byte[] getUncompressedBlock(long searchChunk) throws IOException;
    BlockRange<T> getBlockRange(long searchChunk) throws IOException;
}
