package org.jumbodb.database.service.query.index.longval.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch;
import org.jumbodb.data.common.snappy.SnappyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class LongSnappyIndexStrategy extends NumberSnappyIndexStrategy<Long, Long, NumberSnappyIndexFile<Long>> {

    public static final int SNAPPY_INDEX_CHUNK_SIZE = 32 * 1020; // must be a multiple of 20! (8 byte long data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(LongSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> getQueryOperationsStrategies() {
        Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> operations = new HashMap<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>>();
        operations.put(QueryOperation.EQ, new LongEqOperationSearch(this));
        operations.put(QueryOperation.NE, new LongNeOperationSearch(this));
        operations.put(QueryOperation.GT, new LongGtOperationSearch(this));
        operations.put(QueryOperation.LT, new LongLtOperationSearch(this));
        operations.put(QueryOperation.BETWEEN, new LongBetweenOperationSearch(this));
        return operations;
    }

    @Override
    public int getSnappyChunkSize() {
        return SNAPPY_INDEX_CHUNK_SIZE;
    }

    @Override
    public Long readValueFromDataInputStream(DataInputStream dis) throws IOException {
        return dis.readLong();
    }

    @Override
    public Long readLastValue(byte[] uncompressed) {
        return SnappyUtil.readLong(uncompressed, uncompressed.length - 20);
    }

    @Override
    public Long readFirstValue(byte[] uncompressed) {
        return SnappyUtil.readLong(uncompressed, 0);
    }

    @Override
    public NumberSnappyIndexFile<Long> createIndexFile(Long from, Long to, File indexFile) {
        return new NumberSnappyIndexFile<Long>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        return "LONG_SNAPPY_V1";
    }
}
