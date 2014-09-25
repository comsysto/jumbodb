package org.jumbodb.database.service.query.index.lz4;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.data.common.compression.CompressionUtil;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.longval.*;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class LongLz4IndexStrategy extends NumberLz4IndexStrategy<Long, Long, NumberIndexFile<Long>> {

    public static final int LZ4_INDEX_BLOCK_SIZE = 32 * 1020; // must be a multiple of 20! (8 byte long data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(LongLz4IndexStrategy.class);

    @Override
    public Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> operations = new HashMap<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>>();
        operations.put(QueryOperation.EQ, new LongEqOperationSearch());
        operations.put(QueryOperation.NE, new LongNeOperationSearch());
        operations.put(QueryOperation.GT, new LongGtOperationSearch());
        operations.put(QueryOperation.GT_EQ, new LongGtEqOperationSearch());
        operations.put(QueryOperation.LT, new LongLtOperationSearch());
        operations.put(QueryOperation.LT_EQ, new LongLtEqOperationSearch());
        operations.put(QueryOperation.BETWEEN, new LongBetweenOperationSearch());
        return operations;
    }

    @Override
    public int getCompressionBlockSize() {
        return LZ4_INDEX_BLOCK_SIZE;
    }

    @Override
    public Long readValueFromDataInput(DataInput dis) throws IOException {
        return dis.readLong();
    }

    @Override
    public Long readLastValue(byte[] uncompressed) {
        return CompressionUtil.readLong(uncompressed, uncompressed.length - 20);
    }

    @Override
    public Long readFirstValue(byte[] uncompressed) {
        return CompressionUtil.readLong(uncompressed, 0);
    }

    @Override
    public NumberIndexFile<Long> createIndexFile(Long from, Long to, File indexFile) {
        return new NumberIndexFile<Long>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        return "LONG_LZ4";
    }
}