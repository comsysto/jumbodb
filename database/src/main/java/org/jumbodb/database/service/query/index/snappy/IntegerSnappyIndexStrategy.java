package org.jumbodb.database.service.query.index.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.common.integer.IntegerBetweenOperationSearch;
import org.jumbodb.database.service.query.index.common.integer.IntegerEqOperationSearch;
import org.jumbodb.database.service.query.index.common.integer.IntegerGtOperationSearch;
import org.jumbodb.database.service.query.index.common.integer.IntegerLtOperationSearch;
import org.jumbodb.database.service.query.index.common.integer.IntegerNeOperationSearch;
import org.jumbodb.data.common.snappy.SnappyUtil;
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
public class IntegerSnappyIndexStrategy extends NumberSnappyIndexStrategy<Integer, Integer, NumberIndexFile<Integer>> {

    public static final int SNAPPY_INDEX_CHUNK_SIZE = 32 * 1024; // must be a multiple of 16! (4 byte integer data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(IntegerSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, IndexOperationSearch<Integer, Integer, NumberIndexFile<Integer>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<Integer, Integer, NumberIndexFile<Integer>>> operations = new HashMap<QueryOperation, IndexOperationSearch<Integer, Integer, NumberIndexFile<Integer>>>();
        operations.put(QueryOperation.EQ, new IntegerEqOperationSearch());
        operations.put(QueryOperation.NE, new IntegerNeOperationSearch());
        operations.put(QueryOperation.GT, new IntegerGtOperationSearch());
        operations.put(QueryOperation.LT, new IntegerLtOperationSearch());
        operations.put(QueryOperation.BETWEEN, new IntegerBetweenOperationSearch());
        return operations;
    }

    @Override
    public int getSnappyChunkSize() {
        return SNAPPY_INDEX_CHUNK_SIZE;
    }

    @Override
    public Integer readValueFromDataInput(DataInput dis) throws IOException {
        return dis.readInt();
    }

    @Override
    public Integer readLastValue(byte[] uncompressed) {
        return SnappyUtil.readInt(uncompressed, uncompressed.length - 16);
    }

    @Override
    public Integer readFirstValue(byte[] uncompressed) {
        return SnappyUtil.readInt(uncompressed, 0);
    }

    @Override
    public NumberIndexFile<Integer> createIndexFile(Integer from, Integer to, File indexFile) {
        return new NumberIndexFile<Integer>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        // CARSTEN rename remove version everywhere
        return "INTEGER_SNAPPY_V1";
    }
}
