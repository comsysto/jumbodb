package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch;
import org.jumbodb.data.common.snappy.SnappyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class IntegerSnappyIndexStrategy extends NumberSnappyIndexStrategy<Integer, Integer, NumberSnappyIndexFile<Integer>> {

    public static final int SNAPPY_INDEX_CHUNK_SIZE = 32 * 1024; // must be a multiple of 16! (4 byte integer data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(IntegerSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, OperationSearch<Integer, Integer, NumberSnappyIndexFile<Integer>>> getQueryOperationsStrategies() {
        Map<QueryOperation, OperationSearch<Integer, Integer, NumberSnappyIndexFile<Integer>>> operations = new HashMap<QueryOperation, OperationSearch<Integer, Integer, NumberSnappyIndexFile<Integer>>>();
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
    public NumberSnappyIndexFile<Integer> createIndexFile(Integer from, Integer to, File indexFile) {
        return new NumberSnappyIndexFile<Integer>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        return "INTEGER_SNAPPY_V1";
    }
}
