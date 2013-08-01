package org.jumbodb.database.service.query.index.doubleval.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexStrategy;
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch;
import org.jumbodb.database.service.query.snappy.SnappyUtil;
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
public class DoubleSnappyIndexStrategy extends NumberSnappyIndexStrategy<Double, Double, NumberSnappyIndexFile<Double>> {

    public static final int SNAPPY_INDEX_CHUNK_SIZE = 32 * 1020; // must be a multiple of 20! (8 byte double data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(DoubleSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, OperationSearch<Double, Double, NumberSnappyIndexFile<Double>>> getQueryOperationsStrategies() {
        Map<QueryOperation, OperationSearch<Double, Double, NumberSnappyIndexFile<Double>>> operations = new HashMap<QueryOperation, OperationSearch<Double, Double, NumberSnappyIndexFile<Double>>>();
        operations.put(QueryOperation.EQ, new DoubleEqOperationSearch(this));
        operations.put(QueryOperation.NE, new DoubleNeOperationSearch(this));
        operations.put(QueryOperation.GT, new DoubleGtOperationSearch(this));
        operations.put(QueryOperation.LT, new DoubleLtOperationSearch(this));
        operations.put(QueryOperation.BETWEEN, new DoubleBetweenOperationSearch(this));
        return operations;
    }

    @Override
    public int getSnappyChunkSize() {
        return SNAPPY_INDEX_CHUNK_SIZE;
    }

    @Override
    public Double readValueFromDataInput(DataInput dis) throws IOException {
        return dis.readDouble();
    }

    @Override
    public Double readLastValue(byte[] uncompressed) {
        return SnappyUtil.readDouble(uncompressed, uncompressed.length - 20);
    }

    @Override
    public Double readFirstValue(byte[] uncompressed) {
        return SnappyUtil.readDouble(uncompressed, 0);
    }

    @Override
    public NumberSnappyIndexFile<Double> createIndexFile(Double from, Double to, File indexFile) {
        return new NumberSnappyIndexFile<Double>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        return "DOUBLE_SNAPPY_V1";
    }
}
