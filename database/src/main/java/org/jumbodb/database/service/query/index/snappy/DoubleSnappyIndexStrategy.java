package org.jumbodb.database.service.query.index.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.data.common.compression.CompressionUtil;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.doubleval.*;
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
public class DoubleSnappyIndexStrategy extends NumberSnappyIndexStrategy<Double, Double, NumberIndexFile<Double>> {

    public static final int SNAPPY_INDEX_BLOCK_SIZE = 32 * 1020; // must be a multiple of 20! (8 byte double data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(DoubleSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, IndexOperationSearch<Double, Double, NumberIndexFile<Double>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<Double, Double, NumberIndexFile<Double>>> operations = new HashMap<QueryOperation, IndexOperationSearch<Double, Double, NumberIndexFile<Double>>>();
        operations.put(QueryOperation.EQ, new DoubleEqOperationSearch());
        operations.put(QueryOperation.NE, new DoubleNeOperationSearch());
        operations.put(QueryOperation.GT, new DoubleGtOperationSearch());
        operations.put(QueryOperation.GT_EQ, new DoubleGtEqOperationSearch());
        operations.put(QueryOperation.LT, new DoubleLtOperationSearch());
        operations.put(QueryOperation.LT_EQ, new DoubleLtEqOperationSearch());
        operations.put(QueryOperation.BETWEEN, new DoubleBetweenOperationSearch());
        return operations;
    }

    @Override
    public int getCompressionBlockSize() {
        return SNAPPY_INDEX_BLOCK_SIZE;
    }

    @Override
    public Double readValueFromDataInput(DataInput dis) throws IOException {
        return dis.readDouble();
    }

    @Override
    public Double readLastValue(byte[] uncompressed) {
        return CompressionUtil.readDouble(uncompressed, uncompressed.length - 20);
    }

    @Override
    public Double readFirstValue(byte[] uncompressed) {
        return CompressionUtil.readDouble(uncompressed, 0);
    }

    @Override
    public NumberIndexFile<Double> createIndexFile(Double from, Double to, File indexFile) {
        return new NumberIndexFile<Double>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        return "DOUBLE_SNAPPY";
    }
}
