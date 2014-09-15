package org.jumbodb.database.service.query.index.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.doubleval.DoubleBetweenOperationSearch;
import org.jumbodb.database.service.query.index.common.doubleval.DoubleEqOperationSearch;
import org.jumbodb.database.service.query.index.common.doubleval.DoubleGtOperationSearch;
import org.jumbodb.database.service.query.index.common.doubleval.DoubleLtOperationSearch;
import org.jumbodb.database.service.query.index.common.doubleval.DoubleNeOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
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
public class DoubleSnappyIndexStrategy extends NumberSnappyIndexStrategy<Double, Double, NumberIndexFile<Double>> {

    public static final int SNAPPY_INDEX_CHUNK_SIZE = 32 * 1020; // must be a multiple of 20! (8 byte double data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(DoubleSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, IndexOperationSearch<Double, Double, NumberIndexFile<Double>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<Double, Double, NumberIndexFile<Double>>> operations = new HashMap<QueryOperation, IndexOperationSearch<Double, Double, NumberIndexFile<Double>>>();
        operations.put(QueryOperation.EQ, new DoubleEqOperationSearch());
        operations.put(QueryOperation.NE, new DoubleNeOperationSearch());
        operations.put(QueryOperation.GT, new DoubleGtOperationSearch());
        operations.put(QueryOperation.LT, new DoubleLtOperationSearch());
        operations.put(QueryOperation.BETWEEN, new DoubleBetweenOperationSearch());
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
    public NumberIndexFile<Double> createIndexFile(Double from, Double to, File indexFile) {
        return new NumberIndexFile<Double>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        // CARSTEN rename remove version everywhere
        return "DOUBLE_SNAPPY_V1";
    }
}
