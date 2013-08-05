package org.jumbodb.database.service.query.index.floatval.snappy;

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
public class FloatSnappyIndexStrategy extends NumberSnappyIndexStrategy<Float, Float, NumberSnappyIndexFile<Float>> {

    public static final int SNAPPY_INDEX_CHUNK_SIZE = 32 * 1024; // must be a multiple of 16! (4 byte float data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(FloatSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, OperationSearch<Float, Float, NumberSnappyIndexFile<Float>>> getQueryOperationsStrategies() {
        Map<QueryOperation, OperationSearch<Float, Float, NumberSnappyIndexFile<Float>>> operations = new HashMap<QueryOperation, OperationSearch<Float, Float, NumberSnappyIndexFile<Float>>>();
        operations.put(QueryOperation.EQ, new FloatEqOperationSearch(this));
        operations.put(QueryOperation.NE, new FloatNeOperationSearch(this));
        operations.put(QueryOperation.GT, new FloatGtOperationSearch(this));
        operations.put(QueryOperation.LT, new FloatLtOperationSearch(this));
        operations.put(QueryOperation.BETWEEN, new FloatBetweenOperationSearch(this));
        return operations;
    }

    @Override
    public int getSnappyChunkSize() {
        return SNAPPY_INDEX_CHUNK_SIZE;
    }

    @Override
    public Float readValueFromDataInput(DataInput dis) throws IOException {
        return dis.readFloat();
    }

    @Override
    public Float readLastValue(byte[] uncompressed) {
        return SnappyUtil.readFloat(uncompressed, uncompressed.length - 16);
    }

    @Override
    public Float readFirstValue(byte[] uncompressed) {
        return SnappyUtil.readFloat(uncompressed, 0);
    }

    @Override
    public NumberSnappyIndexFile<Float> createIndexFile(Float from, Float to, File indexFile) {
        return new NumberSnappyIndexFile<Float>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        return "FLOAT_SNAPPY_V1";
    }
}
