package org.jumbodb.database.service.query.index.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.data.common.compression.CompressionUtil;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.floatval.*;
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
public class FloatSnappyIndexStrategy extends NumberSnappyIndexStrategy<Float, Float, NumberIndexFile<Float>> {

    public static final int SNAPPY_INDEX_BLOCK_SIZE = 32 * 1024; // must be a multiple of 16! (4 byte float data, 4 byte file name hash, 8 byte offset)

    private Logger log = LoggerFactory.getLogger(FloatSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, IndexOperationSearch<Float, Float, NumberIndexFile<Float>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<Float, Float, NumberIndexFile<Float>>> operations = new HashMap<QueryOperation, IndexOperationSearch<Float, Float, NumberIndexFile<Float>>>();
        operations.put(QueryOperation.EQ, new FloatEqOperationSearch());
        operations.put(QueryOperation.NE, new FloatNeOperationSearch());
        operations.put(QueryOperation.GT, new FloatGtOperationSearch());
        operations.put(QueryOperation.GT_EQ, new FloatGtEqOperationSearch());
        operations.put(QueryOperation.LT, new FloatLtOperationSearch());
        operations.put(QueryOperation.LT_EQ, new FloatLtEqOperationSearch());
        operations.put(QueryOperation.BETWEEN, new FloatBetweenOperationSearch());
        return operations;
    }

    @Override
    public int getCompressionBlockSize() {
        return SNAPPY_INDEX_BLOCK_SIZE;
    }

    @Override
    public Float readValueFromDataInput(DataInput dis) throws IOException {
        return dis.readFloat();
    }

    @Override
    public Float readLastValue(byte[] uncompressed) {
        return CompressionUtil.readFloat(uncompressed, uncompressed.length - 16);
    }

    @Override
    public Float readFirstValue(byte[] uncompressed) {
        return CompressionUtil.readFloat(uncompressed, 0);
    }

    @Override
    public NumberIndexFile<Float> createIndexFile(Float from, Float to, File indexFile) {
        return new NumberIndexFile<Float>(from, to, indexFile);
    }

    @Override
    public String getStrategyName() {
        return "FLOAT_SNAPPY";
    }
}
