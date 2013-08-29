package org.jumbodb.database.service.query.index.hashcode32.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerSnappyIndexStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class HashCode32SnappyIndexStrategy extends IntegerSnappyIndexStrategy {


    private Logger log = LoggerFactory.getLogger(HashCode32SnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, OperationSearch<Integer, Integer, NumberSnappyIndexFile<Integer>>> getQueryOperationsStrategies() {
        Map<QueryOperation, OperationSearch<Integer, Integer, NumberSnappyIndexFile<Integer>>> operations = new HashMap<QueryOperation, OperationSearch<Integer, Integer, NumberSnappyIndexFile<Integer>>>();
        operations.put(QueryOperation.EQ, new HashCode32EqOperationSearch());
        return operations;
    }

    @Override
    public String getStrategyName() {
        return "HASHCODE32_SNAPPY_V1";
    }
}
