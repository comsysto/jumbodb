package org.jumbodb.database.service.query.index.hashcode.snappy;

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
public class HashCodeSnappyIndexStrategy extends IntegerSnappyIndexStrategy {


    private Logger log = LoggerFactory.getLogger(HashCodeSnappyIndexStrategy.class);

    @Override
    public Map<QueryOperation, OperationSearch<Integer, NumberSnappyIndexFile<Integer>>> getQueryOperationsStrategies() {
        Map<QueryOperation, OperationSearch<Integer, NumberSnappyIndexFile<Integer>>> operations = new HashMap<QueryOperation, OperationSearch<Integer, NumberSnappyIndexFile<Integer>>>();
        operations.put(QueryOperation.EQ, new HashCodeEqOperationSearch(this));
        return operations;
    }

    @Override
    public String getStrategyName() {
        return "HASHCODE_SNAPPY_V1";
    }
}
