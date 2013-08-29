package org.jumbodb.database.service.query.index.hashcode64.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch;
import org.jumbodb.database.service.query.index.datetime.snappy.DateTimeBetweenOperationSearch;
import org.jumbodb.database.service.query.index.datetime.snappy.DateTimeGtOperationSearch;
import org.jumbodb.database.service.query.index.datetime.snappy.DateTimeLtOperationSearch;
import org.jumbodb.database.service.query.index.datetime.snappy.DateTimeNeOperationSearch;
import org.jumbodb.database.service.query.index.longval.snappy.LongSnappyIndexStrategy;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class HashCode64SnappyIndexStrategy extends LongSnappyIndexStrategy {
    @Override
    public Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> getQueryOperationsStrategies() {
        Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> operations = new HashMap<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>>();
        operations.put(QueryOperation.EQ, new HashCode64EqOperationSearch());
        return operations;
    }

    @Override
    public String getStrategyName() {
        return "HASHCODE64_SNAPPY_V1";
    }
}
