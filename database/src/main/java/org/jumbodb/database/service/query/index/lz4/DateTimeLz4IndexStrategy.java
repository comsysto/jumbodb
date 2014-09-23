package org.jumbodb.database.service.query.index.lz4;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.datetime.*;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class DateTimeLz4IndexStrategy extends LongLz4IndexStrategy {
    @Override
    public Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> operations = new HashMap<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>>();
        operations.put(QueryOperation.EQ, new DateTimeEqOperationSearch());
        operations.put(QueryOperation.NE, new DateTimeNeOperationSearch());
        operations.put(QueryOperation.GT, new DateTimeGtOperationSearch());
        operations.put(QueryOperation.GT_EQ, new DateTimeGtEqOperationSearch());
        operations.put(QueryOperation.LT, new DateTimeLtOperationSearch());
        operations.put(QueryOperation.LT_EQ, new DateTimeLtEqOperationSearch());
        operations.put(QueryOperation.BETWEEN, new DateTimeBetweenOperationSearch());
        return operations;
    }

    @Override
    public String getStrategyName() {
        return "DATETIME_LZ4";
    }
}
