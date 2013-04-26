package org.jumbodb.database.service.query.index.datetime.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.basic.numeric.NumberSnappyIndexFile;
import org.jumbodb.database.service.query.index.basic.numeric.OperationSearch;
import org.jumbodb.database.service.query.index.longval.snappy.*;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class DateTimeSnappyIndexStrategy extends LongSnappyIndexStrategy {
    @Override
    public Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> getQueryOperationsStrategies() {
        Map<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>> operations = new HashMap<QueryOperation, OperationSearch<Long, Long, NumberSnappyIndexFile<Long>>>();
        operations.put(QueryOperation.EQ, new DateTimeEqOperationSearch(this));
        operations.put(QueryOperation.NE, new DateTimeNeOperationSearch(this));
        operations.put(QueryOperation.GT, new DateTimeGtOperationSearch(this));
        operations.put(QueryOperation.LT, new DateTimeLtOperationSearch(this));
        operations.put(QueryOperation.BETWEEN, new DateTimeBetweenOperationSearch(this));
        return operations;
    }

    @Override
    public String getStrategyName() {
        return "DATETIME_SNAPPY_V1";
    }
}
