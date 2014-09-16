package org.jumbodb.database.service.query.index.snappy;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.datetime.DateTimeBetweenOperationSearch;
import org.jumbodb.database.service.query.index.common.datetime.DateTimeEqOperationSearch;
import org.jumbodb.database.service.query.index.common.datetime.DateTimeGtOperationSearch;
import org.jumbodb.database.service.query.index.common.datetime.DateTimeLtOperationSearch;
import org.jumbodb.database.service.query.index.common.datetime.DateTimeNeOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class DateTimeSnappyIndexStrategy extends LongSnappyIndexStrategy {
    @Override
    public Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> operations = new HashMap<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>>();
        operations.put(QueryOperation.EQ, new DateTimeEqOperationSearch());
        operations.put(QueryOperation.NE, new DateTimeNeOperationSearch());
        operations.put(QueryOperation.GT, new DateTimeGtOperationSearch());
        operations.put(QueryOperation.LT, new DateTimeLtOperationSearch());
        operations.put(QueryOperation.BETWEEN, new DateTimeBetweenOperationSearch());
        return operations;
    }

    @Override
    public String getStrategyName() {
        return "DATETIME_SNAPPY";
    }
}
