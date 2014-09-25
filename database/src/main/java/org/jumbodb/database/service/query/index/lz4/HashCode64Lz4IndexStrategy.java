package org.jumbodb.database.service.query.index.lz4;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.hashcode64.HashCode64EqOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.jumbodb.database.service.query.index.snappy.LongSnappyIndexStrategy;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class HashCode64Lz4IndexStrategy extends LongLz4IndexStrategy {
    @Override
    public Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>> operations = new HashMap<QueryOperation, IndexOperationSearch<Long, Long, NumberIndexFile<Long>>>();
        operations.put(QueryOperation.EQ, new HashCode64EqOperationSearch());
        return operations;
    }

    @Override
    public String getStrategyName() {
        return "HASHCODE64_LZ4";
    }
}