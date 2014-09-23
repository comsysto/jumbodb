package org.jumbodb.database.service.query.index.lz4;

import org.jumbodb.common.query.QueryOperation;
import org.jumbodb.database.service.query.index.common.IndexOperationSearch;
import org.jumbodb.database.service.query.index.common.hashcode32.HashCode32EqOperationSearch;
import org.jumbodb.database.service.query.index.common.numeric.NumberIndexFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class HashCode32Lz4IndexStrategy extends IntegerLz4IndexStrategy {
    private Logger log = LoggerFactory.getLogger(HashCode32Lz4IndexStrategy.class);

    @Override
    public Map<QueryOperation, IndexOperationSearch<Integer, Integer, NumberIndexFile<Integer>>> getQueryOperationsStrategies() {
        Map<QueryOperation, IndexOperationSearch<Integer, Integer, NumberIndexFile<Integer>>> operations = new HashMap<QueryOperation, IndexOperationSearch<Integer, Integer, NumberIndexFile<Integer>>>();
        operations.put(QueryOperation.EQ, new HashCode32EqOperationSearch());
        return operations;
    }

    @Override
    public String getStrategyName() {
        return "HASHCODE32_LZ4";
    }
}
