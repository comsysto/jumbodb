package org.jumbodb.database.service.query.index.basic.numeric;

import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerEqOperationSearch;
import org.jumbodb.database.service.query.index.integer.snappy.IntegerSnappyIndexFile;

/**
 * @author Carsten Hufe
 */
public abstract class NumberGtOperationSearch<T extends Number, IF extends NumberSnappyIndexFile<T>> extends NumberEqOperationSearch<T, IF> {
    public NumberGtOperationSearch(NumberSnappyIndexStrategy<T, IF> strategy) {
        super(strategy);
    }
}
