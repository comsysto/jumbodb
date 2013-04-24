package org.jumbodb.database.service.query.index.basic.numeric;

/**
 * @author Carsten Hufe
 */
public abstract class NumberGtOperationSearch<T extends Number, IF extends NumberSnappyIndexFile<T>> extends NumberEqOperationSearch<T, IF> {
    public NumberGtOperationSearch(NumberSnappyIndexStrategy<T, IF> strategy) {
        super(strategy);
    }
}
