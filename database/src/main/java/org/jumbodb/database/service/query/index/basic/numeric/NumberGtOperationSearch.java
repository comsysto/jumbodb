package org.jumbodb.database.service.query.index.basic.numeric;

/**
 * @author Carsten Hufe
 */
public abstract class NumberGtOperationSearch<T, IFV, IF extends NumberSnappyIndexFile<IFV>> extends NumberEqOperationSearch<T, IFV, IF> {
    public NumberGtOperationSearch(NumberSnappyIndexStrategy<T, IFV, IF> strategy) {
        super(strategy);
    }
}
