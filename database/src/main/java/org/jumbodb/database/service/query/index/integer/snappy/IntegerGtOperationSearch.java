package org.jumbodb.database.service.query.index.integer.snappy;

import org.jumbodb.database.service.query.snappy.SnappyChunks;
import org.jumbodb.database.service.query.snappy.SnappyUtil;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Carsten Hufe
 */
public class IntegerGtOperationSearch extends IntegerEqOperationSearch {
    @Override
    public boolean matching(Integer currentValue, Integer searchValue) {
        return currentValue > searchValue;
    }
}
