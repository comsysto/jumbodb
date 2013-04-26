package org.jumbodb.database.service.query.index.hashcode64.snappy;

import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.HashCode64;
import org.jumbodb.common.query.QueryClause;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Carsten Hufe
 */
public class HashCode64QueryValueRetriever implements QueryValueRetriever {
    private Long value;

    public HashCode64QueryValueRetriever(QueryClause queryClause) {
        value = HashCode64.hash((String) queryClause.getValue());
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
