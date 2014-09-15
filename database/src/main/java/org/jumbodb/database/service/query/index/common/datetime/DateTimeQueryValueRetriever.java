package org.jumbodb.database.service.query.index.common.datetime;

import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.common.QueryValueRetriever;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Carsten Hufe
 */
public class DateTimeQueryValueRetriever implements QueryValueRetriever {
    public static final String DATE_SEARCH_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private Long value;

    public DateTimeQueryValueRetriever(IndexQuery indexQuery) {
        Object obj = indexQuery.getValue();
        if (obj instanceof String) {
            String date = (String) obj;
            SimpleDateFormat sdf = new SimpleDateFormat(DATE_SEARCH_PATTERN);
            try {
                Date parse = sdf.parse(date);
                value = parse.getTime();
            } catch (ParseException e) {
                throw new UnhandledException(e);
            }
        } else {
            value = ((Number) obj).longValue();
        }
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
