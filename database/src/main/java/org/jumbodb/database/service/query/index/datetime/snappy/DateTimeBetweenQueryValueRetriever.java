package org.jumbodb.database.service.query.index.datetime.snappy;

import org.apache.commons.lang.UnhandledException;
import org.jumbodb.common.query.IndexQuery;
import org.jumbodb.database.service.query.index.basic.numeric.QueryValueRetriever;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author Carsten Hufe
 */
public class DateTimeBetweenQueryValueRetriever implements QueryValueRetriever {
    private List<Long> value;

    public DateTimeBetweenQueryValueRetriever(IndexQuery indexQuery) {
        SimpleDateFormat sdf = new SimpleDateFormat(DateTimeQueryValueRetriever.DATE_SEARCH_PATTERN);
        value = new ArrayList<Long>(2);
        List<?> vals = (List<?>) indexQuery.getValue();
        try {
            for (Object obj : vals) {
                if (obj instanceof String) {
                    String date = (String) obj;
                    Date parse = sdf.parse(date);
                    value.add(parse.getTime());
                } else {
                    value.add(((Number) obj).longValue());
                }
            }
        } catch (ParseException e) {
            throw new UnhandledException(e);
        }
    }

    @Override
    public <T> T getValue() {
        return (T) value;
    }
}
