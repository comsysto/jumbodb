package org.jumbodb.database.service.query;

import org.jumbodb.common.query.JumboQuery;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * User: carsten
 * Date: 2/6/13
 * Time: 4:16 PM
 */
public interface ResultCallback {
    void writeResult(byte[] result) throws IOException;
    boolean needsMore(JumboQuery jumboQuery) throws IOException;
    void collect(CancelableTask cancelableTask);
}
