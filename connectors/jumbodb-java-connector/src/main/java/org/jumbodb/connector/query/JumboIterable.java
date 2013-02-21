package org.jumbodb.connector.query;

import java.util.Iterator;

/**
 * User: carsten
 * Date: 2/6/13
 * Time: 4:39 PM
 */
public class JumboIterable<T> implements Iterable<T> {
    private Iterator<T> it;

    public JumboIterable(Iterator<T> it) {
        this.it = it;
    }

    @Override
    public Iterator<T> iterator() {
        return it;
    }
}
