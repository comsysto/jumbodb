package org.jumbodb.connector.query;

import java.util.Iterator;

/**
 * User: carsten
 * Date: 2/6/13
 * Time: 4:39 PM
 */
public class JumboIterator<T> implements Iterator<T> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        return null;
    }

    @Override
    public void remove() {
        // nothing to do
    }
}
