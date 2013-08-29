package org.jumbodb.database.service.query.index.basic.numeric;

/**
 * @author Carsten Hufe
 */
public class BlockRange<T> {
    private T firstValue;
    private T lastValue;

    public BlockRange(T firstValue, T lastValue) {
        this.firstValue = firstValue;
        this.lastValue = lastValue;
    }

    public T getFirstValue() {
        return firstValue;
    }

    public T getLastValue() {
        return lastValue;
    }

    @Override
    public String toString() {
        return "BlockRange{" +
                "firstValue=" + firstValue +
                ", lastValue=" + lastValue +
                '}';
    }
}
