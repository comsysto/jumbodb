package org.jumbodb.common.query;

/**
 * @author Carsten Hufe
 */
public class BetweenQuery {
    private Number from;
    private Number to;

    public BetweenQuery() {
    }

    public BetweenQuery(Number from, Number to) {
        this.from = from;
        this.to = to;
    }

    public Number getFrom() {
        return from;
    }

    public void setFrom(Number from) {
        this.from = from;
    }

    public Number getTo() {
        return to;
    }

    public void setTo(Number to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "BetweenOperation{" +
                "from=" + from +
                ", to=" + to +
                '}';
    }
}
