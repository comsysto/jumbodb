package org.jumbodb.common.query;

/**
 * Created by Carsten on 27.09.2014.
 */
public class OrderField {
    private String name;
    private boolean asc;

    public OrderField(String name, boolean asc) {
        this.name = name;
        this.asc = asc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isAsc() {
        return asc;
    }

    public void setAsc(boolean asc) {
        this.asc = asc;
    }
}
