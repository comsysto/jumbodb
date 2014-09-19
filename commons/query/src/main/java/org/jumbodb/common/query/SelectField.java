package org.jumbodb.common.query;

/**
 * @author Carsten
 */
public class SelectField {
    public static final String ALL = "*";
    private String name;
    private String alias;
    private SelectFieldFunction function;
    private boolean distinct;

    public SelectField() {
    }

    public SelectField(String name, String alias) {
        this(name, alias, SelectFieldFunction.NONE, false);
    }

    public SelectField(String name, String alias, SelectFieldFunction function, boolean distinct) {
        this.name = name;
        this.alias = alias;
        this.function = function;
        this.distinct = distinct;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getAlias() {
        return alias;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SelectFieldFunction getFunction() {
        return function;
    }

    public void setFunction(SelectFieldFunction function) {
        this.function = function;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    @Override
    public String toString() {
        return "SelectField{" +
                "name='" + name + '\'' +
                ", function=" + function +
                ", distinct=" + distinct +
                '}';
    }
}
