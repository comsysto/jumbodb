package org.jumbodb.common.query;

/**
 * @author Carsten
 */
public class SelectField {
    public static final String ALL = "*";
    private String columnName;
    private String alias;
    private SelectFieldFunction function;
    private boolean distinct;

    public SelectField() {
    }

    public SelectField(String columnName, String alias) {
        this(columnName, alias, SelectFieldFunction.NONE, false);
    }

    public SelectField(String columnName, String alias, SelectFieldFunction function, boolean distinct) {
        this.columnName = columnName;
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

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
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
                "name='" + columnName + '\'' +
                ", function=" + function +
                ", distinct=" + distinct +
                '}';
    }
}
