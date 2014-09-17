package org.jumbodb.common.query;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.LinkedList;
import java.util.List;

@JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataQuery {
    private Object left;
    private FieldType leftType;
    private QueryOperation queryOperation;
    private Object right;
    private FieldType rightType;
    private DataQuery and;
    private List<DataQuery> ors = new LinkedList<DataQuery>();

    public DataQuery() {
    }

    public DataQuery(List<DataQuery> ors) {
        queryOperation = QueryOperation.OR;
        this.ors = ors;
    }

    public DataQuery(Object left, FieldType leftType, QueryOperation queryOperation, Object right, FieldType rightType) {
        this.left = left;
        this.leftType = leftType;
        this.queryOperation = queryOperation;
        this.right = right;
        this.rightType = rightType;
    }

    public DataQuery(Object left, FieldType leftType, QueryOperation queryOperation, Object right, FieldType rightType, DataQuery and) {
        this.left = left;
        this.leftType = leftType;
        this.queryOperation = queryOperation;
        this.right = right;
        this.rightType = rightType;
        this.and = and;
    }

    public Object getLeft() {
        return left;
    }

    public void setLeft(Object left) {
        this.left = left;
    }

    public FieldType getLeftType() {
        return leftType;
    }

    public void setLeftType(FieldType leftType) {
        this.leftType = leftType;
    }

    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    public void setQueryOperation(QueryOperation queryOperation) {
        this.queryOperation = queryOperation;
    }

    public Object getRight() {
        return right;
    }

    public void setRight(Object right) {
        this.right = right;
    }

    public FieldType getRightType() {
        return rightType;
    }

    public void setRightType(FieldType rightType) {
        this.rightType = rightType;
    }

    public DataQuery getAnd() {
        return and;
    }

    public void setAnd(DataQuery and) {
        this.and = and;
    }

    public List<DataQuery> getOrs() {
        return ors;
    }

    public void setOrs(List<DataQuery> ors) {
        this.ors = ors;
    }

    @Override
    public String toString() {
        return "DataQuery{" +
          "left='" + left + '\'' +
          ", leftType=" + leftType +
          ", queryOperation=" + queryOperation +
          ", right='" + right + '\'' +
          ", rightType=" + rightType +
          ", and=" + and +
          ", ors=" + ors +
          '}';
    }
}

