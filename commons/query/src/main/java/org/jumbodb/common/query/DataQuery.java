package org.jumbodb.common.query;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataQuery {
    private Object left;
    private FieldType leftType = FieldType.NOT_SET;
    private QueryOperation queryOperation;
    private Object right;
    private FieldType rightType = FieldType.NOT_SET;
    private HintType hintType;
    private DataQuery dataAnd;
    private List<DataQuery> dataOrs = new LinkedList<DataQuery>();

    public DataQuery() {
    }

    public DataQuery(List<DataQuery> dataOrs) {
        queryOperation = QueryOperation.OR;
        this.dataOrs = dataOrs;
    }

    public DataQuery(Object left, FieldType leftType, QueryOperation queryOperation) {
        this.left = left;
        this.leftType = leftType;
        this.queryOperation = queryOperation;
    }

    public DataQuery(Object left, FieldType leftType, QueryOperation queryOperation, Object right, FieldType rightType) {
        this.left = left;
        this.leftType = leftType;
        this.queryOperation = queryOperation;
        this.right = right;
        this.rightType = rightType;
    }

    public DataQuery(Object left, FieldType leftType, QueryOperation queryOperation, Object right, FieldType rightType, DataQuery dataAnd) {
        this.left = left;
        this.leftType = leftType;
        this.queryOperation = queryOperation;
        this.right = right;
        this.rightType = rightType;
        this.dataAnd = dataAnd;
    }

    public DataQuery(Object left, FieldType leftType, QueryOperation queryOperation, Object right, FieldType rightType, HintType hintType) {
        this.left = left;
        this.leftType = leftType;
        this.queryOperation = queryOperation;
        this.right = right;
        this.rightType = rightType;
        this.hintType = hintType;
    }

    public DataQuery(Object left, FieldType leftType, QueryOperation queryOperation, Object right, FieldType rightType, HintType hintType, DataQuery dataAnd) {
        this.left = left;
        this.leftType = leftType;
        this.queryOperation = queryOperation;
        this.right = right;
        this.rightType = rightType;
        this.hintType = hintType;
        this.dataAnd = dataAnd;
    }

    public void setHintType(HintType hintType) {
        this.hintType = hintType;
    }

    public HintType getHintType() {
        return hintType;
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

    public DataQuery getDataAnd() {
        return dataAnd;
    }

    public void setDataAnd(DataQuery dataAnd) {
        this.dataAnd = dataAnd;
    }

    public List<DataQuery> getDataOrs() {
        return dataOrs;
    }

    public void setDataOrs(List<DataQuery> dataOrs) {
        this.dataOrs = dataOrs;
    }

    @Override
    public String toString() {
        return "DataQuery{" +
                "left=" + left +
                ", leftType=" + leftType +
                ", queryOperation=" + queryOperation +
                ", right=" + right +
                ", rightType=" + rightType +
                ", hintType=" + hintType +
                ", dataAnd=" + dataAnd +
                ", dataOrs=" + dataOrs +
                '}';
    }
}

