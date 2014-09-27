package org.jumbodb.common.query;

public enum QueryOperation {
    OR("OR"),
    EXISTS("EXISTS"),
    NOT_EXISTS("NOT EXISTS"),
    EQ("=="),
    NE("!="),
    LT("<"),
    LT_EQ("<="),
    GT(">"),
    GT_EQ(">="),
    BETWEEN("BETWEEN ... AND ..."),
    GEO_BOUNDARY_BOX("GEO_BOUNDARY_BOX(...)"),
    GEO_WITHIN_RANGE_METER("GEO_WITHIN_RANGE_METER(...)"),
    LIKE("LIKE"),
    NOT_LIKE("NOT LIKE"),
    IS_NULL("IS NULL"),
    IS_NOT_NULL("IS NOT NULL");

    private String operation;

    QueryOperation(String operation) {
        this.operation = operation;
    }

    public String getOperation() {
        return operation;
    }
}