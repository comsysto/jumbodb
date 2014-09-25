package org.jumbodb.common.query;

public enum QueryOperation {


    OR("OR"),
    EXISTS("EXISTS"),
    NOT_EXISTS("EXISTS"),
    EQ("=="),
    NE("!="),
    LT("<"),
    LT_EQ("<="),
    GT(">"),
    GT_EQ(">="),
    BETWEEN("BETWEEN ... AND ..."),
    GEO_BOUNDARY_BOX("geoFindInBoundaryBox(...)"),
    GEO_WITHIN_RANGE_METER("geoWithinRangeInMeter(...)");
    // CARSTEN implement these types:
    // CARSTEN LIKE, IS NULL, IS NOT NULL, IN

    // CARSTEN IN muste be implemented for all strategies

    private String operation;

    QueryOperation(String operation) {
        this.operation = operation;
    }

    public String getOperation() {
        return operation;
    }
}