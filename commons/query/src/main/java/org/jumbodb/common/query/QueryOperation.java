package org.jumbodb.common.query;

public enum QueryOperation {
    // CARSTEN fill with meta info to make error messages better ... GT(">") etc ...
    OR, EQ, NE, LT, GT, BETWEEN, GEO_BOUNDARY_BOX, GEO_WITHIN_RANGE_METER
    // CARSTEN implement these types:
    // CARSTEN LIKE, LT_EQ, GT_EQ, IS NULL, IS NOT NULL, EXISTS, NOT EXISTS, IN

    // CARSTEN IN muste be implemented for all strategies
}