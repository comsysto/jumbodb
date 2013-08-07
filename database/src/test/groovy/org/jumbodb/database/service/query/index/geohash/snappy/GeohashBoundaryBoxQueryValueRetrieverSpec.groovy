package org.jumbodb.database.service.query.index.geohash.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class GeohashBoundaryBoxQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify boundary box parsing #queryValue"() {
        expect:
        def retriever = new GeohashBoundaryBoxQueryValueRetriever(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, queryValue))
        def value = retriever.getValue()
        value instanceof GeohashBoundaryBox
        value.getLatitude1() == queryValue[0][0]
        value.getLongitude1() == queryValue[0][1]
        value.getLatitude2() == queryValue[1][0]
        value.getLongitude2() == queryValue[1][1]
        Integer.toBinaryString(value.getGeohashFirstMatchingBits()) == firstMatchingBits
        value.getBitsToShift() == numberOfMatchingBits
        where:
        queryValue                                       | firstMatchingBits                  | numberOfMatchingBits
        [[48.207688, 11.331185], [48.215382, 11.352847]] | "11111111111101000010010000010000" | 10 // olching
        [[51.516652, -0.131793], [51.586833, 0.077033]]  | "1111010111010111011100010010000"  | 0 // london 0 meridian
    }

    def "expect exception on bullshit string"() {
        when:
        new GeohashBoundaryBoxQueryValueRetriever(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, ["bullshit", "andso"]))
        then:
        thrown ClassCastException
    }
}