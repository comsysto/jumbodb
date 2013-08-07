package org.jumbodb.database.service.query.index.geohash.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class GeohashWithingRangeMeterQueryValueRetrieverSpec extends Specification {
    @Unroll
    def "verify boundary box parsing #queryValue"() {
        expect:
        def retriever = new GeohashWithingRangeMeterQueryValueRetriever(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, queryValue))
        def value = retriever.getValue()
        value instanceof GeohashRangeMeterBox
        value.getLatitude() == queryValue[0][0]
        value.getLongitude() == queryValue[0][1]
        value.getRangeInMeter() == queryValue[1]
        Integer.toBinaryString(value.getGeohashFirstMatchingBits()) == firstMatchingBits
        value.getBitsToShift() == numberOfMatchingBits
        where:
        queryValue                      | firstMatchingBits                  | numberOfMatchingBits
        [[48.207688, 11.331185], 10000] | "11111111111111010000100100000100" | 12 // olching
        [[51.516652, -0.131793], 30000] | "11110101110101110"                | 14 // london 0 meridian damn
    }

    def "expect exception on bullshit string"() {
        when:
        new GeohashWithingRangeMeterQueryValueRetriever(new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, ["bullshit", "andso"]))
        then:
        thrown ClassCastException
    }
}