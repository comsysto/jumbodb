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
        def retriever = new GeohashQueryValueRetriever(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, queryValue))
        def container = retriever.getValue()
        container instanceof GeohashContainer
        def value = container.getSplittedBoxes()[0] // upperLeftBox
        Integer.toBinaryString(value.getGeohashFirstMatchingBits()) == firstMatchingBits
        value.getBitsToShift() == numberOfMatchingBits
        where:
        queryValue                                       | firstMatchingBits                  | numberOfMatchingBits
        [[48.207688, 11.331185], [48.215382, 11.352847]] | "11111111111101000010010000010000" | 10 // olching
        [[51.516652d, -0.131793], [51.586833, 0.077033]] | "111101011101011101110"            | 10
    }

    def "expect exception on bullshit string"() {
        when:
        new GeohashQueryValueRetriever(new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, ["bullshit", "andso"]))
        then:
        thrown ClassCastException
    }
}