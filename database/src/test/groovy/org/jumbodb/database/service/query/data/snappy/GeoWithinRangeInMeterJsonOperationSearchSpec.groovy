package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.JsonQuery
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class GeoWithinRangeInMeterJsonOperationSearchSpec extends Specification {
    def operation = new GeoWithinRangeInMeterJsonOperationSearch()

    @Unroll
    def "should match inside the range [#lat, #lon] and [#testLat, #testLon] with distance #distance meter == #isWithingRange"() {
        expect:
        List<Double> p1 = Arrays.asList(lat, lon)
        def q = new JsonQuery("testField", QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(p1, distance));
        operation.matches(q, Arrays.asList(testLat, testLon)) == isWithingRange
        where:
        lat       | lon       | distance | testLat   | testLon   | isWithingRange
        48.229361 | 11.299782 | 7234     | 48.188407 | 11.375656 | true
        48.229361 | 11.299782 | 12560    | 48.172039 | 11.445866 | false
        51.629100 | -0.200500 | 29016    | 51.534377 | 0.190887  | true
        51.629100 | -0.200500 | 47009    | 51.527543 | 0.460052  | false
    }
}
