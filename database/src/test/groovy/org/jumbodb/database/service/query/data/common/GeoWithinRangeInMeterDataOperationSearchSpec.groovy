package org.jumbodb.database.service.query.data.common

import org.jumbodb.common.query.DataQuery
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class GeoWithinRangeInMeterDataOperationSearchSpec extends Specification {
    def operation = new GeoWithinRangeInMeterDataOperationSearch()

    @Unroll
    def "should match inside the range [#lat, #lon] and [#testLat, #testLon] with distance #distance meter == #isWithingRange"() {
        expect:
        List<Double> p1 = Arrays.asList(lat, lon)
        operation.matches(Arrays.asList(testLat, testLon), Arrays.asList(p1, distance)) == isWithingRange
        where:
        lat       | lon       | distance | testLat   | testLon   | isWithingRange
        48.229361 | 11.299782 | 7234     | 48.188407 | 11.375656 | true
        48.229361 | 11.299782 | 12560    | 48.172039 | 11.445866 | false
        51.629100 | -0.200500 | 29016    | 51.534377 | 0.190887  | true
        51.629100 | -0.200500 | 47009    | 51.527543 | 0.460052  | false
    }
}
