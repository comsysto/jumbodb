package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation

/**
 * @author Carsten Hufe
 */
class GeoWithinRangeInMeterJsonOperationSearchSpec extends spock.lang.Specification {
    def operation = new GeoWithinRangeInMeterJsonOperationSearch()

    def "should match inside the range"() {
        expect:
        List<Double> p1 = Arrays.asList(lat, lon)
        QueryClause q = new QueryClause(QueryOperation.GEO_WITHIN_RANGE_METER, Arrays.asList(p1, distance));
        operation.matches(q, Arrays.asList(testLat, testLon)) == result
        where:
        lat       | lon       | distance | testLat   | testLon   | result
        48.229361 | 11.299782 | 7234     | 48.188407 | 11.375656 | true
        48.229361 | 11.299782 | 12560    | 48.172039 | 11.445866 | false
        51.629100 | -0.200500 | 29016    | 51.534377 | 0.190887  | true
        51.629100 | -0.200500 | 47009    | 51.527543 | 0.460052  | false
    }
}
