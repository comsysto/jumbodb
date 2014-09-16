package org.jumbodb.database.service.query.data.common

import org.jumbodb.common.query.DataQuery
import org.jumbodb.common.query.QueryOperation
import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class GeoBoundaryBoxDataOperationSearchSpec extends Specification {
    def operation = new GeoBoundaryBoxDataOperationSearch()

    @Unroll
    def "should match the boundary box [[#lat1, #lon1][#lat2, #lon2]] with point [#testLat, #testLon] == #isInBoundaryBox"() {
        expect:
        List<Double> p1 = Arrays.asList(lat1, lon1)
        List<Double> p2 = Arrays.asList(lat2, lon2)
        operation.matches(Arrays.asList(testLat, testLon), Arrays.asList(p1, p2)) == isInBoundaryBox
        where:
        lat1      | lon1      | lat2      | lon2      | testLat   | testLon   | isInBoundaryBox
        48.229361 | 11.299782 | 48.175588 | 11.428185 | 48.188407 | 11.375656 | true
        48.229361 | 11.299782 | 48.175588 | 11.428185 | 48.172039 | 11.445866 | false
        51.629100 | -0.200500 | 51.469408 | 0.314484  | 51.534377 | 0.190887  | true
        51.629100 | -0.200500 | 51.469408 | 0.314484  | 51.527543 | 0.460052  | false
    }
}
