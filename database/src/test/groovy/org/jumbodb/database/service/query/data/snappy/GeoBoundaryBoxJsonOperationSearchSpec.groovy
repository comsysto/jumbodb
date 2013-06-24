package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation

/**
 * @author Carsten Hufe
 */
class GeoBoundaryBoxJsonOperationSearchSpec extends spock.lang.Specification {
    def operation = new GeoBoundaryBoxJsonOperationSearch()

    def "should match the boundary box"() {
        expect:
        List<Double> p1 = Arrays.asList(lat1, lon1)
        List<Double> p2 = Arrays.asList(lat2, lon2)
        QueryClause q = new QueryClause(QueryOperation.GEO_BOUNDARY_BOX, Arrays.asList(p1, p2));
        operation.matches(q, Arrays.asList(testLat, testLon)) == isInBoundaryBox
        where:
        lat1      | lon1      | lat2      | lon2      | testLat   | testLon   | isInBoundaryBox
        48.229361 | 11.299782 | 48.175588 | 11.428185 | 48.188407 | 11.375656 | true
        48.229361 | 11.299782 | 48.175588 | 11.428185 | 48.172039 | 11.445866 | false
        51.629100 | -0.200500 | 51.469408 | 0.314484  | 51.534377 | 0.190887  | true
        51.629100 | -0.200500 | 51.469408 | 0.314484  | 51.527543 | 0.460052  | false
    }
}
