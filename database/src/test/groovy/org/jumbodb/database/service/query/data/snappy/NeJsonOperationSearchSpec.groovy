package org.jumbodb.database.service.query.data.snappy

import org.jumbodb.common.query.QueryClause
import org.jumbodb.common.query.QueryOperation
import spock.lang.Unroll

/**
 * @author Carsten Hufe
 */
class NeJsonOperationSearchSpec extends spock.lang.Specification {
    def operation = new NeJsonOperationSearch()

    @Unroll
    def "matches non equality #value ne #testValue == #isNotEquals"() {
        expect:
        def queryClause = new QueryClause(QueryOperation.EQ, value)
        operation.matches(queryClause, testValue) == isNotEquals
        where:
        value     | testValue   | isNotEquals
        "testStr" | "testStr"   | false
        "testStr" | "testStrNr" | true
        1         | 1           | false
        1         | 2           | true
    }
}
